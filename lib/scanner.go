package lib

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
)

// Config controls a snapshot-consistent, parallel export of one PostgreSQL
// table.
type Config struct {
	// ConnString is the PostgreSQL connection string used for the snapshot
	// transaction and for every worker connection.
	ConnString string

	// Table is the relation to export, optionally schema-qualified (for
	// example "public.users").
	Table string

	// Workers is the maximum number of concurrent shard exports, i.e. the
	// maximum number of PostgreSQL connections opened in parallel. Defaults to
	// 4. It does not limit the number of shards.
	Workers int

	// PagesPerShard is the target number of 8 KB pages covered by one shard;
	// it controls shard granularity. Larger values mean fewer, longer-running
	// shards. Defaults to 1024 (~8 MB per shard).
	PagesPerShard int64

	// ConsumerFactory creates a decoded row consumer per shard. When set,
	// rows are parsed from the COPY BINARY stream and handed to the consumer
	// one by one.
	ConsumerFactory func(shardID string) (RowConsumer, error)

	// WriterFactory creates a raw COPY BINARY sink per shard. The stream is
	// passed through untouched. Writers returned by the factory are closed by
	// the scanner if they implement io.Closer.
	WriterFactory func(shardID string) (io.Writer, error)

	// Writers are pre-created raw COPY BINARY sinks, indexed by planned shard
	// order. They are not closed by the scanner. Use WriterFactory when the
	// number of shards is not known in advance.
	Writers []io.Writer
}

// Shard describes one immutable ctid page range. Start is inclusive, End is
// exclusive.
type Shard struct {
	// ID is stable for a given table content: it contains the table name, the
	// relfilenode and the page range.
	ID string
	// Table is the exported relation.
	Table string
	// RelFilenode is the physical file node captured when planning the shard.
	RelFilenode uint32
	// Start and End are page numbers: [Start, End).
	Start int64
	End   int64
}

func (s Shard) String() string { return s.ID }

// Scanner orchestrates one complete parallel snapshot export.
type Scanner struct {
	cfg Config
}

// NewScanner returns a Scanner, applying defaults for Workers and
// PagesPerShard.
func NewScanner(cfg Config) *Scanner {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.PagesPerShard <= 0 {
		cfg.PagesPerShard = 1024
	}
	return &Scanner{cfg: cfg}
}

// Run executes the export:
//
//  1. On an admin connection, open a REPEATABLE READ, READ ONLY transaction and
//     export its snapshot with pg_export_snapshot().
//  2. Read column metadata and split the relation's page space into ctid
//     ranges.
//  3. For every shard, open a worker connection, import the
//     snapshot with SET TRANSACTION SNAPSHOT, and stream
//     COPY (SELECT ... WHERE ctid ...) TO STDOUT (FORMAT BINARY).
//
// The admin transaction stays open until all workers have finished, because the
// snapshot is released when it ends.
func (s *Scanner) Run(ctx context.Context) error {
	cfg := s.cfg
	if err := cfg.validate(); err != nil {
		return err
	}

	// ---------- Phase 1: export the snapshot ----------
	adminConn, err := pgx.Connect(ctx, cfg.ConnString)
	if err != nil {
		return fmt.Errorf("copg: admin connect: %w", err)
	}
	defer adminConn.Close(context.WithoutCancel(ctx))

	tx, err := adminConn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("copg: begin snapshot transaction: %w", err)
	}
	// Must run only after all workers are done; otherwise the snapshot ID is
	// invalidated.
	defer tx.Rollback(context.WithoutCancel(ctx))

	var snapshotID string
	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID); err != nil {
		return fmt.Errorf("copg: export snapshot: %w", err)
	}
	if !snapshotIDPattern.MatchString(snapshotID) {
		return fmt.Errorf("copg: unexpected snapshot id %q", snapshotID)
	}

	// ---------- Phase 2: metadata and ctid planning ----------
	cols, err := fetchColumnMeta(ctx, tx, cfg.Table)
	if err != nil {
		return err
	}

	rel, err := fetchRelInfo(ctx, tx, cfg.Table)
	if err != nil {
		return err
	}
	if rel.BlockSize <= 0 {
		return fmt.Errorf("copg: invalid block size %d", rel.BlockSize)
	}
	nblocks := rel.SizeBytes / rel.BlockSize
	if nblocks > math.MaxUint32 {
		return fmt.Errorf("copg: relation has %d pages, which exceeds ctid's 32-bit page space", nblocks)
	}

	shards := planShards(cfg.Table, rel.RelFilenode, nblocks, cfg.PagesPerShard)

	if cfg.ConsumerFactory == nil && cfg.WriterFactory == nil && len(cfg.Writers) < len(shards) {
		return fmt.Errorf("copg: %d raw writers provided but %d shards planned; provide WriterFactory or more Writers", len(cfg.Writers), len(shards))
	}

	// ---------- Phase 3: parallel COPY workers ----------
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Run every shard on at most cfg.Workers connections. Shards are
	// independent, so a bounded worker pool keeps parallelism and shard
	// granularity decoupled.
	errCh := make(chan error, len(shards)+1)
	workerCount := cfg.Workers
	if workerCount > len(shards) {
		workerCount = len(shards)
	}

	jobs := make(chan int)
	var wg sync.WaitGroup
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				shard := shards[idx]
				if err := s.runShard(runCtx, snapshotID, shard, cols, idx); err != nil {
					errCh <- fmt.Errorf("copg: shard %s: %w", shard.ID, err)
					cancel() // stop sibling workers early
					return
				}
			}
		}()
	}

schedule:
	for idx := range shards {
		select {
		case jobs <- idx:
		case <-runCtx.Done():
			break schedule
		}
	}
	close(jobs)
	wg.Wait()
	close(errCh)

	if err := firstError(errCh); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Guard: VACUUM FULL, CLUSTER and TRUNCATE rewrite the relation and change
	// the meaning of ctid. Detect it on a fresh connection (a new snapshot, so
	// the check is not hidden by the export transaction's MVCC view).
	after, err := fetchRelInfoDetached(ctx, cfg.ConnString, cfg.Table)
	if err != nil {
		return err
	}
	if after.RelFilenode != rel.RelFilenode {
		return fmt.Errorf("copg: relation %s was rewritten during export (relfilenode %d -> %d); output may be incomplete or inconsistent",
			cfg.Table, rel.RelFilenode, after.RelFilenode)
	}

	return nil
}

func (c Config) validate() error {
	if c.ConnString == "" {
		return errors.New("copg: ConnString is required")
	}
	if c.Table == "" {
		return errors.New("copg: Table is required")
	}
	if c.ConsumerFactory == nil && c.WriterFactory == nil && len(c.Writers) == 0 {
		return errors.New("copg: no output configured (set ConsumerFactory, WriterFactory or Writers)")
	}
	return nil
}

// runShard creates the configured output and exports one shard into it.
func (s *Scanner) runShard(ctx context.Context, snapshotID string, shard Shard, cols []ColumnMeta, idx int) error {
	cfg := s.cfg

	var consumer RowConsumer
	var raw io.Writer
	closeRaw := false

	switch {
	case cfg.ConsumerFactory != nil:
		c, err := cfg.ConsumerFactory(shard.ID)
		if err != nil {
			return fmt.Errorf("create consumer: %w", err)
		}
		if c == nil {
			return errors.New("consumer factory returned nil")
		}
		consumer = c

	case cfg.WriterFactory != nil:
		w, err := cfg.WriterFactory(shard.ID)
		if err != nil {
			return fmt.Errorf("create writer: %w", err)
		}
		if w == nil {
			return errors.New("writer factory returned nil")
		}
		raw = w
		closeRaw = true

	default:
		raw = cfg.Writers[idx]
	}

	err := s.copyWorker(ctx, snapshotID, shard, cols, consumer, raw)

	// Always release resources, whether the copy succeeded or not.
	if consumer != nil {
		if cerr := consumer.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("close consumer: %w", cerr)
		}
	}
	if closeRaw {
		if c, ok := raw.(io.Closer); ok {
			if cerr := c.Close(); cerr != nil && err == nil {
				err = fmt.Errorf("close writer: %w", cerr)
			}
		}
	}
	return err
}

// copyWorker imports the exported snapshot on its own connection and streams
// the assigned ctid range, either verbatim (raw != nil) or row by row into the
// consumer.
func (s *Scanner) copyWorker(ctx context.Context, snapshotID string, shard Shard, cols []ColumnMeta, consumer RowConsumer, raw io.Writer) error {
	conn, err := pgx.Connect(ctx, s.cfg.ConnString)
	if err != nil {
		return fmt.Errorf("worker connect: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("begin worker transaction: %w", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx))

	// SET TRANSACTION SNAPSHOT does not accept bind parameters; snapshotID is
	// validated against snapshotIDPattern above, and is quoted here anyway.
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+quoteLiteral(snapshotID)); err != nil {
		return fmt.Errorf("import snapshot: %w", err)
	}

	sql := buildCopySQL(s.cfg.Table, cols, shard)

	if consumer == nil {
		if raw == nil {
			return errors.New("no output target for shard")
		}
		if _, err := conn.PgConn().CopyTo(ctx, raw, sql); err != nil {
			return fmt.Errorf("copy to stdout: %w", err)
		}
		return nil
	}
	return copyIntoConsumer(ctx, conn, sql, cols, consumer)
}

// copyIntoConsumer pipes the server's COPY BINARY stream into a BinaryReader
// and decodes each row before handing it to consumer. io.Pipe provides
// backpressure, keeping memory usage bounded.
func copyIntoConsumer(ctx context.Context, conn *pgx.Conn, sql string, cols []ColumnMeta, consumer RowConsumer) error {
	pr, pw := io.Pipe()
	copyErrCh := make(chan error, 1)

	go func() {
		_, err := conn.PgConn().CopyTo(ctx, pw, sql)
		pw.CloseWithError(err)
		copyErrCh <- err
	}()

	decoder := NewDecoder()
	// Unknown/custom types are kept as raw bytes rather than failing the
	// whole export; the output layer decides how to represent them.
	decoder.FallbackRaw = true
	reader := NewBinaryReader(pr)

	for {
		row, err := ReadDecodedRow(reader, decoder, cols)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			pr.CloseWithError(err)
			<-copyErrCh
			return fmt.Errorf("read row: %w", err)
		}
		if err := consumer.Consume(ctx, row); err != nil {
			pr.CloseWithError(err)
			<-copyErrCh
			return fmt.Errorf("consume row: %w", err)
		}
	}

	if err := <-copyErrCh; err != nil {
		pr.Close()
		return fmt.Errorf("copy to stdout: %w", err)
	}
	pr.Close()
	// The consumer is closed by the caller once all rows have been consumed.
	return nil
}

// buildCopySQL wraps the ctid-filtered SELECT in COPY ... TO STDOUT (FORMAT
// BINARY). An empty shard uses WHERE false so it returns a valid, empty stream.
func buildCopySQL(table string, cols []ColumnMeta, shard Shard) string {
	columnList := "*"
	if len(cols) > 0 {
		names := make([]string, len(cols))
		for i, c := range cols {
			names[i] = quoteIdentifier(c.Name)
		}
		columnList = strings.Join(names, ", ")
	}

	where := "false"
	if shard.Start < shard.End {
		where = fmt.Sprintf("ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid", shard.Start, shard.End)
	}

	return fmt.Sprintf("COPY (SELECT %s FROM %s WHERE %s) TO STDOUT (FORMAT BINARY)",
		columnList, quoteIdentifier(table), where)
}

// maxShards bounds the number of planned shards so a very large table with a
// tiny PagesPerShard cannot produce an unbounded amount of state.
const maxShards = 1 << 14 // 16384

// planShards splits [0, nblocks) into ranges of roughly pagesPerShard pages.
// The result is deterministic for a given relfilenode and page count, which is
// what makes shard IDs stable across runs.
func planShards(table string, relFilenode uint32, nblocks int64, pagesPerShard int64) []Shard {
	if nblocks < 0 {
		nblocks = 0
	}
	if pagesPerShard < 1 {
		pagesPerShard = 1
	}

	shardCount := 1
	if nblocks > 0 {
		shardCount = int((nblocks + pagesPerShard - 1) / pagesPerShard)
	}
	if shardCount > maxShards {
		shardCount = maxShards
	}
	if shardCount < 1 {
		shardCount = 1
	}

	if nblocks == 0 {
		// Empty relation: emit a single empty shard so the COPY still returns a
		// well-formed (empty) binary stream.
		return []Shard{newShard(table, relFilenode, 0, 0)}
	}

	step := (nblocks + int64(shardCount) - 1) / int64(shardCount)
	shards := make([]Shard, 0, shardCount)
	for i := 0; i < shardCount; i++ {
		start := int64(i) * step
		if start >= nblocks {
			break
		}
		end := min(start+step, nblocks)
		shards = append(shards, newShard(table, relFilenode, start, end))
	}
	return shards
}

func newShard(table string, relFilenode uint32, start, end int64) Shard {
	return Shard{
		ID:          shardID(table, relFilenode, start, end),
		Table:       table,
		RelFilenode: relFilenode,
		Start:       start,
		End:         end,
	}
}

func shardID(table string, relFilenode uint32, start, end int64) string {
	return fmt.Sprintf("%s@%d:[%d,%d)", table, relFilenode, start, end)
}

type relInfo struct {
	RelKind     string
	RelFilenode uint32
	SizeBytes   int64
	BlockSize   int64
}

func fetchRelInfo(ctx context.Context, q rowQuerier, table string) (relInfo, error) {
	const query = `
		SELECT c.relkind::text,
		       c.relfilenode,
		       pg_relation_size(c.oid),
		       current_setting('block_size')::bigint
		FROM pg_class c
		WHERE c.oid = $1::regclass`

	var ri relInfo
	err := q.QueryRow(ctx, query, quoteIdentifier(table)).Scan(
		&ri.RelKind, &ri.RelFilenode, &ri.SizeBytes, &ri.BlockSize,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ri, fmt.Errorf("copg: relation %s does not exist", table)
		}
		return ri, fmt.Errorf("copg: inspect relation %s: %w", table, err)
	}

	switch ri.RelKind {
	case "r", "m": // ordinary table or materialized view
	default:
		return ri, fmt.Errorf("copg: relation %s has relkind %q; only tables and materialized views can be exported by ctid", table, ri.RelKind)
	}
	return ri, nil
}

func fetchRelInfoDetached(ctx context.Context, connString, table string) (relInfo, error) {
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return relInfo{}, fmt.Errorf("copg: verification connect: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))
	return fetchRelInfo(ctx, conn, table)
}

func firstError(errs <-chan error) error {
	var first error
	for err := range errs {
		switch {
		case first == nil:
			first = err
		case errors.Is(first, context.Canceled) && !errors.Is(err, context.Canceled):
			// Prefer the root cause over a sibling's cancellation error.
			first = err
		}
	}
	return first
}

var snapshotIDPattern = regexp.MustCompile(`^[0-9A-Za-z-]+$`)

// quoteIdentifier safely quotes a possibly schema-qualified identifier.
func quoteIdentifier(name string) string {
	parts := strings.Split(name, ".")
	quoted := make([]string, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if len(part) >= 2 && part[0] == '"' && part[len(part)-1] == '"' {
			part = strings.ReplaceAll(part[1:len(part)-1], `""`, `"`)
		}
		quoted[i] = `"` + strings.ReplaceAll(part, `"`, `""`) + `"`
	}
	return strings.Join(quoted, ".")
}

// quoteLiteral returns a SQL string literal.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
