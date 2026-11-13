package lib

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
)

// RowConsumer is the single per-shard output abstraction of a Scanner. One
// instance is created per shard by Config.ConsumerFactory, asked to export
// exactly one shard through Consume, and closed afterwards.
type RowConsumer interface {
	// Consume exports one shard. conn is a worker connection with the exported
	// snapshot imported and its transaction open, sql is the shard's
	// COPY (SELECT ...) TO STDOUT (FORMAT BINARY) statement, and cols describes
	// the SELECT list.
	//
	// Implementations either pass the COPY BINARY bytes through untouched
	// (NewRaw) or decode rows themselves, like JSONL does (see copyRows).
	Consume(ctx context.Context, conn *pgx.Conn, sql string, cols []ColumnMeta) error

	// Close flushes pending data and releases resources.
	Close() error
}

// copyRows runs a shard's COPY statement on conn and calls fn once per decoded
// row. io.Pipe provides backpressure, so memory stays bounded no matter how
// large the rows are. The first error from either side aborts the copy.
//
// It is the decode loop behind JSONL and stays unexported so the public
// surface remains one interface (RowConsumer) and two constructors (NewRaw,
// NewJSONL).
func copyRows(ctx context.Context, conn *pgx.Conn, sql string, cols []ColumnMeta, fn func(row *DecodedRow) error) error {
	pr, pw := io.Pipe()
	copyErrCh := make(chan error, 1)

	go func() {
		_, err := conn.PgConn().CopyTo(ctx, pw, sql)
		pw.CloseWithError(err)
		copyErrCh <- err
	}()

	decoder := NewDecoder()
	// Unknown/custom types are kept as raw bytes rather than failing the whole
	// export; the output layer decides how to represent them.
	decoder.FallbackRaw = true
	reader := newBinaryReader(pr)

	for {
		row, err := readDecodedRow(reader, decoder, cols)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			pr.CloseWithError(err)
			<-copyErrCh
			return fmt.Errorf("read row: %w", err)
		}
		if err := fn(row); err != nil {
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
	return nil
}
