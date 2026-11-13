# copg

Snapshot-consistent, parallel PostgreSQL table export, in the spirit of DuckDB's `postgres_scanner` / Filament. Written against `jackc/pgx/v5`.

The export follows the same protocol orchestration as the reference docs:

1. An admin connection opens a `REPEATABLE READ`, `READ ONLY` transaction and calls `pg_export_snapshot()`.
2. Column metadata is read from `pg_attribute`/`pg_type`, and the relation's page space is split into `ctid` ranges (`[start, end)` pages).
3. Every shard gets its own connection, imports the snapshot with `SET TRANSACTION SNAPSHOT`, and streams `COPY (SELECT ... WHERE ctid >= '(start,0)' AND ctid < '(end,0)') TO STDOUT (FORMAT BINARY)`.
4. The admin transaction stays open until every worker has finished, otherwise the snapshot would be released. Afterwards the relation's `relfilenode` is re-checked so a concurrent `VACUUM FULL` / `CLUSTER` / `TRUNCATE` is reported instead of silently producing inconsistent output.

## Contents

| File | Purpose |
| --- | --- |
| `copg/scanner.go` | Snapshot export orchestration, ctid shard planning, worker pool |
| `copg/binary.go` | Minimal PostgreSQL `COPY BINARY` stream parser |
| `copg/metadata.go` | Column metadata (`pg_attribute` + OIDs) and OID-based value decoding via `pgtype.Map` |
| `copg/select.go` | SELECT list resolution: explicit expressions, result descriptors, aggregate/window guard |
| `copg/consumer.go` | `RowConsumer` output interface and the shared decode loop |
| `copg/consumer_raw.go` | `NewRaw`: untouched COPY BINARY passthrough consumer |
| `copg/consumer_jsonl.go` | `JSONL`: one JSON object per row, columns as keys |
| `main.go` | CLI: parallel sharded export to raw COPY BINARY files |

## Library usage

There is one output abstraction: `copg.RowConsumer`, created per shard by
`Config.ConsumerFactory`. The consumer receives the shard and its worker
connection, so it decides for itself how to write the data — no mode switch, no
capability checks, and no special cases in the scanner.

Raw COPY BINARY passthrough (no parsing, lowest overhead) wraps an `io.Writer`
with `copg.NewRaw`, which returns a `RowConsumer` whose shard bytes are the
untouched `COPY BINARY` stream:

```go
scanner := copg.NewScanner(copg.Config{
	ConnString:    dsn,
	Table:         "public.users",
	Workers:       8,
	PagesPerShard: 2048,
	ConsumerFactory: func(shardID string) (copg.RowConsumer, error) {
		f, err := os.Create(shardFileName(shardID) + ".copy")
		if err != nil {
			return nil, err
		}
		return copg.NewRaw(f), nil
	},
})
err := scanner.Run(ctx)
```

Decoded rows into a custom consumer (you decode the stream in `Consume`, as `JSONL` does):

```go
scanner := copg.NewScanner(copg.Config{
	ConnString:    dsn,
	Table:         "public.users",
	Workers:       4,
	ConsumerFactory: func(shardID string) (copg.RowConsumer, error) {
		return newMyConsumer(shardID)
	},
})
err = scanner.Run(ctx)
```

Each shard as newline-delimited JSON (one object per row, column names as keys):

```go
scanner := copg.NewScanner(copg.Config{
	ConnString:    dsn,
	Table:         "public.users",
	ConsumerFactory: func(shardID string) (copg.RowConsumer, error) {
		return copg.NewJSONLFile(shardFileName(shardID) + ".jsonl")
	},
})
err := scanner.Run(ctx)
```

`RowConsumer` is a single method pair:

```go
type RowConsumer interface {
	Consume(ctx context.Context, conn *pgx.Conn, sql string, cols []copg.ColumnMeta) error
	Close() error
}
```

- `copg.NewRaw(w)` implements it by copying `sql`'s `COPY BINARY` stream into
  `w` untouched.
- `copg.JSONL` implements it by decoding the stream itself (the shared
  `copyRows` loop in `consumer.go`) and encoding each row as JSON.

`conn` is a worker connection inside the exported snapshot, so the consumer
never deals with transactions, snapshots or shard boundaries.

Both implementations live in the core package:

| Constructor | Output |
| --- | --- |
| `copg.NewRaw(w)` | untouched `COPY BINARY` stream |
| `copg.NewJSONL(w)` | one JSON object per row, buffered |
| `copg.NewJSONLFile(path)` | same, into a file that `Close` closes |

The scanner closes every consumer once its shard is finished. A raw consumer's
`Close` closes the wrapped writer if it implements `io.Closer`; `copg.JSONL`
flushes and closes its file.

`Consume` receives the exported columns, so consumers can use names instead of positional indexes; `copg.JSONL` does. The value mapping is documented on the type: timestamps become RFC 3339, `numeric` stays a JSON number (NaN/±Infinity become strings), `uuid` becomes a canonical string, `interval` becomes PostgreSQL's text form, arrays become JSON arrays, and `bytea` or types `pgtype` cannot decode become base64.

`ConsumerFactory` receives a stable shard ID of the form `public.users@16384:[0,4096)`, which is deterministic for a given table content (it includes the table name, `relfilenode` and page range).

## Column selection and expressions

By default every live column is exported, as reported by `pg_attribute`. `Columns` replaces that SELECT list with SQL expressions evaluated per row:

```go
scanner := copg.NewScanner(copg.Config{
	ConnString: dsn,
	Table:      "public.users",
	Columns: []string{
		`id`,
		`lower(email) AS email_lc`,
		`created_at::date AS created_on`,
		`payload->>'plan' AS plan`,
	},
	ConsumerFactory: func(shardID string) (copg.RowConsumer, error) {
		return copg.NewJSONLFile(shardFileName(shardID) + ".jsonl")
	},
})
```

Entries are trusted SQL, not identifiers, so function calls, casts and operators all work. Column metadata is then read from the query's own result descriptor (`Prepare` + `FieldDescription.DataTypeOID`) instead of `pg_attribute`, which is what keeps `COPY BINARY` decoding — and therefore `jsonl` output — working for computed columns.

Two constraints follow from sharding by `ctid`:

- **Aggregates and window functions are rejected.** Each shard sees only its own page range, so `count(*)` or `row_number()` would be evaluated once per shard. The scanner explains the probe query (`EXPLAIN (VERBOSE, FORMAT JSON)`) and fails with the offending expression instead of writing wrong data. Export the rows and aggregate afterwards.
- **Alias distinct expressions.** Output names come from PostgreSQL, so `lower(a), lower(b)` both become `lower` and are rejected; add `AS`. A computed column without an alias is named `?column?`.

Volatile functions (`random()`, `nextval()`, `clock_timestamp()`) are evaluated once per row and are not reproducible across runs, and `nextval()` consumes sequence values. Generated (`GENERATED ALWAYS AS ... STORED`) columns are ordinary relation columns and need no special treatment.

## CLI

```sh
# Raw COPY BINARY shards (default)
go run . \
  -conn 'postgres://user:pass@localhost:5432/mydb?sslmode=disable' \
  -table public.users -out ./export -workers 8

# One JSONL file per shard
go run . \
  -conn 'postgres://user:pass@localhost:5432/mydb?sslmode=disable' \
  -table public.users -format jsonl -out ./export

# Only some columns, including computed ones
go run . \
  -conn 'postgres://user:pass@localhost:5432/mydb?sslmode=disable' \
  -table public.users -format jsonl -out ./export \
  -column id \
  -column 'lower(email) AS email_lc' \
  -column "to_char(created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at"
```

Flags: `-conn`, `-table`, `-column`, `-format` (`raw` or `jsonl`), `-out`, `-workers`, `-pages-per-shard`. `-conn` defaults to `$COPG_CONN`.

`-format raw` writes `<shard>.copy` files containing the untouched `COPY BINARY` stream. `-format jsonl` writes `<shard>.jsonl` files with one JSON object per row and the table's column names as keys, decoded through `pgtype` (`copg.NewJSONLFile`).

```sh
$ head -1 export/shard_public_users_16384_0_4096_.jsonl
{"id":1,"email":"a@example.com","created_at":"2024-05-06T07:08:09.123Z","score":1.50}
```

`-column` takes one SQL expression per occurrence and is repeated once per exported column, in order. Values are never split, so commas, parentheses and quotes inside an expression are preserved verbatim:

```sh
-column id -column "concat(a, b) AS ab" -column "payload->>'x,y' AS v"
```

Omitting the flag exports every column.

## Notes and limits

- `ctid` range scans are optimized from PostgreSQL 14 onward; older versions may fall back to sequential scans and lose parallelism.
- Every shard needs its own connection (`SET TRANSACTION SNAPSHOT` is session-scoped); keep `Workers` below `max_connections`.
- `PagesPerShard` controls shard size; `Workers` only caps concurrency.
- Deleted/sparse pages produce empty shards, which is expected.
- Resume is not supported: an interrupted export is restarted from scratch. A PostgreSQL snapshot cannot outlive the session that exported it, so resuming across runs would stitch together shards from different MVCC states. Shards are deterministic and files are recreated, so a restart simply redoes the work.
