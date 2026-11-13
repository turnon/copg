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
| `copg/consumer.go` | `RowConsumer` / `ColumnAware` output abstractions |
| `copg/jsonl.go` | `JSONLWriter`: one JSON object per row, columns as keys |
| `main.go` | CLI: parallel sharded export to raw COPY BINARY files |

## Library usage

Raw COPY BINARY passthrough (no parsing, lowest overhead):

```go
scanner := copg.NewScanner(copg.Config{
	ConnString:    dsn,
	Table:         "public.users",
	Workers:       8,
	PagesPerShard: 2048,
	WriterFactory: func(shardID string) (io.Writer, error) {
		return os.Create(shardFileName(shardID) + ".copy")
	},
})
err := scanner.Run(ctx)
```

Decoded rows into a custom consumer:

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

Consumers that need column names implement `copg.ColumnAware`; the scanner calls `Columns` once per shard before the first `Consume`. `JSONLWriter` does. The value mapping is documented on the type: timestamps become RFC 3339, `numeric` stays a JSON number (NaN/±Infinity become strings), `uuid` becomes a canonical string, `interval` becomes PostgreSQL's text form, arrays become JSON arrays, and `bytea` or types `pgtype` cannot decode become base64.

`WriterFactory` and `ConsumerFactory` receive a stable shard ID of the form `public.users@16384:[0,4096)`, which is deterministic for a given table content (it includes the table name, `relfilenode` and page range).

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
```

Flags: `-conn`, `-table`, `-format` (`raw` or `jsonl`), `-out`, `-workers`, `-pages-per-shard`. `-conn` defaults to `$COPG_CONN`.

`-format raw` writes `<shard>.copy` files containing the untouched `COPY BINARY` stream. `-format jsonl` writes `<shard>.jsonl` files with one JSON object per row and the table's column names as keys, decoded through `pgtype`.

```sh
$ head -1 export/shard_public_users_16384_0_4096_.jsonl
{"id":1,"email":"a@example.com","created_at":"2024-05-06T07:08:09.123Z","score":1.50}
```

## Notes and limits

- `ctid` range scans are optimized from PostgreSQL 14 onward; older versions may fall back to sequential scans and lose parallelism.
- Every shard needs its own connection (`SET TRANSACTION SNAPSHOT` is session-scoped); keep `Workers` below `max_connections`.
- `PagesPerShard` controls shard size; `Workers` only caps concurrency.
- Deleted/sparse pages produce empty shards, which is expected.
- Resume is not supported: an interrupted export is restarted from scratch. A PostgreSQL snapshot cannot outlive the session that exported it, so resuming across runs would stitch together shards from different MVCC states. Shards are deterministic and files are recreated, so a restart simply redoes the work.
