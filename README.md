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
| `copg/progress.go` | Resume support (`ProgressStore`, file/memory implementations) |
| `copg/consumer.go` | `RowConsumer` output abstraction |
| `copg/parquet.go` | Parquet consumer built on `parquet-go`, plus schema derivation from PostgreSQL types |
| `main.go` | CLI: `-format parquet` (decoded rows) or `-format binary` (raw COPY BINARY passthrough) |

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

Decoded rows into Parquet (worker-shared schema, resumable):

```go
cols, err := copg.FetchColumnMeta(ctx, dsn, "public.users")
schema := copg.BuildParquetSchema("public.users", cols)

store, err := copg.NewFileProgressStore("users.progress")

scanner := copg.NewScanner(copg.Config{
	ConnString:    dsn,
	Table:         "public.users",
	Workers:       4,
	ProgressStore: store,
	ConsumerFactory: func(shardID string) (copg.RowConsumer, error) {
		return copg.NewParquetConsumerWithSchema(shardID+".parquet", cols, schema, 4096)
	},
})
err = scanner.Run(ctx)
```

`WriterFactory` and `ConsumerFactory` receive a stable shard ID of the form `public.users@16384:[0,4096)`, which doubles as the resume key (it includes the table name, `relfilenode` and page range).

## CLI

```sh
# Parquet output
go run . \
  -conn 'postgres://user:pass@localhost:5432/mydb?sslmode=disable' \
  -table public.users -format parquet -out ./export -workers 8

# Raw COPY BINARY shards, resumable
go run . \
  -conn 'postgres://user:pass@localhost:5432/mydb?sslmode=disable' \
  -table public.users -format binary -out ./export -progress ./export.progress
```

Flags: `-conn`, `-table`, `-out`, `-format`, `-workers`, `-pages-per-shard`, `-progress`, `-batch`. `-conn` defaults to `$COPG_CONN`.

## Type mapping (Parquet)

| PostgreSQL | Parquet |
| --- | --- |
| `bool` | BOOLEAN |
| `int2`, `int4`, `oid` | INT(32) |
| `int8` | INT(64) |
| `float4` | FLOAT |
| `float8` | DOUBLE |
| `date` | DATE |
| `timestamp`, `timestamptz` | TIMESTAMP(MICROS) |
| `uuid` | UUID |
| `json`, `jsonb` | JSON |
| `text`, `varchar`, `bpchar`, `char`, `name`, `numeric`, `time`, `timetz`, `interval`, arrays | STRING (text form; `numeric` keeps full precision as text) |
| everything else | BYTE_ARRAY (raw COPY BINARY bytes) |

All columns are `OPTIONAL` because PostgreSQL columns are nullable. Unknown or custom types are decoded with `Decoder.FallbackRaw`, keeping raw bytes instead of failing the export.

## Notes and limits

- `ctid` range scans are optimized from PostgreSQL 14 onward; older versions may fall back to sequential scans and lose parallelism.
- Every shard needs its own connection (`SET TRANSACTION SNAPSHOT` is session-scoped); keep `Workers` below `max_connections`.
- `PagesPerShard` controls shard size, i.e. resume granularity; `Workers` only caps concurrency.
- Deleted/sparse pages produce empty shards, which is expected.
- `FileProgressStore` appends one completed shard ID per line; a crash may leave a partial line, which is simply ignored on the next run.
