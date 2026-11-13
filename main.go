// Command copg exports a PostgreSQL table into snapshot-consistent shards as
// raw PostgreSQL COPY BINARY files.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/turnon/copg/lib"
)

const (
	formatRaw   = "raw"
	formatJSONL = "jsonl"
)

type options struct {
	conn          string
	table         string
	columns       columnList
	workers       int
	pagesPerShard int64
	outDir        string
	format        string
}

// columnList collects -column values, one expression per occurrence.
type columnList []string

func (c *columnList) String() string { return strings.Join(*c, ", ") }

func (c *columnList) Set(v string) error {
	expr := strings.TrimSpace(v)
	if expr == "" {
		return errors.New("empty column expression")
	}
	*c = append(*c, expr)
	return nil
}

func main() {
	var opts options

	fs := flag.NewFlagSet("copg", flag.ExitOnError)
	fs.StringVar(&opts.conn, "conn", "", "PostgreSQL connection string (default $COPG_CONN)")
	fs.StringVar(&opts.table, "table", "", "table to export, optionally schema-qualified (required)")
	fs.Var(&opts.columns, "column", "SQL expression to export, e.g. 'lower(email) AS email_lc'; repeat once per column (default: all columns)")
	fs.IntVar(&opts.workers, "workers", 4, "maximum number of parallel shards/connections")
	fs.Int64Var(&opts.pagesPerShard, "pages-per-shard", 1024, "target pages (8 KB each) per shard")
	fs.StringVar(&opts.outDir, "out", "copg-out", "output directory")
	fs.StringVar(&opts.format, "format", "raw", "output format: raw (COPY BINARY) or jsonl (one JSON object per row)")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: copg -conn <dsn> -table <table> [flags]\n\nFlags:\n")
		fs.PrintDefaults()
		fmt.Fprintf(fs.Output(), "\nExamples:\n")
		fmt.Fprintf(fs.Output(), "  copg -conn 'postgres://u:p@localhost/db?sslmode=disable' -table public.users -out ./export\n")
		fmt.Fprintf(fs.Output(), "  copg -conn 'postgres://u:p@localhost/db?sslmode=disable' -table public.users -format jsonl -out ./export\n")
		fmt.Fprintf(fs.Output(), "  copg -conn 'postgres://u:p@localhost/db?sslmode=disable' -table public.users -workers 8 -out ./export\n")
		fmt.Fprintf(fs.Output(), "  copg -conn 'postgres://u:p@localhost/db?sslmode=disable' -table public.users -format jsonl \\\n")
		fmt.Fprintf(fs.Output(), "       -column id -column 'lower(email) AS email_lc' -column \"to_char(created_at, 'YYYY-MM-DD') AS created_on\" \\\n")
		fmt.Fprintf(fs.Output(), "       -out ./export\n")
	}
	fs.Parse(os.Args[1:])

	// Resolved after parsing so -h does not print the DSN (and its password).
	if opts.conn == "" {
		opts.conn = os.Getenv("COPG_CONN")
	}
	if opts.conn == "" || opts.table == "" {
		fs.Usage()
		os.Exit(2)
	}
	switch opts.format {
	case formatRaw, formatJSONL:
	default:
		fmt.Fprintf(os.Stderr, "copg: unknown -format %q (want %q or %q)\n", opts.format, formatRaw, formatJSONL)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, opts, opts.columns); err != nil {
		fmt.Fprintln(os.Stderr, withPrefix(err))
		os.Exit(1)
	}
}

// withPrefix tags unexpected errors; lib errors already carry the prefix.
func withPrefix(err error) string {
	msg := err.Error()
	if strings.HasPrefix(msg, "copg:") {
		return msg
	}
	return "copg: " + msg
}

func run(ctx context.Context, opts options, columns []string) error {
	if err := os.MkdirAll(opts.outDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	cfg := lib.Config{
		ConnString:    opts.conn,
		Table:         opts.table,
		Columns:       columns,
		Workers:       opts.workers,
		PagesPerShard: opts.pagesPerShard,
	}

	switch opts.format {
	case formatRaw:
		cfg.WriterFactory = func(shardID string) (io.Writer, error) {
			path := filepath.Join(opts.outDir, shardFileName(shardID, ".copy"))
			return os.Create(path)
		}
	case formatJSONL:
		cfg.ConsumerFactory = func(shardID string) (lib.RowConsumer, error) {
			return lib.NewJSONLFile(filepath.Join(opts.outDir, shardFileName(shardID, ".jsonl")))
		}
	}

	fmt.Printf("exporting %s to %s as %s with up to %d workers (%d pages per shard)\n",
		opts.table, opts.outDir, opts.format, opts.workers, opts.pagesPerShard)
	if len(columns) > 0 {
		fmt.Printf("columns: %s\n", strings.Join(columns, ", "))
	}

	scanner := lib.NewScanner(cfg)
	if err := scanner.Run(ctx); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	reportOutput(opts.outDir)
	return nil
}

// reportOutput prints a short summary of the files written to dir.
func reportOutput(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Printf("export complete: %s\n", dir)
		return
	}
	var files int
	var bytes int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files++
		bytes += info.Size()
	}
	fmt.Printf("export complete: %d file(s), %s total in %s\n", files, humanBytes(bytes), dir)
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	d := float64(n)
	for _, suffix := range []string{"KiB", "MiB", "GiB", "TiB", "PiB"} {
		d /= unit
		if d < unit {
			return fmt.Sprintf("%.2f %s", d, suffix)
		}
	}
	return fmt.Sprintf("%.2f EiB", d/unit)
}

var unsafeFileChars = regexp.MustCompile(`[^A-Za-z0-9]+`)

// shardFileName turns a shard ID such as "public.users@16384:[0,4096)" into a
// safe file name.
func shardFileName(shardID, ext string) string {
	name := "shard_" + strings.Trim(unsafeFileChars.ReplaceAllString(shardID, "_"), "_")
	const maxBase = 100
	if len(name) > maxBase {
		name = name[:maxBase]
	}
	return name + ext
}
