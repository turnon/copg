// Command copg exports a PostgreSQL table into snapshot-consistent shards as
// raw PostgreSQL COPY BINARY files.
package main

import (
	"context"
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

type options struct {
	conn          string
	table         string
	workers       int
	pagesPerShard int64
	outDir        string
}

func main() {
	var opts options

	fs := flag.NewFlagSet("copg", flag.ExitOnError)
	fs.StringVar(&opts.conn, "conn", os.Getenv("COPG_CONN"), "PostgreSQL connection string (default $COPG_CONN)")
	fs.StringVar(&opts.table, "table", "", "table to export, optionally schema-qualified (required)")
	fs.IntVar(&opts.workers, "workers", 4, "maximum number of parallel shards/connections")
	fs.Int64Var(&opts.pagesPerShard, "pages-per-shard", 1024, "target pages (8 KB each) per shard")
	fs.StringVar(&opts.outDir, "out", "copg-out", "output directory")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: copg -conn <dsn> -table <table> [flags]\n\nFlags:\n")
		fs.PrintDefaults()
		fmt.Fprintf(fs.Output(), "\nExamples:\n")
		fmt.Fprintf(fs.Output(), "  copg -conn 'postgres://u:p@localhost/db?sslmode=disable' -table public.users -out ./export\n")
		fmt.Fprintf(fs.Output(), "  copg -conn 'postgres://u:p@localhost/db?sslmode=disable' -table public.users -workers 8 -out ./export\n")
	}
	fs.Parse(os.Args[1:])

	if opts.conn == "" || opts.table == "" {
		fs.Usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, opts); err != nil {
		fmt.Fprintln(os.Stderr, "copg:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, opts options) error {
	if err := os.MkdirAll(opts.outDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	cfg := lib.Config{
		ConnString:    opts.conn,
		Table:         opts.table,
		Workers:       opts.workers,
		PagesPerShard: opts.pagesPerShard,
	}

	cfg.WriterFactory = func(shardID string) (io.Writer, error) {
		path := filepath.Join(opts.outDir, shardFileName(shardID, ".copy"))
		return os.Create(path)
	}

	fmt.Printf("exporting %s to %s with up to %d workers (%d pages per shard)\n",
		opts.table, opts.outDir, opts.workers, opts.pagesPerShard)

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
