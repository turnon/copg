package lib

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
)

// NewRaw returns a RowConsumer that streams a shard's COPY BINARY bytes
// into w untouched. This is the lowest-overhead mode: no parsing, no decoding.
//
// w is closed by the Scanner when the shard finishes if it implements
// io.Closer, so *os.File and friends need no extra bookkeeping.
func NewRaw(w io.Writer) RowConsumer {
	return consumer{w: w}
}

// consumer is a RowConsumer that wants the raw stream.
type consumer struct {
	w io.Writer
}

// Consume implements RowConsumer by copying the stream verbatim.
func (c consumer) Consume(ctx context.Context, conn *pgx.Conn, sql string, _ []ColumnMeta) error {
	if _, err := conn.PgConn().CopyTo(ctx, c.w, sql); err != nil {
		return fmt.Errorf("copg: raw: copy to stdout: %w", err)
	}
	return nil
}

// Close implements RowConsumer by closing the wrapped writer when it can.
func (c consumer) Close() error {
	if closer, ok := c.w.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
