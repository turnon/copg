package lib

import "context"

// RowConsumer consumes decoded rows produced by a Scanner worker. Each shard
// gets its own consumer instance, and Close is called exactly once when the
// shard has been fully written (or when writing failed).
type RowConsumer interface {
	// Consume writes one decoded row.
	Consume(ctx context.Context, row *DecodedRow) error
	// Close flushes pending data and releases resources.
	Close() error
}
