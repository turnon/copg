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

// ColumnAware is an optional interface a RowConsumer may implement to learn the
// exported relation's column layout. The Scanner calls Columns once, right
// after the consumer is created and before the first Consume, so consumers can
// use column names instead of positional indexes.
type ColumnAware interface {
	Columns(cols []ColumnMeta)
}
