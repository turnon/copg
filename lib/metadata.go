package lib

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// ColumnMeta describes one column of the exported relation. The OID is the
// PostgreSQL type OID stored in pg_attribute.atttypid; COPY BINARY itself does
// not carry type information, so it has to be fetched separately.
type ColumnMeta struct {
	Name     string // column name
	OID      uint32 // pg_attribute.atttypid
	TypeName string // pg_type.typname, e.g. "int4"
	NotNull  bool   // pg_attribute.attnotnull
}

// rowsQuerier is implemented by *pgx.Conn, pgx.Tx and pgxpool.Pool.
type rowsQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// rowQuerier is implemented by *pgx.Conn, pgx.Tx and pgxpool.Pool.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// fetchColumnMeta queries pg_attribute/pg_type for the relation identified by
// table (which may be schema-qualified).
func fetchColumnMeta(ctx context.Context, q rowsQuerier, table string) ([]ColumnMeta, error) {
	const query = `
		SELECT a.attname,
		       a.atttypid::oid,
		       t.typname,
		       a.attnotnull
		FROM pg_attribute a
		JOIN pg_type t ON t.oid = a.atttypid
		WHERE a.attrelid = $1::regclass
		  AND a.attnum > 0
		  AND NOT a.attisdropped
		ORDER BY a.attnum`

	rows, err := q.Query(ctx, query, quoteIdentifier(table))
	if err != nil {
		return nil, fmt.Errorf("copg: query pg_attribute for %s: %w", table, err)
	}
	defer rows.Close()

	var metas []ColumnMeta
	for rows.Next() {
		var m ColumnMeta
		var oid uint32
		if err := rows.Scan(&m.Name, &oid, &m.TypeName, &m.NotNull); err != nil {
			return nil, fmt.Errorf("copg: scan pg_attribute: %w", err)
		}
		m.OID = oid
		metas = append(metas, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("copg: read pg_attribute: %w", err)
	}
	if len(metas) == 0 {
		return nil, fmt.Errorf("copg: relation %s has no columns (does it exist?)", table)
	}
	return metas, nil
}

// Decoder decodes raw COPY BINARY field bytes into Go values using pgtype's
// OID-based codec registry.
//
// A Decoder is not safe for concurrent use; create one per worker.
type Decoder struct {
	typeMap *pgtype.Map

	// FallbackRaw, when true, makes DecodeField return the raw bytes (instead
	// of an error) for types that pgtype cannot decode, e.g. custom composite
	// types. The caller can then decide how to interpret them.
	FallbackRaw bool
}

// NewDecoder returns a Decoder backed by a fresh pgtype.Map, which already
// knows all built-in PostgreSQL types.
func NewDecoder() *Decoder {
	return &Decoder{typeMap: pgtype.NewMap()}
}

// DecodeField decodes one field. raw == nil means SQL NULL. oid is the column
// type OID from ColumnMeta.
func (d *Decoder) DecodeField(oid uint32, raw []byte) (any, error) {
	if raw == nil {
		return nil, nil
	}
	var dst any
	if err := d.typeMap.Scan(oid, pgtype.BinaryFormatCode, raw, &dst); err != nil {
		if d.FallbackRaw {
			return append([]byte(nil), raw...), nil
		}
		return nil, fmt.Errorf("copg: decode oid %d: %w", oid, err)
	}
	return dst, nil
}

// DecodedRow is a row whose fields have been decoded to Go values. A nil entry
// represents SQL NULL.
type DecodedRow struct {
	Values []any
}

// Map returns the row as a map keyed by column name.
func (r *DecodedRow) Map(cols []ColumnMeta) map[string]any {
	m := make(map[string]any, len(r.Values))
	for i, v := range r.Values {
		if i < len(cols) {
			m[cols[i].Name] = v
		}
	}
	return m
}

// ReadDecodedRow reads the next row from br and decodes each field according to
// the column metadata. It returns io.EOF at the end of the stream.
func ReadDecodedRow(br *BinaryReader, decoder *Decoder, cols []ColumnMeta) (*DecodedRow, error) {
	raw, err := br.ReadRow()
	if err != nil {
		return nil, err
	}
	if len(raw) != len(cols) {
		return nil, fmt.Errorf("copg: column count mismatch: stream has %d, metadata has %d", len(raw), len(cols))
	}

	decoded := &DecodedRow{Values: make([]any, len(raw))}
	for i, field := range raw {
		v, err := decoder.DecodeField(cols[i].OID, field)
		if err != nil {
			return nil, fmt.Errorf("copg: column %s: %w", cols[i].Name, err)
		}
		decoded.Values[i] = v
	}
	return decoded, nil
}
