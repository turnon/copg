package lib

import (
	"bufio"
	"context"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// jsonlBufferSize is the per-consumer write buffer. Rows are written in chunks
// so a shard does not turn into one syscall per row.
const jsonlBufferSize = 64 << 10

// JSONLWriter is a RowConsumer that writes one JSON object per row, using the
// exported table's column names as keys and preserving column order.
//
// It implements ColumnAware: the Scanner hands it the column metadata once,
// before the first Consume call. Without that metadata it cannot name fields
// and Consume fails.
//
// Type mapping (decoded values come from pgtype, see Decoder):
//
//	NULL                               -> null
//	bool, int*, float*                 -> JSON scalar (NaN/±Infinity become strings)
//	text, varchar, char, json, enum    -> JSON string (json/jsonb as embedded text)
//	timestamp, timestamptz, date, time  -> RFC 3339 string
//	numeric                            -> bare JSON number (or "NaN"/"Infinity"/"-Infinity")
//	interval                           -> PostgreSQL text form, e.g. "1 day 02:03:04"
//	uuid                               -> canonical UUID string
//	arrays                             -> JSON arrays, element-wise
//	bytea and types pgtype cannot decode -> base64 string
//
// JSONLWriter is not safe for concurrent use; create one per shard.
type JSONLWriter struct {
	w      *bufio.Writer
	closer io.Closer
	cols   []ColumnMeta
	line   []byte
	closed bool
}

// NewJSONLWriter returns a JSONLWriter that writes to w. Close flushes w but
// does not close it, so callers can keep ownership of stdout and friends.
func NewJSONLWriter(w io.Writer) *JSONLWriter {
	return &JSONLWriter{w: bufio.NewWriterSize(w, jsonlBufferSize)}
}

// NewJSONLFile creates (or truncates) path and returns a JSONLWriter that
// writes to it. Close flushes and closes the file.
func NewJSONLFile(path string) (*JSONLWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	jw := NewJSONLWriter(f)
	jw.closer = f
	return jw, nil
}

// Columns implements ColumnAware.
func (j *JSONLWriter) Columns(cols []ColumnMeta) {
	j.cols = cols
}

// Consume implements RowConsumer.
func (j *JSONLWriter) Consume(_ context.Context, row *DecodedRow) error {
	if len(j.cols) == 0 {
		return fmt.Errorf("copg: jsonl: no column metadata; Columns was not called")
	}
	if len(row.Values) != len(j.cols) {
		return fmt.Errorf("copg: jsonl: row has %d values, metadata has %d columns", len(row.Values), len(j.cols))
	}

	line := j.line[:0]
	line = append(line, '{')
	for i, v := range row.Values {
		if i > 0 {
			line = append(line, ',')
		}
		line = appendJSONString(line, j.cols[i].Name)
		line = append(line, ':')
		var err error
		line, err = appendJSONValue(line, v)
		if err != nil {
			j.line = line
			return fmt.Errorf("copg: jsonl: column %s: %w", j.cols[i].Name, err)
		}
	}
	line = append(line, '}', '\n')
	j.line = line

	if _, err := j.w.Write(line); err != nil {
		return fmt.Errorf("copg: jsonl: write: %w", err)
	}
	return nil
}

// Close implements RowConsumer. It is safe to call more than once.
func (j *JSONLWriter) Close() error {
	if j.closed {
		return nil
	}
	j.closed = true

	err := j.w.Flush()
	if j.closer != nil {
		if cerr := j.closer.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		return fmt.Errorf("copg: jsonl: close: %w", err)
	}
	return nil
}

// appendJSONValue encodes one decoded value. dst is appended to and returned so
// callers can reuse a single row buffer.
func appendJSONValue(dst []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(dst, "null"...), nil
	case bool:
		return strconv.AppendBool(dst, x), nil
	case int64:
		return strconv.AppendInt(dst, x, 10), nil
	case int32:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int16:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case uint64:
		return strconv.AppendUint(dst, x, 10), nil
	case uint32:
		return strconv.AppendUint(dst, uint64(x), 10), nil
	case float64:
		return appendJSONFloat(dst, x), nil
	case float32:
		return appendJSONFloat(dst, float64(x)), nil
	case string:
		return appendJSONString(dst, x), nil
	case []byte:
		// bytea, "char", bit strings and anything FallbackRaw handed through.
		return appendJSONString(dst, base64.StdEncoding.EncodeToString(x)), nil
	case [16]byte:
		// uuid.UUID's underlying representation.
		return appendJSONString(dst, formatUUID(x)), nil
	case time.Time:
		return appendJSONTime(dst, x), nil
	case pgtype.Numeric:
		return appendJSONNumeric(dst, x)
	case pgtype.Interval:
		return appendValuer(dst, x)
	case []any:
		dst = append(dst, '[')
		for i, elem := range x {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			dst, err = appendJSONValue(dst, elem)
			if err != nil {
				return dst, err
			}
		}
		return append(dst, ']'), nil
	case map[string]any:
		return appendJSONMap(dst, x)
	}

	// pgtype value types (interval, bits, geometric types, hstore, ...) know
	// their PostgreSQL text form through driver.Valuer.
	if valuer, ok := v.(driver.Valuer); ok {
		return appendValuer(dst, valuer)
	}
	// Types with a native JSON representation.
	if marshaler, ok := v.(json.Marshaler); ok {
		b, err := marshaler.MarshalJSON()
		if err != nil {
			return dst, err
		}
		return append(dst, b...), nil
	}
	// Last resort: whatever encoding/json makes of it.
	b, err := json.Marshal(v)
	if err != nil {
		return dst, fmt.Errorf("cannot encode Go type %T as JSON: %w", v, err)
	}
	return append(dst, b...), nil
}

func appendValuer(dst []byte, v driver.Valuer) ([]byte, error) {
	dv, err := v.Value()
	if err != nil {
		return dst, err
	}
	// driver.Value is restricted to nil, int64, float64, bool, []byte, string
	// and time.Time, so this recursion terminates.
	return appendJSONValue(dst, dv)
}

func appendJSONMap(dst []byte, m map[string]any) ([]byte, error) {
	dst = append(dst, '{')
	for i, k := range slices.Sorted(maps.Keys(m)) {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONString(dst, k)
		dst = append(dst, ':')
		var err error
		dst, err = appendJSONValue(dst, m[k])
		if err != nil {
			return dst, err
		}
	}
	return append(dst, '}'), nil
}

func appendJSONString(dst []byte, s string) []byte {
	// json.Marshal escapes quotes, control characters and invalid UTF-8; it
	// cannot fail for a string.
	b, err := json.Marshal(s)
	if err != nil {
		return append(dst, `""`...)
	}
	return append(dst, b...)
}

// appendJSONFloat keeps PostgreSQL's non-finite floats representable: JSON has
// no NaN/Infinity literal, so they become strings, as in PostgreSQL's own text
// output.
func appendJSONFloat(dst []byte, f float64) []byte {
	switch {
	case math.IsNaN(f):
		return appendJSONString(dst, "NaN")
	case math.IsInf(f, 1):
		return appendJSONString(dst, "Infinity")
	case math.IsInf(f, -1):
		return appendJSONString(dst, "-Infinity")
	}
	return strconv.AppendFloat(dst, f, 'g', -1, 64)
}

// appendJSONTime writes RFC 3339 with nanoseconds and the session's offset.
// time.Time.MarshalJSON rejects years outside [0,9999], which PostgreSQL
// timestamps may exceed, so the text is formatted directly.
func appendJSONTime(dst []byte, t time.Time) []byte {
	dst = append(dst, '"')
	dst = t.AppendFormat(dst, time.RFC3339Nano)
	return append(dst, '"')
}

// appendJSONNumeric writes a bare number when the value is finite and valid
// JSON, and a string for NaN/Infinity (PostgreSQL's text form).
func appendJSONNumeric(dst []byte, n pgtype.Numeric) ([]byte, error) {
	v, err := n.Value()
	if err != nil {
		return dst, err
	}
	if v == nil {
		return append(dst, "null"...), nil
	}
	s, ok := v.(string)
	if !ok {
		return dst, fmt.Errorf("unexpected numeric text type %T", v)
	}
	if n.InfinityModifier == pgtype.Finite && json.Valid([]byte(s)) {
		return append(dst, s...), nil
	}
	return appendJSONString(dst, s), nil
}

func formatUUID(b [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
