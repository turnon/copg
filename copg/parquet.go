package copg

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/parquet-go/parquet-go"
)

// parquetKind is the logical encoding used for a PostgreSQL column in Parquet.
type parquetKind int

const (
	pkBool parquetKind = iota
	pkInt32
	pkInt64
	pkFloat
	pkDouble
	pkString
	pkBytes
	pkDate
	pkTimestamp
	pkUUID
	pkJSON
)

// parquetKindForColumn maps a PostgreSQL type OID to a Parquet column kind.
// Types without a dedicated mapping are stored as BYTE_ARRAY so that no
// information is lost when the decoder falls back to raw bytes.
func parquetKindForColumn(c ColumnMeta) parquetKind {
	// Arrays are decoded by pgx into []any; store their JSON form as a string.
	if strings.HasPrefix(c.TypeName, "_") {
		return pkString
	}
	switch c.OID {
	case pgtype.BoolOID:
		return pkBool
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.OIDOID:
		return pkInt32
	case pgtype.Int8OID:
		return pkInt64
	case pgtype.Float4OID:
		return pkFloat
	case pgtype.Float8OID:
		return pkDouble
	case pgtype.ByteaOID:
		return pkBytes
	case pgtype.DateOID:
		return pkDate
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return pkTimestamp
	case pgtype.UUIDOID:
		return pkUUID
	case pgtype.JSONOID, pgtype.JSONBOID:
		return pkJSON
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.QCharOID,
		pgtype.NameOID, pgtype.NumericOID, pgtype.TimeOID, pgtype.TimetzOID,
		pgtype.IntervalOID:
		return pkString
	default:
		return pkBytes
	}
}

// BuildParquetSchema builds a Parquet schema for the given columns. Every
// column is marked OPTIONAL, because PostgreSQL columns are nullable unless a
// NOT NULL constraint is present and NULLs are always representable here.
//
// Workers exporting different shards should share one schema instance so all
// output files are compatible.
func BuildParquetSchema(name string, cols []ColumnMeta) *parquet.Schema {
	if name == "" {
		name = "copg"
	}
	fields := make(parquet.Group, len(cols))
	for _, c := range cols {
		fields[c.Name] = parquet.Optional(parquetNode(parquetKindForColumn(c)))
	}
	return parquet.NewSchema(name, fields)
}

func parquetNode(kind parquetKind) parquet.Node {
	switch kind {
	case pkBool:
		return parquet.Leaf(parquet.BooleanType)
	case pkInt32:
		return parquet.Int(32)
	case pkInt64:
		return parquet.Int(64)
	case pkFloat:
		return parquet.Leaf(parquet.FloatType)
	case pkDouble:
		return parquet.Leaf(parquet.DoubleType)
	case pkString:
		return parquet.String()
	case pkUUID:
		return parquet.UUID()
	case pkDate:
		return parquet.Date()
	case pkTimestamp:
		return parquet.Timestamp(parquet.Microsecond)
	case pkJSON:
		return parquet.JSON()
	default:
		return parquet.Leaf(parquet.ByteArrayType)
	}
}

// ParquetConsumer writes decoded rows to a Parquet file. It implements
// RowConsumer and is intended for use by a single Scanner worker.
type ParquetConsumer struct {
	file   *os.File
	writer *parquet.Writer
	cols   []ColumnMeta
	kinds  []parquetKind
	perm   []int // perm[i] is the Parquet column index of cols[i]
	batch  int
	rows   []parquet.Row
	closed bool
}

// NewParquetConsumer creates a Parquet file at path using a schema derived from
// cols.
func NewParquetConsumer(path string, cols []ColumnMeta) (*ParquetConsumer, error) {
	return NewParquetConsumerWithSchema(path, cols, nil, 0)
}

// NewParquetConsumerWithSchema is like NewParquetConsumer but lets callers pass
// a pre-built schema (shared across workers) and a write batch size.
func NewParquetConsumerWithSchema(path string, cols []ColumnMeta, schema *parquet.Schema, batchSize int) (*ParquetConsumer, error) {
	if path == "" {
		return nil, fmt.Errorf("copg: parquet path is empty")
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("copg: parquet consumer requires at least one column")
	}
	if schema == nil {
		schema = BuildParquetSchema("copg", cols)
	}
	if batchSize <= 0 {
		batchSize = 1024
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("copg: create parquet file %s: %w", path, err)
	}
	w := parquet.NewWriter(f, schema)

	// parquet.Group sorts fields by name, so the physical column order of the
	// schema is not necessarily the PostgreSQL column order. Map each column of
	// the exported relation onto its position in the schema.
	leafColumns := schema.Columns()
	nameToIndex := make(map[string]int, len(leafColumns))
	for i, path := range leafColumns {
		if len(path) > 0 {
			nameToIndex[path[0]] = i
		}
	}
	perm := make([]int, len(cols))
	seen := make(map[int]bool, len(cols))
	for i, c := range cols {
		idx, ok := nameToIndex[c.Name]
		if !ok {
			f.Close()
			return nil, fmt.Errorf("copg: parquet schema has no column %q", c.Name)
		}
		if seen[idx] {
			f.Close()
			return nil, fmt.Errorf("copg: parquet schema maps column %q twice", c.Name)
		}
		seen[idx] = true
		perm[i] = idx
	}
	if len(leafColumns) != len(cols) {
		f.Close()
		return nil, fmt.Errorf("copg: parquet schema has %d columns but %d were requested", len(leafColumns), len(cols))
	}

	p := &ParquetConsumer{
		file:   f,
		writer: w,
		cols:   cols,
		kinds:  make([]parquetKind, len(cols)),
		perm:   perm,
		batch:  batchSize,
		rows:   make([]parquet.Row, 0, batchSize),
	}
	for i, c := range cols {
		p.kinds[i] = parquetKindForColumn(c)
	}
	return p, nil
}

// Consume implements RowConsumer.
func (p *ParquetConsumer) Consume(ctx context.Context, row *DecodedRow) error {
	if p.closed {
		return fmt.Errorf("copg: consume on closed parquet consumer")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(row.Values) != len(p.cols) {
		return fmt.Errorf("copg: parquet column count mismatch: got %d, want %d", len(row.Values), len(p.cols))
	}

	values := make(parquet.Row, len(p.cols))
	for i, v := range row.Values {
		pvi, err := toParquetValue(p.kinds[i], v)
		if err != nil {
			return fmt.Errorf("copg: column %s: %w", p.cols[i].Name, err)
		}
		colIndex := p.perm[i]
		definitionLevel := 1
		if v == nil {
			definitionLevel = 0 // NULL: optional columns have max definition level 1
		}
		values[colIndex] = pvi.Level(0, definitionLevel, colIndex)
	}
	p.rows = append(p.rows, values)

	if len(p.rows) >= p.batch {
		return p.flush()
	}
	return nil
}

func (p *ParquetConsumer) flush() error {
	if len(p.rows) == 0 {
		return nil
	}
	if _, err := p.writer.WriteRows(p.rows); err != nil {
		return fmt.Errorf("copg: write parquet rows: %w", err)
	}
	p.rows = p.rows[:0]
	return nil
}

// Close implements RowConsumer. It flushes buffered rows and closes the file.
func (p *ParquetConsumer) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	flushErr := p.flush()
	writerErr := p.writer.Close()
	fileErr := p.file.Close()

	if flushErr != nil {
		return flushErr
	}
	if writerErr != nil {
		return fmt.Errorf("copg: close parquet writer: %w", writerErr)
	}
	if fileErr != nil {
		return fmt.Errorf("copg: close parquet file: %w", fileErr)
	}
	return nil
}

func toParquetValue(kind parquetKind, v any) (parquet.Value, error) {
	if v == nil {
		return parquet.Value{}, nil
	}

	switch kind {
	case pkBool:
		if b, ok := v.(bool); ok {
			return parquet.BooleanValue(b), nil
		}
		return parquet.Value{}, fmt.Errorf("cannot convert %T to boolean", v)

	case pkInt32:
		n, err := toInt64(v)
		if err != nil {
			return parquet.Value{}, err
		}
		if n < math.MinInt32 || n > math.MaxInt32 {
			return parquet.Value{}, fmt.Errorf("value %d overflows int32", n)
		}
		return parquet.Int32Value(int32(n)), nil

	case pkInt64:
		n, err := toInt64(v)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.Int64Value(n), nil

	case pkFloat:
		f, err := toFloat64(v)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.FloatValue(float32(f)), nil

	case pkDouble:
		f, err := toFloat64(v)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.DoubleValue(f), nil

	case pkString:
		s, err := stringify(v)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.ByteArrayValue([]byte(s)), nil

	case pkBytes:
		b, err := toBytes(v)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.ByteArrayValue(b), nil

	case pkDate:
		switch x := v.(type) {
		case time.Time:
			days := int32(x.UTC().Unix() / 86400)
			return parquet.Int32Value(days), nil
		case pgtype.InfinityModifier:
			return parquet.Value{}, fmt.Errorf("infinite date (%s) cannot be represented in parquet", x)
		default:
			n, err := toInt64(v)
			if err != nil {
				return parquet.Value{}, err
			}
			return parquet.Int32Value(int32(n)), nil
		}

	case pkTimestamp:
		switch x := v.(type) {
		case time.Time:
			return parquet.Int64Value(x.UnixMicro()), nil
		case pgtype.InfinityModifier:
			return parquet.Value{}, fmt.Errorf("infinite timestamp (%s) cannot be represented in parquet", x)
		default:
			n, err := toInt64(v)
			if err != nil {
				return parquet.Value{}, err
			}
			return parquet.Int64Value(n), nil
		}

	case pkUUID:
		b, err := uuidBytes(v)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.FixedLenByteArrayValue(b), nil

	case pkJSON:
		b, err := jsonBytes(v)
		if err != nil {
			return parquet.Value{}, err
		}
		return parquet.ByteArrayValue(b), nil
	}
	return parquet.Value{}, fmt.Errorf("unsupported parquet kind %d", kind)
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		return int64(x), nil
	case float32:
		return int64(x), nil
	case float64:
		return int64(x), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(x), 10, 64)
	case []byte:
		return strconv.ParseInt(strings.TrimSpace(string(x)), 10, 64)
	case driver.Valuer:
		dv, err := x.Value()
		if err != nil {
			return 0, err
		}
		return toInt64(dv)
	default:
		return 0, fmt.Errorf("cannot convert %T to integer", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float32:
		return float64(x), nil
	case float64:
		return x, nil
	case int:
		return float64(x), nil
	case int8:
		return float64(x), nil
	case int16:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case uint:
		return float64(x), nil
	case uint8:
		return float64(x), nil
	case uint16:
		return float64(x), nil
	case uint32:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case string:
		return strconv.ParseFloat(strings.TrimSpace(x), 64)
	case []byte:
		return strconv.ParseFloat(strings.TrimSpace(string(x)), 64)
	case driver.Valuer:
		dv, err := x.Value()
		if err != nil {
			return 0, err
		}
		return toFloat64(dv)
	default:
		return 0, fmt.Errorf("cannot convert %T to float", v)
	}
}

func toBytes(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	case [16]byte:
		return x[:], nil
	default:
		s, err := stringify(v)
		if err != nil {
			return nil, err
		}
		return []byte(s), nil
	}
}

func uuidBytes(v any) ([]byte, error) {
	switch x := v.(type) {
	case [16]byte:
		return x[:], nil
	case []byte:
		if len(x) == 16 {
			return x, nil
		}
		return nil, fmt.Errorf("invalid uuid byte length %d", len(x))
	case string:
		clean := strings.ReplaceAll(x, "-", "")
		b, err := hex.DecodeString(clean)
		if err != nil || len(b) != 16 {
			return nil, fmt.Errorf("invalid uuid %q", x)
		}
		return b, nil
	default:
		if s, err := stringify(v); err == nil {
			return uuidBytes(s)
		}
		return nil, fmt.Errorf("cannot convert %T to uuid", v)
	}
}

func jsonBytes(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	case nil:
		return []byte("null"), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal json: %w", err)
		}
		return b, nil
	}
}

// stringify renders a decoded value as text for Parquet string columns.
func stringify(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "", nil
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case time.Time:
		return x.Format(time.RFC3339Nano), nil
	case [16]byte:
		return fmt.Sprintf("%x-%x-%x-%x-%x", x[0:4], x[4:6], x[6:8], x[8:10], x[10:16]), nil
	case pgtype.Interval:
		if !x.Valid {
			return "", nil
		}
		return fmt.Sprintf("%d mons %d days %d micros", x.Months, x.Days, x.Microseconds), nil
	case fmt.Stringer:
		return x.String(), nil
	}

	if valuer, ok := v.(driver.Valuer); ok {
		dv, err := valuer.Value()
		if err != nil {
			return "", err
		}
		if dv == nil {
			return "", nil
		}
		return stringify(dv)
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map, reflect.Struct:
		b, err := json.Marshal(v)
		if err == nil {
			return string(b), nil
		}
	}
	return fmt.Sprint(v), nil
}
