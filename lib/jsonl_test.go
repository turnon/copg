package lib

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestJSONLWriterConsume(t *testing.T) {
	var buf bytes.Buffer
	jw := NewJSONLWriter(&buf)
	jw.Columns([]ColumnMeta{{Name: "id"}, {Name: "name"}, {Name: "score"}, {Name: "tags"}})

	ctx := context.Background()
	rows := []*DecodedRow{
		{Values: []any{int64(1), "alice", 1.5, []any{"a", "b"}}},
		{Values: []any{int64(2), nil, 0.0, []any{}}},
		{Values: []any{int64(3), "line\nbreak \"quoted\"", -0.25, nil}},
	}
	for _, r := range rows {
		if err := jw.Consume(ctx, r); err != nil {
			t.Fatalf("Consume: %v", err)
		}
	}
	if err := jw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != len(rows) {
		t.Fatalf("got %d lines, want %d: %q", len(lines), len(rows), buf.String())
	}
	for i, line := range lines {
		if !strings.HasPrefix(line, `{"id":`) {
			t.Errorf("line %d does not start with the first column: %s", i, line)
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("line %d is not valid JSON: %v: %s", i, err, line)
		}
		if got := m["id"].(float64); got != float64(i+1) {
			t.Errorf("line %d: id = %v, want %d", i, got, i+1)
		}
	}

	want := `{"id":2,"name":null,"score":0,"tags":[]}`
	if lines[1] != want {
		t.Errorf("line 1 = %s, want %s", lines[1], want)
	}
}

func TestJSONLWriterColumnOrder(t *testing.T) {
	var buf bytes.Buffer
	jw := NewJSONLWriter(&buf)
	// Deliberately not alphabetical: output must follow the table's order.
	jw.Columns([]ColumnMeta{{Name: "z"}, {Name: "a"}})
	if err := jw.Consume(context.Background(), &DecodedRow{Values: []any{int64(1), int64(2)}}); err != nil {
		t.Fatal(err)
	}
	if err := jw.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := buf.String(), `{"z":1,"a":2}`+"\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestJSONLWriterRequiresColumns(t *testing.T) {
	jw := NewJSONLWriter(&bytes.Buffer{})
	err := jw.Consume(context.Background(), &DecodedRow{Values: []any{int64(1)}})
	if err == nil || !strings.Contains(err.Error(), "no column metadata") {
		t.Fatalf("got %v, want a missing-metadata error", err)
	}
}

func TestJSONLWriterValueTypes(t *testing.T) {
	ts := time.Date(2024, 5, 6, 7, 8, 9, 123000000, time.UTC)
	uuid := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
		0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}

	cases := []struct {
		name string
		in   any
		want string
	}{
		{"nil", nil, "null"},
		{"bool", true, "true"},
		{"int16", int16(-3), "-3"},
		{"int32", int32(7), "7"},
		{"float32", float32(1.5), "1.5"},
		{"nan", math.NaN(), `"NaN"`},
		{"inf", math.Inf(1), `"Infinity"`},
		{"neg_inf", math.Inf(-1), `"-Infinity"`},
		{"text", `a"b\c`, `"a\"b\\c"`},
		{"bytea", []byte{0xde, 0xad}, `"3q0="`},
		{"uuid", uuid, `"12345678-9abc-def0-1122-334455667788"`},
		{"time", ts, `"2024-05-06T07:08:09.123Z"`},
		{"time_far_future", time.Date(12000, 1, 1, 0, 0, 0, 0, time.UTC), `"12000-01-01T00:00:00Z"`},
		{"numeric", pgtype.Numeric{Int: big.NewInt(-123), Exp: -2, Valid: true}, "-1.23"},
		{"numeric_int", pgtype.Numeric{Int: big.NewInt(1000), Exp: 2, Valid: true}, "100000"},
		{"numeric_nan", pgtype.Numeric{NaN: true, Valid: true}, `"NaN"`},
		{"numeric_inf", pgtype.Numeric{InfinityModifier: pgtype.Infinity, Valid: true}, `"Infinity"`},
		{"numeric_null", pgtype.Numeric{}, "null"},
		{"interval", pgtype.Interval{Months: 1, Days: 2, Microseconds: 3723000000, Valid: true}, `"1 mon 2 day 01:02:03"`},
		{"nested_array", []any{int64(1), []any{"x", nil}}, `[1,["x",null]]`},
		{"map", map[string]any{"b": int64(2), "a": []any{"y"}}, `{"a":["y"],"b":2}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := appendJSONValue(nil, tc.in)
			if err != nil {
				t.Fatalf("appendJSONValue(%#v): %v", tc.in, err)
			}
			if string(got) != tc.want {
				t.Errorf("appendJSONValue(%#v) = %s, want %s", tc.in, got, tc.want)
			}
			if !json.Valid(got) {
				t.Errorf("output is not valid JSON: %s", got)
			}
		})
	}
}

func TestJSONLWriterClosesFile(t *testing.T) {
	path := t.TempDir() + "/shard.jsonl"
	jw, err := NewJSONLFile(path)
	if err != nil {
		t.Fatal(err)
	}
	jw.Columns([]ColumnMeta{{Name: "n"}})
	if err := jw.Consume(context.Background(), &DecodedRow{Values: []any{int64(42)}}); err != nil {
		t.Fatal(err)
	}
	if err := jw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := jw.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"n":42}`+"\n" {
		t.Errorf("file = %q", b)
	}
}
