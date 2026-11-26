package lib

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestBuildCopySQL(t *testing.T) {
	const dml = `COPY (SELECT %s FROM "public"."users" WHERE %s) TO STDOUT (FORMAT BINARY)`

	cases := []struct {
		name       string
		selectList string
		filter     string
		shard      Shard
		wantList   string
		wantCond   string
	}{
		{
			name:       "shard range without filter",
			selectList: `"id", "email"`,
			shard:      Shard{Start: 0, End: 4096},
			wantList:   `"id", "email"`,
			wantCond:   "ctid >= '(0,0)'::tid AND ctid < '(4096,0)'::tid",
		},
		{
			name:     "empty select list",
			shard:    Shard{Start: 4096, End: 8192},
			wantList: "*",
			wantCond: "ctid >= '(4096,0)'::tid AND ctid < '(8192,0)'::tid",
		},
		{
			name:       "filter is parenthesized",
			selectList: `"id"`,
			filter:     "col_1 = 'x' AND col_2 IS NOT NULL",
			shard:      Shard{Start: 0, End: 4096},
			wantList:   `"id"`,
			wantCond:   "ctid >= '(0,0)'::tid AND ctid < '(4096,0)'::tid AND (col_1 = 'x' AND col_2 IS NOT NULL)",
		},
		{
			name:       "top-level OR cannot escape the ctid range",
			selectList: `"id"`,
			filter:     "a = 1 OR b IS NULL",
			shard:      Shard{Start: 0, End: 4096},
			wantList:   `"id"`,
			wantCond:   "ctid >= '(0,0)'::tid AND ctid < '(4096,0)'::tid AND (a = 1 OR b IS NULL)",
		},
		{
			name:       "empty shard keeps the filter",
			selectList: `"id"`,
			filter:     "a = 1",
			shard:      Shard{Start: 4096, End: 4096},
			wantList:   `"id"`,
			wantCond:   "false AND (a = 1)",
		},
		{
			name:       "blank filter is ignored",
			selectList: `"id"`,
			filter:     "   ",
			shard:      Shard{Start: 0, End: 4096},
			wantList:   `"id"`,
			wantCond:   "ctid >= '(0,0)'::tid AND ctid < '(4096,0)'::tid",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildCopySQL("public.users", tc.selectList, tc.filter, tc.shard)
			want := strings.Replace(dml, "%s", tc.wantList, 1)
			want = strings.Replace(want, "%s", tc.wantCond, 1)
			if got != want {
				t.Errorf("buildCopySQL() =\n  %s\nwant\n  %s", got, want)
			}
		})
	}
}

// TestEmptyShardStream covers a shard whose ctid range (or -where predicate)
// matches no rows: PostgreSQL still sends a well-formed header and trailer,
// which must decode as zero rows rather than an error.
func TestEmptyShardStream(t *testing.T) {
	var stream bytes.Buffer
	stream.WriteString(copyBinarySignature)
	if err := binary.Write(&stream, binary.BigEndian, int32(0)); err != nil { // flags
		t.Fatal(err)
	}
	if err := binary.Write(&stream, binary.BigEndian, int32(0)); err != nil { // extension length
		t.Fatal(err)
	}
	if err := binary.Write(&stream, binary.BigEndian, int16(-1)); err != nil { // trailer
		t.Fatal(err)
	}

	cols := []ColumnMeta{{Name: "id", OID: 23, TypeName: "int4"}}
	decoder := NewDecoder()
	decoder.FallbackRaw = true
	reader := newBinaryReader(bytes.NewReader(stream.Bytes()))

	if _, err := readDecodedRow(reader, decoder, cols); !errors.Is(err, io.EOF) {
		t.Fatalf("readDecodedRow on an empty shard stream = %v, want io.EOF", err)
	}
}
