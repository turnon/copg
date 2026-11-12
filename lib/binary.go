package lib

import (
	"encoding/binary"
	"fmt"
	"io"
)

// copyBinarySignature is the 11-byte signature that starts every PostgreSQL
// COPY BINARY stream.
const copyBinarySignature = "PGCOPY\n\xff\r\n\x00"

// Row is one decoded (but not type-decoded) tuple from a COPY BINARY stream.
// A nil field represents SQL NULL.
type Row [][]byte

// BinaryReader reads rows from a PostgreSQL COPY ... (FORMAT BINARY) stream.
//
// Format reference:
//
//	header:  11-byte signature, int32 flags, int32 extension length
//	tuple:   int16 field count, then per field int32 length + bytes (-1 = NULL)
//	trailer: int16 -1
type BinaryReader struct {
	r          io.Reader
	headerRead bool
	trailer    bool
}

// NewBinaryReader returns a reader that parses the COPY BINARY data from r.
func NewBinaryReader(r io.Reader) *BinaryReader {
	return &BinaryReader{r: r}
}

// ReadRow returns the next row. It returns io.EOF after the stream trailer has
// been consumed.
func (br *BinaryReader) ReadRow() (Row, error) {
	if !br.headerRead {
		if err := br.readHeader(); err != nil {
			return nil, err
		}
		br.headerRead = true
	}
	if br.trailer {
		return nil, io.EOF
	}

	var fieldCount int16
	if err := binary.Read(br.r, binary.BigEndian, &fieldCount); err != nil {
		return nil, err
	}
	if fieldCount == -1 { // trailer
		br.trailer = true
		return nil, io.EOF
	}
	if fieldCount < 0 {
		return nil, fmt.Errorf("copg: invalid field count %d", fieldCount)
	}

	row := make(Row, int(fieldCount))
	for i := range row {
		var length int32
		if err := binary.Read(br.r, binary.BigEndian, &length); err != nil {
			return nil, err
		}
		if length == -1 { // NULL
			row[i] = nil
			continue
		}
		if length < 0 {
			return nil, fmt.Errorf("copg: invalid field length %d in column %d", length, i)
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(br.r, buf); err != nil {
			return nil, err
		}
		row[i] = buf
	}
	return row, nil
}

func (br *BinaryReader) readHeader() error {
	header := make([]byte, 19) // signature (11) + flags (4) + extension length (4)
	if _, err := io.ReadFull(br.r, header); err != nil {
		return fmt.Errorf("copg: read COPY binary header: %w", err)
	}
	if string(header[:11]) != copyBinarySignature {
		return fmt.Errorf("copg: unexpected COPY binary signature %q", header[:11])
	}
	return nil
}
