package litestream

import (
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

const (
	MetaDirSuffix = "-litestream"

	WALDirName = "wal"
	WALExt     = ".wal"

	GenerationNameLen = 16
)

// Pos is a position in the WAL for a generation.
type Pos struct {
	Generation string // generation name
	Index      int    // wal file index
	Offset     int64  // offset within wal file
}

// String returns a string representation.
func (p Pos) String() string {
	if p.IsZero() {
		return "<>"
	}
	return fmt.Sprintf("<%s,%d,%d>", p.Generation, p.Index, p.Offset)
}

// IsZero returns true if p is the zero value.
func (p Pos) IsZero() bool {
	return p == (Pos{})
}

// Checksum computes a running SQLite checksum over a byte slice.
func Checksum(bo binary.ByteOrder, s0, s1 uint32, b []byte) (uint32, uint32) {
	assert(len(b)%8 == 0, "misaligned checksum byte slice")

	// Iterate over 8-byte units and compute checksum.
	for i := 0; i < len(b); i += 8 {
		s0 += bo.Uint32(b[i:]) + s1
		s1 += bo.Uint32(b[i+4:]) + s0
	}
	return s0, s1
}

const (
	// WALHeaderSize is the size of the WAL header, in bytes.
	WALHeaderSize = 32

	// WALFrameHeaderSize is the size of the WAL frame header, in bytes.
	WALFrameHeaderSize = 24
)

// calcWALSize returns the size of the WAL, in bytes, for a given number of pages.
func calcWALSize(pageSize int, n int) int64 {
	return int64(WALHeaderSize + ((WALFrameHeaderSize + pageSize) * n))
}

// rollback rolls back tx. Ignores already-rolled-back errors.
func rollback(tx *sql.Tx) error {
	if err := tx.Rollback(); err != nil && !strings.Contains(err.Error(), `transaction has already been committed or rolled back`) {
		return err
	}
	return nil
}

// readWALHeader returns the header read from a WAL file.
func readWALHeader(filename string) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, WALHeaderSize)
	n, err := io.ReadFull(f, buf)
	return buf[:n], err
}

func readCheckpointSeqNo(hdr []byte) uint32 {
	return binary.BigEndian.Uint32(hdr[12:])
}

// readFileAt reads a slice from a file.
func readFileAt(filename string, offset, n int64) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, n)
	if n, err := f.ReadAt(buf, offset); err != nil {
		return buf[:n], err
	} else if n < len(buf) {
		return buf[:n], io.ErrUnexpectedEOF
	}
	return buf, nil
}

func ParseWALFilename(name string) (index int, err error) {
	v, err := strconv.ParseInt(strings.TrimSuffix(name, WALExt), 16, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid wal filename: %q", name)
	}
	return int(v), nil
}

func FormatWALFilename(index int) string {
	assert(index >= 0, "wal index must be non-negative")
	return fmt.Sprintf("%016x%s", index, WALExt)
}

// HexDump returns hexdump output but with duplicate lines removed.
func HexDump(b []byte) string {
	const prefixN = len("00000000")

	var output []string
	var prev string
	var ellipsis bool

	lines := strings.Split(strings.TrimSpace(hex.Dump(b)), "\n")
	for i, line := range lines {
		// Add line to output if it is not repeating or the last line.
		if i == 0 || i == len(lines)-1 || trimPrefixN(line, prefixN) != trimPrefixN(prev, prefixN) {
			output = append(output, line)
			prev, ellipsis = line, false
			continue
		}

		// Add an ellipsis for the first duplicate line.
		if !ellipsis {
			output = append(output, "...")
			ellipsis = true
			continue
		}
	}

	return strings.Join(output, "\n")
}

func trimPrefixN(s string, n int) string {
	if len(s) < n {
		return ""
	}
	return s[n:]
}

func assert(condition bool, message string) {
	if !condition {
		panic("assertion failed: " + message)
	}
}
