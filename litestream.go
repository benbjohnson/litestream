package litestream

import (
	"encoding/binary"
	"encoding/hex"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

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
