package litestream

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"strings"
)

// Magic number specified at the beginning of WAL files.
const (
	MagicLittleEndian = 0x377f0682
	MagicBigEndian    = 0x377f0683
)

const (
	WriteVersionOffset = 18
	ReadVersionOffset  = 19
)

// ReadVersion returns the SQLite write & read version.
// Returns 1 for legacy & 2 for WAL.
func ReadVersion(b []byte) (writeVersion, readVersion uint8, err error) {
	if len(b) < ReadVersionOffset {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return b[WriteVersionOffset], b[ReadVersionOffset], nil
}

// Checksum computes a running checksum over a byte slice.
func Checksum(bo binary.ByteOrder, s uint64, b []byte) (_ uint64, err error) {
	// Ensure byte slice length is divisible by 8.
	if len(b)%8 != 0 {
		return 0, ErrChecksumMisaligned
	}

	// Iterate over 8-byte units and compute checksum.
	s0, s1 := uint32(s>>32), uint32(s&0xFFFFFFFF)
	for i := 0; i < len(b); i += 8 {
		s0 += bo.Uint32(b[i:]) + s1
		s1 += bo.Uint32(b[i+4:]) + s0
	}
	return uint64(s0)<<32 | uint64(s1), nil
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
