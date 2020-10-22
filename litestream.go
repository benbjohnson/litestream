package litestream

import (
	"io"
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
