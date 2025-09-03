package litestream

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/superfly/ltx"
	_ "modernc.org/sqlite"
)

// Naming constants.
const (
	MetaDirSuffix = "-litestream"
)

// SQLite checkpoint modes.
const (
	CheckpointModePassive  = "PASSIVE"
	CheckpointModeFull     = "FULL"
	CheckpointModeRestart  = "RESTART"
	CheckpointModeTruncate = "TRUNCATE"
)

// Litestream errors.
var (
	ErrNoSnapshots      = errors.New("no snapshots available")
	ErrChecksumMismatch = errors.New("invalid replica, checksum mismatch")
)

// SQLite WAL constants.
const (
	WALHeaderChecksumOffset      = 24
	WALFrameHeaderChecksumOffset = 16
)

var (
	// LogWriter is the destination writer for all logging.
	LogWriter = os.Stdout

	// LogFlags are the flags passed to log.New().
	LogFlags = 0
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

const (
	// WALHeaderSize is the size of the WAL header, in bytes.
	WALHeaderSize = 32

	// WALFrameHeaderSize is the size of the WAL frame header, in bytes.
	WALFrameHeaderSize = 24
)

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

// readWALFileAt reads a slice from a file. Do not use this with database files
// as it causes problems with non-OFD locks.
func readWALFileAt(filename string, offset, n int64) ([]byte, error) {
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

// removeTmpFiles recursively finds and removes .tmp files.
func removeTmpFiles(root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		switch {
		case err != nil:
			return nil // skip errored files
		case info.IsDir():
			return nil // skip directories
		case !strings.HasSuffix(path, ".tmp"):
			return nil // skip non-temp files
		default:
			return os.Remove(path)
		}
	})
}

// LTXDir returns the path to an LTX directory.
func LTXDir(root string) string {
	return path.Join(root, "ltx")
}

// LTXLevelDir returns the path to an LTX level directory.
func LTXLevelDir(root string, level int) string {
	return path.Join(LTXDir(root), strconv.Itoa(level))
}

// LTXFilePath returns the path to a single LTX file.
func LTXFilePath(root string, level int, minTXID, maxTXID ltx.TXID) string {
	return path.Join(LTXLevelDir(root, level), ltx.FormatFilename(minTXID, maxTXID))
}

func assert(condition bool, message string) {
	if !condition {
		panic("assertion failed: " + message)
	}
}
