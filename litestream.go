package litestream

import (
	"cmp"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/superfly/ltx"
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

// SQLite WAL constants
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

func init() {
	sql.Register("litestream-sqlite3", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if err := conn.SetFileControlInt("main", sqlite3.SQLITE_FCNTL_PERSIST_WAL, 1); err != nil {
				return fmt.Errorf("cannot set file control: %w", err)
			}
			return nil
		},
	})
}

// LTXFileIterator represents an iterator over a collection of LTX files.
type LTXFileIterator interface {
	io.Closer

	// Prepares the next LTX file for reading with the Item() method.
	// Returns true if another item is available. Returns false if no more
	// items are available or if an error occured.
	Next() bool

	// Returns an error that occurred during iteration.
	Err() error

	// Returns metadata for the currently positioned LTX file.
	Item() *LTXFileInfo
}

// SliceLTXFileIterator returns all LTX files from an iterator as a slice.
func SliceLTXFileIterator(itr LTXFileIterator) ([]*LTXFileInfo, error) {
	var a []*LTXFileInfo
	for itr.Next() {
		a = append(a, itr.Item())
	}
	return a, itr.Close()
}

var _ LTXFileIterator = (*LTXFileInfoSliceIterator)(nil)

// LTXFileInfoSliceIterator represents an iterator for iterating over a slice of LTX files.
type LTXFileInfoSliceIterator struct {
	init bool
	a    []*LTXFileInfo
}

// NewLTXFileInfoSliceIterator returns a new instance of LTXFileInfoSliceIterator.
// This function will sort the slice in place before returning the iterator.
func NewLTXFileInfoSliceIterator(a []*LTXFileInfo) *LTXFileInfoSliceIterator {
	slices.SortFunc(a, func(x, y *LTXFileInfo) int {
		if v := cmp.Compare(x.Level, y.Level); v != 0 {
			return v
		}
		return cmp.Compare(x.MinTXID, y.MinTXID)
	})

	return &LTXFileInfoSliceIterator{a: a}
}

// Close always returns nil.
func (itr *LTXFileInfoSliceIterator) Close() error { return nil }

// Next moves to the next wal segment. Returns true if another segment is available.
func (itr *LTXFileInfoSliceIterator) Next() bool {
	if !itr.init {
		itr.init = true
		return len(itr.a) > 0
	}
	itr.a = itr.a[1:]
	return len(itr.a) > 0
}

// Err always returns nil.
func (itr *LTXFileInfoSliceIterator) Err() error { return nil }

// LTXFile returns the metadata from the currently positioned wal segment.
func (itr *LTXFileInfoSliceIterator) Item() *LTXFileInfo {
	if len(itr.a) == 0 {
		return nil
	}
	return itr.a[0]
}

// LTXFileInfo represents file information about a WAL segment file.
type LTXFileInfo struct {
	Level     int
	MinTXID   ltx.TXID
	MaxTXID   ltx.TXID
	Size      int64
	CreatedAt time.Time
}

// Pos returns the WAL position when the segment was made.
func (info *LTXFileInfo) Pos() ltx.Pos {
	return ltx.Pos{TXID: info.MaxTXID}
}

// LTXFileInfoSlice represents a slice of WAL segment metadata.
type LTXFileInfoSlice []LTXFileInfo

func (a LTXFileInfoSlice) Len() int { return len(a) }

func (a LTXFileInfoSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a LTXFileInfoSlice) Less(i, j int) bool {
	if a[i].Level != a[j].Level {
		return a[i].Level < a[j].Level
	}
	return a[i].MinTXID < a[j].MinTXID
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
		if err != nil {
			return nil // skip errored files
		} else if info.IsDir() {
			return nil // skip directories
		} else if !strings.HasSuffix(path, ".tmp") {
			return nil // skip non-temp files
		}
		return os.Remove(path)
	})
}

// LTXDir returns the path to an LTX directory
func LTXDir(root string) string {
	return path.Join(root, "ltx")
}

// LTXLevelDir returns the path to an LTX level directory
func LTXLevelDir(root string, level int) string {
	return path.Join(LTXDir(root), "levels", strconv.Itoa(level))
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
