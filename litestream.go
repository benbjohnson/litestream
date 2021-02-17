package litestream

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Naming constants.
const (
	MetaDirSuffix = "-litestream"

	WALDirName  = "wal"
	WALExt      = ".wal"
	SnapshotExt = ".snapshot"

	GenerationNameLen = 16
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

// SnapshotInfo represents file information about a snapshot.
type SnapshotInfo struct {
	Name       string
	Replica    string
	Generation string
	Index      int
	Size       int64
	CreatedAt  time.Time
}

// FilterSnapshotsAfter returns all snapshots that were created on or after t.
func FilterSnapshotsAfter(a []*SnapshotInfo, t time.Time) []*SnapshotInfo {
	other := make([]*SnapshotInfo, 0, len(a))
	for _, snapshot := range a {
		if !snapshot.CreatedAt.Before(t) {
			other = append(other, snapshot)
		}
	}
	return other
}

// FindMinSnapshotByGeneration finds the snapshot with the lowest index in a generation.
func FindMinSnapshotByGeneration(a []*SnapshotInfo, generation string) *SnapshotInfo {
	var min *SnapshotInfo
	for _, snapshot := range a {
		if snapshot.Generation != generation {
			continue
		} else if min == nil || snapshot.Index < min.Index {
			min = snapshot
		}
	}
	return min
}

// WALInfo represents file information about a WAL file.
type WALInfo struct {
	Name       string
	Replica    string
	Generation string
	Index      int
	Offset     int64
	Size       int64
	CreatedAt  time.Time
}

// Pos is a position in the WAL for a generation.
type Pos struct {
	Generation string // generation name
	Index      int    // wal file index
	Offset     int64  // offset within wal file
}

// String returns a string representation.
func (p Pos) String() string {
	if p.IsZero() {
		return ""
	}
	return fmt.Sprintf("%s/%08x:%d", p.Generation, p.Index, p.Offset)
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
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
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

// IsGenerationName returns true if s is the correct length and is only lowercase hex characters.
func IsGenerationName(s string) bool {
	if len(s) != GenerationNameLen {
		return false
	}
	for _, ch := range s {
		if !isHexChar(ch) {
			return false
		}
	}
	return true
}

// IsSnapshotPath returns true if s is a path to a snapshot file.
func IsSnapshotPath(s string) bool {
	return snapshotPathRegex.MatchString(s)
}

// ParseSnapshotPath returns the index for the snapshot.
// Returns an error if the path is not a valid snapshot path.
func ParseSnapshotPath(s string) (index int, ext string, err error) {
	s = filepath.Base(s)

	a := snapshotPathRegex.FindStringSubmatch(s)
	if a == nil {
		return 0, "", fmt.Errorf("invalid snapshot path: %s", s)
	}

	i64, _ := strconv.ParseUint(a[1], 16, 64)
	return int(i64), a[2], nil
}

var snapshotPathRegex = regexp.MustCompile(`^([0-9a-f]{8})(.snapshot(?:.lz4)?)$`)

// IsWALPath returns true if s is a path to a WAL file.
func IsWALPath(s string) bool {
	return walPathRegex.MatchString(s)
}

// ParseWALPath returns the index & offset for the WAL file.
// Returns an error if the path is not a valid snapshot path.
func ParseWALPath(s string) (index int, offset int64, ext string, err error) {
	s = filepath.Base(s)

	a := walPathRegex.FindStringSubmatch(s)
	if a == nil {
		return 0, 0, "", fmt.Errorf("invalid wal path: %s", s)
	}

	i64, _ := strconv.ParseUint(a[1], 16, 64)
	off64, _ := strconv.ParseUint(a[2], 16, 64)
	return int(i64), int64(off64), a[3], nil
}

// FormatWALPath formats a WAL filename with a given index.
func FormatWALPath(index int) string {
	assert(index >= 0, "wal index must be non-negative")
	return fmt.Sprintf("%08x%s", index, WALExt)
}

// FormatWALPathWithOffset formats a WAL filename with a given index & offset.
func FormatWALPathWithOffset(index int, offset int64) string {
	assert(index >= 0, "wal index must be non-negative")
	assert(offset >= 0, "wal offset must be non-negative")
	return fmt.Sprintf("%08x_%08x%s", index, offset, WALExt)
}

var walPathRegex = regexp.MustCompile(`^([0-9a-f]{8})(?:_([0-9a-f]{8}))?(.wal(?:.lz4)?)$`)

// isHexChar returns true if ch is a lowercase hex character.
func isHexChar(ch rune) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')
}

// createFile creates the file and attempts to set the UID/GID.
func createFile(filename string, perm os.FileMode, uid, gid int) (*os.File, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return nil, err
	}
	_ = f.Chown(uid, gid)
	return f, nil
}

// mkdirAll is a copy of os.MkdirAll() except that it attempts to set the
// uid/gid for each created directory.
func mkdirAll(path string, perm os.FileMode, uid, gid int) error {
	// Fast path: if we can tell whether path is a directory or file, stop with success or error.
	dir, err := os.Stat(path)
	if err == nil {
		if dir.IsDir() {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(path)
	for i > 0 && os.IsPathSeparator(path[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(path[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent.
		err = mkdirAll(fixRootDirectory(path[:j-1]), perm, uid, gid)
		if err != nil {
			return err
		}
	}

	// Parent now exists; invoke Mkdir and use its result.
	err = os.Mkdir(path, perm)
	if err != nil {
		// Handle arguments like "foo/." by
		// double-checking that directory doesn't exist.
		dir, err1 := os.Lstat(path)
		if err1 == nil && dir.IsDir() {
			_ = os.Chown(path, uid, gid)
			return nil
		}
		return err
	}
	_ = os.Chown(path, uid, gid)
	return nil
}

// Tracef is used for low-level tracing.
var Tracef = func(format string, a ...interface{}) {}

func assert(condition bool, message string) {
	if !condition {
		panic("assertion failed: " + message)
	}
}
