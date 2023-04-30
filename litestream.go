package litestream

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
)

// Naming constants.
const (
	MetaDirSuffix = "-litestream"

	WALDirName    = "wal"
	WALExt        = ".wal"
	WALSegmentExt = ".wal.lz4"
	SnapshotExt   = ".snapshot.lz4"

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
	ErrNoGeneration     = errors.New("no generation available")
	ErrNoSnapshots      = errors.New("no snapshots available")
	ErrChecksumMismatch = errors.New("invalid replica, checksum mismatch")
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

// SnapshotIterator represents an iterator over a collection of snapshot metadata.
type SnapshotIterator interface {
	io.Closer

	// Prepares the the next snapshot for reading with the Snapshot() method.
	// Returns true if another snapshot is available. Returns false if no more
	// snapshots are available or if an error occured.
	Next() bool

	// Returns an error that occurred during iteration.
	Err() error

	// Returns metadata for the currently positioned snapshot.
	Snapshot() SnapshotInfo
}

// SliceSnapshotIterator returns all snapshots from an iterator as a slice.
func SliceSnapshotIterator(itr SnapshotIterator) ([]SnapshotInfo, error) {
	var a []SnapshotInfo
	for itr.Next() {
		a = append(a, itr.Snapshot())
	}
	return a, itr.Close()
}

var _ SnapshotIterator = (*SnapshotInfoSliceIterator)(nil)

// SnapshotInfoSliceIterator represents an iterator for iterating over a slice of snapshots.
type SnapshotInfoSliceIterator struct {
	init bool
	a    []SnapshotInfo
}

// NewSnapshotInfoSliceIterator returns a new instance of SnapshotInfoSliceIterator.
func NewSnapshotInfoSliceIterator(a []SnapshotInfo) *SnapshotInfoSliceIterator {
	return &SnapshotInfoSliceIterator{a: a}
}

// Close always returns nil.
func (itr *SnapshotInfoSliceIterator) Close() error { return nil }

// Next moves to the next snapshot. Returns true if another snapshot is available.
func (itr *SnapshotInfoSliceIterator) Next() bool {
	if !itr.init {
		itr.init = true
		return len(itr.a) > 0
	}
	itr.a = itr.a[1:]
	return len(itr.a) > 0
}

// Err always returns nil.
func (itr *SnapshotInfoSliceIterator) Err() error { return nil }

// Snapshot returns the metadata from the currently positioned snapshot.
func (itr *SnapshotInfoSliceIterator) Snapshot() SnapshotInfo {
	if len(itr.a) == 0 {
		return SnapshotInfo{}
	}
	return itr.a[0]
}

// WALSegmentIterator represents an iterator over a collection of WAL segments.
type WALSegmentIterator interface {
	io.Closer

	// Prepares the the next WAL for reading with the WAL() method.
	// Returns true if another WAL is available. Returns false if no more
	// WAL files are available or if an error occured.
	Next() bool

	// Returns an error that occurred during iteration.
	Err() error

	// Returns metadata for the currently positioned WAL segment file.
	WALSegment() WALSegmentInfo
}

// SliceWALSegmentIterator returns all WAL segment files from an iterator as a slice.
func SliceWALSegmentIterator(itr WALSegmentIterator) ([]WALSegmentInfo, error) {
	var a []WALSegmentInfo
	for itr.Next() {
		a = append(a, itr.WALSegment())
	}
	return a, itr.Close()
}

var _ WALSegmentIterator = (*WALSegmentInfoSliceIterator)(nil)

// WALSegmentInfoSliceIterator represents an iterator for iterating over a slice of wal segments.
type WALSegmentInfoSliceIterator struct {
	init bool
	a    []WALSegmentInfo
}

// NewWALSegmentInfoSliceIterator returns a new instance of WALSegmentInfoSliceIterator.
func NewWALSegmentInfoSliceIterator(a []WALSegmentInfo) *WALSegmentInfoSliceIterator {
	return &WALSegmentInfoSliceIterator{a: a}
}

// Close always returns nil.
func (itr *WALSegmentInfoSliceIterator) Close() error { return nil }

// Next moves to the next wal segment. Returns true if another segment is available.
func (itr *WALSegmentInfoSliceIterator) Next() bool {
	if !itr.init {
		itr.init = true
		return len(itr.a) > 0
	}
	itr.a = itr.a[1:]
	return len(itr.a) > 0
}

// Err always returns nil.
func (itr *WALSegmentInfoSliceIterator) Err() error { return nil }

// WALSegment returns the metadata from the currently positioned wal segment.
func (itr *WALSegmentInfoSliceIterator) WALSegment() WALSegmentInfo {
	if len(itr.a) == 0 {
		return WALSegmentInfo{}
	}
	return itr.a[0]
}

// SnapshotInfo represents file information about a snapshot.
type SnapshotInfo struct {
	Generation string
	Index      int
	Size       int64
	CreatedAt  time.Time
}

// Pos returns the WAL position when the snapshot was made.
func (info *SnapshotInfo) Pos() Pos {
	return Pos{Generation: info.Generation, Index: info.Index}
}

// SnapshotInfoSlice represents a slice of snapshot metadata.
type SnapshotInfoSlice []SnapshotInfo

func (a SnapshotInfoSlice) Len() int { return len(a) }

func (a SnapshotInfoSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a SnapshotInfoSlice) Less(i, j int) bool {
	if a[i].Generation != a[j].Generation {
		return a[i].Generation < a[j].Generation
	}
	return a[i].Index < a[j].Index
}

// FilterSnapshotsAfter returns all snapshots that were created on or after t.
func FilterSnapshotsAfter(a []SnapshotInfo, t time.Time) []SnapshotInfo {
	other := make([]SnapshotInfo, 0, len(a))
	for _, snapshot := range a {
		if !snapshot.CreatedAt.Before(t) {
			other = append(other, snapshot)
		}
	}
	return other
}

// FindMinSnapshotByGeneration finds the snapshot with the lowest index in a generation.
func FindMinSnapshotByGeneration(a []SnapshotInfo, generation string) *SnapshotInfo {
	var min *SnapshotInfo
	for i := range a {
		snapshot := &a[i]

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
	Generation string
	Index      int
	CreatedAt  time.Time
}

// WALInfoSlice represents a slice of WAL metadata.
type WALInfoSlice []WALInfo

func (a WALInfoSlice) Len() int { return len(a) }

func (a WALInfoSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a WALInfoSlice) Less(i, j int) bool {
	if a[i].Generation != a[j].Generation {
		return a[i].Generation < a[j].Generation
	}
	return a[i].Index < a[j].Index
}

// WALSegmentInfo represents file information about a WAL segment file.
type WALSegmentInfo struct {
	Generation string
	Index      int
	Offset     int64
	Size       int64
	CreatedAt  time.Time
}

// Pos returns the WAL position when the segment was made.
func (info *WALSegmentInfo) Pos() Pos {
	return Pos{Generation: info.Generation, Index: info.Index, Offset: info.Offset}
}

// WALSegmentInfoSlice represents a slice of WAL segment metadata.
type WALSegmentInfoSlice []WALSegmentInfo

func (a WALSegmentInfoSlice) Len() int { return len(a) }

func (a WALSegmentInfoSlice) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a WALSegmentInfoSlice) Less(i, j int) bool {
	if a[i].Generation != a[j].Generation {
		return a[i].Generation < a[j].Generation
	} else if a[i].Index != a[j].Index {
		return a[i].Index < a[j].Index
	}
	return a[i].Offset < a[j].Offset
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

// Truncate returns p with the offset truncated to zero.
func (p Pos) Truncate() Pos {
	return Pos{Generation: p.Generation, Index: p.Index}
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

// GenerationsPath returns the path to a generation root directory.
func GenerationsPath(root string) string {
	return path.Join(root, "generations")
}

// GenerationPath returns the path to a generation's root directory.
func GenerationPath(root, generation string) (string, error) {
	dir := GenerationsPath(root)
	if generation == "" {
		return "", fmt.Errorf("generation required")
	}
	return path.Join(dir, generation), nil
}

// SnapshotsPath returns the path to a generation's snapshot directory.
func SnapshotsPath(root, generation string) (string, error) {
	dir, err := GenerationPath(root, generation)
	if err != nil {
		return "", err
	}
	return path.Join(dir, "snapshots"), nil
}

// SnapshotPath returns the path to an uncompressed snapshot file.
func SnapshotPath(root, generation string, index int) (string, error) {
	dir, err := SnapshotsPath(root, generation)
	if err != nil {
		return "", err
	}
	return path.Join(dir, FormatSnapshotPath(index)), nil
}

// WALPath returns the path to a generation's WAL directory
func WALPath(root, generation string) (string, error) {
	dir, err := GenerationPath(root, generation)
	if err != nil {
		return "", err
	}
	return path.Join(dir, "wal"), nil
}

// WALSegmentPath returns the path to a WAL segment file.
func WALSegmentPath(root, generation string, index int, offset int64) (string, error) {
	dir, err := WALPath(root, generation)
	if err != nil {
		return "", err
	}
	return path.Join(dir, FormatWALSegmentPath(index, offset)), nil
}

// IsSnapshotPath returns true if s is a path to a snapshot file.
func IsSnapshotPath(s string) bool {
	return snapshotPathRegex.MatchString(s)
}

// ParseSnapshotPath returns the index for the snapshot.
// Returns an error if the path is not a valid snapshot path.
func ParseSnapshotPath(s string) (index int, err error) {
	s = filepath.Base(s)

	a := snapshotPathRegex.FindStringSubmatch(s)
	if a == nil {
		return 0, fmt.Errorf("invalid snapshot path: %s", s)
	}

	i64, _ := strconv.ParseUint(a[1], 16, 64)
	return int(i64), nil
}

// FormatSnapshotPath formats a snapshot filename with a given index.
func FormatSnapshotPath(index int) string {
	assert(index >= 0, "snapshot index must be non-negative")
	return fmt.Sprintf("%08x%s", index, SnapshotExt)
}

var snapshotPathRegex = regexp.MustCompile(`^([0-9a-f]{8})\.snapshot\.lz4$`)

// IsWALPath returns true if s is a path to a WAL file.
func IsWALPath(s string) bool {
	return walPathRegex.MatchString(s)
}

// ParseWALPath returns the index for the WAL file.
// Returns an error if the path is not a valid WAL path.
func ParseWALPath(s string) (index int, err error) {
	s = filepath.Base(s)

	a := walPathRegex.FindStringSubmatch(s)
	if a == nil {
		return 0, fmt.Errorf("invalid wal path: %s", s)
	}

	i64, _ := strconv.ParseUint(a[1], 16, 64)
	return int(i64), nil
}

// FormatWALPath formats a WAL filename with a given index.
func FormatWALPath(index int) string {
	assert(index >= 0, "wal index must be non-negative")
	return fmt.Sprintf("%08x%s", index, WALExt)
}

var walPathRegex = regexp.MustCompile(`^([0-9a-f]{8})\.wal$`)

// ParseWALSegmentPath returns the index & offset for the WAL segment file.
// Returns an error if the path is not a valid wal segment path.
func ParseWALSegmentPath(s string) (index int, offset int64, err error) {
	s = filepath.Base(s)

	a := walSegmentPathRegex.FindStringSubmatch(s)
	if a == nil {
		return 0, 0, fmt.Errorf("invalid wal segment path: %s", s)
	}

	i64, _ := strconv.ParseUint(a[1], 16, 64)
	off64, _ := strconv.ParseUint(a[2], 16, 64)
	return int(i64), int64(off64), nil
}

// FormatWALSegmentPath formats a WAL segment filename with a given index & offset.
func FormatWALSegmentPath(index int, offset int64) string {
	assert(index >= 0, "wal index must be non-negative")
	assert(offset >= 0, "wal offset must be non-negative")
	return fmt.Sprintf("%08x_%08x%s", index, offset, WALSegmentExt)
}

var walSegmentPathRegex = regexp.MustCompile(`^([0-9a-f]{8})(?:_([0-9a-f]{8}))\.wal\.lz4$`)

// isHexChar returns true if ch is a lowercase hex character.
func isHexChar(ch rune) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')
}

func assert(condition bool, message string) {
	if !condition {
		panic("assertion failed: " + message)
	}
}
