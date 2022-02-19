package litestream

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/benbjohnson/litestream/internal"
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
	ErrDBClosed          = errors.New("database closed")
	ErrNoGeneration      = errors.New("no generation available")
	ErrGenerationChanged = errors.New("generation changed")
	ErrNoSnapshots       = errors.New("no snapshots available")
	ErrNoWALSegments     = errors.New("no wal segments available")
	ErrChecksumMismatch  = errors.New("invalid replica, checksum mismatch")
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
	return fmt.Sprintf("%s/%s:%s", p.Generation, FormatIndex(p.Index), FormatOffset(p.Offset))
}

// IsZero returns true if p is the zero value.
func (p Pos) IsZero() bool {
	return p == (Pos{})
}

// Truncate returns p with the offset truncated to zero.
func (p Pos) Truncate() Pos {
	return Pos{Generation: p.Generation, Index: p.Index}
}

// ComparePos returns -1 if a is less than b, 1 if a is greater than b, and
// returns 0 if a and b are equal. Only index & offset are compared.
// Returns an error if generations are not equal.
func ComparePos(a, b Pos) (int, error) {
	if a.Generation != b.Generation {
		return 0, fmt.Errorf("generation mismatch")
	}

	if a.Index < b.Index {
		return -1, nil
	} else if a.Index > b.Index {
		return 1, nil
	} else if a.Offset < b.Offset {
		return -1, nil
	} else if a.Offset > b.Offset {
		return 1, nil
	}
	return 0, nil
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

	// WALIndexHeaderSize is the size of the SHM index header, in bytes.
	WALIndexHeaderSize = 136
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

// FormatIndex formats an index as a hex value.
func FormatIndex(index int) string {
	return fmt.Sprintf("%016x", index)
}

// ParseIndex parses a hex-formatted index into an integer.
func ParseIndex(s string) (int, error) {
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return -1, fmt.Errorf("cannot parse index: %q", s)
	} else if v > uint64(internal.MaxInt) {
		return -1, fmt.Errorf("index too large: %q", s)
	}
	return int(v), nil
}

// FormatOffset formats an offset as a hex value.
func FormatOffset(offset int64) string {
	return fmt.Sprintf("%016x", offset)
}

// ParseOffset parses a hex-formatted offset into an integer.
func ParseOffset(s string) (int64, error) {
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return -1, fmt.Errorf("cannot parse offset: %q", s)
	} else if v > math.MaxInt64 {
		return -1, fmt.Errorf("offset too large: %q", s)
	}
	return int64(v), nil
}

const (
	StreamRecordTypeSnapshot   = 1
	StreamRecordTypeWALSegment = 2
)

const StreamRecordHeaderSize = 0 +
	4 + 4 + // type, flags
	8 + 8 + 8 + 8 // generation, index, offset, size

type StreamRecordHeader struct {
	Type       int
	Flags      int
	Generation string
	Index      int
	Offset     int64
	Size       int64
}

func (hdr *StreamRecordHeader) Pos() Pos {
	return Pos{
		Generation: hdr.Generation,
		Index:      hdr.Index,
		Offset:     hdr.Offset,
	}
}

func (hdr *StreamRecordHeader) MarshalBinary() ([]byte, error) {
	generation, err := strconv.ParseUint(hdr.Generation, 16, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid generation: %q", generation)
	}

	data := make([]byte, StreamRecordHeaderSize)
	binary.BigEndian.PutUint32(data[0:4], uint32(hdr.Type))
	binary.BigEndian.PutUint32(data[4:8], uint32(hdr.Flags))
	binary.BigEndian.PutUint64(data[8:16], generation)
	binary.BigEndian.PutUint64(data[16:24], uint64(hdr.Index))
	binary.BigEndian.PutUint64(data[24:32], uint64(hdr.Offset))
	binary.BigEndian.PutUint64(data[32:40], uint64(hdr.Size))
	return data, nil
}

// UnmarshalBinary from data into hdr.
func (hdr *StreamRecordHeader) UnmarshalBinary(data []byte) error {
	if len(data) < StreamRecordHeaderSize {
		return io.ErrUnexpectedEOF
	}
	hdr.Type = int(binary.BigEndian.Uint32(data[0:4]))
	hdr.Flags = int(binary.BigEndian.Uint32(data[4:8]))
	hdr.Generation = fmt.Sprintf("%16x", binary.BigEndian.Uint64(data[8:16]))
	hdr.Index = int(binary.BigEndian.Uint64(data[16:24]))
	hdr.Offset = int64(binary.BigEndian.Uint64(data[24:32]))
	hdr.Size = int64(binary.BigEndian.Uint64(data[32:40]))
	return nil
}

// StreamClient represents a client for streaming changes to a replica DB.
type StreamClient interface {
	Stream(ctx context.Context) (StreamReader, error)
}

// StreamReader represents a reader that streams snapshot and WAL records.
type StreamReader interface {
	io.ReadCloser
	Next() (*StreamRecordHeader, error)
}

// removeDBFiles deletes the database and related files (journal, shm, wal).
func removeDBFiles(filename string) error {
	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cannot delete database %q: %w", filename, err)
	} else if err := os.Remove(filename + "-journal"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cannot delete journal for %q: %w", filename, err)
	} else if err := os.Remove(filename + "-shm"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cannot delete shared memory for %q: %w", filename, err)
	} else if err := os.Remove(filename + "-wal"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cannot delete wal for %q: %w", filename, err)
	}
	return nil
}

// isHexChar returns true if ch is a lowercase hex character.
func isHexChar(ch rune) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')
}

// Tracef is used for low-level tracing.
var Tracef = func(format string, a ...interface{}) {}

func assert(condition bool, message string) {
	if !condition {
		panic("assertion failed: " + message)
	}
}
