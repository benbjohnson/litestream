package litestream

import (
	"fmt"
	"path"
	"regexp"
	"strconv"
	"time"
)

// PosV3 represents a position in a v0.3.x backup.
type PosV3 struct {
	Generation string // 16-char hex string
	Index      int    // WAL index
	Offset     int64  // Offset within WAL segment
}

// IsZero returns true if the position is the zero value.
func (p PosV3) IsZero() bool {
	return p == (PosV3{})
}

// String returns a string representation of the position.
func (p PosV3) String() string {
	if p.IsZero() {
		return ""
	}
	return fmt.Sprintf("%s/%08x:%016x", p.Generation, p.Index, p.Offset)
}

// SnapshotInfoV3 contains metadata about a v0.3.x snapshot.
type SnapshotInfoV3 struct {
	Generation string
	Index      int
	Size       int64
	CreatedAt  time.Time
}

// Pos returns the position of this snapshot.
func (info SnapshotInfoV3) Pos() PosV3 {
	return PosV3{Generation: info.Generation, Index: info.Index, Offset: 0}
}

// WALSegmentInfoV3 contains metadata about a v0.3.x WAL segment.
type WALSegmentInfoV3 struct {
	Generation string
	Index      int
	Offset     int64
	Size       int64
	CreatedAt  time.Time
}

// Pos returns the position of this WAL segment.
func (info WALSegmentInfoV3) Pos() PosV3 {
	return PosV3{Generation: info.Generation, Index: info.Index, Offset: info.Offset}
}

// v0.3.x path constants.
const (
	GenerationsDirV3 = "generations"
	SnapshotsDirV3   = "snapshots"
	WALDirV3         = "wal"
)

// GenerationsPathV3 returns the path to the generations directory.
func GenerationsPathV3(root string) string {
	return path.Join(root, GenerationsDirV3)
}

// GenerationPathV3 returns the path to a specific generation.
func GenerationPathV3(root, generation string) string {
	return path.Join(root, GenerationsDirV3, generation)
}

// SnapshotsPathV3 returns the path to snapshots within a generation.
func SnapshotsPathV3(root, generation string) string {
	return path.Join(root, GenerationsDirV3, generation, SnapshotsDirV3)
}

// WALPathV3 returns the path to WAL segments within a generation.
func WALPathV3(root, generation string) string {
	return path.Join(root, GenerationsDirV3, generation, WALDirV3)
}

// SnapshotPathV3 returns the full path to a v0.3.x snapshot file.
func SnapshotPathV3(root, generation string, index int) string {
	return path.Join(SnapshotsPathV3(root, generation), FormatSnapshotFilenameV3(index))
}

// WALSegmentPathV3 returns the full path to a v0.3.x WAL segment file.
func WALSegmentPathV3(root, generation string, index int, offset int64) string {
	return path.Join(WALPathV3(root, generation), FormatWALSegmentFilenameV3(index, offset))
}

// FormatSnapshotFilenameV3 returns the filename for a v0.3.x snapshot.
// Format: {index:08x}.snapshot.lz4
func FormatSnapshotFilenameV3(index int) string {
	return fmt.Sprintf("%08x.snapshot.lz4", index)
}

// FormatWALSegmentFilenameV3 returns the filename for a v0.3.x WAL segment.
// Format: {index:08x}-{offset:016x}.wal.lz4
func FormatWALSegmentFilenameV3(index int, offset int64) string {
	return fmt.Sprintf("%08x-%016x.wal.lz4", index, offset)
}

var (
	snapshotRegexV3   = regexp.MustCompile(`^([0-9a-f]{8})\.snapshot\.lz4$`)
	walSegmentRegexV3 = regexp.MustCompile(`^([0-9a-f]{8})-([0-9a-f]{16})\.wal\.lz4$`)
	generationRegexV3 = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

// ParseSnapshotFilenameV3 parses a v0.3.x snapshot filename and returns the index.
// Returns an error if the filename does not match the expected format.
func ParseSnapshotFilenameV3(filename string) (index int, err error) {
	m := snapshotRegexV3.FindStringSubmatch(filename)
	if m == nil {
		return 0, fmt.Errorf("invalid v0.3.x snapshot filename: %q", filename)
	}
	idx, _ := strconv.ParseInt(m[1], 16, 64)
	return int(idx), nil
}

// ParseWALSegmentFilenameV3 parses a v0.3.x WAL segment filename.
// Returns the WAL index and byte offset, or an error if the filename is invalid.
func ParseWALSegmentFilenameV3(filename string) (index int, offset int64, err error) {
	m := walSegmentRegexV3.FindStringSubmatch(filename)
	if m == nil {
		return 0, 0, fmt.Errorf("invalid v0.3.x WAL segment filename: %q", filename)
	}
	idx, _ := strconv.ParseInt(m[1], 16, 64)
	off, _ := strconv.ParseInt(m[2], 16, 64)
	return int(idx), off, nil
}

// IsGenerationIDV3 returns true if s is a valid v0.3.x generation ID (16 hex chars).
func IsGenerationIDV3(s string) bool {
	return generationRegexV3.MatchString(s)
}
