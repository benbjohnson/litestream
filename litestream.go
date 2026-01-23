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
	"strconv"
	"strings"
	"time"

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
	ErrLTXCorrupted     = errors.New("ltx file corrupted")
	ErrLTXMissing       = errors.New("ltx file missing")
)

// LTXError provides detailed context for LTX file errors with recovery hints.
type LTXError struct {
	Op      string // Operation that failed (e.g., "open", "read", "validate")
	Path    string // File path
	Level   int    // LTX level (0 = L0, etc.)
	MinTXID uint64 // Minimum transaction ID
	MaxTXID uint64 // Maximum transaction ID
	Err     error  // Underlying error
	Hint    string // Recovery hint for users
}

func (e *LTXError) Error() string {
	if e.Path != "" {
		return e.Op + " ltx file " + e.Path + ": " + e.Err.Error()
	}
	return e.Op + " ltx file: " + e.Err.Error()
}

func (e *LTXError) Unwrap() error { return e.Err }

// NewLTXError creates a new LTX error with appropriate hints based on the error type.
func NewLTXError(op, path string, level int, minTXID, maxTXID uint64, err error) *LTXError {
	ltxErr := &LTXError{
		Op:      op,
		Path:    path,
		Level:   level,
		MinTXID: minTXID,
		MaxTXID: maxTXID,
		Err:     err,
	}

	// Set appropriate hint based on error type
	if os.IsNotExist(err) || errors.Is(err, ErrLTXMissing) {
		ltxErr.Hint = "LTX file is missing. This can happen after VACUUM, manual checkpoint, or state corruption. " +
			"Run 'litestream reset <db>' or delete the .sqlite-litestream directory and restart."
	} else if errors.Is(err, ErrLTXCorrupted) || errors.Is(err, ErrChecksumMismatch) {
		ltxErr.Hint = "LTX file is corrupted. Delete the .sqlite-litestream directory and restart to recover from replica."
	}

	return ltxErr
}

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

// Legacy v0.3.x format types and helpers.
// These are used for backward compatibility when restoring from v0.3.x backups.

// LegacyPos represents a position in a v0.3.x replica.
// In v0.3.x, positions were tracked by generation, index, and offset within the WAL.
type LegacyPos struct {
	Generation string // 16-character hex string
	Index      int    // WAL index
	Offset     int64  // Offset within the WAL segment
}

// IsZero returns true if the position is empty.
func (p LegacyPos) IsZero() bool {
	return p.Generation == "" && p.Index == 0 && p.Offset == 0
}

// LegacySnapshotInfo represents metadata for a v0.3.x snapshot file.
type LegacySnapshotInfo struct {
	Generation string    // Generation ID (16-char hex)
	Index      int       // Snapshot index
	Size       int64     // File size in bytes
	CreatedAt  time.Time // File creation time
}

// LegacyWALSegmentInfo represents metadata for a v0.3.x WAL segment file.
type LegacyWALSegmentInfo struct {
	Generation string    // Generation ID (16-char hex)
	Index      int       // WAL index
	Offset     int64     // Offset within this segment
	Size       int64     // File size in bytes
	CreatedAt  time.Time // File creation time
}

// Legacy path constants
const (
	LegacyGenerationsDir = "generations"
	LegacySnapshotsDir   = "snapshots"
	LegacyWALDir         = "wal"
)

// LegacyGenerationPath returns the path to a generation directory.
func LegacyGenerationPath(root, generation string) string {
	return path.Join(root, LegacyGenerationsDir, generation)
}

// LegacySnapshotDir returns the path to a generation's snapshot directory.
func LegacySnapshotDir(root, generation string) string {
	return path.Join(LegacyGenerationPath(root, generation), LegacySnapshotsDir)
}

// LegacyWALDir returns the path to a generation's WAL directory.
func LegacyWALDirPath(root, generation string) string {
	return path.Join(LegacyGenerationPath(root, generation), LegacyWALDir)
}

// LegacySnapshotPath returns the path to a specific snapshot file.
func LegacySnapshotPath(root, generation string, index int) string {
	return path.Join(LegacySnapshotDir(root, generation), FormatLegacySnapshotFilename(index))
}

// LegacyWALSegmentPath returns the path to a specific WAL segment file.
func LegacyWALSegmentPath(root, generation string, index int, offset int64) string {
	return path.Join(LegacyWALDirPath(root, generation), FormatLegacyWALSegmentFilename(index, offset))
}

// FormatLegacySnapshotFilename returns the filename for a legacy snapshot.
// Format: <index>.snapshot.lz4
func FormatLegacySnapshotFilename(index int) string {
	return strconv.Itoa(index) + ".snapshot.lz4"
}

// FormatLegacyWALSegmentFilename returns the filename for a legacy WAL segment.
// Format: <index>-<offset>.wal.lz4
func FormatLegacyWALSegmentFilename(index int, offset int64) string {
	return strconv.Itoa(index) + "-" + strconv.FormatInt(offset, 10) + ".wal.lz4"
}

// ParseLegacySnapshotFilename parses a legacy snapshot filename.
// Returns the index or an error if the filename is invalid.
func ParseLegacySnapshotFilename(name string) (index int, err error) {
	if !strings.HasSuffix(name, ".snapshot.lz4") {
		return 0, errors.New("invalid legacy snapshot filename")
	}
	name = strings.TrimSuffix(name, ".snapshot.lz4")
	return strconv.Atoi(name)
}

// ParseLegacyWALSegmentFilename parses a legacy WAL segment filename.
// Returns the index and offset or an error if the filename is invalid.
func ParseLegacyWALSegmentFilename(name string) (index int, offset int64, err error) {
	if !strings.HasSuffix(name, ".wal.lz4") {
		return 0, 0, errors.New("invalid legacy WAL segment filename")
	}
	name = strings.TrimSuffix(name, ".wal.lz4")

	parts := strings.Split(name, "-")
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid legacy WAL segment filename format")
	}

	index, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid index: %w", err)
	}

	offset, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid offset: %w", err)
	}

	return index, offset, nil
}

// IsValidGenerationID returns true if s is a valid 16-character hex generation ID.
func IsValidGenerationID(s string) bool {
	if len(s) != 16 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
