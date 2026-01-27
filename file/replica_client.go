package file

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

func init() {
	litestream.RegisterReplicaClientFactory("file", NewReplicaClientFromURL)
}

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "file"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)
var _ litestream.ReplicaClientV3 = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing LTX files to disk.
type ReplicaClient struct {
	path string // destination path

	Replica *litestream.Replica
	logger  *slog.Logger
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient(path string) *ReplicaClient {
	return &ReplicaClient{
		logger: slog.Default().WithGroup(ReplicaClientType),
		path:   path,
	}
}

// NewReplicaClientFromURL creates a new ReplicaClient from URL components.
// This is used by the replica client factory registration.
func NewReplicaClientFromURL(scheme, host, urlPath string, query url.Values, userinfo *url.Userinfo) (litestream.ReplicaClient, error) {
	// For file URLs, the path is the full path
	if urlPath == "" {
		return nil, fmt.Errorf("file replica path required")
	}
	return NewReplicaClient(urlPath), nil
}

// db returns the database, if available.
func (c *ReplicaClient) db() *litestream.DB {
	if c.Replica == nil {
		return nil
	}
	return c.Replica.DB()
}

// Type returns "file" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init is a no-op for file replica client as no initialization is required.
func (c *ReplicaClient) Init(ctx context.Context) error {
	return nil
}

// Path returns the destination path to replicate the database to.
func (c *ReplicaClient) Path() string {
	return c.path
}

// LTXLevelDir returns the path to a given level.
func (c *ReplicaClient) LTXLevelDir(level int) string {
	return filepath.FromSlash(litestream.LTXLevelDir(c.path, level))
}

// LTXFilePath returns the path to an LTX file.
func (c *ReplicaClient) LTXFilePath(level int, minTXID, maxTXID ltx.TXID) string {
	return filepath.FromSlash(litestream.LTXFilePath(c.path, level, minTXID, maxTXID))
}

// LTXFiles returns an iterator over all LTX files on the replica for the given level.
// The useMetadata parameter is ignored for file backend as ModTime is always available from readdir.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	f, err := os.Open(c.LTXLevelDir(level))
	if os.IsNotExist(err) {
		return ltx.NewFileInfoSliceIterator(nil), nil
	} else if err != nil {
		return nil, err
	}
	defer f.Close()

	fis, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}

	// Iterate over every file and convert to metadata.
	// ModTime contains the accurate timestamp set by Chtimes in WriteLTXFile.
	infos := make([]*ltx.FileInfo, 0, len(fis))
	for _, fi := range fis {
		minTXID, maxTXID, err := ltx.ParseFilename(fi.Name())
		if err != nil {
			continue
		} else if minTXID < seek {
			continue
		}

		infos = append(infos, &ltx.FileInfo{
			Level:     level,
			MinTXID:   minTXID,
			MaxTXID:   maxTXID,
			Size:      fi.Size(),
			CreatedAt: fi.ModTime().UTC(),
		})
	}

	return ltx.NewFileInfoSliceIterator(infos), nil
}

// OpenLTXFile returns a reader for an LTX file at the given position.
// Returns os.ErrNotExist if no matching index/offset is found.
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	path := c.LTXFilePath(level, minTXID, maxTXID)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, litestream.NewLTXError("open", path, level, uint64(minTXID), uint64(maxTXID), err)
		}
		return nil, fmt.Errorf("open ltx file %s: %w", path, err)
	}

	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}

	if size > 0 {
		return internal.LimitReadCloser(f, size), nil
	}

	return f, nil
}

// WriteLTXFile writes an LTX file to the replica.
// Extracts timestamp from LTX header and sets it as the file's ModTime to preserve original creation time.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, rd io.Reader) (info *ltx.FileInfo, err error) {
	var fileInfo, dirInfo os.FileInfo
	if db := c.db(); db != nil {
		fileInfo, dirInfo = db.FileInfo(), db.DirInfo()
	}

	// Use TeeReader to peek at LTX header while preserving data for upload
	var buf bytes.Buffer
	teeReader := io.TeeReader(rd, &buf)

	// Extract timestamp from LTX header
	hdr, _, err := ltx.PeekHeader(teeReader)
	if err != nil {
		return nil, fmt.Errorf("extract timestamp from LTX header: %w", err)
	}
	timestamp := time.UnixMilli(hdr.Timestamp).UTC()

	// Combine buffered data with rest of reader
	fullReader := io.MultiReader(&buf, rd)

	// Ensure parent directory exists.
	filename := c.LTXFilePath(level, minTXID, maxTXID)
	if err := internal.MkdirAll(filepath.Dir(filename), dirInfo); err != nil {
		return nil, err
	}

	// Write LTX file to temporary file next to destination path.
	tmpFilename := filename + ".tmp"
	f, err := internal.CreateFile(tmpFilename, fileInfo)
	if err != nil {
		return nil, err
	}

	// Clean up temp file on error. On successful rename, the temp file
	// becomes the final file and should not be removed.
	defer func() {
		_ = f.Close()
		if err != nil {
			_ = os.Remove(tmpFilename)
		}
	}()

	if _, err := io.Copy(f, fullReader); err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}

	// Build metadata.
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	info = &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      fi.Size(),
		CreatedAt: timestamp,
	}

	if err := f.Close(); err != nil {
		return nil, err
	}

	// Move LTX file to final path when it has been written & synced to disk.
	if err := os.Rename(tmpFilename, filename); err != nil {
		return nil, err
	}

	// Set file ModTime to preserve original timestamp
	if err := os.Chtimes(filename, timestamp, timestamp); err != nil {
		return nil, err
	}

	return info, nil
}

// DeleteLTXFiles deletes LTX files.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	for _, info := range a {
		filename := c.LTXFilePath(info.Level, info.MinTXID, info.MaxTXID)

		c.logger.Debug("deleting ltx file", "level", info.Level, "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "path", filename)

		if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// DeleteAll deletes all LTX files.
func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	if err := os.RemoveAll(c.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// GenerationsV3 returns a list of v0.3.x generation IDs in the replica.
func (c *ReplicaClient) GenerationsV3(ctx context.Context) ([]string, error) {
	genPath := filepath.Join(c.path, litestream.GenerationsDirV3)
	entries, err := os.ReadDir(genPath)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	var generations []string
	for _, entry := range entries {
		if entry.IsDir() && litestream.IsGenerationIDV3(entry.Name()) {
			generations = append(generations, entry.Name())
		}
	}
	slices.Sort(generations)
	return generations, nil
}

// SnapshotsV3 returns snapshots for a generation, sorted by index.
func (c *ReplicaClient) SnapshotsV3(ctx context.Context, generation string) ([]litestream.SnapshotInfoV3, error) {
	snapshotsPath := filepath.Join(c.path, litestream.GenerationsDirV3, generation, litestream.SnapshotsDirV3)
	entries, err := os.ReadDir(snapshotsPath)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	var snapshots []litestream.SnapshotInfoV3
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		index, err := litestream.ParseSnapshotFilenameV3(entry.Name())
		if err != nil {
			continue // skip invalid filenames
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, litestream.SnapshotInfoV3{
			Generation: generation,
			Index:      index,
			Size:       info.Size(),
			CreatedAt:  info.ModTime(),
		})
	}
	slices.SortFunc(snapshots, func(a, b litestream.SnapshotInfoV3) int {
		return a.Index - b.Index
	})
	return snapshots, nil
}

// WALSegmentsV3 returns WAL segments for a generation, sorted by index then offset.
func (c *ReplicaClient) WALSegmentsV3(ctx context.Context, generation string) ([]litestream.WALSegmentInfoV3, error) {
	walPath := filepath.Join(c.path, litestream.GenerationsDirV3, generation, litestream.WALDirV3)
	entries, err := os.ReadDir(walPath)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	var segments []litestream.WALSegmentInfoV3
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		index, offset, err := litestream.ParseWALSegmentFilenameV3(entry.Name())
		if err != nil {
			continue // skip invalid filenames
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		segments = append(segments, litestream.WALSegmentInfoV3{
			Generation: generation,
			Index:      index,
			Offset:     offset,
			Size:       info.Size(),
			CreatedAt:  info.ModTime(),
		})
	}
	slices.SortFunc(segments, func(a, b litestream.WALSegmentInfoV3) int {
		if a.Index != b.Index {
			return a.Index - b.Index
		}
		return int(a.Offset - b.Offset)
	})
	return segments, nil
}

// OpenSnapshotV3 opens a v0.3.x snapshot file for reading.
// The returned reader provides LZ4-decompressed data.
func (c *ReplicaClient) OpenSnapshotV3(ctx context.Context, generation string, index int) (io.ReadCloser, error) {
	path := filepath.Join(c.path, litestream.GenerationsDirV3, generation, litestream.SnapshotsDirV3, litestream.FormatSnapshotFilenameV3(index))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return internal.NewLZ4Reader(f), nil
}

// OpenWALSegmentV3 opens a v0.3.x WAL segment file for reading.
// The returned reader provides LZ4-decompressed data.
func (c *ReplicaClient) OpenWALSegmentV3(ctx context.Context, generation string, index int, offset int64) (io.ReadCloser, error) {
	path := filepath.Join(c.path, litestream.GenerationsDirV3, generation, litestream.WALDirV3, litestream.FormatWALSegmentFilenameV3(index, offset))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return internal.NewLZ4Reader(f), nil
}
