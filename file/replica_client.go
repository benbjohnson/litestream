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
	f, err := os.Open(c.LTXFilePath(level, minTXID, maxTXID))
	if err != nil {
		return nil, err
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
	f, err := internal.CreateFile(filename+".tmp", fileInfo)
	if err != nil {
		return nil, err
	}
	defer f.Close()

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
	if err := os.Rename(filename+".tmp", filename); err != nil {
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
