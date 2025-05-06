package file

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"github.com/superfly/ltx"
)

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "file"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing LTX files to disk.
type ReplicaClient struct {
	path string // destination path

	Replica *litestream.Replica
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient(path string) *ReplicaClient {
	return &ReplicaClient{
		path: path,
	}
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

// LTXFiles returns an iterator over all available LTX files.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int) (ltx.FileIterator, error) {
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
	infos := make([]*ltx.FileInfo, 0, len(fis))
	for _, fi := range fis {
		minTXID, maxTXID, err := ltx.ParseFilename(fi.Name())
		if err != nil {
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
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (io.ReadCloser, error) {
	return os.Open(c.LTXFilePath(level, minTXID, maxTXID))
}

// WriteLTXFile writes an LTX file to disk.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, rd io.Reader) (info *ltx.FileInfo, err error) {

	var fileInfo, dirInfo os.FileInfo
	if db := c.db(); db != nil {
		fileInfo, dirInfo = db.FileInfo(), db.DirInfo()
	}

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

	if _, err := io.Copy(f, rd); err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}

	// Build metadata.
	fi, err := os.Stat(filename + ".tmp")
	if err != nil {
		return nil, err
	}
	info = &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      fi.Size(),
		CreatedAt: fi.ModTime().UTC(),
	}

	// Move LTX file to final path when it has been written & synced to disk.
	if err := os.Rename(filename+".tmp", filename); err != nil {
		return nil, err
	}

	return info, nil
}

// DeleteLTXFiles deletes LTX files.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	for _, info := range a {
		filename := c.LTXFilePath(info.Level, info.MinTXID, info.MaxTXID)

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
