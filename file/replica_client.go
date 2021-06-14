package file

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "file"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing snapshots & WAL segments to disk.
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

// GenerationsDir returns the path to a generation root directory.
func (c *ReplicaClient) GenerationsDir() (string, error) {
	if c.path == "" {
		return "", fmt.Errorf("file replica path required")
	}
	return filepath.Join(c.path, "generations"), nil
}

// GenerationDir returns the path to a generation's root directory.
func (c *ReplicaClient) GenerationDir(generation string) (string, error) {
	dir, err := c.GenerationsDir()
	if err != nil {
		return "", err
	} else if generation == "" {
		return "", fmt.Errorf("generation required")
	}
	return filepath.Join(dir, generation), nil
}

// SnapshotsDir returns the path to a generation's snapshot directory.
func (c *ReplicaClient) SnapshotsDir(generation string) (string, error) {
	dir, err := c.GenerationDir(generation)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "snapshots"), nil
}

// SnapshotPath returns the path to an uncompressed snapshot file.
func (c *ReplicaClient) SnapshotPath(generation string, index int) (string, error) {
	dir, err := c.SnapshotsDir(generation)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, litestream.FormatIndex(index)+".snapshot.lz4"), nil
}

// WALDir returns the path to a generation's WAL directory
func (c *ReplicaClient) WALDir(generation string) (string, error) {
	dir, err := c.GenerationDir(generation)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "wal"), nil
}

// WALSegmentPath returns the path to a WAL segment file.
func (c *ReplicaClient) WALSegmentPath(generation string, index int, offset int64) (string, error) {
	dir, err := c.WALDir(generation)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, litestream.FormatIndex(index), fmt.Sprintf("%08x.wal.lz4", offset)), nil
}

// Generations returns a list of available generation names.
func (c *ReplicaClient) Generations(ctx context.Context) ([]string, error) {
	root, err := c.GenerationsDir()
	if err != nil {
		return nil, fmt.Errorf("cannot determine generations path: %w", err)
	}

	fis, err := ioutil.ReadDir(root)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	var generations []string
	for _, fi := range fis {
		if !litestream.IsGenerationName(fi.Name()) {
			continue
		} else if !fi.IsDir() {
			continue
		}
		generations = append(generations, fi.Name())
	}
	return generations, nil
}

// DeleteGeneration deletes all snapshots & WAL segments within a generation.
func (c *ReplicaClient) DeleteGeneration(ctx context.Context, generation string) error {
	dir, err := c.GenerationDir(generation)
	if err != nil {
		return fmt.Errorf("cannot determine generation path: %w", err)
	}

	if err := os.RemoveAll(dir); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Snapshots returns an iterator over all available snapshots for a generation.
func (c *ReplicaClient) Snapshots(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
	dir, err := c.SnapshotsDir(generation)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(dir)
	if os.IsNotExist(err) {
		return litestream.NewSnapshotInfoSliceIterator(nil), nil
	} else if err != nil {
		return nil, err
	}
	defer f.Close()

	fis, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}

	// Iterate over every file and convert to metadata.
	infos := make([]litestream.SnapshotInfo, 0, len(fis))
	for _, fi := range fis {
		// Parse index from filename.
		index, err := internal.ParseSnapshotPath(filepath.Base(fi.Name()))
		if err != nil {
			continue
		}

		infos = append(infos, litestream.SnapshotInfo{
			Generation: generation,
			Index:      index,
			Size:       fi.Size(),
			CreatedAt:  fi.ModTime().UTC(),
		})
	}

	sort.Sort(litestream.SnapshotInfoSlice(infos))

	return litestream.NewSnapshotInfoSliceIterator(infos), nil
}

// WriteSnapshot writes LZ4 compressed data from rd into a file on disk.
func (c *ReplicaClient) WriteSnapshot(ctx context.Context, generation string, index int, rd io.Reader) (info litestream.SnapshotInfo, err error) {
	filename, err := c.SnapshotPath(generation, index)
	if err != nil {
		return info, err
	}

	var fileInfo, dirInfo os.FileInfo
	if db := c.db(); db != nil {
		fileInfo, dirInfo = db.FileInfo(), db.DirInfo()
	}

	// Ensure parent directory exists.
	if err := internal.MkdirAll(filepath.Dir(filename), dirInfo); err != nil {
		return info, err
	}

	// Write snapshot to temporary file next to destination path.
	f, err := internal.CreateFile(filename+".tmp", fileInfo)
	if err != nil {
		return info, err
	}
	defer f.Close()

	if _, err := io.Copy(f, rd); err != nil {
		return info, err
	} else if err := f.Sync(); err != nil {
		return info, err
	} else if err := f.Close(); err != nil {
		return info, err
	}

	// Build metadata.
	fi, err := os.Stat(filename + ".tmp")
	if err != nil {
		return info, err
	}
	info = litestream.SnapshotInfo{
		Generation: generation,
		Index:      index,
		Size:       fi.Size(),
		CreatedAt:  fi.ModTime().UTC(),
	}

	// Move snapshot to final path when it has been fully written & synced to disk.
	if err := os.Rename(filename+".tmp", filename); err != nil {
		return info, err
	}

	return info, nil
}

// SnapshotReader returns a reader for snapshot data at the given generation/index.
// Returns os.ErrNotExist if no matching index is found.
func (c *ReplicaClient) SnapshotReader(ctx context.Context, generation string, index int) (io.ReadCloser, error) {
	filename, err := c.SnapshotPath(generation, index)
	if err != nil {
		return nil, err
	}
	return os.Open(filename)
}

// DeleteSnapshot deletes a snapshot with the given generation & index.
func (c *ReplicaClient) DeleteSnapshot(ctx context.Context, generation string, index int) error {
	filename, err := c.SnapshotPath(generation, index)
	if err != nil {
		return fmt.Errorf("cannot determine snapshot path: %w", err)
	}
	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// WALSegments returns an iterator over all available WAL files for a generation.
func (c *ReplicaClient) WALSegments(ctx context.Context, generation string) (litestream.WALSegmentIterator, error) {
	dir, err := c.WALDir(generation)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(dir)
	if os.IsNotExist(err) {
		return litestream.NewWALSegmentInfoSliceIterator(nil), nil
	} else if err != nil {
		return nil, err
	}
	defer f.Close()

	fis, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}

	// Iterate over every file and convert to metadata.
	indexes := make([]int, 0, len(fis))
	for _, fi := range fis {
		index, err := litestream.ParseIndex(fi.Name())
		if err != nil || !fi.IsDir() {
			continue
		}
		indexes = append(indexes, index)
	}

	sort.Ints(indexes)

	return newWALSegmentIterator(dir, generation, indexes), nil
}

// WriteWALSegment writes LZ4 compressed data from rd into a file on disk.
func (c *ReplicaClient) WriteWALSegment(ctx context.Context, pos litestream.Pos, rd io.Reader) (info litestream.WALSegmentInfo, err error) {
	filename, err := c.WALSegmentPath(pos.Generation, pos.Index, pos.Offset)
	if err != nil {
		return info, err
	}

	var fileInfo, dirInfo os.FileInfo
	if db := c.db(); db != nil {
		fileInfo, dirInfo = db.FileInfo(), db.DirInfo()
	}

	// Ensure parent directory exists.
	if err := internal.MkdirAll(filepath.Dir(filename), dirInfo); err != nil {
		return info, err
	}

	// Write WAL segment to temporary file next to destination path.
	f, err := internal.CreateFile(filename+".tmp", fileInfo)
	if err != nil {
		return info, err
	}
	defer f.Close()

	if _, err := io.Copy(f, rd); err != nil {
		return info, err
	} else if err := f.Sync(); err != nil {
		return info, err
	} else if err := f.Close(); err != nil {
		return info, err
	}

	// Build metadata.
	fi, err := os.Stat(filename + ".tmp")
	if err != nil {
		return info, err
	}
	info = litestream.WALSegmentInfo{
		Generation: pos.Generation,
		Index:      pos.Index,
		Offset:     pos.Offset,
		Size:       fi.Size(),
		CreatedAt:  fi.ModTime().UTC(),
	}

	// Move WAL segment to final path when it has been written & synced to disk.
	if err := os.Rename(filename+".tmp", filename); err != nil {
		return info, err
	}

	return info, nil
}

// WALSegmentReader returns a reader for a section of WAL data at the given position.
// Returns os.ErrNotExist if no matching index/offset is found.
func (c *ReplicaClient) WALSegmentReader(ctx context.Context, pos litestream.Pos) (io.ReadCloser, error) {
	filename, err := c.WALSegmentPath(pos.Generation, pos.Index, pos.Offset)
	if err != nil {
		return nil, err
	}
	return os.Open(filename)
}

// DeleteWALSegments deletes WAL segments at the given positions.
func (c *ReplicaClient) DeleteWALSegments(ctx context.Context, a []litestream.Pos) error {
	for _, pos := range a {
		filename, err := c.WALSegmentPath(pos.Generation, pos.Index, pos.Offset)
		if err != nil {
			return err
		}
		if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

type walSegmentIterator struct {
	dir        string
	generation string
	indexes    []int

	infos []litestream.WALSegmentInfo
	err   error
}

func newWALSegmentIterator(dir, generation string, indexes []int) *walSegmentIterator {
	return &walSegmentIterator{
		dir:        dir,
		generation: generation,
		indexes:    indexes,
	}
}

func (itr *walSegmentIterator) Close() (err error) {
	return itr.err
}

func (itr *walSegmentIterator) Next() bool {
	// Exit if an error has already occurred.
	if itr.err != nil {
		return false
	}

	for {
		// Move to the next segment in cache, if available.
		if len(itr.infos) > 1 {
			itr.infos = itr.infos[1:]
			return true
		}
		itr.infos = itr.infos[:0] // otherwise clear infos

		// Move to the next index unless this is the first time initializing.
		if itr.infos != nil && len(itr.indexes) > 0 {
			itr.indexes = itr.indexes[1:]
		}

		// If no indexes remain, stop iteration.
		if len(itr.indexes) == 0 {
			return false
		}

		// Read segments into a cache for the current index.
		index := itr.indexes[0]
		f, err := os.Open(filepath.Join(itr.dir, litestream.FormatIndex(index)))
		if err != nil {
			itr.err = err
			return false
		}
		defer f.Close()

		fis, err := f.Readdir(-1)
		if err != nil {
			itr.err = err
			return false
		}
		for _, fi := range fis {
			filename := filepath.Base(fi.Name())
			if fi.IsDir() {
				continue
			}

			offset, err := litestream.ParseOffset(strings.TrimSuffix(filename, ".wal.lz4"))
			if err != nil {
				continue
			}

			itr.infos = append(itr.infos, litestream.WALSegmentInfo{
				Generation: itr.generation,
				Index:      index,
				Offset:     offset,
				Size:       fi.Size(),
				CreatedAt:  fi.ModTime().UTC(),
			})
		}

		if len(itr.infos) > 0 {
			return true
		}
	}
}

func (itr *walSegmentIterator) Err() error { return itr.err }

func (itr *walSegmentIterator) WALSegment() litestream.WALSegmentInfo {
	if len(itr.infos) == 0 {
		return litestream.WALSegmentInfo{}
	}
	return itr.infos[0]
}
