package storj

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"storj.io/uplink"
)

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "storj"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing snapshots & WAL segments to disk.
type ReplicaClient struct {
	mu      sync.Mutex
	access  *uplink.Access  // storj access
	project *uplink.Project // storj project
	bkt     *uplink.Bucket  // storj bucket handle

	// AccessGrant information
	AccessGrant string

	// bucket information
	Bucket string
	Path   string
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{}
}

// Type returns "storj" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init initializes the connection to storj.io. No-op if already initialized.
func (c *ReplicaClient) Init(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.project != nil {
		return nil
	}

	if c.access, err = uplink.ParseAccess(c.AccessGrant); err != nil {
		return err
	}

	if c.project, err = uplink.OpenProject(ctx, c.access); err != nil {
		return err
	}

	if c.bkt, err = c.project.EnsureBucket(ctx, c.Bucket); err != nil {
		return err
	}

	return nil
}

// Generations returns a list of available generation names.
func (c *ReplicaClient) Generations(ctx context.Context) ([]string, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	// Loop over results and only build list of generation-formatted names.

	it := c.project.ListObjects(ctx, c.Bucket, &uplink.ListObjectsOptions{Prefix: litestream.GenerationsPath(c.Path) + "/", System: true})
	var generations []string
	for it.Next() {
		obj := it.Item()

		name := path.Base(strings.TrimSuffix(obj.Key, "/"))
		if !litestream.IsGenerationName(name) {
			continue
		}
		generations = append(generations, name)
	}

	return generations, nil
}

// DeleteGeneration deletes all snapshots & WAL segments within a generation.
func (c *ReplicaClient) DeleteGeneration(ctx context.Context, generation string) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	dir, err := litestream.GenerationPath(c.Path, generation)
	if err != nil {
		return fmt.Errorf("cannot determine generation path: %w", err)
	}

	// Iterate over every object in generation and delete it.
	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()
	it := c.project.ListObjects(ctx, c.bkt.Name, &uplink.ListObjectsOptions{Prefix: dir + "/", System: true})
	for it.Next() {
		obj := it.Item()

		if _, err := c.project.DeleteObject(ctx, c.bkt.Name, obj.Key); err != nil {
			return fmt.Errorf("cannot delete object %q: %w", obj.Key, err)
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

// Snapshots returns an iterator over all available snapshots for a generation.
func (c *ReplicaClient) Snapshots(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}
	dir, err := litestream.SnapshotsPath(c.Path, generation)
	if err != nil {
		return nil, fmt.Errorf("cannot determine snapshots path: %w", err)
	}
	it := c.project.ListObjects(ctx, c.bkt.Name, &uplink.ListObjectsOptions{Prefix: dir + "/", System: true})
	return newSnapshotIterator(generation, it), nil
}

// WriteSnapshot writes LZ4 compressed data from rd to the object storage.
func (c *ReplicaClient) WriteSnapshot(ctx context.Context, generation string, index int, rd io.Reader) (info litestream.SnapshotInfo, err error) {
	if err := c.Init(ctx); err != nil {
		return info, err
	}

	key, err := litestream.SnapshotPath(c.Path, generation, index)
	if err != nil {
		return info, fmt.Errorf("cannot determine snapshot path: %w", err)
	}
	startTime := time.Now()

	var uploadOptions *uplink.UploadOptions = nil
	var upload *uplink.Upload
	upload, err = c.project.UploadObject(ctx, c.bkt.Name, key, uploadOptions)
	if err != nil {
		return info, err
	}

	n, err := io.Copy(upload, rd)
	if err != nil {
		return info, err
	}

	if err := upload.Commit(); err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(n))

	return litestream.SnapshotInfo{
		Generation: generation,
		Index:      index,
		Size:       n,
		CreatedAt:  startTime.UTC(),
	}, nil
}

// SnapshotReader returns a reader for snapshot data at the given generation/index.
func (c *ReplicaClient) SnapshotReader(ctx context.Context, generation string, index int) (io.ReadCloser, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	key, err := litestream.SnapshotPath(c.Path, generation, index)
	if err != nil {
		return nil, fmt.Errorf("cannot determine snapshot path: %w", err)
	}

	var downloadOptions *uplink.DownloadOptions = nil
	download, err := c.project.DownloadObject(ctx, c.bkt.Name, key, downloadOptions)

	if err != nil {
		return nil, fmt.Errorf("cannot start new reader for %q: %w", key, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(download.Info().System.ContentLength))

	return download, nil
}

// DeleteSnapshot deletes a snapshot with the given generation & index.
func (c *ReplicaClient) DeleteSnapshot(ctx context.Context, generation string, index int) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	key, err := litestream.SnapshotPath(c.Path, generation, index)
	if err != nil {
		return fmt.Errorf("cannot determine snapshot path: %w", err)
	}

	if _, err := c.project.DeleteObject(ctx, c.bkt.Name, key); err != nil {
		return fmt.Errorf("cannot delete snapshot %q: %w", key, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	return nil
}

// WALSegments returns an iterator over all available WAL files for a generation.
func (c *ReplicaClient) WALSegments(ctx context.Context, generation string) (litestream.WALSegmentIterator, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}
	dir, err := litestream.WALPath(c.Path, generation)
	if err != nil {
		return nil, fmt.Errorf("cannot determine wal path: %w", err)
	}
	it := c.project.ListObjects(ctx, c.bkt.Name, &uplink.ListObjectsOptions{Prefix: dir + "/", System: true})
	return newWALSegmentIterator(generation, it), nil
}

// WriteWALSegment writes LZ4 compressed data from rd into a file on disk.
func (c *ReplicaClient) WriteWALSegment(ctx context.Context, pos litestream.Pos, rd io.Reader) (info litestream.WALSegmentInfo, err error) {
	if err := c.Init(ctx); err != nil {
		return info, err
	}

	key, err := litestream.WALSegmentPath(c.Path, pos.Generation, pos.Index, pos.Offset)
	if err != nil {
		return info, fmt.Errorf("cannot determine wal segment path: %w", err)
	}
	startTime := time.Now()

	var uploadOptions *uplink.UploadOptions = nil
	var upload *uplink.Upload
	upload, err = c.project.UploadObject(ctx, c.bkt.Name, key, uploadOptions)
	if err != nil {
		return info, err
	}

	n, err := io.Copy(upload, rd)
	if err != nil {
		return info, err
	}

	if err := upload.Commit(); err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(n))

	return litestream.WALSegmentInfo{
		Generation: pos.Generation,
		Index:      pos.Index,
		Offset:     pos.Offset,
		Size:       n,
		CreatedAt:  startTime.UTC(),
	}, nil
}

// WALSegmentReader returns a reader for a section of WAL data at the given index.
// Returns os.ErrNotExist if no matching index/offset is found.
func (c *ReplicaClient) WALSegmentReader(ctx context.Context, pos litestream.Pos) (io.ReadCloser, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	key, err := litestream.WALSegmentPath(c.Path, pos.Generation, pos.Index, pos.Offset)
	if err != nil {
		return nil, fmt.Errorf("cannot determine wal segment path: %w", err)
	}

	var downloadOptions *uplink.DownloadOptions = nil
	var download *uplink.Download
	download, err = c.project.DownloadObject(ctx, c.bkt.Name, key, downloadOptions)
	if err != nil {
		return nil, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(download.Info().System.ContentLength))

	return download, nil
}

// DeleteWALSegments deletes WAL segments with at the given positions.
func (c *ReplicaClient) DeleteWALSegments(ctx context.Context, a []litestream.Pos) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	for _, pos := range a {
		key, err := litestream.WALSegmentPath(c.Path, pos.Generation, pos.Index, pos.Offset)
		if err != nil {
			return fmt.Errorf("cannot determine wal segment path: %w", err)
		}

		if _, err := c.project.DeleteObject(ctx, c.bkt.Name, key); err != nil {
			return fmt.Errorf("cannot delete wal segment %q: %w", key, err)
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

type snapshotIterator struct {
	generation string

	it   *uplink.ObjectIterator
	info litestream.SnapshotInfo
}

func newSnapshotIterator(generation string, it *uplink.ObjectIterator) *snapshotIterator {
	return &snapshotIterator{
		generation: generation,
		it:         it,
	}
}

func (itr *snapshotIterator) Close() (err error) {
	// return itr.it.Err()
	return nil
}

func (itr *snapshotIterator) Next() bool {
	// Exit if an error has already occurred.
	if itr.it.Err() != nil {
		return false
	}

	if itr.it.Next() {
		obj := itr.it.Item()
		// Parse index, otherwise skip to the next object.
		index, err := litestream.ParseSnapshotPath(path.Base(obj.Key))
		if err == nil {
			// Store current snapshot and return.
			itr.info = litestream.SnapshotInfo{
				Generation: itr.generation,
				Index:      index,
				Size:       obj.System.ContentLength,
				CreatedAt:  obj.System.Created.UTC(),
			}
			return true
		}
	}
	return false
}

func (itr *snapshotIterator) Err() error { return itr.it.Err() }

func (itr *snapshotIterator) Snapshot() litestream.SnapshotInfo { return itr.info }

type walSegmentIterator struct {
	generation string

	it   *uplink.ObjectIterator
	info litestream.WALSegmentInfo
}

func newWALSegmentIterator(generation string, it *uplink.ObjectIterator) *walSegmentIterator {
	return &walSegmentIterator{
		generation: generation,
		it:         it,
	}
}

func (itr *walSegmentIterator) Close() (err error) {
	// return itr.it.Err()
	return nil
}

func (itr *walSegmentIterator) Next() bool {
	// Exit if an error has already occurred.
	if itr.it.Err() != nil {
		return false
	}

	for itr.it.Next() {
		// Fetch next object.
		obj := itr.it.Item()
		if itr.it.Err() != nil {
			return false
		}

		// Parse index & offset, otherwise skip to the next object.
		index, offset, err := litestream.ParseWALSegmentPath(path.Base(obj.Key))
		if err != nil {
			continue
		}

		// Store current snapshot and return.
		itr.info = litestream.WALSegmentInfo{
			Generation: itr.generation,
			Index:      index,
			Offset:     offset,
			Size:       obj.System.ContentLength,
			CreatedAt:  obj.System.Created.UTC(),
		}
		return true
	}
	return false
}

func (itr *walSegmentIterator) Err() error { return itr.it.Err() }

func (itr *walSegmentIterator) WALSegment() litestream.WALSegmentInfo {
	return itr.info
}
