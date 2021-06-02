package gcs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"google.golang.org/api/iterator"
)

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "gcs"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing snapshots & WAL segments to disk.
type ReplicaClient struct {
	mu     sync.Mutex
	client *storage.Client       // gcs client
	bkt    *storage.BucketHandle // gcs bucket handle

	// GCS bucket information
	Bucket string
	Path   string
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{}
}

// Type returns "gcs" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init initializes the connection to GCS. No-op if already initialized.
func (c *ReplicaClient) Init(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		return nil
	}

	if c.client, err = storage.NewClient(ctx); err != nil {
		return err
	}
	c.bkt = c.client.Bucket(c.Bucket)

	return nil
}

// Generations returns a list of available generation names.
func (c *ReplicaClient) Generations(ctx context.Context) ([]string, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	// Construct query to only pull generation directory names.
	query := &storage.Query{
		Delimiter: "/",
		Prefix:    litestream.GenerationsPath(c.Path) + "/",
	}

	// Loop over results and only build list of generation-formatted names.
	it := c.bkt.Objects(ctx, query)
	var generations []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}

		name := path.Base(strings.TrimSuffix(attrs.Prefix, "/"))
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
	for it := c.bkt.Objects(ctx, &storage.Query{Prefix: dir + "/"}); ; {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		if err := c.bkt.Object(attrs.Name).Delete(ctx); isNotExists(err) {
			continue
		} else if err != nil {
			return fmt.Errorf("cannot delete object %q: %w", attrs.Name, err)
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	// log.Printf("%s(%s): retainer: deleting generation: %s", r.db.Path(), r.Name(), generation)

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
	return newSnapshotIterator(generation, c.bkt.Objects(ctx, &storage.Query{Prefix: dir + "/"})), nil
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

	w := c.bkt.Object(key).NewWriter(ctx)
	defer w.Close()

	n, err := io.Copy(w, rd)
	if err != nil {
		return info, err
	} else if err := w.Close(); err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(n))

	// log.Printf("%s(%s): snapshot: creating %s/%08x t=%s", r.db.Path(), r.Name(), generation, index, time.Since(startTime).Truncate(time.Millisecond))

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

	r, err := c.bkt.Object(key).NewReader(ctx)
	if isNotExists(err) {
		return nil, os.ErrNotExist
	} else if err != nil {
		return nil, fmt.Errorf("cannot start new reader for %q: %w", key, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(r.Attrs.Size))

	return r, nil
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

	if err := c.bkt.Object(key).Delete(ctx); err != nil && !isNotExists(err) {
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
	return newWALSegmentIterator(generation, c.bkt.Objects(ctx, &storage.Query{Prefix: dir + "/"})), nil
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

	w := c.bkt.Object(key).NewWriter(ctx)
	defer w.Close()

	n, err := io.Copy(w, rd)
	if err != nil {
		return info, err
	} else if err := w.Close(); err != nil {
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

	r, err := c.bkt.Object(key).NewReader(ctx)
	if isNotExists(err) {
		return nil, os.ErrNotExist
	} else if err != nil {
		return nil, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(r.Attrs.Size))

	return r, nil
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

		if err := c.bkt.Object(key).Delete(ctx); err != nil && !isNotExists(err) {
			return fmt.Errorf("cannot delete wal segment %q: %w", key, err)
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

type snapshotIterator struct {
	generation string

	it   *storage.ObjectIterator
	info litestream.SnapshotInfo
	err  error
}

func newSnapshotIterator(generation string, it *storage.ObjectIterator) *snapshotIterator {
	return &snapshotIterator{
		generation: generation,
		it:         it,
	}
}

func (itr *snapshotIterator) Close() (err error) {
	return itr.err
}

func (itr *snapshotIterator) Next() bool {
	// Exit if an error has already occurred.
	if itr.err != nil {
		return false
	}

	for {
		// Fetch next object.
		attrs, err := itr.it.Next()
		if err == iterator.Done {
			return false
		} else if err != nil {
			itr.err = err
			return false
		}

		// Parse index, otherwise skip to the next object.
		index, err := litestream.ParseSnapshotPath(path.Base(attrs.Name))
		if err != nil {
			continue
		}

		// Store current snapshot and return.
		itr.info = litestream.SnapshotInfo{
			Generation: itr.generation,
			Index:      index,
			Size:       attrs.Size,
			CreatedAt:  attrs.Created.UTC(),
		}
		return true
	}
}

func (itr *snapshotIterator) Err() error { return itr.err }

func (itr *snapshotIterator) Snapshot() litestream.SnapshotInfo { return itr.info }

type walSegmentIterator struct {
	generation string

	it   *storage.ObjectIterator
	info litestream.WALSegmentInfo
	err  error
}

func newWALSegmentIterator(generation string, it *storage.ObjectIterator) *walSegmentIterator {
	return &walSegmentIterator{
		generation: generation,
		it:         it,
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
		// Fetch next object.
		attrs, err := itr.it.Next()
		if err == iterator.Done {
			return false
		} else if err != nil {
			itr.err = err
			return false
		}

		// Parse index & offset, otherwise skip to the next object.
		index, offset, err := litestream.ParseWALSegmentPath(path.Base(attrs.Name))
		if err != nil {
			continue
		}

		// Store current snapshot and return.
		itr.info = litestream.WALSegmentInfo{
			Generation: itr.generation,
			Index:      index,
			Offset:     offset,
			Size:       attrs.Size,
			CreatedAt:  attrs.Created.UTC(),
		}
		return true
	}
}

func (itr *walSegmentIterator) Err() error { return itr.err }

func (itr *walSegmentIterator) WALSegment() litestream.WALSegmentInfo {
	return itr.info
}

func isNotExists(err error) bool {
	return err == storage.ErrObjectNotExist
}
