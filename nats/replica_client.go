package nats

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/valyala/bytebufferpool"
)

const (
	ReplicaClientType     = "nats"
	DefaultBucketName     = "litestream"
	DefaultBucketReplicas = 3
	DefaultBucketMaxBytes = 0               // unlimited
	DefaultBucketTTL      = 0 * time.Second // unlimited

	generationsPrefix = "generations/"
)

type ReplicaClient struct {
	nc          *nats.Conn
	objectStore jetstream.ObjectStore

	// NATS connection info
	BucketName     string
	BucketReplicas int
	BucketMaxBytes int64
	BucketTTL      time.Duration
}

func NewReplicaClient(nc *nats.Conn) *ReplicaClient {
	return &ReplicaClient{
		nc:             nc,
		BucketName:     DefaultBucketName,
		BucketReplicas: DefaultBucketReplicas,
		BucketMaxBytes: DefaultBucketMaxBytes,
		BucketTTL:      DefaultBucketTTL,
	}
}

// Type returns the type of client.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Returns a list of available generations. Order is undefined.
func (c *ReplicaClient) Generations(ctx context.Context) (_ []string, err error) {
	_, infos, err := c.objectStoreAndInfos(ctx)
	if err != nil {
		return nil, err
	}

	generations := make([]string, 0, len(infos))
	for _, info := range infos {
		offset := len(generationsPrefix)
		endGenIdx := strings.Index(info.Name[offset:], "/")
		generation := info.Name[offset : offset+endGenIdx]
		generations = append(generations, generation)
	}

	return generations, nil
}

// Deletes all snapshots & WAL segments within a generation.
func (c *ReplicaClient) DeleteGeneration(ctx context.Context, generation string) (err error) {
	objectStore, infos, err := c.objectStoreAndInfos(ctx)
	if err != nil {
		return err
	}

	for _, info := range infos {
		offset := len(generationsPrefix)
		infoGeneration := info.Name[offset : offset+len(generation)]
		if infoGeneration != generation {
			continue
		}

		if err = objectStore.Delete(ctx, info.Name); err != nil {
			return err
		}
	}

	return nil
}

// Returns an iterator of all snapshots within a generation on the replica. Order is undefined.
func (c *ReplicaClient) Snapshots(ctx context.Context, generation string) (_ litestream.SnapshotIterator, err error) {
	if generation == "" {
		return nil, litestream.ErrSnapshotPathNoGeneration
	}

	_, infos, err := c.objectStoreAndInfos(ctx)
	if err != nil {
		return nil, err
	}

	genInfos := onlyPrefixInfos(infos, generationsPrefix+generation)

	litestreamInfos := make([]litestream.SnapshotInfo, len(genInfos))
	for i, info := range genInfos {
		index, err := litestream.ParseSnapshotPath(info.Name)
		if err != nil {
			continue
		}

		litestreamInfos[i] = litestream.SnapshotInfo{
			Generation: generation,
			Index:      index,
			Size:       int64(info.Size),
			CreatedAt:  info.ModTime,
		}
	}

	return litestream.NewSnapshotInfoSliceIterator(litestreamInfos), nil
}

// Writes LZ4 compressed snapshot data to the replica at a given index
// within a generation. Returns metadata for the snapshot.
func (c *ReplicaClient) WriteSnapshot(ctx context.Context, generation string, index int, r io.Reader) (info litestream.SnapshotInfo, err error) {
	if generation == "" {
		return info, litestream.ErrSnapshotPathNoGeneration
	}

	objectStore, err := c.upsertObjectStore(ctx)
	if err != nil {
		return info, err
	}

	filename, err := litestream.SnapshotPath("", generation, index)
	if err != nil {
		return info, err
	}

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	if _, err := io.Copy(buf, r); err != nil {
		return info, err
	}

	b := buf.Bytes()
	natsInfo, err := objectStore.PutBytes(ctx, filename, b)
	if err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(len(b)))

	return litestream.SnapshotInfo{
		Generation: generation,
		Index:      index,
		Size:       int64(natsInfo.Size),
		CreatedAt:  natsInfo.ModTime,
	}, nil
}

// Deletes a snapshot with the given generation & index.
func (c *ReplicaClient) DeleteSnapshot(ctx context.Context, generation string, index int) (err error) {
	objectStore, infos, err := c.objectStoreAndInfos(ctx)
	if err != nil {
		return err
	}

	filename, err := litestream.SnapshotPath("", generation, index)
	if err != nil {
		return err
	}

	for _, info := range infos {
		if info.Name == filename {
			return objectStore.Delete(ctx, filename)
		}
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

	return nil
}

// Returns a reader that contains LZ4 compressed snapshot data for a
// given index within a generation. Returns an os.ErrNotFound error if
// the snapshot does not exist.
func (c *ReplicaClient) SnapshotReader(ctx context.Context, generation string, index int) (_ io.ReadCloser, err error) {
	objectStore, err := c.upsertObjectStore(ctx)
	if err != nil {
		return nil, err
	}

	filename, err := litestream.SnapshotPath("", generation, index)
	if err != nil {
		return nil, litestream.ErrSnapshotDoesNotExist
	}

	natsReader, err := objectStore.Get(ctx, filename)
	if err != nil {
		return nil, litestream.ErrSnapshotDoesNotExist
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()

	return natsReader, nil
}

// Returns an iterator of all WAL segments within a generation on the replica. Order is undefined.
func (c *ReplicaClient) WALSegments(ctx context.Context, generation string) (_ litestream.WALSegmentIterator, err error) {
	if generation == "" {
		return nil, litestream.ErrWALPathNoGeneration
	}

	_, infos, err := c.objectStoreAndInfos(ctx)
	if err != nil {
		return nil, err
	}

	dir, err := litestream.WALPath("", generation)
	if err != nil {
		return nil, err
	}

	genInfos := onlyPrefixInfos(infos, dir)

	litestreamInfos := make([]litestream.WALSegmentInfo, 0, len(genInfos))
	for _, info := range genInfos {
		index, offset, err := litestream.ParseWALSegmentPath(info.Name)
		if err != nil {
			continue
		}

		litestreamInfos = append(litestreamInfos, litestream.WALSegmentInfo{
			Generation: generation,
			Index:      index,
			Offset:     offset,
			Size:       int64(info.Size),
			CreatedAt:  info.ModTime.UTC(),
		})
	}

	return litestream.NewWALSegmentInfoSliceIterator(litestreamInfos), nil
}

// Writes an LZ4 compressed WAL segment at a given position.
// Returns metadata for the written segment.
func (c *ReplicaClient) WriteWALSegment(ctx context.Context, pos litestream.Pos, r io.Reader) (info litestream.WALSegmentInfo, err error) {
	if pos.Generation == "" {
		return info, litestream.ErrWALPathNoGeneration
	}

	objectStore, err := c.upsertObjectStore(ctx)
	if err != nil {
		return info, err
	}

	filename, err := litestream.WALSegmentPath("", pos.Generation, pos.Index, pos.Offset)
	if err != nil {
		return info, err
	}

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	if _, err := io.Copy(buf, r); err != nil {
		return info, err
	}

	b := buf.Bytes()
	natsInfo, err := objectStore.PutBytes(ctx, filename, b)
	if err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(len(b)))

	return litestream.WALSegmentInfo{
		Generation: pos.Generation,
		Index:      pos.Index,
		Offset:     pos.Offset,
		Size:       int64(natsInfo.Size),
		CreatedAt:  natsInfo.ModTime,
	}, nil
}

// Deletes one or more WAL segments at the given positions.
func (c *ReplicaClient) DeleteWALSegments(ctx context.Context, a []litestream.Pos) (err error) {
	objectStore, err := c.upsertObjectStore(ctx)
	if err != nil {
		return err
	}

	for _, pos := range a {
		filename, err := litestream.WALSegmentPath("", pos.Generation, pos.Index, pos.Offset)
		if err != nil {
			return litestream.ErrWALSegmentPathNoGeneration
		}

		if err = objectStore.Delete(ctx, filename); err != nil {
			return err
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

// Returns a reader that contains an LZ4 compressed WAL segment at a given
// index/offset within a generation. Returns an os.ErrNotFound error if the
// WAL segment does not exist.
func (c *ReplicaClient) WALSegmentReader(ctx context.Context, pos litestream.Pos) (_ io.ReadCloser, err error) {
	filename, err := litestream.WALSegmentPath("", pos.Generation, pos.Index, pos.Offset)
	if err != nil {
		return nil, err
	}

	objectStore, err := c.upsertObjectStore(ctx)
	if err != nil {
		return nil, err
	}

	natsReader, err := objectStore.Get(ctx, filename)
	if err != nil {
		if err == jetstream.ErrObjectNotFound {
			return nil, litestream.ErrSnapshotDoesNotExist
		}
		return nil, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()

	return natsReader, nil
}

func (c *ReplicaClient) upsertObjectStore(ctx context.Context) (_ jetstream.ObjectStore, err error) {
	if c.nc == nil {
		return nil, fmt.Errorf("nats connection is required")
	}

	if c.objectStore != nil {
		return c.objectStore, nil
	}

	if c.BucketName == "" {
		return nil, fmt.Errorf("bucket name is required")
	}

	js, err := jetstream.New(c.nc)
	if err != nil {
		return nil, err
	}
	objecStore, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:   c.BucketName,
		Replicas: c.BucketReplicas,
		MaxBytes: c.BucketMaxBytes,
		TTL:      c.BucketTTL,
	})
	if err != nil {
		return nil, err
	}

	c.objectStore = objecStore

	return objecStore, nil
}

func (c *ReplicaClient) Cleanup(ctx context.Context) (err error) {
	if c.nc == nil {
		c.nc.Close()
		c.nc = nil
	}

	return nil
}

func (c *ReplicaClient) objectStoreAndInfos(ctx context.Context) (_ jetstream.ObjectStore, _ []*jetstream.ObjectInfo, err error) {
	objectStore, err := c.upsertObjectStore(ctx)
	if err != nil {
		return nil, nil, err
	}

	infos, err := objectStore.List(ctx)
	if err != nil && err != jetstream.ErrNoObjectsFound {
		return nil, nil, err
	}

	return objectStore, infos, nil
}

func onlyPrefixInfos(infos []*jetstream.ObjectInfo, prefix string) []*jetstream.ObjectInfo {
	onlyGenerationInfos := make([]*jetstream.ObjectInfo, 0, len(infos))
	for _, info := range infos {
		if strings.HasPrefix(info.Name, prefix) {
			onlyGenerationInfos = append(onlyGenerationInfos, info)
		}
	}
	return onlyGenerationInfos
}
