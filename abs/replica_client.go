package abs

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"golang.org/x/sync/errgroup"
)

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "abs"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing snapshots & WAL segments to disk.
type ReplicaClient struct {
	mu           sync.Mutex
	containerURL *azblob.ContainerURL

	// Azure credentials
	AccountName string
	AccountKey  string
	Endpoint    string

	// Azure Blob Storage container information
	Bucket string
	Path   string
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{}
}

// Type returns "abs" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init initializes the connection to Azure. No-op if already initialized.
func (c *ReplicaClient) Init(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.containerURL != nil {
		return nil
	}

	// Read account key from environment, if available.
	accountKey := c.AccountKey
	if accountKey == "" {
		accountKey = os.Getenv("LITESTREAM_AZURE_ACCOUNT_KEY")
	}

	// Authenticate to ACS.
	credential, err := azblob.NewSharedKeyCredential(c.AccountName, accountKey)
	if err != nil {
		return err
	}

	// Construct & parse endpoint unless already set.
	endpoint := c.Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", c.AccountName)
	}
	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("cannot parse azure endpoint: %w", err)
	}

	// Build pipeline and reference to container.
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: 24 * time.Hour,
		},
	})
	containerURL := azblob.NewServiceURL(*endpointURL, pipeline).NewContainerURL(c.Bucket)
	c.containerURL = &containerURL

	return nil
}

// Generations returns a list of available generation names.
func (c *ReplicaClient) Generations(ctx context.Context) ([]string, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	var generations []string
	var marker azblob.Marker
	for marker.NotDone() {
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

		resp, err := c.containerURL.ListBlobsHierarchySegment(ctx, marker, "/", azblob.ListBlobsSegmentOptions{
			Prefix: path.Join(c.Path, "generations") + "/",
		})
		if err != nil {
			return nil, err
		}
		marker = resp.NextMarker

		for _, prefix := range resp.Segment.BlobPrefixes {
			name := path.Base(strings.TrimSuffix(prefix.Name, "/"))
			if !litestream.IsGenerationName(name) {
				continue
			}
			generations = append(generations, name)
		}
	}

	return generations, nil
}

// DeleteGeneration deletes all snapshots & WAL segments within a generation.
func (c *ReplicaClient) DeleteGeneration(ctx context.Context, generation string) error {
	if err := c.Init(ctx); err != nil {
		return err
	} else if generation == "" {
		return fmt.Errorf("generation required")
	}

	prefix := path.Join(c.Path, "generations", generation) + "/"

	var marker azblob.Marker
	for marker.NotDone() {
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

		resp, err := c.containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: prefix})
		if err != nil {
			return err
		}
		marker = resp.NextMarker

		for _, item := range resp.Segment.BlobItems {
			internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

			blobURL := c.containerURL.NewBlobURL(item.Name)
			if _, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{}); isNotExists(err) {
				continue
			} else if err != nil {
				return err
			}
		}
	}

	// log.Printf("%s(%s): retainer: deleting generation: %s", r.db.Path(), r.Name(), generation)

	return nil
}

// Snapshots returns an iterator over all available snapshots for a generation.
func (c *ReplicaClient) Snapshots(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}
	return newSnapshotIterator(ctx, generation, c), nil
}

// WriteSnapshot writes LZ4 compressed data from rd to the object storage.
func (c *ReplicaClient) WriteSnapshot(ctx context.Context, generation string, index int, rd io.Reader) (info litestream.SnapshotInfo, err error) {
	if err := c.Init(ctx); err != nil {
		return info, err
	} else if generation == "" {
		return info, fmt.Errorf("generation required")
	}

	key := path.Join(c.Path, "generations", generation, "snapshots", litestream.FormatIndex(index)+".snapshot.lz4")
	startTime := time.Now()

	rc := internal.NewReadCounter(rd)

	blobURL := c.containerURL.NewBlockBlobURL(key)
	if _, err := azblob.UploadStreamToBlockBlob(ctx, rc, blobURL, azblob.UploadStreamToBlockBlobOptions{
		BlobHTTPHeaders: azblob.BlobHTTPHeaders{ContentType: "application/octet-stream"},
		BlobAccessTier:  azblob.DefaultAccessTier,
	}); err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(rc.N()))

	return litestream.SnapshotInfo{
		Generation: generation,
		Index:      index,
		Size:       rc.N(),
		CreatedAt:  startTime.UTC(),
	}, nil
}

// SnapshotReader returns a reader for snapshot data at the given generation/index.
func (c *ReplicaClient) SnapshotReader(ctx context.Context, generation string, index int) (io.ReadCloser, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	} else if generation == "" {
		return nil, fmt.Errorf("generation required")
	}

	key := path.Join(c.Path, "generations", generation, "snapshots", litestream.FormatIndex(index)+".snapshot.lz4")

	blobURL := c.containerURL.NewBlobURL(key)
	resp, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if isNotExists(err) {
		return nil, os.ErrNotExist
	} else if err != nil {
		return nil, fmt.Errorf("cannot start new reader for %q: %w", key, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(resp.ContentLength()))

	return resp.Body(azblob.RetryReaderOptions{}), nil
}

// DeleteSnapshot deletes a snapshot with the given generation & index.
func (c *ReplicaClient) DeleteSnapshot(ctx context.Context, generation string, index int) error {
	if err := c.Init(ctx); err != nil {
		return err
	} else if generation == "" {
		return fmt.Errorf("generation required")
	}

	key := path.Join(c.Path, "generations", generation, "snapshots", litestream.FormatIndex(index)+".snapshot.lz4")

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

	blobURL := c.containerURL.NewBlobURL(key)
	if _, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{}); isNotExists(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("cannot delete snapshot %q: %w", key, err)
	}
	return nil
}

// WALSegments returns an iterator over all available WAL files for a generation.
func (c *ReplicaClient) WALSegments(ctx context.Context, generation string) (litestream.WALSegmentIterator, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}
	return newWALSegmentIterator(ctx, generation, c), nil
}

// WriteWALSegment writes LZ4 compressed data from rd into a file on disk.
func (c *ReplicaClient) WriteWALSegment(ctx context.Context, pos litestream.Pos, rd io.Reader) (info litestream.WALSegmentInfo, err error) {
	if err := c.Init(ctx); err != nil {
		return info, err
	} else if pos.Generation == "" {
		return info, fmt.Errorf("generation required")
	}

	key := path.Join(c.Path, "generations", pos.Generation, "wal", litestream.FormatIndex(pos.Index), litestream.FormatOffset(pos.Offset)+".wal.lz4")
	startTime := time.Now()

	rc := internal.NewReadCounter(rd)

	blobURL := c.containerURL.NewBlockBlobURL(key)
	if _, err := azblob.UploadStreamToBlockBlob(ctx, rc, blobURL, azblob.UploadStreamToBlockBlobOptions{
		BlobHTTPHeaders: azblob.BlobHTTPHeaders{ContentType: "application/octet-stream"},
		BlobAccessTier:  azblob.DefaultAccessTier,
	}); err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(rc.N()))

	return litestream.WALSegmentInfo{
		Generation: pos.Generation,
		Index:      pos.Index,
		Offset:     pos.Offset,
		Size:       rc.N(),
		CreatedAt:  startTime.UTC(),
	}, nil
}

// WALSegmentReader returns a reader for a section of WAL data at the given index.
// Returns os.ErrNotExist if no matching index/offset is found.
func (c *ReplicaClient) WALSegmentReader(ctx context.Context, pos litestream.Pos) (io.ReadCloser, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	} else if pos.Generation == "" {
		return nil, fmt.Errorf("generation required")
	}

	key := path.Join(c.Path, "generations", pos.Generation, "wal", litestream.FormatIndex(pos.Index), litestream.FormatOffset(pos.Offset)+".wal.lz4")

	blobURL := c.containerURL.NewBlobURL(key)
	resp, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if isNotExists(err) {
		return nil, os.ErrNotExist
	} else if err != nil {
		return nil, fmt.Errorf("cannot start new reader for %q: %w", key, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(resp.ContentLength()))

	return resp.Body(azblob.RetryReaderOptions{}), nil
}

// DeleteWALSegments deletes WAL segments with at the given positions.
func (c *ReplicaClient) DeleteWALSegments(ctx context.Context, a []litestream.Pos) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	for _, pos := range a {
		if pos.Generation == "" {
			return fmt.Errorf("generation required")
		}

		key := path.Join(c.Path, "generations", pos.Generation, "wal", litestream.FormatIndex(pos.Index), litestream.FormatOffset(pos.Offset)+".wal.lz4")

		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

		blobURL := c.containerURL.NewBlobURL(key)
		if _, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{}); isNotExists(err) {
			continue
		} else if err != nil {
			return fmt.Errorf("cannot delete wal segment %q: %w", key, err)
		}
	}

	return nil
}

type snapshotIterator struct {
	client     *ReplicaClient
	generation string

	ch     chan litestream.SnapshotInfo
	g      errgroup.Group
	ctx    context.Context
	cancel func()

	info litestream.SnapshotInfo
	err  error
}

func newSnapshotIterator(ctx context.Context, generation string, client *ReplicaClient) *snapshotIterator {
	itr := &snapshotIterator{
		client:     client,
		generation: generation,
		ch:         make(chan litestream.SnapshotInfo),
	}

	itr.ctx, itr.cancel = context.WithCancel(ctx)
	itr.g.Go(itr.fetch)

	return itr
}

// fetch runs in a separate goroutine to fetch pages of objects and stream them to a channel.
func (itr *snapshotIterator) fetch() error {
	defer close(itr.ch)

	if itr.generation == "" {
		return fmt.Errorf("generation required")
	}

	prefix := path.Join(itr.client.Path, "generations", itr.generation) + "/"

	var marker azblob.Marker
	for marker.NotDone() {
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

		resp, err := itr.client.containerURL.ListBlobsFlatSegment(itr.ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: prefix})
		if err != nil {
			return err
		}
		marker = resp.NextMarker

		for _, item := range resp.Segment.BlobItems {
			index, err := internal.ParseSnapshotPath(path.Base(item.Name))
			if err != nil {
				continue
			}

			info := litestream.SnapshotInfo{
				Generation: itr.generation,
				Index:      index,
				Size:       *item.Properties.ContentLength,
				CreatedAt:  item.Properties.CreationTime.UTC(),
			}

			select {
			case <-itr.ctx.Done():
			case itr.ch <- info:
			}
		}
	}
	return nil
}

func (itr *snapshotIterator) Close() (err error) {
	err = itr.err

	// Cancel context and wait for error group to finish.
	itr.cancel()
	if e := itr.g.Wait(); e != nil && err == nil {
		err = e
	}

	return err
}

func (itr *snapshotIterator) Next() bool {
	// Exit if an error has already occurred.
	if itr.err != nil {
		return false
	}

	// Return false if context was canceled or if there are no more snapshots.
	// Otherwise fetch the next snapshot and store it on the iterator.
	select {
	case <-itr.ctx.Done():
		return false
	case info, ok := <-itr.ch:
		if !ok {
			return false
		}
		itr.info = info
		return true
	}
}

func (itr *snapshotIterator) Err() error { return itr.err }

func (itr *snapshotIterator) Snapshot() litestream.SnapshotInfo {
	return itr.info
}

type walSegmentIterator struct {
	client     *ReplicaClient
	generation string

	ch     chan litestream.WALSegmentInfo
	g      errgroup.Group
	ctx    context.Context
	cancel func()

	info litestream.WALSegmentInfo
	err  error
}

func newWALSegmentIterator(ctx context.Context, generation string, client *ReplicaClient) *walSegmentIterator {
	itr := &walSegmentIterator{
		client:     client,
		generation: generation,
		ch:         make(chan litestream.WALSegmentInfo),
	}

	itr.ctx, itr.cancel = context.WithCancel(ctx)
	itr.g.Go(itr.fetch)

	return itr
}

// fetch runs in a separate goroutine to fetch pages of objects and stream them to a channel.
func (itr *walSegmentIterator) fetch() error {
	defer close(itr.ch)

	if itr.generation == "" {
		return fmt.Errorf("generation required")
	}
	prefix := path.Join(itr.client.Path, "generations", itr.generation, "wal")

	var marker azblob.Marker
	for marker.NotDone() {
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

		resp, err := itr.client.containerURL.ListBlobsFlatSegment(itr.ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: prefix})
		if err != nil {
			return err
		}
		marker = resp.NextMarker

		for _, item := range resp.Segment.BlobItems {
			key := strings.TrimPrefix(item.Name, prefix+"/")
			index, offset, err := internal.ParseWALSegmentPath(key)
			if err != nil {
				continue
			}

			info := litestream.WALSegmentInfo{
				Generation: itr.generation,
				Index:      index,
				Offset:     offset,
				Size:       *item.Properties.ContentLength,
				CreatedAt:  item.Properties.CreationTime.UTC(),
			}

			select {
			case <-itr.ctx.Done():
			case itr.ch <- info:
			}
		}
	}
	return nil
}

func (itr *walSegmentIterator) Close() (err error) {
	err = itr.err

	// Cancel context and wait for error group to finish.
	itr.cancel()
	if e := itr.g.Wait(); e != nil && err == nil {
		err = e
	}

	return err
}

func (itr *walSegmentIterator) Next() bool {
	// Exit if an error has already occurred.
	if itr.err != nil {
		return false
	}

	// Return false if context was canceled or if there are no more segments.
	// Otherwise fetch the next segment and store it on the iterator.
	select {
	case <-itr.ctx.Done():
		return false
	case info, ok := <-itr.ch:
		if !ok {
			return false
		}
		itr.info = info
		return true
	}
}

func (itr *walSegmentIterator) Err() error { return itr.err }

func (itr *walSegmentIterator) WALSegment() litestream.WALSegmentInfo {
	return itr.info
}

func isNotExists(err error) bool {
	switch err := err.(type) {
	case azblob.StorageError:
		return err.ServiceCode() == azblob.ServiceCodeBlobNotFound
	default:
		return false
	}
}
