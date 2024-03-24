package abs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"golang.org/x/sync/errgroup"
)

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "abs"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing snapshots & WAL segments to disk.
type ReplicaClient struct {
	mu     sync.Mutex
	client *container.Client

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

	// Construct & parse endpoint unless already set.
	endpoint := c.Endpoint
	if endpoint == "" {
		if c.AccountName == "" {
			return fmt.Errorf("account name is required")
		}
		endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", c.AccountName)
	}
	if !strings.HasSuffix(endpoint, "/") {
		endpoint += "/"
	}
	endpoint += c.Bucket

	// Read account key from environment, if available.
	accountKey := c.AccountKey
	if accountKey == "" {
		accountKey = os.Getenv("LITESTREAM_AZURE_ACCOUNT_KEY")
	}

	// Authenticate to ACS and build client
	options := container.ClientOptions{
		ClientOptions: policy.ClientOptions{
			Retry: policy.RetryOptions{
				TryTimeout: 24 * time.Hour,
			},
			Telemetry: policy.TelemetryOptions{
				ApplicationID: "litestream",
			},
		},
	}

	if accountKey == "" {
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return err
		}
		c.client, err = container.NewClient(endpoint, credential, &options)
		if err != nil {
			return fmt.Errorf("cannot create Azure Blob Storage client: %w", err)
		}
	} else {
		credential, err := azblob.NewSharedKeyCredential(c.AccountName, accountKey)
		if err != nil {
			return err
		}
		c.client, err = container.NewClientWithSharedKeyCredential(endpoint, credential, &options)
		if err != nil {
			return fmt.Errorf("cannot create Azure Blob Storage client: %w", err)
		}
	}

	return nil
}

// Generations returns a list of available generation names.
func (c *ReplicaClient) Generations(ctx context.Context) ([]string, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	var generations []string

	pager := c.client.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
		Prefix: to.Ptr(litestream.GenerationsPath(c.Path) + "/"),
	})
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, prefix := range resp.Segment.BlobPrefixes {
			if prefix == nil || prefix.Name == nil {
				continue
			}
			name := path.Base(strings.TrimSuffix(*prefix.Name, "/"))
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
	}

	dir, err := litestream.GenerationPath(c.Path, generation)
	if err != nil {
		return fmt.Errorf("cannot determine generation path: %w", err)
	}

	pager := c.client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: to.Ptr(dir + "/"),
	})
	for pager.More() {
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

		resp, err := pager.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, item := range resp.Segment.BlobItems {
			if item == nil || item.Name == nil {
				continue
			}
			internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

			_, err = c.client.NewBlobClient(*item.Name).Delete(ctx, nil)
			if err != nil {
				if bloberror.HasCode(err, bloberror.BlobNotFound) {
					continue
				}
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
	}

	key, err := litestream.SnapshotPath(c.Path, generation, index)
	if err != nil {
		return info, fmt.Errorf("cannot determine snapshot path: %w", err)
	}
	startTime := time.Now()

	rc := internal.NewReadCounter(rd)

	_, err = c.client.NewBlockBlobClient(key).UploadStream(ctx, rc, &blockblob.UploadStreamOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: to.Ptr("application/octet-stream"),
		},
	})
	if err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(rc.N()))

	// log.Printf("%s(%s): snapshot: creating %s/%08x t=%s", r.db.Path(), r.Name(), generation, index, time.Since(startTime).Truncate(time.Millisecond))

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
	}

	key, err := litestream.SnapshotPath(c.Path, generation, index)
	if err != nil {
		return nil, fmt.Errorf("cannot determine snapshot path: %w", err)
	}

	resp, err := c.client.NewBlobClient(key).DownloadStream(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("cannot start new reader for %q: %w", key, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(*resp.ContentLength))

	return resp.NewRetryReader(ctx, nil), nil
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

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

	_, err = c.client.NewBlobClient(key).Delete(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil
		}
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
	}

	key, err := litestream.WALSegmentPath(c.Path, pos.Generation, pos.Index, pos.Offset)
	if err != nil {
		return info, fmt.Errorf("cannot determine wal segment path: %w", err)
	}
	startTime := time.Now()

	rc := internal.NewReadCounter(rd)

	_, err = c.client.NewBlockBlobClient(key).UploadStream(ctx, rc, &blockblob.UploadStreamOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: to.Ptr("application/octet-stream"),
		},
	})
	if err != nil {
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
	}

	key, err := litestream.WALSegmentPath(c.Path, pos.Generation, pos.Index, pos.Offset)
	if err != nil {
		return nil, fmt.Errorf("cannot determine wal segment path: %w", err)
	}

	resp, err := c.client.NewBlobClient(key).DownloadStream(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("cannot start new reader for %q: %w", key, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(*resp.ContentLength))

	return resp.NewRetryReader(ctx, nil), nil
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

		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

		_, err = c.client.NewBlobClient(key).Delete(ctx, nil)
		if err != nil {
			if bloberror.HasCode(err, bloberror.BlobNotFound) {
				continue
			}
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

	dir, err := litestream.SnapshotsPath(itr.client.Path, itr.generation)
	if err != nil {
		return fmt.Errorf("cannot determine snapshots path: %w", err)
	}

	pager := itr.client.client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: to.Ptr(dir + "/"),
	})
	for pager.More() {
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

		resp, err := pager.NextPage(itr.ctx)
		if err != nil {
			return err
		}

		for _, item := range resp.Segment.BlobItems {
			if item == nil || item.Name == nil {
				continue
			}
			key := path.Base(*item.Name)
			index, err := litestream.ParseSnapshotPath(key)
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

	dir, err := litestream.WALPath(itr.client.Path, itr.generation)
	if err != nil {
		return fmt.Errorf("cannot determine wal path: %w", err)
	}

	pager := itr.client.client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: to.Ptr(dir + "/"),
	})
	for pager.More() {
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

		resp, err := pager.NextPage(itr.ctx)
		if err != nil {
			return err
		}

		for _, item := range resp.Segment.BlobItems {
			if item == nil || item.Name == nil {
				continue
			}
			key := path.Base(*item.Name)
			index, offset, err := litestream.ParseWALSegmentPath(key)
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
