package abs

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "abs"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing LTX files to Azure Blob Storage.
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

// LTXFiles returns an iterator over all available LTX files.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int) (ltx.FileIterator, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}
	return newLTXFileIterator(ctx, c, level), nil
}

// WriteLTXFile writes an LTX file to remote storage.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, rd io.Reader) (info *ltx.FileInfo, err error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	key := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)
	startTime := time.Now()

	rc := internal.NewReadCounter(rd)

	blobURL := c.containerURL.NewBlockBlobURL(key)
	if _, err := azblob.UploadStreamToBlockBlob(ctx, rc, blobURL, azblob.UploadStreamToBlockBlobOptions{
		BlobHTTPHeaders: azblob.BlobHTTPHeaders{ContentType: "application/octet-stream"},
		BlobAccessTier:  azblob.DefaultAccessTier,
	}); err != nil {
		return nil, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(rc.N()))

	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      rc.N(),
		CreatedAt: startTime.UTC(),
	}, nil
}

// OpenLTXFile returns a reader for an LTX file.
// Returns os.ErrNotExist if no matching min/max TXID is not found.
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (io.ReadCloser, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	key := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)
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

// DeleteLTXFiles deletes LTX files.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	for _, info := range a {
		key := litestream.LTXFilePath(c.Path, info.Level, info.MaxTXID, info.MaxTXID)
		blobURL := c.containerURL.NewBlobURL(key)
		if _, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{}); isNotExists(err) {
			continue
		} else if err != nil {
			return fmt.Errorf("cannot delete ltx file %q: %w", key, err)
		}

		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

// DeleteAll deletes all LTX files.
func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	var marker azblob.Marker
	for marker.NotDone() {
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

		resp, err := c.containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: "/"})
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

	return nil
}

type ltxFileIterator struct {
	client *ReplicaClient
	level  int

	ch     chan ltx.FileInfo
	g      errgroup.Group
	ctx    context.Context
	cancel func()

	info ltx.FileInfo
	err  error
}

func newLTXFileIterator(ctx context.Context, client *ReplicaClient, level int) *ltxFileIterator {
	itr := &ltxFileIterator{
		client: client,
		level:  level,
		ch:     make(chan ltx.FileInfo),
	}

	itr.ctx, itr.cancel = context.WithCancel(ctx)
	itr.g.Go(itr.fetch)

	return itr
}

// fetch runs in a separate goroutine to fetch pages of objects and stream them to a channel.
func (itr *ltxFileIterator) fetch() error {
	defer close(itr.ch)

	dir := litestream.LTXLevelDir(itr.client.Path, itr.level)

	var marker azblob.Marker
	for marker.NotDone() {
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

		resp, err := itr.client.containerURL.ListBlobsFlatSegment(itr.ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: dir + "/"})
		if err != nil {
			return err
		}
		marker = resp.NextMarker

		for _, item := range resp.Segment.BlobItems {
			key := path.Base(item.Name)
			minTXID, maxTXID, err := ltx.ParseFilename(key)
			if err != nil {
				continue
			}

			info := ltx.FileInfo{
				Level:     itr.level,
				MinTXID:   minTXID,
				MaxTXID:   maxTXID,
				Size:      *item.Properties.ContentLength,
				CreatedAt: item.Properties.CreationTime.UTC(),
			}

			select {
			case <-itr.ctx.Done():
			case itr.ch <- info:
			}
		}
	}
	return nil
}

func (itr *ltxFileIterator) Close() (err error) {
	err = itr.err

	// Cancel context and wait for error group to finish.
	itr.cancel()
	if e := itr.g.Wait(); e != nil && err == nil {
		err = e
	}

	return err
}

func (itr *ltxFileIterator) Next() bool {
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

func (itr *ltxFileIterator) Err() error { return itr.err }

func (itr *ltxFileIterator) Item() *ltx.FileInfo {
	return &itr.info
}

func isNotExists(err error) bool {
	switch err := err.(type) {
	case azblob.StorageError:
		return err.ServiceCode() == azblob.ServiceCodeBlobNotFound
	default:
		return false
	}
}
