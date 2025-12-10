package gs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/superfly/ltx"
	"google.golang.org/api/iterator"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

func init() {
	litestream.RegisterReplicaClientFactory("gs", NewReplicaClientFromURL)
}

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "gs"

// MetadataKeyTimestamp is the metadata key for storing LTX file timestamps in GCS.
const MetadataKeyTimestamp = "litestream-timestamp"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing LTX files to Google Cloud Storage.
type ReplicaClient struct {
	mu     sync.Mutex
	client *storage.Client       // gs client
	bkt    *storage.BucketHandle // gs bucket handle
	logger *slog.Logger

	// GS bucket information
	Bucket string
	Path   string
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{
		logger: slog.Default().WithGroup(ReplicaClientType),
	}
}

// NewReplicaClientFromURL creates a new ReplicaClient from URL components.
// This is used by the replica client factory registration.
func NewReplicaClientFromURL(scheme, host, urlPath string, query url.Values, userinfo *url.Userinfo) (litestream.ReplicaClient, error) {
	if host == "" {
		return nil, fmt.Errorf("bucket required for gs replica URL")
	}

	client := NewReplicaClient()
	client.Bucket = host
	client.Path = urlPath
	return client, nil
}

// Type returns "gs" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init initializes the connection to GS. No-op if already initialized.
func (c *ReplicaClient) Init(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		return nil
	}

	if c.client, err = storage.NewClient(ctx); err != nil {
		return fmt.Errorf("failed to create GCS client (bucket: %s): %w", c.Bucket, err)
	}
	c.bkt = c.client.Bucket(c.Bucket)

	return nil
}

// DeleteAll deletes all LTX files.
func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	// Iterate over every object and delete it.
	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()
	for it := c.bkt.Objects(ctx, &storage.Query{Prefix: c.Path + "/"}); ; {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			return fmt.Errorf("failed to list objects in GCS bucket %s (path: %s): %w", c.Bucket, c.Path, err)
		}

		if err := c.bkt.Object(attrs.Name).Delete(ctx); isNotExists(err) {
			continue
		} else if err != nil {
			return fmt.Errorf("gs: cannot delete object %q: %w", attrs.Name, err)
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	// log.Printf("%s(%s): retainer: deleting", r.db.Path(), r.Name())

	return nil
}

// LTXFiles returns an iterator over all available LTX files for a level.
// GCS always uses accurate timestamps from metadata since they're included in LIST operations at zero cost.
// The useMetadata parameter is ignored.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	dir := litestream.LTXLevelDir(c.Path, level)
	prefix := dir + "/"
	if seek != 0 {
		prefix += seek.String()
	}

	return newLTXFileIterator(c.bkt.Objects(ctx, &storage.Query{Prefix: prefix}), c, level), nil
}

// WriteLTXFile writes an LTX file from rd to a remote path.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, rd io.Reader) (info *ltx.FileInfo, err error) {
	if err := c.Init(ctx); err != nil {
		return info, err
	}

	key := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)

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

	w := c.bkt.Object(key).NewWriter(ctx)
	defer w.Close()

	// Store timestamp in GCS metadata for accurate timestamp retrieval
	w.Metadata = map[string]string{
		MetadataKeyTimestamp: timestamp.Format(time.RFC3339Nano),
	}

	n, err := io.Copy(w, fullReader)
	if err != nil {
		return info, err
	} else if err := w.Close(); err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(n))

	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      n,
		CreatedAt: timestamp,
	}, nil
}

// OpenLTXFile returns a reader for a given LTX file.
// Returns os.ErrNotExist if no matching index/offset is found.
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	key := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)

	// In the GCS client, a length of -1 reads to EOF while 0 returns no data.
	// Callers pass size=0 to indicate "entire object" so translate that here.
	// See: https://pkg.go.dev/cloud.google.com/go/storage#ObjectHandle.NewRangeReader
	length := size
	if length <= 0 {
		length = -1
	}

	r, err := c.bkt.Object(key).NewRangeReader(ctx, offset, length)
	if isNotExists(err) {
		return nil, os.ErrNotExist
	} else if err != nil {
		return nil, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(r.Attrs.Size))

	return r, nil
}

// DeleteLTXFiles deletes a set of LTX files.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	for _, info := range a {
		key := litestream.LTXFilePath(c.Path, info.Level, info.MinTXID, info.MaxTXID)

		c.logger.Debug("deleting ltx file", "level", info.Level, "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "key", key)

		if err := c.bkt.Object(key).Delete(ctx); err != nil && !isNotExists(err) {
			return fmt.Errorf("gs: cannot delete ltx file %q: %w", key, err)
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

type ltxFileIterator struct {
	it     *storage.ObjectIterator
	client *ReplicaClient
	level  int
	info   *ltx.FileInfo
	err    error
}

func newLTXFileIterator(it *storage.ObjectIterator, client *ReplicaClient, level int) *ltxFileIterator {
	return &ltxFileIterator{
		it:     it,
		client: client,
		level:  level,
	}
}

func (itr *ltxFileIterator) Close() (err error) {
	return itr.err
}

func (itr *ltxFileIterator) Next() bool {
	// Exit if an error has already occurred.
	if itr.err != nil {
		return false
	}

	for {
		// Fetch next object.
		attrs, err := itr.it.Next()
		if errors.Is(err, iterator.Done) {
			return false
		} else if err != nil {
			itr.err = err
			return false
		}

		// Parse index & offset, otherwise skip to the next object.
		minTXID, maxTXID, err := ltx.ParseFilename(path.Base(attrs.Name))
		if err != nil {
			continue
		}

		// Always use accurate timestamp from metadata since it's zero-cost
		// GCS includes metadata in LIST operations, so no extra API call needed
		createdAt := attrs.Created.UTC()
		if attrs.Metadata != nil {
			if ts, ok := attrs.Metadata[MetadataKeyTimestamp]; ok {
				if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
					createdAt = parsed
				}
			}
		}

		// Store current snapshot and return.
		itr.info = &ltx.FileInfo{
			Level:     itr.level,
			MinTXID:   minTXID,
			MaxTXID:   maxTXID,
			Size:      attrs.Size,
			CreatedAt: createdAt,
		}

		return true
	}
}

func (itr *ltxFileIterator) Err() error { return itr.err }

func (itr *ltxFileIterator) Item() *ltx.FileInfo {
	return itr.info
}

func isNotExists(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist)
}
