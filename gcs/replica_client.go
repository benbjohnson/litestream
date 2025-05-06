package gcs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"github.com/superfly/ltx"
	"google.golang.org/api/iterator"
)

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "gcs"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing LTX files to Google Cloud Storage.
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

// DeleteAll deletes all LTX files.
func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	// Iterate over every object and delete it.
	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()
	for it := c.bkt.Objects(ctx, &storage.Query{Prefix: c.Path + "/"}); ; {
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

	// log.Printf("%s(%s): retainer: deleting", r.db.Path(), r.Name())

	return nil
}

// LTXFiles returns an iterator over all available LTX files for a level.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int) (ltx.FileIterator, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}
	dir := litestream.LTXLevelDir(c.Path, level)
	return newLTXFileIterator(c.bkt.Objects(ctx, &storage.Query{Prefix: dir + "/"}), level), nil
}

// WriteLTXFile writes an LTX file from rd to a remote path.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, rd io.Reader) (info *ltx.FileInfo, err error) {
	if err := c.Init(ctx); err != nil {
		return info, err
	}

	key := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)
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

	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      n,
		CreatedAt: startTime.UTC(),
	}, nil
}

// OpenLTXFile returns a reader for a given LTX file.
// Returns os.ErrNotExist if no matching index/offset is found.
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (io.ReadCloser, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	key := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)

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

// DeleteLTXFiles deletes a set of LTX files.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	for _, info := range a {
		key := litestream.LTXFilePath(c.Path, info.Level, info.MinTXID, info.MaxTXID)
		if err := c.bkt.Object(key).Delete(ctx); err != nil && !isNotExists(err) {
			return fmt.Errorf("cannot delete ltx file %q: %w", key, err)
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

type ltxFileIterator struct {
	it    *storage.ObjectIterator
	level int
	info  *ltx.FileInfo
	err   error
}

func newLTXFileIterator(it *storage.ObjectIterator, level int) *ltxFileIterator {
	return &ltxFileIterator{
		it:    it,
		level: level,
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
		if err == iterator.Done {
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

		// Store current snapshot and return.
		itr.info = &ltx.FileInfo{
			Level:     itr.level,
			MinTXID:   minTXID,
			MaxTXID:   maxTXID,
			Size:      attrs.Size,
			CreatedAt: attrs.Created.UTC(),
		}
		return true
	}
}

func (itr *ltxFileIterator) Err() error { return itr.err }

func (itr *ltxFileIterator) Item() *ltx.FileInfo {
	return itr.info
}

func isNotExists(err error) bool {
	return err == storage.ErrObjectNotExist
}
