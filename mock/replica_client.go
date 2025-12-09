package mock

import (
	"context"
	"io"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
)

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

type ReplicaClient struct {
	InitFunc           func(ctx context.Context) error
	DeleteAllFunc      func(ctx context.Context) error
	LTXFilesFunc       func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error)
	OpenLTXFileFunc    func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
	WriteLTXFileFunc   func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error)
	DeleteLTXFilesFunc func(ctx context.Context, a []*ltx.FileInfo) error
}

func (c *ReplicaClient) Type() string { return "mock" }

func (c *ReplicaClient) Init(ctx context.Context) error {
	if c.InitFunc != nil {
		return c.InitFunc(ctx)
	}
	return nil
}

func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	return c.DeleteAllFunc(ctx)
}

func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	return c.LTXFilesFunc(ctx, level, seek, useMetadata)
}

func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	return c.OpenLTXFileFunc(ctx, level, minTXID, maxTXID, offset, size)
}

func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	return c.WriteLTXFileFunc(ctx, level, minTXID, maxTXID, r)
}

func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	return c.DeleteLTXFilesFunc(ctx, a)
}
