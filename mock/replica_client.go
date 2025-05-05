package mock

import (
	"context"
	"io"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

type ReplicaClient struct {
	DeleteAllFunc      func(ctx context.Context) error
	LTXFilesFunc       func(ctx context.Context, level int) (ltx.LTXFileIterator, error)
	OpenLTXFileFunc    func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (io.ReadCloser, error)
	WriteLTXFileFunc   func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.LTXFileInfo, error)
	DeleteLTXFilesFunc func(ctx context.Context, a []*ltx.LTXFileInfo) error
}

func (c *ReplicaClient) Type() string { return "mock" }

func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	return c.DeleteAllFunc(ctx)
}

func (c *ReplicaClient) LTXFiles(ctx context.Context, level int) (ltx.LTXFileIterator, error) {
	return c.LTXFilesFunc(ctx, level)
}

func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (io.ReadCloser, error) {
	return c.OpenLTXFileFunc(ctx, level, minTXID, maxTXID)
}

func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.LTXFileInfo, error) {
	return c.WriteLTXFileFunc(ctx, level, minTXID, maxTXID, r)
}

func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.LTXFileInfo) error {
	return c.DeleteLTXFilesFunc(ctx, a)
}
