package mock

import (
	"context"
	"io"

	"github.com/benbjohnson/litestream"
)

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

type ReplicaClient struct {
	DeleteAllFunc         func(ctx context.Context) error
	SnapshotsFunc         func(ctx context.Context) (litestream.SnapshotIterator, error)
	WriteSnapshotFunc     func(ctx context.Context, index int, r io.Reader) (litestream.SnapshotInfo, error)
	DeleteSnapshotFunc    func(ctx context.Context, index int) error
	SnapshotReaderFunc    func(ctx context.Context, index int) (io.ReadCloser, error)
	WALSegmentsFunc       func(ctx context.Context) (litestream.WALSegmentIterator, error)
	WriteWALSegmentFunc   func(ctx context.Context, pos litestream.Pos, r io.Reader) (litestream.WALSegmentInfo, error)
	DeleteWALSegmentsFunc func(ctx context.Context, a []litestream.Pos) error
	WALSegmentReaderFunc  func(ctx context.Context, pos litestream.Pos) (io.ReadCloser, error)
}

func (c *ReplicaClient) Type() string { return "mock" }

func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	return c.DeleteAllFunc(ctx)
}

func (c *ReplicaClient) Snapshots(ctx context.Context) (litestream.SnapshotIterator, error) {
	return c.SnapshotsFunc(ctx)
}

func (c *ReplicaClient) WriteSnapshot(ctx context.Context, index int, r io.Reader) (litestream.SnapshotInfo, error) {
	return c.WriteSnapshotFunc(ctx, index, r)
}

func (c *ReplicaClient) DeleteSnapshot(ctx context.Context, index int) error {
	return c.DeleteSnapshotFunc(ctx, index)
}

func (c *ReplicaClient) SnapshotReader(ctx context.Context, index int) (io.ReadCloser, error) {
	return c.SnapshotReaderFunc(ctx, index)
}

func (c *ReplicaClient) WALSegments(ctx context.Context) (litestream.WALSegmentIterator, error) {
	return c.WALSegmentsFunc(ctx)
}

func (c *ReplicaClient) WriteWALSegment(ctx context.Context, pos litestream.Pos, r io.Reader) (litestream.WALSegmentInfo, error) {
	return c.WriteWALSegmentFunc(ctx, pos, r)
}

func (c *ReplicaClient) DeleteWALSegments(ctx context.Context, a []litestream.Pos) error {
	return c.DeleteWALSegmentsFunc(ctx, a)
}

func (c *ReplicaClient) WALSegmentReader(ctx context.Context, pos litestream.Pos) (io.ReadCloser, error) {
	return c.WALSegmentReaderFunc(ctx, pos)
}
