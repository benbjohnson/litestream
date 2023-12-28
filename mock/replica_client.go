package mock

import (
	"context"
	"io"

	"github.com/benbjohnson/litestream"
)

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

type ReplicaClient struct {
	GenerationsFunc       func(ctx context.Context) ([]string, error)
	DeleteGenerationFunc  func(ctx context.Context, generation string) error
	SnapshotsFunc         func(ctx context.Context, generation string) (litestream.SnapshotIterator, error)
	WriteSnapshotFunc     func(ctx context.Context, info *litestream.SnapshotInfo, r io.Reader) error
	DeleteSnapshotFunc    func(ctx context.Context, info litestream.SnapshotInfo) error
	SnapshotReaderFunc    func(ctx context.Context, info litestream.SnapshotInfo) (io.ReadCloser, error)
	WALSegmentsFunc       func(ctx context.Context, generation string) (litestream.WALSegmentIterator, error)
	WriteWALSegmentFunc   func(ctx context.Context, info *litestream.WALSegmentInfo, r io.Reader) error
	DeleteWALSegmentsFunc func(ctx context.Context, a []litestream.WALSegmentInfo) error
	WALSegmentReaderFunc  func(ctx context.Context, info litestream.WALSegmentInfo) (io.ReadCloser, error)
}

func (c *ReplicaClient) Type() string { return "mock" }

func (c *ReplicaClient) Generations(ctx context.Context) ([]string, error) {
	return c.GenerationsFunc(ctx)
}

func (c *ReplicaClient) DeleteGeneration(ctx context.Context, generation string) error {
	return c.DeleteGenerationFunc(ctx, generation)
}

func (c *ReplicaClient) Snapshots(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
	return c.SnapshotsFunc(ctx, generation)
}

func (c *ReplicaClient) WriteSnapshot(ctx context.Context, info *litestream.SnapshotInfo, r io.Reader) error {
	return c.WriteSnapshotFunc(ctx, info, r)
}

func (c *ReplicaClient) DeleteSnapshot(ctx context.Context, info litestream.SnapshotInfo) error {
	return c.DeleteSnapshotFunc(ctx, info)
}

func (c *ReplicaClient) SnapshotReader(ctx context.Context, info litestream.SnapshotInfo) (io.ReadCloser, error) {
	return c.SnapshotReaderFunc(ctx, info)
}

func (c *ReplicaClient) WALSegments(ctx context.Context, generation string) (litestream.WALSegmentIterator, error) {
	return c.WALSegmentsFunc(ctx, generation)
}

func (c *ReplicaClient) WriteWALSegment(ctx context.Context, info *litestream.WALSegmentInfo, r io.Reader) error {
	return c.WriteWALSegmentFunc(ctx, info, r)
}

func (c *ReplicaClient) DeleteWALSegments(ctx context.Context, a []litestream.WALSegmentInfo) error {
	return c.DeleteWALSegmentsFunc(ctx, a)
}

func (c *ReplicaClient) WALSegmentReader(ctx context.Context, info litestream.WALSegmentInfo) (io.ReadCloser, error) {
	return c.WALSegmentReaderFunc(ctx, info)
}
