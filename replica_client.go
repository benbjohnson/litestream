package litestream

import (
	"context"
	"io"
)

// ReplicaClient represents client to connect to a Replica.
type ReplicaClient interface {
	// Returns the type of client.
	Type() string

	// Deletes all snapshots & WAL segments.
	DeleteAll(ctx context.Context) error

	// Returns an iterator of all snapshots on the replica. Order is undefined.
	Snapshots(ctx context.Context) (SnapshotIterator, error)

	// Writes LZ4 compressed snapshot data to the replica at a given index.
	// Returns metadata for the snapshot.
	WriteSnapshot(ctx context.Context, index int, r io.Reader) (SnapshotInfo, error)

	// Deletes a snapshot with the given index.
	DeleteSnapshot(ctx context.Context, index int) error

	// Returns a reader that contains LZ4 compressed snapshot data for a
	// given index. Returns an os.ErrNotFound error if the snapshot does not exist.
	SnapshotReader(ctx context.Context, index int) (io.ReadCloser, error)

	// Returns an iterator of all WAL segments on the replica. Order is undefined.
	WALSegments(ctx context.Context) (WALSegmentIterator, error)

	// Writes an LZ4 compressed WAL segment at a given position.
	// Returns metadata for the written segment.
	WriteWALSegment(ctx context.Context, pos Pos, r io.Reader) (WALSegmentInfo, error)

	// Deletes one or more WAL segments at the given positions.
	DeleteWALSegments(ctx context.Context, a []Pos) error

	// Returns a reader that contains an LZ4 compressed WAL segment at a given
	// index/offset. Returns an os.ErrNotFound error if the WAL segment does not exist.
	WALSegmentReader(ctx context.Context, pos Pos) (io.ReadCloser, error)
}
