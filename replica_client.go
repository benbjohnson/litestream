package litestream

import (
	"context"
	"io"
)

// ReplicaClient represents client to connect to a Replica.
type ReplicaClient interface {
	// Returns the type of client.
	Type() string

	// Returns a list of available generations. Order is undefined.
	Generations(ctx context.Context) ([]string, error)

	// Deletes all snapshots & WAL segments within a generation.
	DeleteGeneration(ctx context.Context, generation string) error

	// Returns an iterator of all snapshots within a generation on the replica. Order is undefined.
	Snapshots(ctx context.Context, generation string) (SnapshotIterator, error)

	// Writes LZ4 compressed snapshot data to the replica at a given index
	// within a generation. Returns metadata for the snapshot.
	WriteSnapshot(ctx context.Context, generation string, index int, r io.Reader) (SnapshotInfo, error)

	// Deletes a snapshot with the given generation & index.
	DeleteSnapshot(ctx context.Context, generation string, index int) error

	// Returns a reader that contains LZ4 compressed snapshot data for a
	// given index within a generation. Returns an os.ErrNotFound error if
	// the snapshot does not exist.
	SnapshotReader(ctx context.Context, generation string, index int) (io.ReadCloser, error)

	// Returns an iterator of all WAL segments within a generation on the replica. Order is undefined.
	WALSegments(ctx context.Context, generation string) (WALSegmentIterator, error)

	// Writes an LZ4 compressed WAL segment at a given position.
	// Returns metadata for the written segment.
	WriteWALSegment(ctx context.Context, pos Pos, r io.Reader) (WALSegmentInfo, error)

	// Deletes one or more WAL segments at the given positions.
	DeleteWALSegments(ctx context.Context, a []Pos) error

	// Returns a reader that contains an LZ4 compressed WAL segment at a given
	// index/offset within a generation. Returns an os.ErrNotFound error if the
	// WAL segment does not exist.
	WALSegmentReader(ctx context.Context, pos Pos) (io.ReadCloser, error)
}
