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

	// Writes snapshot data to the replica at a given index within a generation.
	// Updates passed metadata with file size and creation time.
	WriteSnapshot(ctx context.Context, info *SnapshotInfo, r io.Reader) error

	// Deletes a snapshot with the given info.
	DeleteSnapshot(ctx context.Context, info SnapshotInfo) error

	// Returns a reader that contains snapshot data for a given index within a generation.
	// Returns an os.ErrNotFound error if the snapshot does not exist.
	SnapshotReader(ctx context.Context, info SnapshotInfo) (io.ReadCloser, error)

	// Returns an iterator of all WAL segments within a generation on the replica. Order is undefined.
	WALSegments(ctx context.Context, generation string) (WALSegmentIterator, error)

	// Writes an WAL segment at a given position.
	// Updates passed metadata with file size and creation time.
	WriteWALSegment(ctx context.Context, info *WALSegmentInfo, r io.Reader) error

	// Deletes one or more WAL segments at the given positions.
	DeleteWALSegments(ctx context.Context, a []WALSegmentInfo) error

	// Returns a reader that contains a WAL segment at a given index/offset within a generation.
	// Returns an os.ErrNotFound error if the WAL segment does not exist.
	WALSegmentReader(ctx context.Context, info WALSegmentInfo) (io.ReadCloser, error)
}
