package litestream

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/benbjohnson/litestream/internal"
	"github.com/pierrec/lz4/v4"
)

// DefaultRestoreParallelism is the default parallelism when downloading WAL files.
const DefaultRestoreParallelism = 8

// ReplicaClient represents client to connect to a Replica.
type ReplicaClient interface {
	// Returns the type of client.
	Type() string

	// Returns a list of available generations.
	Generations(ctx context.Context) ([]string, error)

	// Deletes all snapshots & WAL segments within a generation.
	DeleteGeneration(ctx context.Context, generation string) error

	// Returns an iterator of all snapshots within a generation on the replica.
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

	// Returns an iterator of all WAL segments within a generation on the replica.
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

// FindSnapshotForIndex returns the highest index for a snapshot within a
// generation that occurs before a given index.
func FindSnapshotForIndex(ctx context.Context, client ReplicaClient, generation string, index int) (int, error) {
	itr, err := client.Snapshots(ctx, generation)
	if err != nil {
		return 0, fmt.Errorf("snapshots: %w", err)
	}
	defer itr.Close()

	// Iterate over all snapshots to find the closest to our given index.
	snapshotIndex := -1
	var n int
	for ; itr.Next(); n++ {
		info := itr.Snapshot()
		if info.Index > index {
			continue // after given index, skip
		}

		// Use snapshot if it's more recent.
		if info.Index >= snapshotIndex {
			snapshotIndex = info.Index
		}
	}
	if err := itr.Close(); err != nil {
		return 0, fmt.Errorf("snapshot iteration: %w", err)
	}

	// Ensure we find at least one snapshot and that it's before the given index.
	if n == 0 {
		return 0, ErrNoSnapshots
	} else if snapshotIndex == -1 {
		return 0, fmt.Errorf("no snapshots available at or before index %s", FormatIndex(index))
	}
	return snapshotIndex, nil
}

// GenerationTimeBounds returns the creation time & last updated time of a generation.
// Returns ErrNoSnapshots if no data exists for the generation.
func GenerationTimeBounds(ctx context.Context, client ReplicaClient, generation string) (createdAt, updatedAt time.Time, err error) {
	// Determine bounds for snapshots only first.
	// This will return ErrNoSnapshots if no snapshots exist.
	if createdAt, updatedAt, err = SnapshotTimeBounds(ctx, client, generation); err != nil {
		return createdAt, updatedAt, err
	}

	// Update ending time bounds if WAL segments exist after the last snapshot.
	_, max, err := WALTimeBounds(ctx, client, generation)
	if err != nil && err != ErrNoWALSegments {
		return createdAt, updatedAt, err
	} else if max.After(updatedAt) {
		updatedAt = max
	}

	return createdAt, updatedAt, nil
}

// SnapshotTimeBounds returns the minimum and maximum snapshot timestamps within a generation.
// Returns ErrNoSnapshots if no data exists for the generation.
func SnapshotTimeBounds(ctx context.Context, client ReplicaClient, generation string) (min, max time.Time, err error) {
	itr, err := client.Snapshots(ctx, generation)
	if err != nil {
		return min, max, fmt.Errorf("snapshots: %w", err)
	}
	defer itr.Close()

	// Iterate over all snapshots to find the oldest and newest.
	var n int
	for ; itr.Next(); n++ {
		info := itr.Snapshot()
		if min.IsZero() || info.CreatedAt.Before(min) {
			min = info.CreatedAt
		}
		if max.IsZero() || info.CreatedAt.After(max) {
			max = info.CreatedAt
		}
	}
	if err := itr.Close(); err != nil {
		return min, max, fmt.Errorf("snapshot iteration: %w", err)
	}

	// Return error if no snapshots exist.
	if n == 0 {
		return min, max, ErrNoSnapshots
	}
	return min, max, nil
}

// WALTimeBounds returns the minimum and maximum snapshot timestamps.
// Returns ErrNoWALSegments if no data exists for the generation.
func WALTimeBounds(ctx context.Context, client ReplicaClient, generation string) (min, max time.Time, err error) {
	itr, err := client.WALSegments(ctx, generation)
	if err != nil {
		return min, max, fmt.Errorf("wal segments: %w", err)
	}
	defer itr.Close()

	// Iterate over all WAL segments to find oldest and newest.
	var n int
	for ; itr.Next(); n++ {
		info := itr.WALSegment()
		if min.IsZero() || info.CreatedAt.Before(min) {
			min = info.CreatedAt
		}
		if max.IsZero() || info.CreatedAt.After(max) {
			max = info.CreatedAt
		}
	}
	if err := itr.Close(); err != nil {
		return min, max, fmt.Errorf("wal segment iteration: %w", err)
	}

	if n == 0 {
		return min, max, ErrNoWALSegments
	}
	return min, max, nil
}

// FindLatestGeneration returns the most recent generation for a client.
func FindLatestGeneration(ctx context.Context, client ReplicaClient) (generation string, err error) {
	generations, err := client.Generations(ctx)
	if err != nil {
		return "", fmt.Errorf("generations: %w", err)
	}

	// Search generations for one latest updated.
	var maxTime time.Time
	for i := range generations {
		// Determine the latest update for the generation.
		_, updatedAt, err := GenerationTimeBounds(ctx, client, generations[i])
		if err != nil {
			return "", fmt.Errorf("generation time bounds: %w", err)
		}

		// Use the latest replica if we have multiple candidates.
		if updatedAt.After(maxTime) {
			maxTime = updatedAt
			generation = generations[i]
		}
	}

	if generation == "" {
		return "", ErrNoGeneration
	}
	return generation, nil
}

// ReplicaClientTimeBounds returns time range covered by a replica client
// across all generations. It scans the time range of all generations and
// computes the lower and upper bounds of them.
func ReplicaClientTimeBounds(ctx context.Context, client ReplicaClient) (min, max time.Time, err error) {
	generations, err := client.Generations(ctx)
	if err != nil {
		return min, max, fmt.Errorf("generations: %w", err)
	} else if len(generations) == 0 {
		return min, max, ErrNoGeneration
	}

	// Iterate over generations to determine outer bounds.
	for i := range generations {
		// Determine the time range for the generation.
		createdAt, updatedAt, err := GenerationTimeBounds(ctx, client, generations[i])
		if err != nil {
			return min, max, fmt.Errorf("generation time bounds: %w", err)
		}

		// Update time bounds.
		if min.IsZero() || createdAt.Before(min) {
			min = createdAt
		}
		if max.IsZero() || updatedAt.After(max) {
			max = updatedAt
		}
	}

	return min, max, nil
}

// FindIndexByTimestamp returns the highest index before a given point-in-time
// within a generation. Returns ErrNoSnapshots if no index exists on the replica
// for the generation.
func FindIndexByTimestamp(ctx context.Context, client ReplicaClient, generation string, timestamp time.Time) (index int, err error) {
	snapshotIndex, err := FindSnapshotIndexByTimestamp(ctx, client, generation, timestamp)
	if err == ErrNoSnapshots {
		return 0, err
	} else if err != nil {
		return 0, fmt.Errorf("max snapshot index: %w", err)
	}

	// Determine the highest available WAL index.
	walIndex, err := FindWALIndexByTimestamp(ctx, client, generation, timestamp)
	if err != nil && err != ErrNoWALSegments {
		return 0, fmt.Errorf("max wal index: %w", err)
	}

	// Use snapshot index if it's after the last WAL index.
	if snapshotIndex > walIndex {
		return snapshotIndex, nil
	}
	return walIndex, nil
}

// FindSnapshotIndexByTimestamp returns the highest snapshot index before timestamp.
// Returns ErrNoSnapshots if no snapshots exist for the generation on the replica.
func FindSnapshotIndexByTimestamp(ctx context.Context, client ReplicaClient, generation string, timestamp time.Time) (index int, err error) {
	itr, err := client.Snapshots(ctx, generation)
	if err != nil {
		return 0, fmt.Errorf("snapshots: %w", err)
	}
	defer func() { _ = itr.Close() }()

	// Iterate over snapshots to find the highest index.
	var n int
	for ; itr.Next(); n++ {
		if info := itr.Snapshot(); info.CreatedAt.After(timestamp) {
			continue
		} else if info.Index > index {
			index = info.Index
		}
	}
	if err := itr.Close(); err != nil {
		return 0, fmt.Errorf("snapshot iteration: %w", err)
	}

	// Return an error if no snapshots were found.
	if n == 0 {
		return 0, ErrNoSnapshots
	}
	return index, nil
}

// FindWALIndexByTimestamp returns the highest WAL index before timestamp.
// Returns ErrNoWALSegments if no segments exist for the generation on the replica.
func FindWALIndexByTimestamp(ctx context.Context, client ReplicaClient, generation string, timestamp time.Time) (index int, err error) {
	itr, err := client.WALSegments(ctx, generation)
	if err != nil {
		return 0, fmt.Errorf("wal segments: %w", err)
	}
	defer func() { _ = itr.Close() }()

	// Iterate over WAL segments to find the highest index.
	var n int
	for ; itr.Next(); n++ {
		if info := itr.WALSegment(); info.CreatedAt.After(timestamp) {
			continue
		} else if info.Index > index {
			index = info.Index
		}
	}
	if err := itr.Close(); err != nil {
		return 0, fmt.Errorf("wal segment iteration: %w", err)
	}

	// Return an error if no WAL segments were found.
	if n == 0 {
		return 0, ErrNoWALSegments
	}
	return index, nil
}

// FindMaxIndexByGeneration returns the last index within a generation.
// Returns ErrNoSnapshots if no index exists on the replica for the generation.
func FindMaxIndexByGeneration(ctx context.Context, client ReplicaClient, generation string) (index int, err error) {
	// Determine the highest available snapshot index. Returns an error if no
	// snapshot are available as WALs are not useful without snapshots.
	snapshotIndex, err := FindMaxSnapshotIndexByGeneration(ctx, client, generation)
	if err == ErrNoSnapshots {
		return index, err
	} else if err != nil {
		return index, fmt.Errorf("max snapshot index: %w", err)
	}

	// Determine the highest available WAL index.
	walIndex, err := FindMaxWALIndexByGeneration(ctx, client, generation)
	if err != nil && err != ErrNoWALSegments {
		return index, fmt.Errorf("max wal index: %w", err)
	}

	// Use snapshot index if it's after the last WAL index.
	if snapshotIndex > walIndex {
		return snapshotIndex, nil
	}
	return walIndex, nil
}

// FindMaxSnapshotIndexByGeneration returns the last snapshot index within a generation.
// Returns ErrNoSnapshots if no snapshots exist for the generation on the replica.
func FindMaxSnapshotIndexByGeneration(ctx context.Context, client ReplicaClient, generation string) (index int, err error) {
	itr, err := client.Snapshots(ctx, generation)
	if err != nil {
		return 0, fmt.Errorf("snapshots: %w", err)
	}
	defer func() { _ = itr.Close() }()

	// Iterate over snapshots to find the highest index.
	var n int
	for ; itr.Next(); n++ {
		if info := itr.Snapshot(); info.Index > index {
			index = info.Index
		}
	}
	if err := itr.Close(); err != nil {
		return 0, fmt.Errorf("snapshot iteration: %w", err)
	}

	// Return an error if no snapshots were found.
	if n == 0 {
		return 0, ErrNoSnapshots
	}
	return index, nil
}

// FindMaxWALIndexByGeneration returns the last WAL index within a generation.
// Returns ErrNoWALSegments if no segments exist for the generation on the replica.
func FindMaxWALIndexByGeneration(ctx context.Context, client ReplicaClient, generation string) (index int, err error) {
	itr, err := client.WALSegments(ctx, generation)
	if err != nil {
		return 0, fmt.Errorf("wal segments: %w", err)
	}
	defer func() { _ = itr.Close() }()

	// Iterate over WAL segments to find the highest index.
	var n int
	for ; itr.Next(); n++ {
		if info := itr.WALSegment(); info.Index > index {
			index = info.Index
		}
	}
	if err := itr.Close(); err != nil {
		return 0, fmt.Errorf("wal segment iteration: %w", err)
	}

	// Return an error if no WAL segments were found.
	if n == 0 {
		return 0, ErrNoWALSegments
	}
	return index, nil
}

// Restore restores the database to the given index on a generation.
func Restore(ctx context.Context, client ReplicaClient, filename, generation string, snapshotIndex, targetIndex int, opt RestoreOptions) (err error) {
	// Validate options.
	if filename == "" {
		return fmt.Errorf("restore path required")
	} else if generation == "" {
		return fmt.Errorf("generation required")
	} else if snapshotIndex < 0 {
		return fmt.Errorf("snapshot index required")
	} else if targetIndex < 0 {
		return fmt.Errorf("target index required")
	}

	// Require a default level of parallelism.
	if opt.Parallelism < 1 {
		opt.Parallelism = DefaultRestoreParallelism
	}

	// Ensure logger exists.
	logger := opt.Logger
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}

	// Ensure output path does not already exist.
	// If doesn't exist, also remove the journal, shm, & wal if left behind.
	if _, err := os.Stat(filename); err == nil {
		return fmt.Errorf("cannot restore, output path already exists: %s", filename)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	} else if err := removeDBFiles(filename); err != nil {
		return err
	}

	// Copy snapshot to output path.
	tmpPath := filename + ".tmp"
	logger.Printf("%srestoring snapshot %s/%s to %s", opt.LogPrefix, generation, FormatIndex(snapshotIndex), tmpPath)
	if err := RestoreSnapshot(ctx, client, tmpPath, generation, snapshotIndex, opt.Mode, opt.Uid, opt.Gid); err != nil {
		return fmt.Errorf("cannot restore snapshot: %w", err)
	}

	// Download & apply all WAL files between the snapshot & the target index.
	d := NewWALDownloader(client, tmpPath, generation, snapshotIndex, targetIndex)
	d.Parallelism = opt.Parallelism
	d.Mode = opt.Mode
	d.Uid, d.Gid = opt.Uid, opt.Gid

	for {
		// Read next WAL file from downloader.
		walIndex, walPath, err := d.Next(ctx)
		if err == io.EOF {
			break
		}

		// If we are only reading a single index, a WAL file may not be found.
		if _, ok := err.(*WALNotFoundError); ok && snapshotIndex == targetIndex {
			logger.Printf("%sno wal files found, snapshot only", opt.LogPrefix)
			break
		} else if err != nil {
			return fmt.Errorf("cannot download WAL: %w", err)
		}

		// Apply WAL file.
		startTime := time.Now()
		if err = ApplyWAL(ctx, tmpPath, walPath); err != nil {
			return fmt.Errorf("cannot apply wal: %w", err)
		}
		logger.Printf("%sapplied wal %s/%s elapsed=%s", opt.LogPrefix, generation, FormatIndex(walIndex), time.Since(startTime).String())
	}

	// Copy file to final location.
	logger.Printf("%srenaming database from temporary location", opt.LogPrefix)
	if err := os.Rename(tmpPath, filename); err != nil {
		return err
	}

	return nil
}

// RestoreOptions represents options for DB.Restore().
type RestoreOptions struct {
	// File info used for restored snapshot & WAL files.
	Mode     os.FileMode
	Uid, Gid int

	// Specifies how many WAL files are downloaded in parallel during restore.
	Parallelism int

	// Logging settings.
	Logger    *log.Logger
	LogPrefix string
}

// NewRestoreOptions returns a new instance of RestoreOptions with defaults.
func NewRestoreOptions() RestoreOptions {
	return RestoreOptions{
		Mode:        0600,
		Parallelism: DefaultRestoreParallelism,
	}
}

// RestoreSnapshot copies a snapshot from the replica client to a file.
func RestoreSnapshot(ctx context.Context, client ReplicaClient, filename, generation string, index int, mode os.FileMode, uid, gid int) error {
	f, err := internal.CreateFile(filename, mode, uid, gid)
	if err != nil {
		return err
	}
	defer f.Close()

	rd, err := client.SnapshotReader(ctx, generation, index)
	if err != nil {
		return err
	}
	defer rd.Close()

	if _, err := io.Copy(f, lz4.NewReader(rd)); err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}
