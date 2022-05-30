package litestream

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
)

// Default replica settings.
const (
	DefaultSyncInterval           = 1 * time.Second
	DefaultRetention              = 24 * time.Hour
	DefaultRetentionCheckInterval = 1 * time.Hour
)

// Replica connects a database to a replication destination via a ReplicaClient.
// The replica manages periodic synchronization and maintaining the current
// replica position.
type Replica struct {
	db   *DB
	name string

	mu  sync.RWMutex
	pos Pos // current replicated position
	itr *FileWALSegmentIterator

	muf sync.Mutex
	f   *os.File // long-running file descriptor to avoid non-OFD lock issues

	wg     sync.WaitGroup
	cancel func()

	// Client used to connect to the remote replica.
	client ReplicaClient

	// Time between syncs with the shadow WAL.
	SyncInterval time.Duration

	// Frequency to create new snapshots.
	SnapshotInterval time.Duration

	// Time to keep snapshots and related WAL files.
	// Database is snapshotted after interval, if needed, and older WAL files are discarded.
	Retention time.Duration

	// Time between checks for retention.
	RetentionCheckInterval time.Duration

	// Time between validation checks.
	ValidationInterval time.Duration

	// If true, replica monitors database for changes automatically.
	// Set to false if replica is being used synchronously (such as in tests).
	MonitorEnabled bool

	Logger *log.Logger
}

func NewReplica(db *DB, name string, client ReplicaClient) *Replica {
	r := &Replica{
		db:     db,
		name:   name,
		client: client,
		cancel: func() {},

		SyncInterval:           DefaultSyncInterval,
		Retention:              DefaultRetention,
		RetentionCheckInterval: DefaultRetentionCheckInterval,
		MonitorEnabled:         true,
	}

	prefix := fmt.Sprintf("%s: ", r.Name())
	if db != nil {
		prefix = fmt.Sprintf("%s(%s): ", logPrefixPath(db.Path()), r.Name())
	}
	r.Logger = log.New(LogWriter, prefix, LogFlags)

	return r
}

// Name returns the name of the replica.
func (r *Replica) Name() string {
	if r.name == "" && r.client != nil {
		return r.client.Type()
	}
	return r.name
}

// DB returns a reference to the database the replica is attached to, if any.
func (r *Replica) DB() *DB { return r.db }

// Client returns the client the replica was initialized with.
func (r *Replica) Client() ReplicaClient { return r.client }

// Starts replicating in a background goroutine.
func (r *Replica) Start(ctx context.Context) {
	// Ignore if replica is being used synchronously.
	if !r.MonitorEnabled {
		return
	}

	// Stop previous replication.
	r.Stop()

	// Wrap context with cancelation.
	ctx, r.cancel = context.WithCancel(ctx)

	// Start goroutine to replicate data.
	r.wg.Add(3)
	go func() { defer r.wg.Done(); r.monitor(ctx) }()
	go func() { defer r.wg.Done(); r.retainer(ctx) }()
	go func() { defer r.wg.Done(); r.snapshotter(ctx) }()
}

// Stop cancels any outstanding replication and blocks until finished.
func (r *Replica) Stop() {
	r.cancel()
	r.wg.Wait()

	if r.itr != nil {
		r.itr.Close()
		r.itr = nil
	}
}

// Close will close the DB file descriptor which could release locks on
// per-process locks (e.g. non-Linux OSes).
func (r *Replica) Close() (err error) {
	r.muf.Lock()
	defer r.muf.Unlock()
	if r.f != nil {
		if e := r.f.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// Sync copies new WAL frames from the shadow WAL to the replica client.
func (r *Replica) Sync(ctx context.Context) (err error) {
	// Clear last position if if an error occurs during sync.
	defer func() {
		if err != nil {
			r.mu.Lock()
			r.pos = Pos{}
			r.mu.Unlock()
		}
	}()

	// Find current position of database.
	dpos := r.db.Pos()
	if dpos.IsZero() {
		return ErrNoGeneration
	}
	generation := dpos.Generation

	// Close out iterator if the generation has changed.
	if r.itr != nil && r.itr.Generation() != generation {
		_ = r.itr.Close()
		r.itr = nil
	}

	// Ensure we obtain a WAL iterator before we snapshot so we don't miss any segments.
	resetItr := r.itr == nil
	if resetItr {
		if r.itr, err = r.db.WALSegments(ctx, generation); err != nil {
			return fmt.Errorf("wal segments: %w", err)
		}
	}

	// Create snapshot if no snapshots exist for generation.
	snapshotN, err := r.snapshotN(generation)
	if err != nil {
		return err
	} else if snapshotN == 0 {
		if info, err := r.Snapshot(ctx); err != nil {
			return err
		} else if info.Generation != generation {
			return fmt.Errorf("generation changed during snapshot, exiting sync")
		}
		snapshotN = 1
	}
	replicaSnapshotTotalGaugeVec.WithLabelValues(r.db.Path(), r.Name()).Set(float64(snapshotN))

	// Determine position, if necessary.
	if resetItr {
		pos, err := r.calcPos(ctx, generation)
		if err != nil {
			return fmt.Errorf("cannot determine replica position: %s", err)
		}

		r.mu.Lock()
		r.pos = pos
		r.mu.Unlock()
	}

	// Read all WAL files since the last position.
	if err = r.syncWAL(ctx); err != nil {
		return err
	}

	return nil
}

func (r *Replica) syncWAL(ctx context.Context) (err error) {
	pos := r.Pos()

	// Group segments by index.
	var segments [][]WALSegmentInfo
	for r.itr.Next() {
		info := r.itr.WALSegment()

		if cmp, err := ComparePos(pos, info.Pos()); err != nil {
			return fmt.Errorf("compare pos: %w", err)
		} else if cmp == 1 {
			continue // already processed, skip
		}

		// Start a new chunk if index has changed.
		if len(segments) == 0 || segments[len(segments)-1][0].Index != info.Index {
			segments = append(segments, []WALSegmentInfo{info})
			continue
		}

		// Add segment to the end of the current index, if matching.
		segments[len(segments)-1] = append(segments[len(segments)-1], info)
	}

	// Write out segments to replica by index so they can be combined.
	for i := range segments {
		if err := r.writeIndexSegments(ctx, segments[i]); err != nil {
			return fmt.Errorf("write index segments: index=%d err=%w", segments[i][0].Index, err)
		}
	}

	return nil
}

func (r *Replica) writeIndexSegments(ctx context.Context, segments []WALSegmentInfo) (err error) {
	assert(len(segments) > 0, "segments required for replication")

	// First segment position must be equal to last replica position or
	// the start of the next index.
	if pos := r.Pos(); pos != segments[0].Pos() {
		nextIndexPos := pos.Truncate()
		nextIndexPos.Index++
		if nextIndexPos != segments[0].Pos() {
			return fmt.Errorf("replica skipped position: replica=%s initial=%s", pos, segments[0].Pos())
		}
	}

	pos := segments[0].Pos()
	initialPos := pos

	// Copy shadow WAL to client write via io.Pipe().
	pr, pw := io.Pipe()
	defer func() { _ = pw.CloseWithError(err) }()

	// Copy through pipe into client from the starting position.
	var g errgroup.Group
	g.Go(func() error {
		_, err := r.client.WriteWALSegment(ctx, initialPos, pr)
		return err
	})

	// Wrap writer to LZ4 compress.
	zw := lz4.NewWriter(pw)

	// Write each segment out to the replica.
	for i := range segments {
		info := &segments[i]

		if err := func() error {
			// Ensure segments are in order and no bytes are skipped.
			if pos != info.Pos() {
				return fmt.Errorf("non-contiguous segment: expected=%s current=%s", pos, info.Pos())
			}

			rc, err := r.db.WALSegmentReader(ctx, info.Pos())
			if err != nil {
				return err
			}
			defer rc.Close()

			n, err := io.Copy(zw, lz4.NewReader(rc))
			if err != nil {
				return err
			} else if err := rc.Close(); err != nil {
				return err
			}

			// Track last position written.
			pos = info.Pos()
			pos.Offset += n

			return nil
		}(); err != nil {
			return fmt.Errorf("wal segment: pos=%s err=%w", info.Pos(), err)
		}
	}

	// Flush LZ4 writer, close pipe, and wait for write to finish.
	if err := zw.Close(); err != nil {
		return fmt.Errorf("lz4 writer close: %w", err)
	} else if err := pw.Close(); err != nil {
		return fmt.Errorf("pipe writer close: %w", err)
	} else if err := g.Wait(); err != nil {
		return err
	}

	// Save last replicated position.
	r.mu.Lock()
	r.pos = pos
	r.mu.Unlock()

	replicaWALBytesCounterVec.WithLabelValues(r.db.Path(), r.Name()).Add(float64(pos.Offset - initialPos.Offset))

	// Track total WAL bytes written to replica client.
	replicaWALIndexGaugeVec.WithLabelValues(r.db.Path(), r.Name()).Set(float64(pos.Index))
	replicaWALOffsetGaugeVec.WithLabelValues(r.db.Path(), r.Name()).Set(float64(pos.Offset))

	r.Logger.Printf("wal segment written: %s sz=%d", initialPos, pos.Offset-initialPos.Offset)

	return nil
}

// snapshotN returns the number of snapshots for a generation.
func (r *Replica) snapshotN(generation string) (int, error) {
	itr, err := r.client.Snapshots(context.Background(), generation)
	if err != nil {
		return 0, err
	}
	defer itr.Close()

	var n int
	for itr.Next() {
		n++
	}
	return n, itr.Close()
}

// calcPos returns the last position for the given generation.
func (r *Replica) calcPos(ctx context.Context, generation string) (pos Pos, err error) {
	// Fetch last snapshot. Return error if no snapshots exist.
	snapshot, err := r.maxSnapshot(ctx, generation)
	if err != nil {
		return pos, fmt.Errorf("max snapshot: %w", err)
	} else if snapshot == nil {
		return pos, fmt.Errorf("no snapshot available: generation=%s", generation)
	}

	// Determine last WAL segment available. Use snapshot if none exist.
	segment, err := r.maxWALSegment(ctx, generation)
	if err != nil {
		return pos, fmt.Errorf("max wal segment: %w", err)
	} else if segment == nil {
		return Pos{Generation: snapshot.Generation, Index: snapshot.Index}, nil
	}

	// Read segment to determine size to add to offset.
	rd, err := r.client.WALSegmentReader(ctx, segment.Pos())
	if err != nil {
		return pos, fmt.Errorf("wal segment reader: %w", err)
	}
	defer rd.Close()

	n, err := io.Copy(ioutil.Discard, lz4.NewReader(rd))
	if err != nil {
		return pos, err
	}

	// Return the position at the end of the last WAL segment.
	return Pos{
		Generation: segment.Generation,
		Index:      segment.Index,
		Offset:     segment.Offset + n,
	}, nil
}

// maxSnapshot returns the last snapshot in a generation.
func (r *Replica) maxSnapshot(ctx context.Context, generation string) (*SnapshotInfo, error) {
	itr, err := r.client.Snapshots(ctx, generation)
	if err != nil {
		return nil, err
	}
	defer itr.Close()

	var max *SnapshotInfo
	for itr.Next() {
		if info := itr.Snapshot(); max == nil || info.Index > max.Index {
			max = &info
		}
	}
	return max, itr.Close()
}

// maxWALSegment returns the highest WAL segment in a generation.
func (r *Replica) maxWALSegment(ctx context.Context, generation string) (*WALSegmentInfo, error) {
	itr, err := r.client.WALSegments(ctx, generation)
	if err != nil {
		return nil, err
	}
	defer itr.Close()

	var max *WALSegmentInfo
	for itr.Next() {
		if info := itr.WALSegment(); max == nil || info.Index > max.Index || (info.Index == max.Index && info.Offset > max.Offset) {
			max = &info
		}
	}
	return max, itr.Close()
}

// Pos returns the current replicated position.
// Returns a zero value if the current position cannot be determined.
func (r *Replica) Pos() Pos {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pos
}

// Snapshots returns a list of all snapshots across all generations.
func (r *Replica) Snapshots(ctx context.Context) ([]SnapshotInfo, error) {
	generations, err := r.client.Generations(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot fetch generations: %w", err)
	}

	var a []SnapshotInfo
	for _, generation := range generations {
		if err := func() error {
			itr, err := r.client.Snapshots(ctx, generation)
			if err != nil {
				return err
			}
			defer itr.Close()

			other, err := SliceSnapshotIterator(itr)
			if err != nil {
				return err
			}
			a = append(a, other...)

			return itr.Close()
		}(); err != nil {
			return a, err
		}
	}

	sort.Sort(SnapshotInfoSlice(a))

	return a, nil
}

// Snapshot copies the entire database to the replica path.
func (r *Replica) Snapshot(ctx context.Context) (info SnapshotInfo, err error) {
	if r.db == nil || r.db.db == nil {
		return info, fmt.Errorf("no database available")
	}

	r.muf.Lock()
	defer r.muf.Unlock()

	// Issue a passive checkpoint to flush any pages to disk before snapshotting.
	if _, err := r.db.db.ExecContext(ctx, `PRAGMA wal_checkpoint(PASSIVE);`); err != nil {
		return info, fmt.Errorf("pre-snapshot checkpoint: %w", err)
	}

	// Acquire a read lock on the database during snapshot to prevent checkpoints.
	tx, err := r.db.db.Begin()
	if err != nil {
		return info, err
	} else if _, err := tx.ExecContext(ctx, `SELECT COUNT(1) FROM _litestream_seq;`); err != nil {
		_ = tx.Rollback()
		return info, err
	}
	defer func() { _ = tx.Rollback() }()

	// Obtain current position.
	pos := r.db.Pos()
	if pos.IsZero() {
		return info, ErrNoGeneration
	}

	// Open db file descriptor, if not already open, & position at beginning.
	if r.f == nil {
		if r.f, err = os.Open(r.db.Path()); err != nil {
			return info, err
		}
	}
	if _, err := r.f.Seek(0, io.SeekStart); err != nil {
		return info, err
	}

	// Use a pipe to convert the LZ4 writer to a reader.
	pr, pw := io.Pipe()

	// Copy the database file to the LZ4 writer in a separate goroutine.
	var g errgroup.Group
	g.Go(func() error {
		zr := lz4.NewWriter(pw)
		defer zr.Close()

		if _, err := io.Copy(zr, r.f); err != nil {
			_ = pw.CloseWithError(err)
			return err
		} else if err := zr.Close(); err != nil {
			_ = pw.CloseWithError(err)
			return err
		}
		return pw.Close()
	})

	// Delegate write to client & wait for writer goroutine to finish.
	if info, err = r.client.WriteSnapshot(ctx, pos.Generation, pos.Index, pr); err != nil {
		return info, err
	} else if err := g.Wait(); err != nil {
		return info, err
	}

	r.Logger.Printf("snapshot written %s/%s", pos.Generation, FormatIndex(pos.Index))

	return info, nil
}

// EnforceRetention forces a new snapshot once the retention interval has passed.
// Older snapshots and WAL files are then removed.
func (r *Replica) EnforceRetention(ctx context.Context) (err error) {
	// Obtain list of snapshots that are within the retention period.
	snapshots, err := r.Snapshots(ctx)
	if err != nil {
		return fmt.Errorf("snapshots: %w", err)
	}
	retained := FilterSnapshotsAfter(snapshots, time.Now().Add(-r.Retention))

	// If no retained snapshots exist, create a new snapshot.
	if len(retained) == 0 {
		snapshot, err := r.Snapshot(ctx)
		if err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}
		retained = append(retained, snapshot)
	}

	// Loop over generations and delete unretained snapshots & WAL files.
	generations, err := r.client.Generations(ctx)
	if err != nil {
		return fmt.Errorf("generations: %w", err)
	}
	for _, generation := range generations {
		// Find earliest retained snapshot for this generation.
		snapshot := FindMinSnapshotByGeneration(retained, generation)

		// Delete entire generation if no snapshots are being retained.
		if snapshot == nil {
			if err := r.client.DeleteGeneration(ctx, generation); err != nil {
				return fmt.Errorf("delete generation: %w", err)
			}
			continue
		}

		// Otherwise remove all earlier snapshots & WAL segments.
		if err := r.deleteSnapshotsBeforeIndex(ctx, generation, snapshot.Index); err != nil {
			return fmt.Errorf("delete snapshots before index: %w", err)
		} else if err := r.deleteWALSegmentsBeforeIndex(ctx, generation, snapshot.Index); err != nil {
			return fmt.Errorf("delete wal segments before index: %w", err)
		}
	}

	return nil
}

func (r *Replica) deleteSnapshotsBeforeIndex(ctx context.Context, generation string, index int) error {
	itr, err := r.client.Snapshots(ctx, generation)
	if err != nil {
		return fmt.Errorf("fetch snapshots: %w", err)
	}
	defer itr.Close()

	for itr.Next() {
		info := itr.Snapshot()
		if info.Index >= index {
			continue
		}

		if err := r.client.DeleteSnapshot(ctx, info.Generation, info.Index); err != nil {
			return fmt.Errorf("delete snapshot %s/%s: %w", info.Generation, FormatIndex(info.Index), err)
		}
		r.Logger.Printf("snapshot deleted %s/%s", generation, FormatIndex(index))
	}

	return itr.Close()
}

func (r *Replica) deleteWALSegmentsBeforeIndex(ctx context.Context, generation string, index int) error {
	itr, err := r.client.WALSegments(ctx, generation)
	if err != nil {
		return fmt.Errorf("fetch wal segments: %w", err)
	}
	defer itr.Close()

	var a []Pos
	for itr.Next() {
		info := itr.WALSegment()
		if info.Index >= index {
			continue
		}
		a = append(a, info.Pos())
	}
	if err := itr.Close(); err != nil {
		return err
	}

	if len(a) == 0 {
		return nil
	}

	if err := r.client.DeleteWALSegments(ctx, a); err != nil {
		return fmt.Errorf("delete wal segments: %w", err)
	}

	for _, pos := range a {
		r.Logger.Printf("wal segmented deleted: %s", pos)
	}

	return nil
}

// monitor runs in a separate goroutine and continuously replicates the DB.
func (r *Replica) monitor(ctx context.Context) {
	timer := time.NewTimer(r.SyncInterval)
	defer timer.Stop()

	for {
		if err := r.Sync(ctx); ctx.Err() != nil {
			return
		} else if err != nil && err != ErrNoGeneration {
			r.Logger.Printf("monitor error: %s", err)
		}

		// Wait for a change to the WAL iterator.
		if r.itr != nil {
			select {
			case <-ctx.Done():
				return
			case <-r.itr.NotifyCh():
			}
		}

		// Wait for the sync interval to collect additional changes.
		timer.Reset(r.SyncInterval)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		// Flush any additional notifications from the WAL iterator.
		if r.itr != nil {
			select {
			case <-r.itr.NotifyCh():
			default:
			}
		}
	}
}

// retainer runs in a separate goroutine and handles retention.
func (r *Replica) retainer(ctx context.Context) {
	// Disable retention enforcement if retention period is non-positive.
	if r.Retention <= 0 {
		return
	}

	// Ensure check interval is not longer than retention period.
	checkInterval := r.RetentionCheckInterval
	if checkInterval > r.Retention {
		checkInterval = r.Retention
	}

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.EnforceRetention(ctx); err != nil {
				r.Logger.Printf("retainer error: %s", err)
				continue
			}
		}
	}
}

// snapshotter runs in a separate goroutine and handles snapshotting.
func (r *Replica) snapshotter(ctx context.Context) {
	if r.SnapshotInterval <= 0 {
		return
	}

	ticker := time.NewTicker(r.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.Snapshot(ctx); err != nil && err != ErrNoGeneration {
				r.Logger.Printf("snapshotter error: %s", err)
				continue
			}
		}
	}
}

// GenerationCreatedAt returns the earliest creation time of any snapshot.
// Returns zero time if no snapshots exist.
func (r *Replica) GenerationCreatedAt(ctx context.Context, generation string) (time.Time, error) {
	var min time.Time

	itr, err := r.client.Snapshots(ctx, generation)
	if err != nil {
		return min, err
	}
	defer itr.Close()

	for itr.Next() {
		if info := itr.Snapshot(); min.IsZero() || info.CreatedAt.Before(min) {
			min = info.CreatedAt
		}
	}
	return min, itr.Close()
}

// SnapshotIndexAt returns the highest index for a snapshot within a generation
// that occurs before timestamp. If timestamp is zero, returns the latest snapshot.
func (r *Replica) SnapshotIndexAt(ctx context.Context, generation string, timestamp time.Time) (int, error) {
	itr, err := r.client.Snapshots(ctx, generation)
	if err != nil {
		return 0, err
	}
	defer itr.Close()

	snapshotIndex := -1
	var max time.Time
	for itr.Next() {
		snapshot := itr.Snapshot()
		if !timestamp.IsZero() && snapshot.CreatedAt.After(timestamp) {
			continue // after timestamp, skip
		}

		// Use snapshot if it newer.
		if max.IsZero() || snapshot.CreatedAt.After(max) {
			snapshotIndex, max = snapshot.Index, snapshot.CreatedAt
		}
	}
	if err := itr.Close(); err != nil {
		return 0, err
	} else if snapshotIndex == -1 {
		return 0, ErrNoSnapshots
	}
	return snapshotIndex, nil
}

// LatestReplica returns the most recently updated replica.
func LatestReplica(ctx context.Context, replicas []*Replica) (*Replica, error) {
	var t time.Time
	var r *Replica
	for i := range replicas {
		_, max, err := ReplicaClientTimeBounds(ctx, replicas[i].client)
		if err != nil {
			return nil, err
		} else if r == nil || max.After(t) {
			r, t = replicas[i], max
		}
	}
	return r, nil
}

// Replica metrics.
var (
	replicaSnapshotTotalGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "litestream",
		Subsystem: "replica",
		Name:      "snapshot_total",
		Help:      "The current number of snapshots",
	}, []string{"db", "name"})

	replicaWALBytesCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "litestream",
		Subsystem: "replica",
		Name:      "wal_bytes",
		Help:      "The number wal bytes written",
	}, []string{"db", "name"})

	replicaWALIndexGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "litestream",
		Subsystem: "replica",
		Name:      "wal_index",
		Help:      "The current WAL index",
	}, []string{"db", "name"})

	replicaWALOffsetGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "litestream",
		Subsystem: "replica",
		Name:      "wal_offset",
		Help:      "The current WAL offset",
	}, []string{"db", "name"})
)
