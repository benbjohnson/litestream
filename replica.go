package litestream

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"filippo.io/age"
	"github.com/benbjohnson/litestream/internal"
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

	muf sync.Mutex
	f   *os.File // long-running file descriptor to avoid non-OFD lock issues

	wg     sync.WaitGroup
	cancel func()

	// Client used to connect to the remote replica.
	Client ReplicaClient

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

	// Encryption identities and recipients
	AgeIdentities []age.Identity
	AgeRecipients []age.Recipient
}

func NewReplica(db *DB, name string) *Replica {
	r := &Replica{
		db:     db,
		name:   name,
		cancel: func() {},

		SyncInterval:           DefaultSyncInterval,
		Retention:              DefaultRetention,
		RetentionCheckInterval: DefaultRetentionCheckInterval,
		MonitorEnabled:         true,
	}

	return r
}

// Name returns the name of the replica.
func (r *Replica) Name() string {
	if r.name == "" && r.Client != nil {
		return r.Client.Type()
	}
	return r.name
}

// Logger returns the DB sub-logger for this replica.
func (r *Replica) Logger() *slog.Logger {
	logger := slog.Default()
	if r.db != nil {
		logger = r.db.Logger
	}
	return logger.With("replica", r.Name())
}

// DB returns a reference to the database the replica is attached to, if any.
func (r *Replica) DB() *DB { return r.db }

// Starts replicating in a background goroutine.
func (r *Replica) Start(ctx context.Context) error {
	// Ignore if replica is being used sychronously.
	if !r.MonitorEnabled {
		return nil
	}

	// Stop previous replication.
	r.Stop(false)

	// Wrap context with cancelation.
	ctx, r.cancel = context.WithCancel(ctx)

	// Start goroutine to replicate data.
	r.wg.Add(4)
	go func() { defer r.wg.Done(); r.monitor(ctx) }()
	go func() { defer r.wg.Done(); r.retainer(ctx) }()
	go func() { defer r.wg.Done(); r.snapshotter(ctx) }()
	go func() { defer r.wg.Done(); r.validator(ctx) }()

	return nil
}

// Stop cancels any outstanding replication and blocks until finished.
//
// Performing a hard stop will close the DB file descriptor which could release
// locks on per-process locks. Hard stops should only be performed when
// stopping the entire process.
func (r *Replica) Stop(hard bool) (err error) {
	r.cancel()
	r.wg.Wait()

	r.muf.Lock()
	defer r.muf.Unlock()
	if hard && r.f != nil {
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
	dpos, err := r.db.Pos()
	if err != nil {
		return fmt.Errorf("cannot determine current generation: %w", err)
	} else if dpos.IsZero() {
		return fmt.Errorf("no generation, waiting for data")
	}
	generation := dpos.Generation

	r.Logger().Debug("replica sync", "position", dpos.String())

	// Create a new snapshot and update the current replica position if
	// the generation on the database has changed.
	if r.Pos().Generation != generation {
		// Create snapshot if no snapshots exist for generation.
		snapshotN, err := r.snapshotN(ctx, generation)
		if err != nil {
			return err
		} else if snapshotN == 0 {
			if info, err := r.Snapshot(ctx); err != nil {
				return err
			} else if info.Generation != generation {
				return fmt.Errorf("generation changed during snapshot, exiting sync")
			}
		}

		pos, err := r.calcPos(ctx, generation)
		if err != nil {
			return fmt.Errorf("cannot determine replica position: %s", err)
		}

		r.Logger().Debug("replica sync: calc new pos", "position", pos.String())
		r.mu.Lock()
		r.pos = pos
		r.mu.Unlock()
	}

	// Read all WAL files since the last position.
	for {
		if err = r.syncWAL(ctx); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	return nil
}

func (r *Replica) syncWAL(ctx context.Context) (err error) {
	rd, err := r.db.ShadowWALReader(r.Pos())
	if err == io.EOF {
		return err
	} else if err != nil {
		return fmt.Errorf("replica wal reader: %w", err)
	}
	defer rd.Close()

	// Copy shadow WAL to client write via io.Pipe().
	pr, pw := io.Pipe()
	defer func() { _ = pw.CloseWithError(err) }()

	// Obtain initial position from shadow reader.
	// It may have moved to the next index if previous position was at the end.
	pos := rd.Pos()
	initialPos := pos
	startTime := time.Now()
	var bytesWritten int

	logger := r.Logger()
	logger.Info("write wal segment", "position", initialPos.String())

	// Copy through pipe into client from the starting position.
	var g errgroup.Group
	g.Go(func() error {
		_, err := r.Client.WriteWALSegment(ctx, pos, pr)

		// Always close pipe reader to signal writers.
		if e := pr.CloseWithError(err); err == nil {
			return e
		}

		return err
	})

	var ew io.WriteCloser = pw

	// Add encryption if we have recipients.
	if len(r.AgeRecipients) > 0 {
		var err error
		ew, err = age.Encrypt(pw, r.AgeRecipients...)
		if err != nil {
			return err
		}
		defer ew.Close()
	}

	// Wrap writer to LZ4 compress.
	zw := lz4.NewWriter(ew)

	// Track total WAL bytes written to replica client.
	walBytesCounter := replicaWALBytesCounterVec.WithLabelValues(r.db.Path(), r.Name())

	// Copy header if at offset zero.
	var psalt uint64 // previous salt value
	if pos := rd.Pos(); pos.Offset == 0 {
		buf := make([]byte, WALHeaderSize)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return err
		}

		psalt = binary.BigEndian.Uint64(buf[16:24])

		n, err := zw.Write(buf)
		if err != nil {
			return err
		}
		walBytesCounter.Add(float64(n))
		bytesWritten += n
	}

	// Copy frames.
	for {
		pos := rd.Pos()
		assert(pos.Offset == frameAlign(pos.Offset, r.db.pageSize), "shadow wal reader not frame aligned")

		buf := make([]byte, WALFrameHeaderSize+r.db.pageSize)
		if _, err := io.ReadFull(rd, buf); err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Verify salt matches the previous frame/header read.
		salt := binary.BigEndian.Uint64(buf[8:16])
		if psalt != 0 && psalt != salt {
			return fmt.Errorf("replica salt mismatch: %s", pos.String())
		}
		psalt = salt

		n, err := zw.Write(buf)
		if err != nil {
			return err
		}
		walBytesCounter.Add(float64(n))
		bytesWritten += n
	}

	// Flush LZ4 writer, encryption writer and close pipe.
	if err := zw.Close(); err != nil {
		return err
	} else if err := ew.Close(); err != nil {
		return err
	} else if err := pw.Close(); err != nil {
		return err
	}

	// Wait for client to finish write.
	if err := g.Wait(); err != nil {
		return fmt.Errorf("client write: %w", err)
	}

	// Save last replicated position.
	r.mu.Lock()
	r.pos = rd.Pos()
	r.mu.Unlock()

	// Track current position
	replicaWALIndexGaugeVec.WithLabelValues(r.db.Path(), r.Name()).Set(float64(rd.Pos().Index))
	replicaWALOffsetGaugeVec.WithLabelValues(r.db.Path(), r.Name()).Set(float64(rd.Pos().Offset))

	logger.Info("wal segment written", "position", initialPos.String(), "elapsed", time.Since(startTime).String(), "sz", bytesWritten)
	return nil
}

// snapshotN returns the number of snapshots for a generation.
func (r *Replica) snapshotN(ctx context.Context, generation string) (int, error) {
	itr, err := r.Client.Snapshots(ctx, generation)
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
	rd, err := r.Client.WALSegmentReader(ctx, segment.Pos())
	if err != nil {
		return pos, fmt.Errorf("wal segment reader: %w", err)
	}
	defer rd.Close()

	if len(r.AgeIdentities) > 0 {
		drd, err := age.Decrypt(rd, r.AgeIdentities...)
		if err != nil {
			return pos, err
		}

		rd = io.NopCloser(drd)
	}

	n, err := io.Copy(io.Discard, lz4.NewReader(rd))
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
	itr, err := r.Client.Snapshots(ctx, generation)
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
	itr, err := r.Client.WALSegments(ctx, generation)
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
	generations, err := r.Client.Generations(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot fetch generations: %w", err)
	}

	var a []SnapshotInfo
	for _, generation := range generations {
		if err := func() error {
			itr, err := r.Client.Snapshots(ctx, generation)
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
	if err := r.db.Checkpoint(ctx, CheckpointModePassive); err != nil {
		return info, fmt.Errorf("pre-snapshot checkpoint: %w", err)
	}

	// Prevent internal checkpoints during snapshot.
	r.db.BeginSnapshot()
	defer r.db.EndSnapshot()

	// Acquire a read lock on the database during snapshot to prevent external checkpoints.
	tx, err := r.db.db.Begin()
	if err != nil {
		return info, err
	} else if _, err := tx.ExecContext(ctx, `SELECT COUNT(1) FROM _litestream_seq;`); err != nil {
		_ = tx.Rollback()
		return info, err
	}
	defer func() { _ = tx.Rollback() }()

	// Obtain current position.
	pos, err := r.db.Pos()
	if err != nil {
		return info, fmt.Errorf("cannot determine db position: %w", err)
	} else if pos.IsZero() {
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
		// We need to ensure the pipe is closed.
		defer pw.Close()

		var wc io.WriteCloser = pw

		// Add encryption if we have recipients.
		if len(r.AgeRecipients) > 0 {
			var err error
			wc, err = age.Encrypt(pw, r.AgeRecipients...)
			if err != nil {
				pw.CloseWithError(err)
				return err
			}
			defer wc.Close()
		}

		zr := lz4.NewWriter(wc)
		defer zr.Close()

		if _, err := io.Copy(zr, r.f); err != nil {
			pw.CloseWithError(err)
			return err
		} else if err := zr.Close(); err != nil {
			pw.CloseWithError(err)
			return err
		}
		return wc.Close()
	})

	logger := r.Logger()
	logger.Info("write snapshot", "position", pos.String())

	startTime := time.Now()
	// Delegate write to client & wait for writer goroutine to finish.
	if info, err = r.Client.WriteSnapshot(ctx, pos.Generation, pos.Index, pr); err != nil {
		return info, err
	} else if err := g.Wait(); err != nil {
		return info, err
	}

	logger.Info("snapshot written", "position", pos.String(), "elapsed", time.Since(startTime).String(), "sz", info.Size)
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
	generations, err := r.Client.Generations(ctx)
	if err != nil {
		return fmt.Errorf("generations: %w", err)
	}
	for _, generation := range generations {
		// Find earliest retained snapshot for this generation.
		snapshot := FindMinSnapshotByGeneration(retained, generation)

		// Delete entire generation if no snapshots are being retained.
		if snapshot == nil {
			if err := r.Client.DeleteGeneration(ctx, generation); err != nil {
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
	itr, err := r.Client.Snapshots(ctx, generation)
	if err != nil {
		return fmt.Errorf("fetch snapshots: %w", err)
	}
	defer itr.Close()

	for itr.Next() {
		info := itr.Snapshot()
		if info.Index >= index {
			continue
		}

		if err := r.Client.DeleteSnapshot(ctx, info.Generation, info.Index); err != nil {
			return fmt.Errorf("delete snapshot %s/%08x: %w", info.Generation, info.Index, err)
		}
		r.Logger().Info("snapshot deleted", "generation", generation, "index", index)
	}

	return itr.Close()
}

func (r *Replica) deleteWALSegmentsBeforeIndex(ctx context.Context, generation string, index int) error {
	itr, err := r.Client.WALSegments(ctx, generation)
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

	if err := r.Client.DeleteWALSegments(ctx, a); err != nil {
		return fmt.Errorf("delete wal segments: %w", err)
	}

	r.Logger().Info("wal segmented deleted before", "generation", generation, "index", index, "n", len(a))
	return nil
}

// monitor runs in a separate goroutine and continuously replicates the DB.
func (r *Replica) monitor(ctx context.Context) {
	ticker := time.NewTicker(r.SyncInterval)
	defer ticker.Stop()

	// Continuously check for new data to replicate.
	ch := make(chan struct{})
	close(ch)
	var notify <-chan struct{} = ch

	for initial := true; ; initial = false {
		// Enforce a minimum time between synchronization.
		if !initial {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}

		// Wait for changes to the database.
		select {
		case <-ctx.Done():
			return
		case <-notify:
		}

		// Fetch new notify channel before replicating data.
		notify = r.db.Notify()

		// Synchronize the shadow wal into the replication directory.
		if err := r.Sync(ctx); err != nil {
			r.Logger().Error("monitor error", "error", err)
			continue
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
				r.Logger().Error("retainer error", "error", err)
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

	logger := r.Logger()
	if pos, err := r.db.Pos(); err != nil {
		logger.Error("snapshotter cannot determine generation", "error", err)
	} else if !pos.IsZero() {
		if snapshot, err := r.maxSnapshot(ctx, pos.Generation); err != nil {
			logger.Error("snapshotter cannot determine latest snapshot", "error", err)
		} else if snapshot != nil {
			nextSnapshot := r.SnapshotInterval - time.Since(snapshot.CreatedAt)
			if nextSnapshot < 0 {
				nextSnapshot = 0
			}

			logger.Info("snapshot interval adjusted", "previous", snapshot.CreatedAt.Format(time.RFC3339), "next", nextSnapshot.String())

			select {
			case <-ctx.Done():
				return
			case <-time.After(nextSnapshot):
				if _, err := r.Snapshot(ctx); err != nil && err != ErrNoGeneration {
					logger.Error("snapshotter error", "error", err)
				}
			}
		}
	}

	ticker := time.NewTicker(r.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.Snapshot(ctx); err != nil && err != ErrNoGeneration {
				r.Logger().Error("snapshotter error", "error", err)
				continue
			}
		}
	}
}

// validator runs in a separate goroutine and handles periodic validation.
func (r *Replica) validator(ctx context.Context) {
	// Initialize counters since validation occurs infrequently.
	for _, status := range []string{"ok", "error"} {
		replicaValidationTotalCounterVec.WithLabelValues(r.db.Path(), r.Name(), status).Add(0)
	}

	// Exit validation if interval is not set.
	if r.ValidationInterval <= 0 {
		return
	}

	ticker := time.NewTicker(r.ValidationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Validate(ctx); err != nil {
				r.Logger().Error("validation error", "error", err)
				continue
			}
		}
	}
}

// Validate restores the most recent data from a replica and validates
// that the resulting database matches the current database.
func (r *Replica) Validate(ctx context.Context) error {
	db := r.DB()

	// Restore replica to a temporary directory.
	tmpdir, err := os.MkdirTemp("", "*-litestream")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpdir)

	// Compute checksum of primary database under lock. This prevents a
	// sync from occurring and the database will not be written.
	chksum0, pos, err := db.CRC64(ctx)
	if err != nil {
		return fmt.Errorf("cannot compute checksum: %w", err)
	}

	// Wait until replica catches up to position.
	if err := r.waitForReplica(ctx, pos); err != nil {
		return fmt.Errorf("cannot wait for replica: %w", err)
	}

	restorePath := filepath.Join(tmpdir, "replica")
	if err := r.Restore(ctx, RestoreOptions{
		OutputPath:  restorePath,
		ReplicaName: r.Name(),
		Generation:  pos.Generation,
		Index:       pos.Index - 1,
	}); err != nil {
		return fmt.Errorf("cannot restore: %w", err)
	}

	// Open file handle for restored database.
	// NOTE: This open is ok as the restored database is not managed by litestream.
	f, err := os.Open(restorePath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Read entire file into checksum.
	h := crc64.New(crc64.MakeTable(crc64.ISO))
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	chksum1 := h.Sum64()

	status := "ok"
	mismatch := chksum0 != chksum1
	if mismatch {
		status = "mismatch"
	}
	r.Logger().Info("validator", "status", status, "db", fmt.Sprintf("%016x", chksum0), "replica", fmt.Sprintf("%016x", chksum1), "position", pos.String())

	// Validate checksums match.
	if mismatch {
		replicaValidationTotalCounterVec.WithLabelValues(r.db.Path(), r.Name(), "error").Inc()
		return ErrChecksumMismatch
	}

	replicaValidationTotalCounterVec.WithLabelValues(r.db.Path(), r.Name(), "ok").Inc()

	if err := os.RemoveAll(tmpdir); err != nil {
		return fmt.Errorf("cannot remove temporary validation directory: %w", err)
	}
	return nil
}

// waitForReplica blocks until replica reaches at least the given position.
func (r *Replica) waitForReplica(ctx context.Context, pos Pos) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	once := make(chan struct{}, 1)
	once <- struct{}{}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("replica wait exceeded timeout")
		case <-ticker.C:
		case <-once: // immediate on first check
		}

		// Obtain current position of replica, check if past target position.
		curr := r.Pos()
		if curr.IsZero() {
			r.Logger().Info("validator: no replica position available")
			continue
		}

		// Exit if the generation has changed while waiting as there will be
		// no further progress on the old generation.
		if curr.Generation != pos.Generation {
			return fmt.Errorf("generation changed")
		}

		ready := true
		if curr.Index < pos.Index {
			ready = false
		} else if curr.Index == pos.Index && curr.Offset < pos.Offset {
			ready = false
		}

		// If not ready, restart loop.
		if !ready {
			continue
		}

		// Current position at or after target position.
		return nil
	}
}

// GenerationCreatedAt returns the earliest creation time of any snapshot.
// Returns zero time if no snapshots exist.
func (r *Replica) GenerationCreatedAt(ctx context.Context, generation string) (time.Time, error) {
	var min time.Time

	itr, err := r.Client.Snapshots(ctx, generation)
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

// GenerationTimeBounds returns the creation time & last updated time of a generation.
// Returns zero time if no snapshots or WAL segments exist.
func (r *Replica) GenerationTimeBounds(ctx context.Context, generation string) (createdAt, updatedAt time.Time, err error) {
	// Iterate over snapshots.
	sitr, err := r.Client.Snapshots(ctx, generation)
	if err != nil {
		return createdAt, updatedAt, err
	}
	defer sitr.Close()

	minIndex, maxIndex := -1, -1
	for sitr.Next() {
		info := sitr.Snapshot()
		if createdAt.IsZero() || info.CreatedAt.Before(createdAt) {
			createdAt = info.CreatedAt
		}
		if updatedAt.IsZero() || info.CreatedAt.After(updatedAt) {
			updatedAt = info.CreatedAt
		}
		if minIndex == -1 || info.Index < minIndex {
			minIndex = info.Index
		}
		if info.Index > maxIndex {
			maxIndex = info.Index
		}
	}
	if err := sitr.Close(); err != nil {
		return createdAt, updatedAt, err
	}

	// Iterate over WAL segments.
	witr, err := r.Client.WALSegments(ctx, generation)
	if err != nil {
		return createdAt, updatedAt, err
	}
	defer witr.Close()

	for witr.Next() {
		info := witr.WALSegment()
		if info.Index < minIndex || info.Index > maxIndex {
			continue
		}
		if createdAt.IsZero() || info.CreatedAt.Before(createdAt) {
			createdAt = info.CreatedAt
		}
		if updatedAt.IsZero() || info.CreatedAt.After(updatedAt) {
			updatedAt = info.CreatedAt
		}
	}
	if err := witr.Close(); err != nil {
		return createdAt, updatedAt, err
	}

	return createdAt, updatedAt, nil
}

// CalcRestoreTarget returns a generation to restore from.
func (r *Replica) CalcRestoreTarget(ctx context.Context, opt RestoreOptions) (generation string, updatedAt time.Time, err error) {
	var target struct {
		generation string
		updatedAt  time.Time
	}

	generations, err := r.Client.Generations(ctx)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("cannot fetch generations: %w", err)
	}

	// Search generations for one that contains the requested timestamp.
	for _, generation := range generations {
		// Skip generation if it does not match filter.
		if opt.Generation != "" && generation != opt.Generation {
			continue
		}

		// Determine the time bounds for the generation.
		createdAt, updatedAt, err := r.GenerationTimeBounds(ctx, generation)
		if err != nil {
			return "", time.Time{}, fmt.Errorf("generation created at: %w", err)
		}

		// Skip if it does not contain timestamp.
		if !opt.Timestamp.IsZero() {
			if opt.Timestamp.Before(createdAt) || opt.Timestamp.After(updatedAt) {
				continue
			}
		}

		// Use the latest replica if we have multiple candidates.
		if !updatedAt.After(target.updatedAt) {
			continue
		}

		target.generation = generation
		target.updatedAt = updatedAt
	}

	return target.generation, target.updatedAt, nil
}

// Replica restores the database from a replica based on the options given.
// This method will restore into opt.OutputPath, if specified, or into the
// DB's original database path. It can optionally restore from a specific
// replica or generation or it will automatically choose the best one. Finally,
// a timestamp can be specified to restore the database to a specific
// point-in-time.
func (r *Replica) Restore(ctx context.Context, opt RestoreOptions) (err error) {
	// Validate options.
	if opt.OutputPath == "" {
		return fmt.Errorf("output path required")
	} else if opt.Generation == "" && opt.Index != math.MaxInt32 {
		return fmt.Errorf("must specify generation when restoring to index")
	} else if opt.Index != math.MaxInt32 && !opt.Timestamp.IsZero() {
		return fmt.Errorf("cannot specify index & timestamp to restore")
	}

	// Ensure output path does not already exist.
	if _, err := os.Stat(opt.OutputPath); err == nil {
		return fmt.Errorf("cannot restore, output path already exists: %s", opt.OutputPath)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Find lastest snapshot that occurs before timestamp or index.
	var minWALIndex int
	if opt.Index < math.MaxInt32 {
		if minWALIndex, err = r.SnapshotIndexByIndex(ctx, opt.Generation, opt.Index); err != nil {
			return fmt.Errorf("cannot find snapshot index: %w", err)
		}
	} else {
		if minWALIndex, err = r.SnapshotIndexAt(ctx, opt.Generation, opt.Timestamp); err != nil {
			return fmt.Errorf("cannot find snapshot index by timestamp: %w", err)
		}
	}

	// Compute list of offsets for each WAL index.
	walSegmentMap, err := r.walSegmentMap(ctx, opt.Generation, minWALIndex, opt.Index, opt.Timestamp)
	if err != nil {
		return fmt.Errorf("cannot find max wal index for restore: %w", err)
	}

	// Find the maximum WAL index that occurs before timestamp.
	maxWALIndex := -1
	for index := range walSegmentMap {
		if index > maxWALIndex {
			maxWALIndex = index
		}
	}

	// Ensure that we found the specific index, if one was specified.
	if opt.Index != math.MaxInt32 && opt.Index != maxWALIndex {
		return fmt.Errorf("unable to locate index %d in generation %q, highest index was %d", opt.Index, opt.Generation, maxWALIndex)
	}

	// If no WAL files were found, mark this as a snapshot-only restore.
	snapshotOnly := maxWALIndex == -1

	// Initialize starting position.
	pos := Pos{Generation: opt.Generation, Index: minWALIndex}
	tmpPath := opt.OutputPath + ".tmp"

	// Copy snapshot to output path.
	r.Logger().Info("restoring snapshot", "generation", opt.Generation, "index", minWALIndex, "path", tmpPath)
	if err := r.restoreSnapshot(ctx, pos.Generation, pos.Index, tmpPath); err != nil {
		return fmt.Errorf("cannot restore snapshot: %w", err)
	}

	// If no WAL files available, move snapshot to final path & exit early.
	if snapshotOnly {
		r.Logger().Info("snapshot only, finalizing database")
		return os.Rename(tmpPath, opt.OutputPath)
	}

	// Begin processing WAL files.
	r.Logger().Info("restoring wal files", "generation", opt.Generation, "index_min", minWALIndex, "index_max", maxWALIndex)

	// Fill input channel with all WAL indexes to be loaded in order.
	// Verify every index has at least one offset.
	ch := make(chan int, maxWALIndex-minWALIndex+1)
	for index := minWALIndex; index <= maxWALIndex; index++ {
		if len(walSegmentMap[index]) == 0 {
			return fmt.Errorf("missing WAL index: %s/%08x", opt.Generation, index)
		}
		ch <- index
	}
	close(ch)

	// Track load state for each WAL.
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	walStates := make([]walRestoreState, maxWALIndex-minWALIndex+1)

	parallelism := opt.Parallelism
	if parallelism < 1 {
		parallelism = 1
	}

	// Download WAL files to disk in parallel.
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < parallelism; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					cond.Broadcast()
					return err
				case index, ok := <-ch:
					if !ok {
						cond.Broadcast()
						return nil
					}

					startTime := time.Now()

					err := r.downloadWAL(ctx, opt.Generation, index, walSegmentMap[index], tmpPath)
					if err != nil {
						err = fmt.Errorf("cannot download wal %s/%08x: %w", opt.Generation, index, err)
					}

					// Mark index as ready-to-apply and notify applying code.
					mu.Lock()
					walStates[index-minWALIndex] = walRestoreState{ready: true, err: err}
					mu.Unlock()
					cond.Broadcast()

					// Returning the error here will cancel the other goroutines.
					if err != nil {
						return err
					}

					r.Logger().Info("downloaded wal",
						"generation", opt.Generation, "index", index,
						"elapsed", time.Since(startTime).String(),
					)
				}
			}
		})
	}

	// Apply WAL files in order as they are ready.
	for index := minWALIndex; index <= maxWALIndex; index++ {
		// Wait until next WAL file is ready to apply.
		mu.Lock()
		for !walStates[index-minWALIndex].ready {
			if err := ctx.Err(); err != nil {
				return err
			}
			cond.Wait()
		}
		if err := walStates[index-minWALIndex].err; err != nil {
			return err
		}
		mu.Unlock()

		// Apply WAL to database file.
		startTime := time.Now()
		if err = applyWAL(ctx, index, tmpPath); err != nil {
			return fmt.Errorf("cannot apply wal: %w", err)
		}
		r.Logger().Info("applied wal", "generation", opt.Generation, "index", index, "elapsed", time.Since(startTime).String())
	}

	// Ensure all goroutines finish. All errors should have been handled during
	// the processing of WAL files but this ensures that all processing is done.
	if err := g.Wait(); err != nil {
		return err
	}

	// Copy file to final location.
	r.Logger().Info("renaming database from temporary location")
	if err := os.Rename(tmpPath, opt.OutputPath); err != nil {
		return err
	}

	return nil
}

type walRestoreState struct {
	ready bool
	err   error
}

// SnapshotIndexAt returns the highest index for a snapshot within a generation
// that occurs before timestamp. If timestamp is zero, returns the latest snapshot.
func (r *Replica) SnapshotIndexAt(ctx context.Context, generation string, timestamp time.Time) (int, error) {
	itr, err := r.Client.Snapshots(ctx, generation)
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

// SnapshotIndexbyIndex returns the highest index for a snapshot within a generation
// that occurs before a given index. If index is MaxInt32, returns the latest snapshot.
func (r *Replica) SnapshotIndexByIndex(ctx context.Context, generation string, index int) (int, error) {
	itr, err := r.Client.Snapshots(ctx, generation)
	if err != nil {
		return 0, err
	}
	defer itr.Close()

	snapshotIndex := -1
	for itr.Next() {
		snapshot := itr.Snapshot()

		if index < math.MaxInt32 && snapshot.Index > index {
			continue // after index, skip
		}

		// Use snapshot if it newer.
		if snapshotIndex == -1 || snapshot.Index >= snapshotIndex {
			snapshotIndex = snapshot.Index
		}
	}
	if err := itr.Close(); err != nil {
		return 0, err
	} else if snapshotIndex == -1 {
		return 0, ErrNoSnapshots
	}
	return snapshotIndex, nil
}

// walSegmentMap returns a map of WAL indices to their segments.
// Filters by a max timestamp or a max index.
func (r *Replica) walSegmentMap(ctx context.Context, generation string, minIndex, maxIndex int, maxTimestamp time.Time) (map[int][]int64, error) {
	itr, err := r.Client.WALSegments(ctx, generation)
	if err != nil {
		return nil, err
	}
	defer itr.Close()

	a := []WALSegmentInfo{}
	for itr.Next() {
		a = append(a, itr.WALSegment())
	}

	sort.Sort(WALSegmentInfoSlice(a))

	m := make(map[int][]int64)
	for _, info := range a {
		// Exit if we go past the max timestamp or index.
		if !maxTimestamp.IsZero() && info.CreatedAt.After(maxTimestamp) {
			break // after max timestamp, skip
		} else if info.Index > maxIndex {
			break // after max index, skip
		} else if info.Index < minIndex {
			continue // before min index, continue
		}

		// Verify offsets are added in order.
		offsets := m[info.Index]
		if len(offsets) == 0 && info.Offset != 0 {
			return nil, fmt.Errorf("missing initial wal segment: generation=%s index=%08x offset=%d", generation, info.Index, info.Offset)
		} else if len(offsets) > 0 && offsets[len(offsets)-1] >= info.Offset {
			return nil, fmt.Errorf("wal segments out of order: generation=%s index=%08x offsets=(%d,%d)", generation, info.Index, offsets[len(offsets)-1], info.Offset)
		}

		// Append to the end of the WAL file.
		m[info.Index] = append(offsets, info.Offset)
	}
	return m, itr.Close()
}

// restoreSnapshot copies a snapshot from the replica to a file.
func (r *Replica) restoreSnapshot(ctx context.Context, generation string, index int, filename string) error {
	// Determine the user/group & mode based on the DB, if available.
	var fileInfo, dirInfo os.FileInfo
	if db := r.DB(); db != nil {
		fileInfo, dirInfo = db.fileInfo, db.dirInfo
	}

	if err := internal.MkdirAll(filepath.Dir(filename), dirInfo); err != nil {
		return err
	}

	f, err := internal.CreateFile(filename, fileInfo)
	if err != nil {
		return err
	}
	defer f.Close()

	rd, err := r.Client.SnapshotReader(ctx, generation, index)
	if err != nil {
		return err
	}
	defer rd.Close()

	if len(r.AgeIdentities) > 0 {
		drd, err := age.Decrypt(rd, r.AgeIdentities...)
		if err != nil {
			return err
		}

		rd = io.NopCloser(drd)
	}

	if _, err := io.Copy(f, lz4.NewReader(rd)); err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

// downloadWAL copies a WAL file from the replica to a local copy next to the DB.
// The WAL is later applied by applyWAL(). This function can be run in parallel
// to download multiple WAL files simultaneously.
func (r *Replica) downloadWAL(ctx context.Context, generation string, index int, offsets []int64, dbPath string) (err error) {
	// Determine the user/group & mode based on the DB, if available.
	var fileInfo os.FileInfo
	if db := r.DB(); db != nil {
		fileInfo = db.fileInfo
	}

	// Open readers for every segment in the WAL file, in order.
	var readers []io.Reader
	for _, offset := range offsets {
		rd, err := r.Client.WALSegmentReader(ctx, Pos{Generation: generation, Index: index, Offset: offset})
		if err != nil {
			return err
		}
		defer rd.Close()

		if len(r.AgeIdentities) > 0 {
			drd, err := age.Decrypt(rd, r.AgeIdentities...)
			if err != nil {
				return err
			}

			rd = io.NopCloser(drd)
		}

		readers = append(readers, lz4.NewReader(rd))
	}

	// Open handle to destination WAL path.
	f, err := internal.CreateFile(fmt.Sprintf("%s-%08x-wal", dbPath, index), fileInfo)
	if err != nil {
		return err
	}
	defer f.Close()

	// Combine segments together and copy WAL to target path.
	if _, err := io.Copy(f, io.MultiReader(readers...)); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}
	return nil
}

// Replica metrics.
var (
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

	replicaValidationTotalCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "litestream",
		Subsystem: "replica",
		Name:      "validation_total",
		Help:      "The number of validations performed",
	}, []string{"db", "name", "status"})
)
