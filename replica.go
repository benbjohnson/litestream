package litestream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/superfly/ltx"
	"golang.org/x/sync/semaphore"
)

// Default replica settings.
const (
	DefaultSyncInterval    = 1 * time.Second
	DefaultMaxSyncLTXFiles = 256
)

var errReplicaWaitForData = errors.New("no position, waiting for data")

// Replica connects a database to a replication destination via a ReplicaClient.
// The replica manages periodic synchronization and maintaining the current
// replica position.
type Replica struct {
	db *DB

	mu  sync.RWMutex
	pos ltx.Pos // current replicated position

	syncSem     *semaphore.Weighted
	syncWaiters atomic.Int64 // diagnostic instrumentation: goroutines queued on syncSem

	muf sync.Mutex
	f   *os.File // long-running file descriptor to avoid non-OFD lock issues

	wg     sync.WaitGroup
	cancel func()

	// Client used to connect to the remote replica.
	Client ReplicaClient

	// Time between syncs with the shadow WAL.
	SyncInterval time.Duration

	// Maximum L0 files to upload in a single monitor sync batch.
	// Set to zero to process all pending L0 files in one batch.
	MaxSyncLTXFiles int

	// If true, replica monitors database for changes automatically.
	// Set to false if replica is being used synchronously (such as in tests).
	MonitorEnabled bool

	// If true, automatically reset local state when LTX errors are detected.
	// This allows recovery from corrupted/missing LTX files by resetting
	// the position file and removing local LTX files, forcing a fresh sync.
	// Disabled by default to prevent silent data loss scenarios.
	AutoRecoverEnabled bool
}

func NewReplica(db *DB) *Replica {
	r := &Replica{
		db:      db,
		syncSem: semaphore.NewWeighted(1),
		cancel:  func() {},

		SyncInterval:    DefaultSyncInterval,
		MaxSyncLTXFiles: DefaultMaxSyncLTXFiles,
		MonitorEnabled:  true,
	}

	return r
}

func NewReplicaWithClient(db *DB, client ReplicaClient) *Replica {
	r := NewReplica(db)
	r.Client = client
	return r
}

// Logger returns the DB sub-logger for this replica.
func (r *Replica) Logger() *slog.Logger {
	logger := slog.Default()
	if r.db != nil {
		logger = r.db.Logger
	}
	return logger.With("replica", r.Client.Type())
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
	r.wg.Add(1)
	go func() { defer r.wg.Done(); r.monitor(ctx) }()

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
// Only one Sync can run at a time to prevent concurrent uploads of the same file.
func (r *Replica) Sync(ctx context.Context) (err error) {
	return r.sync(ctx, 0)
}

func (r *Replica) sync(ctx context.Context, maxSyncLTXFiles int) error {
	for {
		result, err := r.syncOnce(ctx, maxSyncLTXFiles)
		if err != nil {
			return err
		} else if !result.limited {
			return nil
		}
	}
}

type replicaSyncResult struct {
	synced  bool
	limited bool
}

func (r *Replica) syncOnce(ctx context.Context, maxSyncLTXFiles int) (result replicaSyncResult, err error) {
	if err := r.lockSync(ctx); err != nil {
		return result, err
	}
	defer r.syncSem.Release(1)

	// Clear last position if if an error occurs during sync.
	defer func() {
		if err != nil {
			r.mu.Lock()
			r.pos = ltx.Pos{}
			r.mu.Unlock()
		}
	}()

	if err := ctx.Err(); err != nil {
		return result, context.Cause(ctx)
	}

	// Calculate current replica position, if unknown.
	if r.Pos().IsZero() {
		pos, err := r.calcPos(ctx)
		if err != nil {
			return result, fmt.Errorf("calc pos: %w", err)
		}
		r.SetPos(pos)
	}

	// Find current position of database.
	dpos, err := r.db.Pos()
	if err != nil {
		return result, fmt.Errorf("cannot determine current position: %w", err)
	} else if dpos.IsZero() {
		return result, errReplicaWaitForData
	}

	r.Logger().Info("replica sync",
		slog.Group("txid",
			slog.String("replica", r.Pos().TXID.String()),
			slog.String("db", dpos.TXID.String()),
		))

	// Replicate all L0 LTX files since last replica position.
	for txID, syncedFileN := r.Pos().TXID+1, 0; txID <= dpos.TXID; txID = r.Pos().TXID + 1 {
		if maxSyncLTXFiles > 0 && syncedFileN >= maxSyncLTXFiles {
			result.limited = true
			// Uploads succeeded, so record sync health; otherwise a
			// sustained backlog reads as unhealthy while progressing.
			r.db.RecordSuccessfulSync()
			r.Logger().Debug("replica sync limited",
				"synced_files", syncedFileN,
				"db_txid", dpos.TXID.String(),
				"replica_txid", r.Pos().TXID.String())
			return result, nil
		}
		if err := ctx.Err(); err != nil {
			return result, context.Cause(ctx)
		}
		if err := r.uploadLTXFile(ctx, 0, txID, txID); err != nil {
			return result, err
		}
		r.SetPos(ltx.Pos{TXID: txID})
		result.synced = true
		syncedFileN++
	}

	// Record successful sync for heartbeat monitoring.
	r.db.RecordSuccessfulSync()

	return result, nil
}

func (r *Replica) lockSync(ctx context.Context) error {
	if r.syncSem.TryAcquire(1) {
		return nil
	}
	r.syncWaiters.Add(1)
	defer r.syncWaiters.Add(-1)
	if err := r.syncSem.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("wait for replica sync: %w", context.Cause(ctx))
	}
	return nil
}

func (r *Replica) uploadLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (err error) {
	filename := r.db.LTXPath(level, minTXID, maxTXID)
	f, err := os.Open(filename)
	if err != nil {
		return NewLTXError("open", filename, level, uint64(minTXID), uint64(maxTXID), err)
	}
	defer func() { _ = f.Close() }()

	info, err := r.Client.WriteLTXFile(ctx, level, minTXID, maxTXID, f)
	if err != nil {
		return fmt.Errorf("write ltx file: %w", err)
	}
	r.Logger().Info("ltx file uploaded",
		"level", info.Level,
		"minTXID", info.MinTXID,
		"maxTXID", info.MaxTXID,
		"size", info.Size)

	// Track current position
	//replicaWALIndexGaugeVec.WithLabelValues(r.db.Path(), r.Name()).Set(float64(rd.Pos().Index))
	//replicaWALOffsetGaugeVec.WithLabelValues(r.db.Path(), r.Name()).Set(float64(rd.Pos().Offset))

	return nil
}

// calcPos returns the last position saved to the replica for level 0.
func (r *Replica) calcPos(ctx context.Context) (pos ltx.Pos, err error) {
	info, err := r.MaxLTXFileInfo(ctx, 0)
	if err != nil {
		return pos, fmt.Errorf("max ltx file: %w", err)
	}
	return ltx.Pos{TXID: info.MaxTXID}, nil
}

// MaxLTXFileInfo returns metadata about the last LTX file for a given level.
// Returns nil if no files exist for the level.
func (r *Replica) MaxLTXFileInfo(ctx context.Context, level int) (info ltx.FileInfo, err error) {
	// Normal operation - use fast timestamps
	itr, err := r.Client.LTXFiles(ctx, level, 0, false)
	if err != nil {
		return info, err
	}
	defer itr.Close()

	for itr.Next() {
		item := itr.Item()
		if item.MaxTXID > info.MaxTXID {
			info = *item
		}
	}
	return info, itr.Close()
}

// Pos returns the current replicated position.
// Returns a zero value if the current position cannot be determined.
func (r *Replica) Pos() ltx.Pos {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pos
}

// SetPos sets the current replicated position.
func (r *Replica) SetPos(pos ltx.Pos) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pos = pos
}

// EnforceRetention forces a new snapshot once the retention interval has passed.
// Older snapshots and WAL files are then removed.
func (r *Replica) EnforceRetention(ctx context.Context) (err error) {
	panic("TODO(ltx): Re-implement after multi-level compaction")

	/*
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

		// Delete unretained snapshots & WAL files.
		snapshot := FindMinSnapshot(retained)

		// Otherwise remove all earlier snapshots & WAL segments.
		if err := r.deleteSnapshotsBeforeIndex(ctx, snapshot.Index); err != nil {
			return fmt.Errorf("delete snapshots before index: %w", err)
		} else if err := r.deleteWALSegmentsBeforeIndex(ctx, snapshot.Index); err != nil {
			return fmt.Errorf("delete wal segments before index: %w", err)
		}

		return nil
	*/
}

/*
func (r *Replica) deleteBeforeTXID(ctx context.Context, level int, txID ltx.TXID) error {
	itr, err := r.Client.LTXFiles(ctx, level)
	if err != nil {
		return fmt.Errorf("fetch ltx files: %w", err)
	}
	defer itr.Close()

	var a []*ltx.FileInfo
	for itr.Next() {
		info := itr.Item()
		if info.MinTXID >= txID {
			continue
		}
		a = append(a, info)
	}
	if err := itr.Close(); err != nil {
		return err
	}

	if len(a) == 0 {
		return nil
	}

	if err := r.Client.DeleteLTXFiles(ctx, a); err != nil {
		return fmt.Errorf("delete wal segments: %w", err)
	}

	r.Logger().Info("ltx files deleted before",
		slog.Int("level", level),
		slog.String("txID", txID.String()),
		slog.Int("n", len(a)))

	return nil
}
*/

type syncScheduler struct {
	interval        time.Duration
	backoff         time.Duration
	maxBackoff      time.Duration
	logInterval     time.Duration
	consecutiveErrs int
	lastLogTime     time.Time
}

func newSyncScheduler(interval time.Duration) syncScheduler {
	return syncScheduler{
		interval:    interval,
		maxBackoff:  DefaultSyncBackoffMax,
		logInterval: SyncErrorLogInterval,
	}
}

func (s *syncScheduler) waitForTick(ctx context.Context, ticker *time.Ticker) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ticker.C:
		return nil
	}
}

func (s *syncScheduler) waitForBackoff(ctx context.Context) error {
	if s.backoff <= 0 {
		return nil
	}
	timer := time.NewTimer(s.backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *syncScheduler) recordError() {
	s.consecutiveErrs++
	if s.backoff == 0 {
		s.backoff = s.interval
	} else {
		s.backoff *= 2
		if s.backoff > s.maxBackoff {
			s.backoff = s.maxBackoff
		}
	}
}

func (s *syncScheduler) shouldLog() bool {
	return time.Since(s.lastLogTime) >= s.logInterval
}

func (s *syncScheduler) markLogged() {
	s.lastLogTime = time.Now()
}

func (s *syncScheduler) recordSuccess(logger *slog.Logger) {
	if s.consecutiveErrs > 0 {
		logger.Info("replica sync recovered", "previous_errors", s.consecutiveErrs)
	}
	s.backoff = 0
	s.consecutiveErrs = 0
}

func (s *syncScheduler) resetBackoff() {
	s.backoff = 0
	s.consecutiveErrs = 0
}

func (r *Replica) monitor(ctx context.Context) {
	ticker := time.NewTicker(r.SyncInterval)
	defer ticker.Stop()

	sched := newSyncScheduler(r.SyncInterval)
	notify := r.db.Notify()

	for initial := true; ; initial = false {
		if !initial {
			if err := sched.waitForTick(ctx, ticker); err != nil {
				return
			}
		}

		if err := sched.waitForBackoff(ctx); err != nil {
			return
		}

		if pos, err := r.db.Pos(); err == nil && pos.TXID <= r.Pos().TXID {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-notify:
			}
		} else if ctx.Err() != nil {
			return
		}

		notify = r.db.Notify()

		if err := r.sync(ctx, r.MaxSyncLTXFiles); err != nil {
			if errors.Is(err, errReplicaWaitForData) {
				continue
			}
			r.handleSyncError(ctx, &sched, err)
			continue
		}

		sched.recordSuccess(r.Logger())
	}
}

func (r *Replica) handleSyncError(ctx context.Context, sched *syncScheduler, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}

	sched.recordError()

	var ltxErr *LTXError
	if errors.As(err, &ltxErr) {
		if sched.shouldLog() {
			args := []any{
				"error", err,
				"path", ltxErr.Path,
				"consecutive_errors", sched.consecutiveErrs,
				"backoff", sched.backoff,
			}
			if ltxErr.Hint != "" {
				args = append(args, "hint", ltxErr.Hint)
			}
			r.Logger().Error("monitor error", args...)
			sched.markLogged()
		}

		if r.AutoRecoverEnabled && ltxErr.IsAutoRecoverable() {
			r.Logger().Warn("auto-recovery enabled, resetting local state")
			if resetErr := r.db.ResetLocalState(ctx); resetErr != nil {
				r.Logger().Error("auto-recovery failed", "error", resetErr)
			} else {
				r.Logger().Info("auto-recovery complete, resuming replication")
				sched.resetBackoff()
			}
		}
	} else if sched.shouldLog() {
		r.Logger().Error("monitor error",
			"error", err,
			"consecutive_errors", sched.consecutiveErrs,
			"backoff", sched.backoff)
		sched.markLogged()
	}
}

// CreatedAt returns the earliest creation time of any LTX file.
// Returns zero time if no LTX files exist.
func (r *Replica) CreatedAt(ctx context.Context) (time.Time, error) {
	var min time.Time

	// Normal operation - use fast timestamps
	itr, err := r.Client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		return min, err
	}
	defer itr.Close()

	if itr.Next() {
		min = itr.Item().CreatedAt
	}
	return min, itr.Close()
}

// TimeBounds returns the creation time & last updated time.
// Returns zero time if no LTX files exist.
func (r *Replica) TimeBounds(ctx context.Context) (createdAt, updatedAt time.Time, err error) {
	for level := SnapshotLevel; level >= 0; level-- {
		itr, err := r.Client.LTXFiles(ctx, level, 0, false)
		if err != nil {
			return createdAt, updatedAt, err
		}

		for itr.Next() {
			info := itr.Item()
			if createdAt.IsZero() || info.CreatedAt.Before(createdAt) {
				createdAt = info.CreatedAt
			}
			if updatedAt.IsZero() || info.CreatedAt.After(updatedAt) {
				updatedAt = info.CreatedAt
			}
		}
		if err := itr.Close(); err != nil {
			return createdAt, updatedAt, err
		}
	}
	return createdAt, updatedAt, nil
}
