package litestream

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/internal"
)

// Default replica settings.
const (
	DefaultSyncInterval = 1 * time.Second
)

// Replica connects a database to a replication destination via a ReplicaClient.
// The replica manages periodic synchronization and maintaining the current
// replica position.
type Replica struct {
	db *DB

	mu  sync.RWMutex
	pos ltx.Pos // current replicated position

	syncMu sync.Mutex // protects Sync() from concurrent calls

	muf sync.Mutex
	f   *os.File // long-running file descriptor to avoid non-OFD lock issues

	wg     sync.WaitGroup
	cancel func()

	// Client used to connect to the remote replica.
	Client ReplicaClient

	// Time between syncs with the shadow WAL.
	SyncInterval time.Duration

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
		db:     db,
		cancel: func() {},

		SyncInterval:   DefaultSyncInterval,
		MonitorEnabled: true,
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
	r.syncMu.Lock()
	defer r.syncMu.Unlock()

	// Clear last position if if an error occurs during sync.
	defer func() {
		if err != nil {
			r.mu.Lock()
			r.pos = ltx.Pos{}
			r.mu.Unlock()
		}
	}()

	// Calculate current replica position, if unknown.
	if r.Pos().IsZero() {
		pos, err := r.calcPos(ctx)
		if err != nil {
			return fmt.Errorf("calc pos: %w", err)
		}
		r.SetPos(pos)
	}

	// Find current position of database.
	dpos, err := r.db.Pos()
	if err != nil {
		return fmt.Errorf("cannot determine current position: %w", err)
	} else if dpos.IsZero() {
		return fmt.Errorf("no position, waiting for data")
	}

	r.Logger().Debug("replica sync", "txid", dpos.TXID.String())

	// Replicate all L0 LTX files since last replica position.
	for txID := r.Pos().TXID + 1; txID <= dpos.TXID; txID = r.Pos().TXID + 1 {
		if err := r.uploadLTXFile(ctx, 0, txID, txID); err != nil {
			return err
		}
		r.SetPos(ltx.Pos{TXID: txID})
	}

	// Record successful sync for heartbeat monitoring.
	r.db.RecordSuccessfulSync()

	return nil
}

func (r *Replica) uploadLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (err error) {
	filename := r.db.LTXPath(level, minTXID, maxTXID)
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := r.Client.WriteLTXFile(ctx, level, minTXID, maxTXID, f); err != nil {
		return fmt.Errorf("write ltx file: %w", err)
	}
	r.Logger().Debug("ltx file uploaded", "filename", filename, "minTXID", minTXID, "maxTXID", maxTXID)

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

// monitor runs in a separate goroutine and continuously replicates the DB.
// Implements exponential backoff on repeated sync errors to prevent log spam
// and reduce load when persistent errors occur. See issue #927.
func (r *Replica) monitor(ctx context.Context) {
	ticker := time.NewTicker(r.SyncInterval)
	defer ticker.Stop()

	// Continuously check for new data to replicate.
	ch := make(chan struct{})
	close(ch)
	var notify <-chan struct{} = ch

	var backoff time.Duration
	var lastLogTime time.Time
	var consecutiveErrs int

	for initial := true; ; initial = false {
		// Enforce a minimum time between synchronization.
		if !initial {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}

		// If in backoff mode, wait additional time before retrying.
		if backoff > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
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
			// Don't log context cancellation errors during shutdown
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				consecutiveErrs++

				// Exponential backoff: SyncInterval -> 2x -> 4x -> ... -> max
				if backoff == 0 {
					backoff = r.SyncInterval
				} else {
					backoff *= 2
					if backoff > DefaultSyncBackoffMax {
						backoff = DefaultSyncBackoffMax
					}
				}

				// Check for LTX errors and include recovery hints
				var ltxErr *LTXError
				if errors.As(err, &ltxErr) {
					// Log with rate limiting to avoid log spam during persistent errors.
					if time.Since(lastLogTime) >= SyncErrorLogInterval {
						if ltxErr.Hint != "" {
							r.Logger().Error("monitor error",
								"error", err,
								"path", ltxErr.Path,
								"hint", ltxErr.Hint,
								"consecutive_errors", consecutiveErrs,
								"backoff", backoff)
						} else {
							r.Logger().Error("monitor error",
								"error", err,
								"path", ltxErr.Path,
								"consecutive_errors", consecutiveErrs,
								"backoff", backoff)
						}
						lastLogTime = time.Now()
					}

					// Attempt auto-recovery if enabled
					if r.AutoRecoverEnabled {
						r.Logger().Warn("auto-recovery enabled, resetting local state")
						if resetErr := r.db.ResetLocalState(ctx); resetErr != nil {
							r.Logger().Error("auto-recovery failed", "error", resetErr)
						} else {
							r.Logger().Info("auto-recovery complete, resuming replication")
							// Reset backoff after successful recovery
							backoff = 0
							consecutiveErrs = 0
						}
					}
				} else {
					// Log with rate limiting to avoid log spam during persistent errors.
					if time.Since(lastLogTime) >= SyncErrorLogInterval {
						r.Logger().Error("monitor error",
							"error", err,
							"consecutive_errors", consecutiveErrs,
							"backoff", backoff)
						lastLogTime = time.Now()
					}
				}
			}
			continue
		}

		// Success - reset backoff and error counter.
		if consecutiveErrs > 0 {
			r.Logger().Info("replica sync recovered", "previous_errors", consecutiveErrs)
		}
		backoff = 0
		consecutiveErrs = 0
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

// CalcRestoreTarget returns a target time restore from.
func (r *Replica) CalcRestoreTarget(ctx context.Context, opt RestoreOptions) (updatedAt time.Time, err error) {
	// Determine the replicated time bounds from LTX files.
	createdAt, updatedAt, err := r.TimeBounds(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("created at: %w", err)
	}

	// Also check v0.3.x time bounds if client supports it.
	if client, ok := r.Client.(ReplicaClientV3); ok {
		v3CreatedAt, v3UpdatedAt, err := r.TimeBoundsV3(ctx, client)
		if err != nil {
			return time.Time{}, fmt.Errorf("v0.3.x time bounds: %w", err)
		}
		// Extend time bounds to include v0.3.x backups.
		if !v3CreatedAt.IsZero() && (createdAt.IsZero() || v3CreatedAt.Before(createdAt)) {
			createdAt = v3CreatedAt
		}
		if !v3UpdatedAt.IsZero() && (updatedAt.IsZero() || v3UpdatedAt.After(updatedAt)) {
			updatedAt = v3UpdatedAt
		}
	}

	// Skip if it does not contain timestamp.
	if !opt.Timestamp.IsZero() {
		if createdAt.IsZero() && updatedAt.IsZero() {
			return time.Time{}, fmt.Errorf("no backups found")
		}
		if opt.Timestamp.Before(createdAt) || opt.Timestamp.After(updatedAt) {
			return time.Time{}, fmt.Errorf("timestamp does not exist")
		}
	}

	return updatedAt, nil
}

// Replica restores the database from a replica based on the options given.
// This method will restore into opt.OutputPath, if specified, or into the
// DB's original database path. It can optionally restore from a specific
// replica or it will automatically choose the best one. Finally,
// a timestamp can be specified to restore the database to a specific
// point-in-time.
//
// When the replica contains both v0.3.x and LTX format backups, this method
// compares snapshots from both formats and uses whichever has the better backup:
// - With timestamp: uses the format with the most recent snapshot before timestamp
// - Without timestamp: uses the format with the most recent backup overall
func (r *Replica) Restore(ctx context.Context, opt RestoreOptions) (err error) {
	// Validate options.
	if opt.OutputPath == "" {
		return fmt.Errorf("output path required")
	} else if opt.TXID != 0 && !opt.Timestamp.IsZero() {
		return fmt.Errorf("cannot specify index & timestamp to restore")
	} else if opt.Follow && opt.TXID != 0 {
		return fmt.Errorf("cannot use follow mode with -txid")
	} else if opt.Follow && !opt.Timestamp.IsZero() {
		return fmt.Errorf("cannot use follow mode with -timestamp")
	}

	// In follow mode, if the database already exists, attempt crash recovery
	// by reading the last applied TXID from the sidecar file.
	if opt.Follow {
		if _, statErr := os.Stat(opt.OutputPath); statErr == nil {
			txid, readErr := ReadTXIDFile(opt.OutputPath)
			if readErr != nil {
				return fmt.Errorf("read txid file for crash recovery: %w", readErr)
			}
			if txid == 0 {
				return fmt.Errorf("cannot resume follow mode: database exists but no -txid file found; delete the database to re-restore: %s", opt.OutputPath)
			}
			// Validate saved TXID is still reachable. If the earliest snapshot
			// starts after our saved TXID, retention has pruned the history
			// and we can't catch up incrementally.
			snapshotItr, itrErr := r.Client.LTXFiles(ctx, SnapshotLevel, 0, false)
			if itrErr != nil {
				return fmt.Errorf("cannot validate saved TXID for crash recovery: %w", itrErr)
			}

			var latestSnapshot *ltx.FileInfo
			for snapshotItr.Next() {
				latestSnapshot = snapshotItr.Item()
			}
			if err := snapshotItr.Err(); err != nil {
				_ = snapshotItr.Close()
				return fmt.Errorf("iterate snapshots for crash recovery validation: %w", err)
			}
			_ = snapshotItr.Close()

			if latestSnapshot != nil {
				if latestSnapshot.MinTXID > txid {
					return fmt.Errorf("cannot resume follow mode: saved TXID %s is behind the earliest snapshot (min TXID %s); replica history has been pruned -- delete %s and %s-txid to re-restore", txid, latestSnapshot.MinTXID, opt.OutputPath, opt.OutputPath)
				}
				if txid > latestSnapshot.MaxTXID {
					return fmt.Errorf("cannot resume follow mode: saved TXID %s is ahead of latest snapshot (max TXID %s); delete %s and %s-txid to re-restore", txid, latestSnapshot.MaxTXID, opt.OutputPath, opt.OutputPath)
				}
			}

			r.Logger().Info("resuming follow mode from crash recovery", "txid", txid, "output", opt.OutputPath)
			return r.follow(ctx, opt.OutputPath, txid, opt.FollowInterval)
		}
	}

	// Ensure output path does not already exist.
	if _, err := os.Stat(opt.OutputPath); err == nil {
		return fmt.Errorf("cannot restore, output path already exists: %s", opt.OutputPath)
	} else if !os.IsNotExist(err) {
		return err
	}

	// Compare v0.3.x and LTX formats to find the best backup (unless TXID is specified).
	// Skip V3 format when follow mode is enabled (V3 doesn't support incremental following).
	if opt.TXID == 0 && !opt.Follow {
		if client, ok := r.Client.(ReplicaClientV3); ok {
			useV3, err := r.shouldUseV3Restore(ctx, client, opt.Timestamp)
			if err != nil {
				return err
			}
			if useV3 {
				return r.RestoreV3(ctx, opt)
			}
		}
	}

	infos, err := CalcRestorePlan(ctx, r.Client, opt.TXID, opt.Timestamp, r.Logger())
	if err != nil {
		return fmt.Errorf("cannot calc restore plan: %w", err)
	}

	r.Logger().Debug("restore plan", "n", len(infos), "txid", infos[len(infos)-1].MaxTXID, "timestamp", infos[len(infos)-1].CreatedAt)

	rdrs := make([]io.Reader, 0, len(infos))
	defer func() {
		for _, rd := range rdrs {
			if closer, ok := rd.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	}()

	for _, info := range infos {
		// Validate file size - must be at least header size to be readable
		if info.Size < ltx.HeaderSize {
			return fmt.Errorf("invalid ltx file: level=%d min=%s max=%s has size %d bytes (minimum %d)",
				info.Level, info.MinTXID, info.MaxTXID, info.Size, ltx.HeaderSize)
		}

		r.Logger().Debug("opening ltx file for restore", "level", info.Level, "min", info.MinTXID, "max", info.MaxTXID)

		// Add file to be compacted.
		f, err := r.Client.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
		if err != nil {
			return fmt.Errorf("open ltx file: %w", err)
		}
		rdrs = append(rdrs, internal.NewResumableReader(ctx, r.Client, info.Level, info.MinTXID, info.MaxTXID, info.Size, f, r.Logger()))
	}

	if len(rdrs) == 0 {
		return fmt.Errorf("no matching backup files available")
	}

	// Create parent directory if it doesn't exist.
	var dirInfo os.FileInfo
	if db := r.DB(); db != nil {
		dirInfo = db.dirInfo
	}
	if err := internal.MkdirAll(filepath.Dir(opt.OutputPath), dirInfo); err != nil {
		return fmt.Errorf("create parent directory: %w", err)
	}

	// Output to temp file & atomically rename.
	tmpOutputPath := opt.OutputPath + ".tmp"
	r.Logger().Debug("compacting into database", "path", tmpOutputPath, "n", len(rdrs))

	f, err := os.Create(tmpOutputPath)
	if err != nil {
		return fmt.Errorf("create temp database path: %w", err)
	}
	defer func() { _ = f.Close() }()

	pr, pw := io.Pipe()

	go func() {
		c, err := ltx.NewCompactor(pw, rdrs)
		if err != nil {
			pw.CloseWithError(fmt.Errorf("new ltx compactor: %w", err))
			return
		}
		c.HeaderFlags = ltx.HeaderFlagNoChecksum
		_ = pw.CloseWithError(c.Compact(ctx))
	}()

	dec := ltx.NewDecoder(pr)
	if err := dec.DecodeDatabaseTo(f); err != nil {
		return fmt.Errorf("decode database: %w", err)
	}

	if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	// Copy file to final location.
	r.Logger().Debug("renaming database from temporary location")
	if err := os.Rename(tmpOutputPath, opt.OutputPath); err != nil {
		return err
	}

	// Enter follow mode if enabled, continuously applying new LTX files.
	if opt.Follow {
		for _, rd := range rdrs {
			if closer, ok := rd.(io.Closer); ok {
				_ = closer.Close()
			}
		}
		rdrs = nil

		maxTXID := infos[len(infos)-1].MaxTXID
		if err := WriteTXIDFile(opt.OutputPath, maxTXID); err != nil {
			return fmt.Errorf("write initial txid file: %w", err)
		}
		return r.follow(ctx, opt.OutputPath, maxTXID, opt.FollowInterval)
	}

	return nil
}

// follow enters a continuous restore loop, polling for new LTX files and
// applying them to the restored database. It blocks until the context is
// cancelled (e.g. Ctrl+C). Returns nil on clean shutdown.
func (r *Replica) follow(ctx context.Context, outputPath string, lastTXID ltx.TXID, interval time.Duration) error {
	f, err := os.OpenFile(outputPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open database for follow: %w", err)
	}
	defer func() {
		_ = f.Sync()
		_ = f.Close()
	}()

	// Read page size from SQLite header (offset 16, 2 bytes, big-endian).
	var buf [2]byte
	if _, err := f.ReadAt(buf[:], 16); err != nil {
		return fmt.Errorf("read page size from database header: %w", err)
	}
	pageSize := uint32(buf[0])<<8 | uint32(buf[1])
	if pageSize == 1 {
		pageSize = 65536
	}

	if interval <= 0 {
		interval = DefaultFollowInterval
	}

	r.Logger().Info("entering follow mode", "output", outputPath, "txid", lastTXID, "interval", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var consecutiveErrors int
	for {
		select {
		case <-ctx.Done():
			r.Logger().Info("follow mode stopped")
			return nil
		case <-ticker.C:
			newTXID, err := r.applyNewLTXFiles(ctx, f, lastTXID, pageSize)
			if err != nil {
				if ctx.Err() != nil {
					r.Logger().Info("follow mode stopped")
					return nil
				}
				consecutiveErrors++
				r.Logger().Error("follow: error applying updates", "err", err, "consecutive_errors", consecutiveErrors)
				continue
			}
			if newTXID > lastTXID {
				if err := WriteTXIDFile(outputPath, newTXID); err != nil {
					return fmt.Errorf("write txid file: %w", err)
				}
				r.Logger().Info("follow: applied updates", "from_txid", lastTXID, "to_txid", newTXID)
				lastTXID = newTXID
				consecutiveErrors = 0
			}
		}
	}
}

// applyNewLTXFiles polls for new LTX files and applies them to the database.
// It starts from level 0 and falls back to higher levels if there are gaps
// (e.g., level 0 files were compacted away).
func (r *Replica) applyNewLTXFiles(ctx context.Context, f *os.File, afterTXID ltx.TXID, pageSize uint32) (ltx.TXID, error) {
	currentTXID := afterTXID

	// Poll level 0 for the most recent incremental files.
	itr, err := r.Client.LTXFiles(ctx, 0, currentTXID+1, false)
	if err != nil {
		return currentTXID, fmt.Errorf("list level 0 ltx files: %w", err)
	}
	closeLevel0 := func(retErr error) (ltx.TXID, error) {
		if closeErr := itr.Close(); closeErr != nil {
			closeErr = fmt.Errorf("close level 0 ltx iterator: %w", closeErr)
			if retErr != nil {
				return currentTXID, errors.Join(retErr, closeErr)
			}
			return currentTXID, closeErr
		}
		return currentTXID, retErr
	}

	var sawLevel0 bool
	for itr.Next() {
		sawLevel0 = true
		info := itr.Item()

		// If there's a gap, try to fill it from higher compaction levels.
		if info.MinTXID > currentTXID+1 {
			bridgedTXID, err := r.fillFollowGap(ctx, f, currentTXID, info.MinTXID, pageSize)
			if err != nil {
				return closeLevel0(err)
			}
			currentTXID = bridgedTXID

			// Re-check if this file is still needed after bridging.
			if info.MaxTXID <= currentTXID {
				continue
			}
			if info.MinTXID > currentTXID+1 {
				return closeLevel0(nil)
			}
		}

		// Skip if already covered by a higher-level file.
		if info.MaxTXID <= currentTXID {
			continue
		}

		if err := r.applyLTXFile(ctx, f, info, pageSize); err != nil {
			return closeLevel0(fmt.Errorf(
				"apply ltx file (level=%d, min=%s, max=%s): %w",
				info.Level, info.MinTXID, info.MaxTXID, err,
			))
		}
		currentTXID = info.MaxTXID
	}

	if iterErr := itr.Err(); iterErr != nil {
		return closeLevel0(fmt.Errorf("iterate level 0 ltx files: %w", iterErr))
	}
	if _, err := closeLevel0(nil); err != nil {
		return currentTXID, err
	}

	if !sawLevel0 {
		bridgedTXID, err := r.fillFollowGap(ctx, f, currentTXID, currentTXID+1, pageSize)
		if err != nil {
			return currentTXID, err
		}
		currentTXID = bridgedTXID
	}

	return currentTXID, nil
}

// applyLTXFile applies a single LTX file's pages to the database file.
// This follows the same pattern as Hydrator.ApplyLTX (vfs.go:712-747).
//
// To prevent concurrent SQLite readers from seeing partial updates, we acquire
// an exclusive file lock before writing. We also rewrite the SQLite header
// (bytes 18-19) to indicate DELETE journal mode instead of WAL mode, and
// randomize the schema change counter (bytes 24-27) to invalidate cached
// schemas in other connections.
func (r *Replica) applyLTXFile(ctx context.Context, f *os.File, info *ltx.FileInfo, pageSize uint32) error {
	rc, err := r.Client.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
	if err != nil {
		return fmt.Errorf("open ltx file: %w", err)
	}
	defer rc.Close()

	dec := ltx.NewDecoder(rc)
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("decode header: %w", err)
	}

	hdr := dec.Header()

	if err := internal.LockFileExclusive(f); err != nil {
		return fmt.Errorf("acquire exclusive lock: %w", err)
	}
	defer internal.UnlockFile(f)

	for {
		var phdr ltx.PageHeader
		data := make([]byte, pageSize)
		if err := dec.DecodePage(&phdr, data); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("decode page: %w", err)
		}

		if phdr.Pgno == 1 && len(data) >= 28 {
			data[18], data[19] = 0x01, 0x01
			_, _ = rand.Read(data[24:28])
		}

		off := int64(phdr.Pgno-1) * int64(pageSize)
		if _, err := f.WriteAt(data, off); err != nil {
			return fmt.Errorf("write page %d: %w", phdr.Pgno, err)
		}
	}

	if hdr.Commit > 0 {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("sync before truncate: %w", err)
		}
		newSize := int64(hdr.Commit) * int64(pageSize)
		if err := f.Truncate(newSize); err != nil {
			return fmt.Errorf("truncate: %w", err)
		}
	}

	if err := dec.Close(); err != nil {
		return fmt.Errorf("close decoder: %w", err)
	}

	return f.Sync()
}

// fillFollowGap attempts to bridge a gap in level 0 files by searching
// higher compaction levels for a file that covers the missing TXID range.
func (r *Replica) fillFollowGap(ctx context.Context, f *os.File, afterTXID ltx.TXID, gapMinTXID ltx.TXID, pageSize uint32) (ltx.TXID, error) {
	currentTXID := afterTXID

	for level := 1; level < SnapshotLevel; level++ {
		itr, err := r.Client.LTXFiles(ctx, level, 0, false)
		if err != nil {
			return currentTXID, fmt.Errorf("list level %d ltx files: %w", level, err)
		}
		closeLevel := func(retErr error) (ltx.TXID, error) {
			if closeErr := itr.Close(); closeErr != nil {
				closeErr = fmt.Errorf("close level %d ltx iterator: %w", level, closeErr)
				if retErr != nil {
					return currentTXID, errors.Join(retErr, closeErr)
				}
				return currentTXID, closeErr
			}
			return currentTXID, retErr
		}

		for itr.Next() {
			info := itr.Item()

			// Skip if there's a gap at this level too.
			if info.MinTXID > currentTXID+1 {
				break
			}

			// Skip if already covered.
			if info.MaxTXID <= currentTXID {
				continue
			}

			if err := r.applyLTXFile(ctx, f, info, pageSize); err != nil {
				return closeLevel(fmt.Errorf(
					"apply gap-fill ltx file (level=%d, min=%s, max=%s): %w",
					info.Level, info.MinTXID, info.MaxTXID, err,
				))
			}
			currentTXID = info.MaxTXID

			// If we've bridged past the gap, we're done.
			if currentTXID+1 >= gapMinTXID {
				return closeLevel(nil)
			}
		}

		if iterErr := itr.Err(); iterErr != nil {
			return closeLevel(fmt.Errorf("iterate level %d ltx files: %w", level, iterErr))
		}
		if _, err := closeLevel(nil); err != nil {
			return currentTXID, err
		}

		// If we made progress at this level, restart from level 1.
		if currentTXID > afterTXID {
			return currentTXID, nil
		}
	}

	return currentTXID, nil
}

// RestoreV3 restores from a v0.3.x format backup.
func (r *Replica) RestoreV3(ctx context.Context, opt RestoreOptions) error {
	client, ok := r.Client.(ReplicaClientV3)
	if !ok {
		return fmt.Errorf("replica client does not support v0.3.x restore")
	}

	// Validate options.
	if opt.OutputPath == "" {
		return fmt.Errorf("output path required")
	}

	// Ensure output path does not already exist.
	if _, err := os.Stat(opt.OutputPath); err == nil {
		return fmt.Errorf("cannot restore, output path already exists: %s", opt.OutputPath)
	} else if !os.IsNotExist(err) {
		return err
	}

	// Find all generations.
	generations, err := client.GenerationsV3(ctx)
	if err != nil {
		return fmt.Errorf("list generations: %w", err)
	}
	if len(generations) == 0 {
		return ErrNoSnapshots
	}

	// Collect all snapshots across all generations.
	var allSnapshots []SnapshotInfoV3
	for _, gen := range generations {
		snapshots, err := client.SnapshotsV3(ctx, gen)
		if err != nil {
			return fmt.Errorf("list snapshots for generation %s: %w", gen, err)
		}
		allSnapshots = append(allSnapshots, snapshots...)
	}
	if len(allSnapshots) == 0 {
		return ErrNoSnapshots
	}

	// Sort all snapshots by CreatedAt for timestamp-based selection.
	sortSnapshotsV3ByCreatedAt(allSnapshots)

	// Find best snapshot across all generations (latest, or before timestamp if specified).
	snapshot := findBestSnapshotV3(allSnapshots, opt.Timestamp)
	if snapshot == nil {
		return ErrNoSnapshots
	}

	r.Logger().Debug("selected v0.3.x snapshot",
		"generation", snapshot.Generation,
		"index", snapshot.Index,
		"created_at", snapshot.CreatedAt)

	// Get WAL segments for the snapshot's generation.
	segments, err := client.WALSegmentsV3(ctx, snapshot.Generation)
	if err != nil {
		return fmt.Errorf("list WAL segments: %w", err)
	}
	segments = filterWALSegmentsV3(segments, snapshot.Index, opt.Timestamp)

	r.Logger().Debug("found v0.3.x WAL segments", "n", len(segments))

	// Create parent directory if it doesn't exist.
	var dirInfo os.FileInfo
	if db := r.DB(); db != nil {
		dirInfo = db.DirInfo()
	}
	if err := internal.MkdirAll(filepath.Dir(opt.OutputPath), dirInfo); err != nil {
		return fmt.Errorf("create parent directory: %w", err)
	}

	// Create temp file for restore.
	tmpPath := opt.OutputPath + ".tmp"
	defer func() { _ = os.Remove(tmpPath) }()

	// Download and decompress snapshot.
	if err := r.downloadSnapshotV3(ctx, client, snapshot.Generation, snapshot.Index, tmpPath); err != nil {
		return fmt.Errorf("download snapshot: %w", err)
	}

	// Apply WAL segments.
	if err := r.applyWALSegmentsV3(ctx, client, snapshot.Generation, segments, tmpPath); err != nil {
		return fmt.Errorf("apply WAL segments: %w", err)
	}

	// Rename to final path.
	if err := os.Rename(tmpPath, opt.OutputPath); err != nil {
		return fmt.Errorf("rename to output path: %w", err)
	}

	return nil
}

// sortSnapshotsV3ByCreatedAt sorts snapshots by creation time in ascending order.
func sortSnapshotsV3ByCreatedAt(snapshots []SnapshotInfoV3) {
	for i := 0; i < len(snapshots)-1; i++ {
		for j := i + 1; j < len(snapshots); j++ {
			if snapshots[i].CreatedAt.After(snapshots[j].CreatedAt) {
				snapshots[i], snapshots[j] = snapshots[j], snapshots[i]
			}
		}
	}
}

// findBestSnapshotV3 finds the best snapshot for restore.
// If timestamp is zero, returns the latest snapshot.
// Otherwise, returns the latest snapshot created before or at the timestamp.
func findBestSnapshotV3(snapshots []SnapshotInfoV3, timestamp time.Time) *SnapshotInfoV3 {
	if len(snapshots) == 0 {
		return nil
	}
	if timestamp.IsZero() {
		return &snapshots[len(snapshots)-1]
	}
	for i := len(snapshots) - 1; i >= 0; i-- {
		if !snapshots[i].CreatedAt.After(timestamp) {
			return &snapshots[i]
		}
	}
	return nil
}

// filterWALSegmentsV3 filters WAL segments to those at or after the snapshot index
// and optionally before the timestamp.
func filterWALSegmentsV3(segments []WALSegmentInfoV3, snapshotIndex int, timestamp time.Time) []WALSegmentInfoV3 {
	var result []WALSegmentInfoV3
	for _, seg := range segments {
		if seg.Index < snapshotIndex {
			continue
		}
		if !timestamp.IsZero() && seg.CreatedAt.After(timestamp) {
			continue
		}
		result = append(result, seg)
	}
	return result
}

// downloadSnapshotV3 downloads and decompresses a v0.3.x snapshot to the destination path.
func (r *Replica) downloadSnapshotV3(ctx context.Context, client ReplicaClientV3, generation string, index int, destPath string) error {
	rc, err := client.OpenSnapshotV3(ctx, generation, index)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := io.Copy(f, rc); err != nil {
		return err
	}
	return f.Sync()
}

// applyWALSegmentsV3 applies WAL segments to the database file.
func (r *Replica) applyWALSegmentsV3(ctx context.Context, client ReplicaClientV3, generation string, segments []WALSegmentInfoV3, dbPath string) error {
	if len(segments) == 0 {
		return nil
	}

	// Write all WAL segments to the WAL file.
	walPath := dbPath + "-wal"
	for _, seg := range segments {
		if err := r.writeWALSegmentV3(ctx, client, generation, seg, walPath); err != nil {
			return fmt.Errorf("write WAL segment %d/%d: %w", seg.Index, seg.Offset, err)
		}
	}

	// Checkpoint to apply WAL to database.
	return checkpointV3(dbPath)
}

// writeWALSegmentV3 writes a single WAL segment to the WAL file.
func (r *Replica) writeWALSegmentV3(ctx context.Context, client ReplicaClientV3, generation string, seg WALSegmentInfoV3, walPath string) error {
	// Download WAL segment.
	rc, err := client.OpenWALSegmentV3(ctx, generation, seg.Index, seg.Offset)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	// Open WAL file for writing.
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Seek to offset and write.
	if _, err := f.Seek(seg.Offset, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.Copy(f, rc); err != nil {
		return err
	}
	return f.Sync()
}

// checkpointV3 checkpoints the WAL file into the database.
func checkpointV3(dbPath string) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}

// findBestV3SnapshotForTimestamp returns the best v0.3.x snapshot for the given timestamp.
// Returns nil if no suitable snapshot exists.
func (r *Replica) findBestV3SnapshotForTimestamp(ctx context.Context, client ReplicaClientV3, timestamp time.Time) (*SnapshotInfoV3, error) {
	generations, err := client.GenerationsV3(ctx)
	if err != nil {
		return nil, fmt.Errorf("list v0.3.x generations: %w", err)
	}
	if len(generations) == 0 {
		return nil, nil
	}

	var allSnapshots []SnapshotInfoV3
	for _, gen := range generations {
		snapshots, err := client.SnapshotsV3(ctx, gen)
		if err != nil {
			return nil, fmt.Errorf("list v0.3.x snapshots for generation %s: %w", gen, err)
		}
		allSnapshots = append(allSnapshots, snapshots...)
	}

	if len(allSnapshots) == 0 {
		return nil, nil
	}

	// Sort by CreatedAt for timestamp-based selection.
	sortSnapshotsV3ByCreatedAt(allSnapshots)

	return findBestSnapshotV3(allSnapshots, timestamp), nil
}

// shouldUseV3Restore determines whether to use v0.3.x restore instead of LTX.
// Returns true if v0.3.x has a better backup for the given options.
func (r *Replica) shouldUseV3Restore(ctx context.Context, client ReplicaClientV3, timestamp time.Time) (bool, error) {
	// Get v0.3.x time bounds.
	_, v3UpdatedAt, err := r.TimeBoundsV3(ctx, client)
	if err != nil {
		return false, fmt.Errorf("get v0.3.x time bounds: %w", err)
	}

	// Get LTX time bounds.
	_, ltxUpdatedAt, err := r.TimeBounds(ctx)
	if err != nil {
		return false, fmt.Errorf("get LTX time bounds: %w", err)
	}

	// If no v0.3.x backups exist, use LTX.
	if v3UpdatedAt.IsZero() {
		return false, nil
	}

	// If no LTX backups exist, use v0.3.x.
	if ltxUpdatedAt.IsZero() {
		r.Logger().Debug("using v0.3.x restore (no LTX backups)")
		return true, nil
	}

	// Both formats have backups - compare based on timestamp or latest.
	if !timestamp.IsZero() {
		// With timestamp: use format with best snapshot before timestamp.
		v3Snapshot, err := r.findBestV3SnapshotForTimestamp(ctx, client, timestamp)
		if err != nil {
			return false, fmt.Errorf("find v0.3.x snapshot: %w", err)
		}

		ltxSnapshot, err := r.findBestLTXSnapshotForTimestamp(ctx, timestamp)
		if err != nil {
			return false, fmt.Errorf("find LTX snapshot: %w", err)
		}

		if v3Snapshot != nil && (ltxSnapshot == nil || v3Snapshot.CreatedAt.After(ltxSnapshot.CreatedAt)) {
			r.Logger().Debug("using v0.3.x restore (better snapshot for timestamp)",
				"v3_snapshot", v3Snapshot.CreatedAt,
				"ltx_snapshot", ltxSnapshot)
			return true, nil
		}
	} else {
		// Without timestamp: use format with most recent backup.
		if v3UpdatedAt.After(ltxUpdatedAt) {
			r.Logger().Debug("using v0.3.x restore (more recent backup)",
				"v3_updated_at", v3UpdatedAt,
				"ltx_updated_at", ltxUpdatedAt)
			return true, nil
		}
	}

	return false, nil
}

// TimeBoundsV3 returns the time bounds of v0.3.x backups.
// Returns zero times if no v0.3.x backups exist.
func (r *Replica) TimeBoundsV3(ctx context.Context, client ReplicaClientV3) (createdAt, updatedAt time.Time, err error) {
	generations, err := client.GenerationsV3(ctx)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	for _, gen := range generations {
		snapshots, err := client.SnapshotsV3(ctx, gen)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		for _, snap := range snapshots {
			if createdAt.IsZero() || snap.CreatedAt.Before(createdAt) {
				createdAt = snap.CreatedAt
			}
			if updatedAt.IsZero() || snap.CreatedAt.After(updatedAt) {
				updatedAt = snap.CreatedAt
			}
		}

		segments, err := client.WALSegmentsV3(ctx, gen)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		for _, seg := range segments {
			if createdAt.IsZero() || seg.CreatedAt.Before(createdAt) {
				createdAt = seg.CreatedAt
			}
			if updatedAt.IsZero() || seg.CreatedAt.After(updatedAt) {
				updatedAt = seg.CreatedAt
			}
		}
	}

	return createdAt, updatedAt, nil
}

// findBestLTXSnapshotForTimestamp returns the best LTX snapshot for the given timestamp.
// Returns nil if no suitable snapshot exists.
func (r *Replica) findBestLTXSnapshotForTimestamp(ctx context.Context, timestamp time.Time) (*ltx.FileInfo, error) {
	// Find snapshots at the snapshot level that are before the timestamp.
	snapshots, err := FindLTXFiles(ctx, r.Client, SnapshotLevel, true, func(info *ltx.FileInfo) (bool, error) {
		return info.CreatedAt.Before(timestamp), nil
	})
	if err != nil {
		return nil, fmt.Errorf("find LTX snapshots: %w", err)
	}
	if len(snapshots) == 0 {
		return nil, nil
	}

	// Return the latest snapshot before the timestamp (last in the sorted list).
	return snapshots[len(snapshots)-1], nil
}

// CalcRestorePlan returns a list of storage paths to restore a snapshot at the given TXID.
func CalcRestorePlan(ctx context.Context, client ReplicaClient, txID ltx.TXID, timestamp time.Time, logger *slog.Logger) ([]*ltx.FileInfo, error) {
	if txID != 0 && !timestamp.IsZero() {
		return nil, fmt.Errorf("cannot specify both TXID & timestamp to restore")
	}

	var infos ltx.FileInfoSlice
	logger = logger.With("target", txID)

	// Start with latest snapshot before target TXID or timestamp.
	// Pass useMetadata flag to enable accurate timestamp fetching for timestamp-based restore.
	var snapshot *ltx.FileInfo
	snapshotItr, err := client.LTXFiles(ctx, SnapshotLevel, 0, !timestamp.IsZero())
	if err != nil {
		return nil, err
	}
	for snapshotItr.Next() {
		info := snapshotItr.Item()
		logger.Debug("finding snapshot before target TXID or timestamp", "snapshot", info.MaxTXID)
		if txID != 0 && info.MaxTXID > txID {
			continue
		}
		if !timestamp.IsZero() && !info.CreatedAt.Before(timestamp) {
			continue
		}
		snapshot = info
	}
	if err := snapshotItr.Close(); err != nil {
		return nil, err
	}
	if snapshot != nil {
		logger.Debug("found snapshot before target TXID or timestamp", "snapshot", snapshot.MaxTXID)
		infos = append(infos, snapshot)
	}

	// Collect candidates across all compaction levels and pick the next file
	// from any level that extends the longest contiguous TXID range.
	const maxLevel = SnapshotLevel - 1
	startTXID := infos.MaxTXID()
	currentMax := startTXID
	if txID != 0 && currentMax >= txID {
		return infos, nil
	}

	cursors := make([]*restoreLevelCursor, 0, maxLevel+1)
	for level := maxLevel; level >= 0; level-- {
		logger.Debug("finding ltx files for level", "level", level)
		itr, err := client.LTXFiles(ctx, level, 0, !timestamp.IsZero())
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, &restoreLevelCursor{
			itr: itr,
		})
	}
	defer func() {
		for _, cursor := range cursors {
			if cursor != nil {
				_ = cursor.itr.Close()
			}
		}
	}()

	for {
		var next *restoreLevelCursor
		for _, cursor := range cursors {
			if err := cursor.refresh(currentMax, txID, timestamp); err != nil {
				return nil, err
			}
			if cursor.candidate == nil {
				continue
			}
			if next == nil || restoreCandidateBetter(next.candidate, cursor.candidate) {
				next = cursor
			}
		}

		if next == nil || next.candidate == nil {
			break
		}

		if next.candidate.MaxTXID <= currentMax {
			next.candidate = nil
			continue
		}

		logger.Debug("matching LTX file for restore",
			"filename", ltx.FormatFilename(next.candidate.MinTXID, next.candidate.MaxTXID),
			"level", next.candidate.Level)
		infos = append(infos, next.candidate)
		currentMax = next.candidate.MaxTXID
		next.candidate = nil

		if txID != 0 && currentMax >= txID {
			break
		}
	}

	if len(infos) > 0 && txID == 0 && timestamp.IsZero() {
		for _, cursor := range cursors {
			if err := cursor.ensureCurrent(); err != nil {
				return nil, err
			}
			if cursor.current != nil && cursor.current.MinTXID > currentMax+1 {
				return nil, fmt.Errorf("non-contiguous ltx files: have up to %s but next file starts at %s", currentMax, cursor.current.MinTXID)
			}
		}
	}

	if len(infos) == 0 {
		return nil, ErrTxNotAvailable
	}
	if txID != 0 && infos.MaxTXID() < txID {
		return nil, ErrTxNotAvailable
	}

	return infos, nil
}

type restoreLevelCursor struct {
	// itr streams LTX file infos for a single level in filename order.
	itr ltx.FileIterator
	// current holds the last item read from itr but not yet evaluated.
	current *ltx.FileInfo
	// candidate is the best eligible file at this level for the currentMax.
	candidate *ltx.FileInfo
	// done indicates the iterator has been exhausted or errored.
	done bool
}

func (c *restoreLevelCursor) refresh(currentMax, txID ltx.TXID, timestamp time.Time) error {
	// Advance the iterator until we've evaluated all files that could be
	// contiguous with currentMax. Keep the best eligible candidate.
	if c.done {
		return nil
	}
	if c.candidate != nil && c.candidate.MaxTXID <= currentMax {
		c.candidate = nil
	}

	for {
		if err := c.ensureCurrent(); err != nil {
			return err
		}
		if c.done {
			return nil
		}

		info := c.current
		if info.MinTXID > currentMax+1 {
			return nil
		}
		c.current = nil

		if info.MaxTXID <= currentMax {
			continue
		}
		if txID != 0 && info.MaxTXID > txID {
			continue
		}
		if !timestamp.IsZero() && !info.CreatedAt.Before(timestamp) {
			continue
		}

		if c.candidate == nil || restoreCandidateBetter(c.candidate, info) {
			c.candidate = info
		}
	}
}

func (c *restoreLevelCursor) ensureCurrent() error {
	// Ensure current is populated with the next iterator item, or mark done.
	if c.done || c.current != nil {
		return nil
	}
	if !c.itr.Next() {
		if err := c.itr.Err(); err != nil {
			return err
		}
		c.done = true
		return nil
	}
	c.current = c.itr.Item()
	return nil
}

func restoreCandidateBetter(curr, next *ltx.FileInfo) bool {
	if next.MaxTXID != curr.MaxTXID {
		return next.MaxTXID > curr.MaxTXID
	}
	if next.MinTXID != curr.MinTXID {
		return next.MinTXID < curr.MinTXID
	}
	if next.Level != curr.Level {
		return next.Level > curr.Level
	}
	return next.CreatedAt.Before(curr.CreatedAt)
}

// TXIDPath returns the path to the TXID sidecar file for the given database path.
// Uses -txid suffix to match SQLite's naming convention for associated files (-wal, -shm).
func TXIDPath(outputPath string) string {
	return outputPath + "-txid"
}

// WriteTXIDFile atomically writes a TXID to a sidecar file at <outputPath>-txid.
// Uses temp-file + fsync + rename for crash safety.
func WriteTXIDFile(outputPath string, txid ltx.TXID) error {
	txidPath := TXIDPath(outputPath)
	tmpPath := txidPath + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create txid temp file: %w", err)
	}
	defer f.Close()
	defer os.Remove(tmpPath)

	if _, err := fmt.Fprintln(f, txid); err != nil {
		return fmt.Errorf("write txid: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync txid file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close txid file: %w", err)
	}

	if err := os.Rename(tmpPath, txidPath); err != nil {
		return fmt.Errorf("rename txid file: %w", err)
	}

	dir, err := os.Open(filepath.Dir(txidPath))
	if err != nil {
		return fmt.Errorf("open txid dir for sync: %w", err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return fmt.Errorf("sync txid dir: %w", err)
	}
	return dir.Close()
}

// ReadTXIDFile reads the TXID from a sidecar file at <outputPath>-txid.
// Returns 0, nil if the file does not exist (first run).
func ReadTXIDFile(outputPath string) (ltx.TXID, error) {
	txidPath := TXIDPath(outputPath)

	data, err := os.ReadFile(txidPath)
	if os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("read txid file: %w", err)
	}

	txid, err := ltx.ParseTXID(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("parse txid file: %w", err)
	}

	return txid, nil
}

// ValidationError represents a single validation issue.
type ValidationError struct {
	Level    int           // compaction level
	Type     string        // "gap", "overlap", or "unsorted"
	Message  string        // human-readable description
	PrevFile *ltx.FileInfo // previous file
	CurrFile *ltx.FileInfo // current file that caused error
}

// ValidateLevel checks LTX files at the given level are sorted and contiguous.
// Returns a slice of validation errors (empty if valid).
func (r *Replica) ValidateLevel(ctx context.Context, level int) ([]ValidationError, error) {
	itr, err := r.Client.LTXFiles(ctx, level, 0, false)
	if err != nil {
		return nil, fmt.Errorf("fetch ltx files: %w", err)
	}
	defer itr.Close()

	var errors []ValidationError
	var prevInfo *ltx.FileInfo

	for itr.Next() {
		info := itr.Item()

		// Skip first file - nothing to compare against
		if prevInfo == nil {
			prevInfo = info
			continue
		}

		// Check for sort order: curr.MinTXID should be >= prev.MinTXID
		if info.MinTXID < prevInfo.MinTXID {
			errors = append(errors, ValidationError{
				Level:    level,
				Type:     "unsorted",
				Message:  fmt.Sprintf("files out of order: curr.MinTXID=%s < prev.MinTXID=%s", info.MinTXID, prevInfo.MinTXID),
				PrevFile: prevInfo,
				CurrFile: info,
			})
			prevInfo = info
			continue
		}

		// Check for TXID contiguity: prev.MaxTXID + 1 should equal curr.MinTXID
		expectedMinTXID := prevInfo.MaxTXID + 1
		if info.MinTXID != expectedMinTXID {
			if info.MinTXID > expectedMinTXID {
				errors = append(errors, ValidationError{
					Level:    level,
					Type:     "gap",
					Message:  fmt.Sprintf("TXID gap: prev.MaxTXID=%s, curr.MinTXID=%s (expected %s)", prevInfo.MaxTXID, info.MinTXID, expectedMinTXID),
					PrevFile: prevInfo,
					CurrFile: info,
				})
			} else {
				errors = append(errors, ValidationError{
					Level:    level,
					Type:     "overlap",
					Message:  fmt.Sprintf("TXID overlap: prev.MaxTXID=%s, curr.MinTXID=%s", prevInfo.MaxTXID, info.MinTXID),
					PrevFile: prevInfo,
					CurrFile: info,
				})
			}
		}

		prevInfo = info
	}

	if err := itr.Close(); err != nil {
		return nil, fmt.Errorf("close iterator: %w", err)
	}

	return errors, nil
}
