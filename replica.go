package litestream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
// Returns zero time if LTX files exist.
func (r *Replica) TimeBounds(ctx context.Context) (createdAt, updatedAt time.Time, err error) {
	// Normal operation - use fast timestamps
	itr, err := r.Client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		return createdAt, updatedAt, err
	}
	defer itr.Close()

	for itr.Next() {
		info := itr.Item()
		if createdAt.IsZero() || info.CreatedAt.Before(createdAt) {
			createdAt = info.CreatedAt
		}
		if updatedAt.IsZero() || info.CreatedAt.After(updatedAt) {
			updatedAt = info.CreatedAt
		}
	}
	return createdAt, updatedAt, itr.Close()
}

// CalcRestoreTarget returns a target time restore from.
func (r *Replica) CalcRestoreTarget(ctx context.Context, opt RestoreOptions) (updatedAt time.Time, err error) {
	// Determine the replicated time bounds.
	createdAt, updatedAt, err := r.TimeBounds(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("created at: %w", err)
	}

	// Skip if it does not contain timestamp.
	if !opt.Timestamp.IsZero() {
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
func (r *Replica) Restore(ctx context.Context, opt RestoreOptions) (err error) {
	// Validate options.
	if opt.OutputPath == "" {
		return fmt.Errorf("output path required")
	} else if opt.TXID != 0 && !opt.Timestamp.IsZero() {
		return fmt.Errorf("cannot specify index & timestamp to restore")
	}

	// Ensure output path does not already exist.
	if _, err := os.Stat(opt.OutputPath); err == nil {
		return fmt.Errorf("cannot restore, output path already exists: %s", opt.OutputPath)
	} else if !os.IsNotExist(err) {
		return err
	}

	// Check if this is a legacy v0.3.x format backup.
	if detector, ok := r.Client.(LegacyDetector); ok {
		isLegacy, err := detector.IsLegacyFormat(ctx)
		if err != nil {
			return fmt.Errorf("check legacy format: %w", err)
		}
		if isLegacy {
			r.Logger().Info("detected v0.3.x legacy backup format, using legacy restore")
			return r.restoreLegacy(ctx, detector, opt)
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
		rdrs = append(rdrs, f)
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
	return os.Rename(tmpOutputPath, opt.OutputPath)
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
	if a, err := FindLTXFiles(ctx, client, SnapshotLevel, !timestamp.IsZero(), func(info *ltx.FileInfo) (bool, error) {
		logger.Debug("finding snapshot before target TXID or timestamp", "snapshot", info.MaxTXID)
		if txID != 0 {
			return info.MaxTXID <= txID, nil
		} else if !timestamp.IsZero() {
			return info.CreatedAt.Before(timestamp), nil
		}
		return true, nil
	}); err != nil {
		return nil, err
	} else if len(a) > 0 {
		logger.Debug("found snapshot before target TXID or timestamp", "snapshot", a[len(a)-1].MaxTXID)
		infos = append(infos, a[len(a)-1])
	}

	// Starting from the highest compaction level, collect all paths after the
	// latest TXID for each level. Compactions are based on the previous level's
	// TXID granularity so the TXIDs should align between compaction levels.
	const maxLevel = SnapshotLevel - 1
	for level := maxLevel; level >= 0; level-- {
		logger.Debug("finding ltx files for level", "level", level)

		// Pass useMetadata flag to enable accurate timestamp fetching for timestamp-based restore.
		a, err := FindLTXFiles(ctx, client, level, !timestamp.IsZero(), func(info *ltx.FileInfo) (bool, error) {
			if info.MaxTXID <= infos.MaxTXID() { // skip if already included in previous levels
				return false, nil
			}

			// Filter by TXID or timestamp, if specified.
			if txID != 0 {
				return info.MaxTXID <= txID, nil
			} else if !timestamp.IsZero() {
				return info.CreatedAt.Before(timestamp), nil
			}
			return true, nil
		})
		if err != nil {
			return nil, err
		}

		// Append each storage path to the list
		for _, info := range a {
			// Skip if this file's range is already covered by previously added files.
			// This can happen when a larger compacted file at the same level covers
			// a smaller file's entire range (see issue #847).
			if info.MaxTXID <= infos.MaxTXID() {
				continue
			}

			// Ensure TXIDs are contiguous between each paths.
			if !ltx.IsContiguous(infos.MaxTXID(), info.MinTXID, info.MaxTXID) {
				return nil, fmt.Errorf("non-contiguous transaction files: prev=%s filename=%s",
					infos.MaxTXID().String(), ltx.FormatFilename(info.MinTXID, info.MaxTXID))
			}

			logger.Debug("matching LTX file for restore",
				"filename", ltx.FormatFilename(info.MinTXID, info.MaxTXID))
			infos = append(infos, info)
		}
	}

	// Return an error if we are unable to find any set of LTX files before
	// target TXID. This shouldn't happen under normal circumstances. Only if
	// lower level LTX files are removed before a snapshot has occurred.
	if len(infos) == 0 {
		return nil, ErrTxNotAvailable
	}

	return infos, nil
}

// restoreLegacy restores a database from a v0.3.x format backup.
// This handles the legacy format with generations/, snapshots/, and wal/ directories.
func (r *Replica) restoreLegacy(ctx context.Context, detector LegacyDetector, opt RestoreOptions) error {
	// Find the latest generation with snapshots.
	generations, err := detector.LegacyGenerations(ctx)
	if err != nil {
		return fmt.Errorf("list generations: %w", err)
	}
	if len(generations) == 0 {
		return fmt.Errorf("no generations found in legacy backup")
	}

	// Find the best generation and snapshot to restore from.
	var bestGen string
	var bestSnapshot *LegacySnapshotInfo

	for i := len(generations) - 1; i >= 0; i-- {
		gen := generations[i]
		snapshots, err := detector.LegacySnapshots(ctx, gen)
		if err != nil {
			r.Logger().Warn("failed to list snapshots for generation", "generation", gen, "error", err)
			continue
		}
		if len(snapshots) == 0 {
			continue
		}

		// Find the latest snapshot before the target timestamp (if specified).
		for j := len(snapshots) - 1; j >= 0; j-- {
			snap := snapshots[j]
			if !opt.Timestamp.IsZero() && snap.CreatedAt.After(opt.Timestamp) {
				continue
			}
			bestGen = gen
			bestSnapshot = &snap
			break
		}

		if bestSnapshot != nil {
			break
		}
	}

	if bestSnapshot == nil {
		return fmt.Errorf("no suitable snapshot found for restore")
	}

	r.Logger().Info("restoring from legacy backup",
		"generation", bestGen,
		"snapshot_index", bestSnapshot.Index,
		"snapshot_time", bestSnapshot.CreatedAt)

	// Create parent directory if it doesn't exist.
	var dirInfo os.FileInfo
	if db := r.DB(); db != nil {
		dirInfo = db.dirInfo
	}
	if err := internal.MkdirAll(filepath.Dir(opt.OutputPath), dirInfo); err != nil {
		return fmt.Errorf("create parent directory: %w", err)
	}

	// Create temp file for the restored database.
	tmpOutputPath := opt.OutputPath + ".tmp"
	defer os.Remove(tmpOutputPath)

	// Download and decompress the snapshot.
	r.Logger().Debug("downloading legacy snapshot", "index", bestSnapshot.Index)
	if err := r.downloadLegacySnapshot(ctx, detector, bestGen, bestSnapshot.Index, tmpOutputPath); err != nil {
		return fmt.Errorf("download snapshot: %w", err)
	}

	// Get WAL segments after the snapshot.
	walSegments, err := detector.LegacyWALSegments(ctx, bestGen)
	if err != nil {
		return fmt.Errorf("list wal segments: %w", err)
	}

	// Filter WAL segments to only include those after the snapshot.
	var segmentsToApply []LegacyWALSegmentInfo
	for _, seg := range walSegments {
		if seg.Index < bestSnapshot.Index {
			continue
		}
		if !opt.Timestamp.IsZero() && seg.CreatedAt.After(opt.Timestamp) {
			continue
		}
		segmentsToApply = append(segmentsToApply, seg)
	}

	if len(segmentsToApply) > 0 {
		r.Logger().Info("applying WAL segments", "count", len(segmentsToApply))
		if err := r.applyLegacyWALSegments(ctx, detector, bestGen, segmentsToApply, tmpOutputPath, opt.Parallelism); err != nil {
			return fmt.Errorf("apply wal segments: %w", err)
		}
	}

	// Rename temp file to final path.
	r.Logger().Debug("renaming database from temporary location")
	return os.Rename(tmpOutputPath, opt.OutputPath)
}

// downloadLegacySnapshot downloads and decompresses a legacy snapshot to the output path.
func (r *Replica) downloadLegacySnapshot(ctx context.Context, detector LegacyDetector, generation string, index int, outputPath string) error {
	rc, err := detector.OpenLegacySnapshot(ctx, generation, index)
	if err != nil {
		return err
	}
	defer rc.Close()

	// Create output file.
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Decompress LZ4 and write to file.
	lz4Reader := internal.NewLZ4Reader(rc)
	defer lz4Reader.Close()

	if _, err := io.Copy(f, lz4Reader); err != nil {
		return fmt.Errorf("decompress snapshot: %w", err)
	}

	if err := f.Sync(); err != nil {
		return err
	}

	return f.Close()
}

// applyLegacyWALSegments downloads and applies WAL segments to the database.
func (r *Replica) applyLegacyWALSegments(ctx context.Context, detector LegacyDetector, generation string, segments []LegacyWALSegmentInfo, dbPath string, parallelism int) error {
	if parallelism <= 0 {
		parallelism = DefaultRestoreParallelism
	}

	// Create temp directory for WAL segments.
	tmpDir, err := os.MkdirTemp("", "litestream-legacy-wal-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	// Download WAL segments in parallel.
	type downloadResult struct {
		index int
		path  string
		err   error
	}

	resultCh := make(chan downloadResult, len(segments))
	sem := make(chan struct{}, parallelism)

	for i, seg := range segments {
		seg := seg
		i := i

		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
		}

		go func() {
			defer func() { <-sem }()

			walPath := filepath.Join(tmpDir, fmt.Sprintf("%08d-%016x.wal", seg.Index, seg.Offset))
			err := r.downloadLegacyWALSegment(ctx, detector, generation, seg.Index, seg.Offset, walPath)
			resultCh <- downloadResult{index: i, path: walPath, err: err}
		}()
	}

	// Collect results.
	walPaths := make([]string, len(segments))
	for range segments {
		result := <-resultCh
		if result.err != nil {
			return fmt.Errorf("download wal segment %d: %w", result.index, result.err)
		}
		walPaths[result.index] = result.path
	}

	// Apply WAL segments sequentially using SQLite.
	for i, walPath := range walPaths {
		if err := r.applyLegacyWAL(ctx, dbPath, walPath); err != nil {
			return fmt.Errorf("apply wal segment %d: %w", i, err)
		}
	}

	return nil
}

// downloadLegacyWALSegment downloads and decompresses a legacy WAL segment.
func (r *Replica) downloadLegacyWALSegment(ctx context.Context, detector LegacyDetector, generation string, index int, offset int64, outputPath string) error {
	rc, err := detector.OpenLegacyWALSegment(ctx, generation, index, offset)
	if err != nil {
		return err
	}
	defer rc.Close()

	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Decompress LZ4 and write to file.
	lz4Reader := internal.NewLZ4Reader(rc)
	defer lz4Reader.Close()

	if _, err := io.Copy(f, lz4Reader); err != nil {
		return fmt.Errorf("decompress wal segment: %w", err)
	}

	if err := f.Sync(); err != nil {
		return err
	}

	return f.Close()
}

// applyLegacyWAL applies a single WAL file to the database using SQLite's recovery mechanism.
func (r *Replica) applyLegacyWAL(ctx context.Context, dbPath, walPath string) error {
	// In v0.3.x, WAL files were applied by:
	// 1. Copying the WAL file to the database's WAL location
	// 2. Opening the database which triggers SQLite to replay the WAL
	// 3. Checkpointing to apply WAL to main database file

	// Copy WAL file to expected location.
	dbWALPath := dbPath + "-wal"
	if err := copyFile(walPath, dbWALPath); err != nil {
		return fmt.Errorf("copy wal file: %w", err)
	}
	defer os.Remove(dbWALPath)

	// Open the database which will trigger WAL recovery.
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL")
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	// Force a checkpoint to apply WAL changes to main database.
	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}

	return nil
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return dstFile.Sync()
}
