package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/superfly/ltx"
	"modernc.org/sqlite"

	"github.com/benbjohnson/litestream/internal"
)

// Default DB settings.
const (
	DefaultMonitorInterval      = 1 * time.Second
	DefaultCheckpointInterval   = 1 * time.Minute
	DefaultBusyTimeout          = 1 * time.Second
	DefaultMinCheckpointPageN   = 1000
	DefaultTruncatePageN        = 121359 // ~500MB with 4KB page size
	DefaultShutdownSyncTimeout  = 30 * time.Second
	DefaultShutdownSyncInterval = 500 * time.Millisecond
)

// DB represents a managed instance of a SQLite database in the file system.
//
// Checkpoint Strategy:
// Litestream uses a progressive 3-tier checkpoint approach to balance WAL size
// management with write availability:
//
//  1. MinCheckpointPageN (PASSIVE): Non-blocking checkpoint at ~1k pages (~4MB).
//     Attempts checkpoint but allows concurrent readers/writers.
//
//  2. CheckpointInterval (PASSIVE): Time-based non-blocking checkpoint.
//     Ensures regular checkpointing even with low write volume.
//
//  3. TruncatePageN (TRUNCATE): Blocking checkpoint at ~121k pages (~500MB).
//     Emergency brake for runaway WAL growth. Can block writes while waiting
//     for long-lived read transactions. Configurable/disableable.
//
// The RESTART checkpoint mode was permanently removed due to production issues
// with indefinite write blocking (issue #724). All checkpoints now use either
// PASSIVE (non-blocking) or TRUNCATE (emergency only) modes.
type DB struct {
	mu       sync.RWMutex
	path     string        // part to database
	metaPath string        // Path to the database metadata.
	db       *sql.DB       // target database
	f        *os.File      // long-running db file descriptor
	rtx      *sql.Tx       // long running read transaction
	pageSize int           // page size, in bytes
	notify   chan struct{} // closes on WAL change
	chkMu    sync.RWMutex  // checkpoint lock

	// syncedSinceCheckpoint tracks whether any data has been synced since
	// the last checkpoint. Used to prevent time-based checkpoints from
	// triggering when there are no actual database changes, which would
	// otherwise create unnecessary LTX files. See issue #896.
	syncedSinceCheckpoint bool

	// last file info for each level
	maxLTXFileInfos struct {
		sync.Mutex
		m map[int]*ltx.FileInfo
	}

	fileInfo os.FileInfo // db info cached during init
	dirInfo  os.FileInfo // parent dir info cached during init

	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	// Metrics
	dbSizeGauge                 prometheus.Gauge
	walSizeGauge                prometheus.Gauge
	totalWALBytesCounter        prometheus.Counter
	txIDGauge                   prometheus.Gauge
	syncNCounter                prometheus.Counter
	syncErrorNCounter           prometheus.Counter
	syncSecondsCounter          prometheus.Counter
	checkpointNCounterVec       *prometheus.CounterVec
	checkpointErrorNCounterVec  *prometheus.CounterVec
	checkpointSecondsCounterVec *prometheus.CounterVec

	// Minimum threshold of WAL size, in pages, before a passive checkpoint.
	// A passive checkpoint will attempt a checkpoint but fail if there are
	// active transactions occurring at the same time.
	//
	// Uses PASSIVE checkpoint mode (non-blocking). Keeps WAL size manageable
	// for faster restores. Default: 1000 pages (~4MB with 4KB page size).
	MinCheckpointPageN int

	// Threshold of WAL size, in pages, before a forced truncation checkpoint.
	// A forced truncation checkpoint will block new transactions and wait for
	// existing transactions to finish before issuing a checkpoint and
	// truncating the WAL.
	//
	// Uses TRUNCATE checkpoint mode (blocking). Prevents unbounded WAL growth
	// from long-lived read transactions. Default: 121359 pages (~500MB with 4KB
	// page size). Set to 0 to disable forced truncation (use with caution as
	// WAL can grow unbounded if read transactions prevent checkpointing).
	TruncatePageN int

	// Time between automatic checkpoints in the WAL. This is done to allow
	// more fine-grained WAL files so that restores can be performed with
	// better precision.
	//
	// Uses PASSIVE checkpoint mode (non-blocking). Default: 1 minute.
	// Set to 0 to disable time-based checkpoints.
	CheckpointInterval time.Duration

	// Frequency at which to perform db sync.
	MonitorInterval time.Duration

	// The timeout to wait for EBUSY from SQLite.
	BusyTimeout time.Duration

	// Minimum time to retain L0 files after they have been compacted into L1.
	L0Retention time.Duration

	// Remote replica for the database.
	// Must be set before calling Open().
	Replica *Replica

	// Shutdown sync retry settings.
	// ShutdownSyncTimeout is the total time to retry syncing on shutdown.
	// ShutdownSyncInterval is the time between retry attempts.
	ShutdownSyncTimeout  time.Duration
	ShutdownSyncInterval time.Duration

	// Where to send log messages, defaults to global slog with database epath.
	Logger *slog.Logger
}

// NewDB returns a new instance of DB for a given path.
func NewDB(path string) *DB {
	dir, file := filepath.Split(path)

	db := &DB{
		path:     path,
		metaPath: filepath.Join(dir, "."+file+MetaDirSuffix),
		notify:   make(chan struct{}),

		MinCheckpointPageN:   DefaultMinCheckpointPageN,
		TruncatePageN:        DefaultTruncatePageN,
		CheckpointInterval:   DefaultCheckpointInterval,
		MonitorInterval:      DefaultMonitorInterval,
		BusyTimeout:          DefaultBusyTimeout,
		L0Retention:          DefaultL0Retention,
		ShutdownSyncTimeout:  DefaultShutdownSyncTimeout,
		ShutdownSyncInterval: DefaultShutdownSyncInterval,
		Logger:               slog.With("db", filepath.Base(path)),
	}
	db.maxLTXFileInfos.m = make(map[int]*ltx.FileInfo)

	db.dbSizeGauge = dbSizeGaugeVec.WithLabelValues(db.path)
	db.walSizeGauge = walSizeGaugeVec.WithLabelValues(db.path)
	db.totalWALBytesCounter = totalWALBytesCounterVec.WithLabelValues(db.path)
	db.txIDGauge = txIDIndexGaugeVec.WithLabelValues(db.path)
	db.syncNCounter = syncNCounterVec.WithLabelValues(db.path)
	db.syncErrorNCounter = syncErrorNCounterVec.WithLabelValues(db.path)
	db.syncSecondsCounter = syncSecondsCounterVec.WithLabelValues(db.path)
	db.checkpointNCounterVec = checkpointNCounterVec.MustCurryWith(prometheus.Labels{"db": db.path})
	db.checkpointErrorNCounterVec = checkpointErrorNCounterVec.MustCurryWith(prometheus.Labels{"db": db.path})
	db.checkpointSecondsCounterVec = checkpointSecondsCounterVec.MustCurryWith(prometheus.Labels{"db": db.path})

	db.ctx, db.cancel = context.WithCancel(context.Background())

	return db
}

// SQLDB returns a reference to the underlying sql.DB connection.
func (db *DB) SQLDB() *sql.DB {
	return db.db
}

// Path returns the path to the database.
func (db *DB) Path() string {
	return db.path
}

// WALPath returns the path to the database's WAL file.
func (db *DB) WALPath() string {
	return db.path + "-wal"
}

// MetaPath returns the path to the database metadata.
func (db *DB) MetaPath() string {
	return db.metaPath
}

// SetMetaPath sets the path to database metadata.
func (db *DB) SetMetaPath(path string) {
	db.metaPath = path
}

// LTXDir returns path of the root LTX directory.
func (db *DB) LTXDir() string {
	return filepath.Join(db.metaPath, "ltx")
}

// LTXLevelDir returns path of the given LTX compaction level.
// Panics if level is negative.
func (db *DB) LTXLevelDir(level int) string {
	return filepath.Join(db.LTXDir(), strconv.Itoa(level))
}

// LTXPath returns the local path of a single LTX file.
// Panics if level or either txn ID is negative.
func (db *DB) LTXPath(level int, minTXID, maxTXID ltx.TXID) string {
	assert(level >= 0, "level cannot be negative")
	return filepath.Join(db.LTXLevelDir(level), ltx.FormatFilename(minTXID, maxTXID))
}

// MaxLTX returns the last LTX file written to level 0.
func (db *DB) MaxLTX() (minTXID, maxTXID ltx.TXID, err error) {
	ents, err := os.ReadDir(db.LTXLevelDir(0))
	if os.IsNotExist(err) {
		return 0, 0, nil // no LTX files written
	} else if err != nil {
		return 0, 0, err
	}

	// Find highest txn ID.
	for _, ent := range ents {
		if min, max, err := ltx.ParseFilename(ent.Name()); err != nil {
			continue // invalid LTX filename
		} else if max > maxTXID {
			minTXID, maxTXID = min, max
		}
	}
	return minTXID, maxTXID, nil
}

// FileInfo returns the cached file stats for the database file when it was initialized.
func (db *DB) FileInfo() os.FileInfo {
	return db.fileInfo
}

// DirInfo returns the cached file stats for the parent directory of the database file when it was initialized.
func (db *DB) DirInfo() os.FileInfo {
	return db.dirInfo
}

// Pos returns the current replication position of the database.
func (db *DB) Pos() (ltx.Pos, error) {
	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		return ltx.Pos{}, err
	} else if minTXID == 0 {
		return ltx.Pos{}, nil // no replication yet
	}

	f, err := os.Open(db.LTXPath(0, minTXID, maxTXID))
	if err != nil {
		return ltx.Pos{}, err
	}
	defer func() { _ = f.Close() }()

	dec := ltx.NewDecoder(f)
	if err := dec.Verify(); err != nil {
		return ltx.Pos{}, fmt.Errorf("ltx verification failed: %w", err)
	}
	return dec.PostApplyPos(), nil
}

// Notify returns a channel that closes when the shadow WAL changes.
func (db *DB) Notify() <-chan struct{} {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.notify
}

// PageSize returns the page size of the underlying database.
// Only valid after database exists & Init() has successfully run.
func (db *DB) PageSize() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.pageSize
}

// Open initializes the background monitoring goroutine.
func (db *DB) Open() (err error) {
	// Validate fields on database.
	if db.MinCheckpointPageN <= 0 {
		return fmt.Errorf("minimum checkpoint page count required")
	}

	// Clear old temporary files that my have been left from a crash.
	if err := removeTmpFiles(db.metaPath); err != nil {
		return fmt.Errorf("cannot remove tmp files: %w", err)
	}

	// Start monitoring SQLite database in a separate goroutine.
	if db.MonitorInterval > 0 {
		db.wg.Add(1)
		go func() { defer db.wg.Done(); db.monitor() }()
	}

	return nil
}

// Close flushes outstanding WAL writes to replicas, releases the read lock,
// and closes the database. Takes a context for final sync.
func (db *DB) Close(ctx context.Context) (err error) {
	db.cancel()
	db.wg.Wait()

	// Perform a final db sync, if initialized.
	if db.db != nil {
		if e := db.Sync(ctx); e != nil {
			err = e
		}
	}

	// Ensure replicas perform a final sync and stop replicating.
	if db.Replica != nil {
		if db.db != nil {
			if e := db.syncReplicaWithRetry(ctx); e != nil && err == nil {
				err = e
			}
		}
		db.Replica.Stop(true)
	}

	// Release the read lock to allow other applications to handle checkpointing.
	if db.rtx != nil {
		if e := db.releaseReadLock(); e != nil && err == nil {
			err = e
		}
	}

	if db.db != nil {
		if e := db.db.Close(); e != nil && err == nil {
			err = e
		}
	}

	if db.f != nil {
		if e := db.f.Close(); e != nil && err == nil {
			err = e
		}
	}

	return err
}

// syncReplicaWithRetry attempts to sync the replica with retry logic for shutdown.
// It retries until success, timeout, or context cancellation.
// If ShutdownSyncTimeout is 0, it performs a single sync attempt without retries.
func (db *DB) syncReplicaWithRetry(ctx context.Context) error {
	if db.Replica == nil {
		return nil
	}

	timeout := db.ShutdownSyncTimeout
	interval := db.ShutdownSyncInterval

	// If timeout is zero, just try once (no retry)
	if timeout == 0 {
		return db.Replica.Sync(ctx)
	}

	// Use default interval if not set
	if interval == 0 {
		interval = DefaultShutdownSyncInterval
	}

	// Create deadline context for total retry duration
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastErr error
	attempt := 0
	startTime := time.Now()

	for {
		attempt++

		// Try sync
		if err := db.Replica.Sync(deadlineCtx); err == nil {
			if attempt > 1 {
				db.Logger.Info("shutdown sync succeeded after retry",
					"attempts", attempt,
					"duration", time.Since(startTime))
			}
			return nil
		} else {
			lastErr = err
		}

		// Check if we should stop retrying
		select {
		case <-deadlineCtx.Done():
			db.Logger.Error("shutdown sync failed after timeout",
				"attempts", attempt,
				"duration", time.Since(startTime),
				"lastError", lastErr)
			return fmt.Errorf("shutdown sync timeout after %d attempts: %w", attempt, lastErr)
		default:
		}

		// Log retry
		db.Logger.Warn("shutdown sync failed, retrying",
			"attempt", attempt,
			"error", lastErr,
			"elapsed", time.Since(startTime),
			"remaining", time.Until(startTime.Add(timeout)))

		// Wait before retry
		select {
		case <-time.After(interval):
		case <-deadlineCtx.Done():
			return fmt.Errorf("shutdown sync timeout after %d attempts: %w", attempt, lastErr)
		}
	}
}

// setPersistWAL sets the PERSIST_WAL file control on the database connection.
// This prevents SQLite from removing the WAL file when connections close.
func (db *DB) setPersistWAL(ctx context.Context) error {
	conn, err := db.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get connection: %w", err)
	}
	defer conn.Close()

	return conn.Raw(func(driverConn interface{}) error {
		fc, ok := driverConn.(sqlite.FileControl)
		if !ok {
			return fmt.Errorf("driver does not implement FileControl")
		}

		_, err := fc.FileControlPersistWAL("main", 1)
		if err != nil {
			return fmt.Errorf("FileControlPersistWAL: %w", err)
		}

		return nil
	})
}

// init initializes the connection to the database.
// Skipped if already initialized or if the database file does not exist.
func (db *DB) init(ctx context.Context) (err error) {
	// Exit if already initialized.
	if db.db != nil {
		return nil
	}

	// Exit if no database file exists.
	fi, err := os.Stat(db.path)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	db.fileInfo = fi

	// Obtain permissions for parent directory.
	if fi, err = os.Stat(filepath.Dir(db.path)); err != nil {
		return err
	}
	db.dirInfo = fi

	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(%d)",
		db.path, db.BusyTimeout.Milliseconds())

	if db.db, err = sql.Open("sqlite", dsn); err != nil {
		return err
	}

	// Set PERSIST_WAL to prevent WAL file removal when database connections close.
	if err := db.setPersistWAL(ctx); err != nil {
		return fmt.Errorf("set PERSIST_WAL: %w", err)
	}

	// Open long-running database file descriptor. Required for non-OFD locks.
	if db.f, err = os.Open(db.path); err != nil {
		return fmt.Errorf("open db file descriptor: %w", err)
	}

	// Ensure database is closed if init fails.
	// Initialization can retry on next sync.
	defer func() {
		if err != nil {
			_ = db.releaseReadLock()
			db.db.Close()
			db.f.Close()
			db.db, db.f = nil, nil
		}
	}()

	// Enable WAL and ensure it is set. New mode should be returned on success:
	// https://www.sqlite.org/pragma.html#pragma_journal_mode
	var mode string
	if err := db.db.QueryRowContext(ctx, `PRAGMA journal_mode = wal;`).Scan(&mode); err != nil {
		return err
	} else if mode != "wal" {
		return fmt.Errorf("enable wal failed, mode=%q", mode)
	}

	// Disable autocheckpoint for litestream's connection.
	if _, err := db.db.ExecContext(ctx, `PRAGMA wal_autocheckpoint = 0;`); err != nil {
		return fmt.Errorf("disable autocheckpoint: %w", err)
	}

	// Create a table to force writes to the WAL when empty.
	// There should only ever be one row with id=1.
	if _, err := db.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);`); err != nil {
		return fmt.Errorf("create _litestream_seq table: %w", err)
	}

	// Create a lock table to force write locks during sync.
	// The sync write transaction always rolls back so no data should be in this table.
	if _, err := db.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);`); err != nil {
		return fmt.Errorf("create _litestream_lock table: %w", err)
	}

	// Start a long-running read transaction to prevent other transactions
	// from checkpointing.
	if err := db.acquireReadLock(ctx); err != nil {
		return fmt.Errorf("acquire read lock: %w", err)
	}

	// Read page size.
	if err := db.db.QueryRowContext(ctx, `PRAGMA page_size;`).Scan(&db.pageSize); err != nil {
		return fmt.Errorf("read page size: %w", err)
	} else if db.pageSize <= 0 {
		return fmt.Errorf("invalid db page size: %d", db.pageSize)
	}

	// Ensure meta directory structure exists.
	if err := internal.MkdirAll(db.metaPath, db.dirInfo); err != nil {
		return err
	}

	// Ensure WAL has at least one frame in it.
	if err := db.ensureWALExists(ctx); err != nil {
		return fmt.Errorf("ensure wal exists: %w", err)
	}

	// Check if database is behind replica (issue #781).
	// This must happen before replica.Start() to detect restore scenarios.
	if db.Replica != nil {
		if err := db.checkDatabaseBehindReplica(ctx); err != nil {
			return fmt.Errorf("check database behind replica: %w", err)
		}
	}

	// If we have an existing replication files, ensure the headers match.
	// if err := db.verifyHeadersMatch(); err != nil {
	// 	return fmt.Errorf("cannot determine last wal position: %w", err)
	// }

	// TODO(gen): Generate diff of current LTX snapshot and save as next LTX file.

	// Start replication.
	if db.Replica != nil {
		db.Replica.Start(db.ctx)
	}

	return nil
}

/*
// verifyHeadersMatch returns an error if
func (db *DB) verifyHeadersMatch() error {
	pos, err := db.Pos()
	if err != nil {
		return false, fmt.Errorf("cannot determine position: %w", err)
	} else if pos.TXID == 0 {
		return true, nil // no replication performed yet
	}

	hdr0, err := readWALHeader(db.WALPath())
	if os.IsNotExist(err) {
		return false, fmt.Errorf("no wal: %w", err)
	} else if err != nil {
		return false, fmt.Errorf("read wal header: %w", err)
	}
	salt1 := binary.BigEndian.Uint32(hdr0[16:])
	salt2 := binary.BigEndian.Uint32(hdr0[20:])

	ltxPath := db.LTXPath(0, pos.TXID, pos.TXID)
	f, err := os.Open(ltxPath)
	if err != nil {
		return false, fmt.Errorf("open ltx path: %w", err)
	}
	defer func() { _ = f.Close() }()

	dec := ltx.NewDecoder(f)
	if err := dec.DecodeHeader(); err != nil {
		return false, fmt.Errorf("decode ltx header: %w", err)
	}
	hdr1 := dec.Header()
	if salt1 != hdr1.WALSalt1 || salt2 != hdr1.WALSalt2 {
		db.Logger.Log(internal.LevelTrace, "salt mismatch",
			"path", ltxPath,
			"wal", [2]uint32{salt1, salt2},
			"ltx", [2]uint32{hdr1.WALSalt1, hdr1.WALSalt2})
		return false, nil
	}
	return true, nil
}
*/

// acquireReadLock begins a read transaction on the database to prevent checkpointing.
func (db *DB) acquireReadLock(ctx context.Context) error {
	if db.rtx != nil {
		return nil
	}

	// Start long running read-transaction to prevent checkpoints.
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// Execute read query to obtain read lock.
	if _, err := tx.ExecContext(ctx, `SELECT COUNT(1) FROM _litestream_seq;`); err != nil {
		_ = tx.Rollback()
		return err
	}

	// Track transaction so we can release it later before checkpoint.
	db.rtx = tx
	return nil
}

// releaseReadLock rolls back the long-running read transaction.
func (db *DB) releaseReadLock() error {
	// Ignore if we do not have a read lock.
	if db.rtx == nil {
		return nil
	}

	// Rollback & clear read transaction.
	err := db.rtx.Rollback()
	db.rtx = nil
	return err
}

// Sync copies pending data from the WAL to the shadow WAL.
func (db *DB) Sync(ctx context.Context) (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Initialize database, if necessary. Exit if no DB exists.
	if err := db.init(ctx); err != nil {
		return err
	} else if db.db == nil {
		db.Logger.Debug("sync: no database found")
		return nil
	}

	// Track total sync metrics.
	t := time.Now()
	defer func() {
		db.syncNCounter.Inc()
		if err != nil {
			db.syncErrorNCounter.Inc()
		}
		db.syncSecondsCounter.Add(float64(time.Since(t).Seconds()))
	}()

	// Ensure WAL has at least one frame in it.
	if err := db.ensureWALExists(ctx); err != nil {
		return fmt.Errorf("ensure wal exists: %w", err)
	}

	origWALSize, newWALSize, synced, err := db.verifyAndSync(ctx, false)
	if err != nil {
		return err
	}

	// Track that data was synced for time-based checkpoint decisions.
	if synced {
		db.syncedSinceCheckpoint = true
	}

	if err := db.checkpointIfNeeded(ctx, origWALSize, newWALSize); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}

	// Compute current index and total shadow WAL size.
	pos, err := db.Pos()
	if err != nil {
		return fmt.Errorf("pos: %w", err)
	}
	db.txIDGauge.Set(float64(pos.TXID))

	// Update file size metrics.
	if fi, err := os.Stat(db.path); err == nil {
		db.dbSizeGauge.Set(float64(fi.Size()))
	}
	db.walSizeGauge.Set(float64(newWALSize))

	// Notify replicas of WAL changes.
	// if changed {
	close(db.notify)
	db.notify = make(chan struct{})
	// }

	return nil
}

func (db *DB) verifyAndSync(ctx context.Context, checkpointing bool) (origWALSize, newWALSize int64, synced bool, err error) {
	// Capture WAL size before sync for checkpoint threshold checks.
	origWALSize, err = db.walFileSize()
	if err != nil {
		return 0, 0, false, fmt.Errorf("stat wal before sync: %w", err)
	}

	// Verify our last sync matches the current state of the WAL.
	// This ensures that the last sync position of the real WAL hasn't
	// been overwritten by another process.
	info, err := db.verify(ctx)
	if err != nil {
		return 0, 0, false, fmt.Errorf("cannot verify wal state: %w", err)
	}

	synced, err = db.sync(ctx, checkpointing, info)
	if err != nil {
		return 0, 0, false, fmt.Errorf("sync: %w", err)
	}

	// Capture WAL size after sync for checkpoint threshold checks.
	newWALSize, err = db.walFileSize()
	if err != nil {
		return 0, 0, false, fmt.Errorf("stat wal after sync: %w", err)
	}

	return origWALSize, newWALSize, synced, nil
}

// checkpointIfNeeded performs a checkpoint based on configured thresholds.
// Checks thresholds in priority order: TruncatePageN → MinCheckpointPageN → CheckpointInterval.
//
// TruncatePageN uses TRUNCATE mode (blocking), others use PASSIVE mode (non-blocking).
//
// Time-based checkpoints only trigger if db.syncedSinceCheckpoint is true, indicating
// that data has been synced since the last checkpoint. This prevents creating unnecessary
// LTX files when the only WAL data is from internal bookkeeping (like _litestream_seq
// updates from previous checkpoints). See issue #896.
func (db *DB) checkpointIfNeeded(ctx context.Context, origWALSize, newWALSize int64) error {
	if db.pageSize == 0 {
		return nil
	}

	// Priority 1: Emergency truncate checkpoint (TRUNCATE mode, blocking)
	// This prevents unbounded WAL growth from long-lived read transactions.
	if db.TruncatePageN > 0 && origWALSize >= calcWALSize(uint32(db.pageSize), uint32(db.TruncatePageN)) {
		db.Logger.Info("forcing truncate checkpoint",
			"wal_size", origWALSize,
			"threshold", calcWALSize(uint32(db.pageSize), uint32(db.TruncatePageN)))
		return db.checkpoint(ctx, CheckpointModeTruncate)
	}

	// Priority 2: Regular checkpoint at min threshold (PASSIVE mode, non-blocking)
	if newWALSize >= calcWALSize(uint32(db.pageSize), uint32(db.MinCheckpointPageN)) {
		if err := db.checkpoint(ctx, CheckpointModePassive); err != nil {
			// PASSIVE checkpoints can fail with SQLITE_BUSY when database is locked.
			// This is expected behavior and not an error - just log and continue.
			if isSQLiteBusyError(err) {
				db.Logger.Log(ctx, internal.LevelTrace, "passive checkpoint skipped", "reason", "database busy")
				return nil
			}
			return err
		}
		return nil
	}

	// Priority 3: Time-based checkpoint (PASSIVE mode, non-blocking)
	// Only trigger if there have been actual changes synced since the last
	// checkpoint. This prevents creating unnecessary LTX files when the only
	// WAL data is from internal bookkeeping (like _litestream_seq updates).
	if db.CheckpointInterval > 0 && db.syncedSinceCheckpoint {
		// Get database file modification time
		fi, err := db.f.Stat()
		if err != nil {
			return fmt.Errorf("stat database: %w", err)
		}

		// Only checkpoint if enough time has passed and WAL has data
		if time.Since(fi.ModTime()) > db.CheckpointInterval && newWALSize > calcWALSize(uint32(db.pageSize), 1) {
			if err := db.checkpoint(ctx, CheckpointModePassive); err != nil {
				// PASSIVE checkpoints can fail with SQLITE_BUSY when database is locked.
				// This is expected behavior and not an error - just log and continue.
				if isSQLiteBusyError(err) {
					db.Logger.Log(ctx, internal.LevelTrace, "passive checkpoint skipped", "reason", "database busy")
					return nil
				}
				return err
			}
			return nil
		}
	}

	return nil
}

// isSQLiteBusyError returns true if the error is an SQLITE_BUSY error.
func isSQLiteBusyError(err error) bool {
	if err == nil {
		return false
	}
	// Check for "database is locked" or "SQLITE_BUSY" in error message
	errStr := err.Error()
	return strings.Contains(errStr, "database is locked") ||
		strings.Contains(errStr, "SQLITE_BUSY")
}

// walFileSize returns the size of the WAL file in bytes.
func (db *DB) walFileSize() (int64, error) {
	fi, err := os.Stat(db.WALPath())
	if os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// calcWALSize returns the size of the WAL for a given page size & count.
// Casts to int64 before multiplication to prevent uint32 overflow with large page sizes.
func calcWALSize(pageSize uint32, pageN uint32) int64 {
	return int64(WALHeaderSize) + (int64(WALFrameHeaderSize+pageSize) * int64(pageN))
}

// ensureWALExists checks that the real WAL exists and has a header.
func (db *DB) ensureWALExists(ctx context.Context) (err error) {
	// Exit early if WAL header exists.
	if fi, err := os.Stat(db.WALPath()); err == nil && fi.Size() >= WALHeaderSize {
		return nil
	}

	// Otherwise create transaction that updates the internal litestream table.
	_, err = db.db.ExecContext(ctx, `INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1`)
	return err
}

// checkDatabaseBehindReplica detects when a database has been restored to an
// earlier state and the replica has a higher TXID. This handles issue #781.
//
// If detected, it clears local L0 files and fetches the latest L0 LTX file
// from the replica to establish a baseline. The next DB.sync() will detect
// the mismatch and trigger a snapshot at the current database state.
func (db *DB) checkDatabaseBehindReplica(ctx context.Context) error {
	// Get database position from local L0 files
	dbPos, err := db.Pos()
	if err != nil {
		return fmt.Errorf("get database position: %w", err)
	}

	// Get replica position from remote
	replicaInfo, err := db.Replica.MaxLTXFileInfo(ctx, 0)
	if err != nil {
		return fmt.Errorf("get replica position: %w", err)
	} else if replicaInfo.MaxTXID == 0 {
		return nil // No remote replica data yet
	}

	// Check if database is behind replica
	if dbPos.TXID >= replicaInfo.MaxTXID {
		return nil // Database is ahead or equal
	}

	db.Logger.Info("detected database behind replica",
		"db_txid", dbPos.TXID,
		"replica_txid", replicaInfo.MaxTXID)

	// Clear local L0 files
	l0Dir := db.LTXLevelDir(0)
	if err := os.RemoveAll(l0Dir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove L0 directory: %w", err)
	}
	if err := internal.MkdirAll(l0Dir, db.dirInfo); err != nil {
		return fmt.Errorf("recreate L0 directory: %w", err)
	}

	// Fetch latest L0 LTX file from replica
	minTXID, maxTXID := replicaInfo.MinTXID, replicaInfo.MaxTXID
	reader, err := db.Replica.Client.OpenLTXFile(ctx, 0, minTXID, maxTXID, 0, 0)
	if err != nil {
		return fmt.Errorf("open remote L0 file: %w", err)
	}
	defer func() { _ = reader.Close() }()

	// Write to temp file and atomically rename
	localPath := db.LTXPath(0, minTXID, maxTXID)
	tmpPath := localPath + ".tmp"

	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp L0 file: %w", err)
	}
	defer func() { _ = os.Remove(tmpPath) }() // Clean up temp file on error

	if _, err := io.Copy(tmpFile, reader); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("copy L0 file: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("sync L0 file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close L0 file: %w", err)
	}

	// Atomically rename temp file to final path
	if err := os.Rename(tmpPath, localPath); err != nil {
		return fmt.Errorf("rename L0 file: %w", err)
	}

	db.Logger.Info("fetched latest L0 file from replica",
		"min_txid", minTXID,
		"max_txid", maxTXID)

	return nil
}

// verify ensures the current LTX state matches where it left off from
// the real WAL. Check info.ok if verification was successful.
func (db *DB) verify(ctx context.Context) (info syncInfo, err error) {
	frameSize := int64(db.pageSize + WALFrameHeaderSize)
	info.snapshotting = true

	pos, err := db.Pos()
	if err != nil {
		return info, fmt.Errorf("pos: %w", err)
	} else if pos.TXID == 0 {
		info.offset = WALHeaderSize
		return info, nil // first sync
	}

	// Determine last WAL offset we save from.
	ltxFile, err := os.Open(db.LTXPath(0, pos.TXID, pos.TXID))
	if err != nil {
		return info, fmt.Errorf("open ltx file: %w", err)
	}
	defer func() { _ = ltxFile.Close() }()

	dec := ltx.NewDecoder(ltxFile)
	if err := dec.DecodeHeader(); err != nil {
		return info, fmt.Errorf("decode ltx file: %w", err)
	}
	info.offset = dec.Header().WALOffset + dec.Header().WALSize
	info.salt1 = dec.Header().WALSalt1
	info.salt2 = dec.Header().WALSalt2

	// If LTX WAL offset is larger than real WAL then the WAL has been truncated
	// so we cannot determine our last state.
	if fi, err := os.Stat(db.WALPath()); err != nil {
		return info, fmt.Errorf("open wal file: %w", err)
	} else if info.offset > fi.Size() {
		info.reason = "wal truncated by another process"
		return info, nil
	}

	// Compare WAL headers. Restart from beginning of WAL if different.
	hdr0, err := readWALHeader(db.WALPath())
	if err != nil {
		return info, fmt.Errorf("cannot read wal header: %w", err)
	}
	salt1 := binary.BigEndian.Uint32(hdr0[16:])
	salt2 := binary.BigEndian.Uint32(hdr0[20:])
	saltMatch := salt1 == dec.Header().WALSalt1 && salt2 == dec.Header().WALSalt2

	// Handle edge case where we're at WAL header (WALOffset=32, WALSize=0).
	// This can happen when an LTX file represents a state at the beginning of the WAL
	// with no frames written yet. We must check this before computing prevWALOffset
	// to avoid underflow (32 - 4120 = -4088).
	// See: https://github.com/benbjohnson/litestream/issues/900
	if info.offset == WALHeaderSize {
		slog.Debug("verify", "saltMatch", saltMatch, "atWALHeader", true)
		if saltMatch {
			info.snapshotting = false
			return info, nil
		}
		info.reason = "wal header salt reset, snapshotting"
		return info, nil
	}

	// If offset is at the beginning of the first page, we can't check for previous page.
	prevWALOffset := info.offset - frameSize
	slog.Debug("verify", "saltMatch", saltMatch, "prevWALOffset", prevWALOffset)

	if prevWALOffset == WALHeaderSize {
		if saltMatch { // No writes occurred since last sync, salt still matches
			info.snapshotting = false
			return info, nil
		}
		// Salt has changed but we don't know if writes occurred since last sync
		info.reason = "wal header salt reset, snapshotting"
		return info, nil
	} else if prevWALOffset < WALHeaderSize {
		return info, fmt.Errorf("prev WAL offset is less than the header size: %d", prevWALOffset)
	}

	// If we can't verify the last page is in the last LTX file, then we need to snapshot.
	lastPageMatch, err := db.lastPageMatch(ctx, dec, prevWALOffset, frameSize)
	if err != nil {
		return info, fmt.Errorf("last page match: %w", err)
	} else if !lastPageMatch {
		info.reason = "last page does not exist in last ltx file, wal overwritten by another process"
		return info, nil
	}

	slog.Debug("verify.2", "lastPageMatch", lastPageMatch)

	// Salt has changed which could indicate a FULL checkpoint.
	// If we have a last page match, then we can assume that the WAL has not been overwritten.
	if !saltMatch {
		db.Logger.Log(ctx, internal.LevelTrace, "wal restarted",
			"salt1", salt1,
			"salt2", salt2)

		info.offset = WALHeaderSize
		info.salt1, info.salt2 = salt1, salt2

		if detected, err := db.detectFullCheckpoint(ctx, [][2]uint32{{salt1, salt2}, {dec.Header().WALSalt1, dec.Header().WALSalt2}}); err != nil {
			return info, fmt.Errorf("detect full checkpoint: %w", err)
		} else if detected {
			info.reason = "full or restart checkpoint detected, snapshotting"
		} else {
			info.snapshotting = false
		}

		return info, nil
	}

	info.snapshotting = false

	return info, nil
}

// lastPageMatch checks if the last page read in the WAL exists in the last LTX file.
func (db *DB) lastPageMatch(ctx context.Context, dec *ltx.Decoder, prevWALOffset, frameSize int64) (bool, error) {
	if prevWALOffset <= WALHeaderSize {
		return false, nil
	}

	frame, err := readWALFileAt(db.WALPath(), prevWALOffset, frameSize)
	if err != nil {
		return false, fmt.Errorf("cannot read last synced wal page: %w", err)
	}
	pgno := binary.BigEndian.Uint32(frame[0:])
	fsalt1 := binary.BigEndian.Uint32(frame[8:])
	fsalt2 := binary.BigEndian.Uint32(frame[12:])
	data := frame[WALFrameHeaderSize:]

	if fsalt1 != dec.Header().WALSalt1 || fsalt2 != dec.Header().WALSalt2 {
		return false, nil
	}

	// Verify that the last page in the WAL exists in the last LTX file.
	buf := make([]byte, dec.Header().PageSize)
	for {
		var hdr ltx.PageHeader
		if err := dec.DecodePage(&hdr, buf); errors.Is(err, io.EOF) {
			return false, nil // page not found in LTX file
		} else if err != nil {
			return false, fmt.Errorf("decode ltx page: %w", err)
		}

		if pgno != hdr.Pgno {
			continue // page number doesn't match
		}
		if !bytes.Equal(data, buf) {
			continue // page data doesn't match
		}
		return true, nil // Page matches
	}
}

// detectFullCheckpoint attempts to detect checks if a FULL or RESTART checkpoint
// has occurred and we may have missed some frames.
func (db *DB) detectFullCheckpoint(ctx context.Context, knownSalts [][2]uint32) (bool, error) {
	walFile, err := os.Open(db.WALPath())
	if err != nil {
		return false, fmt.Errorf("open wal file: %w", err)
	}
	defer walFile.Close()

	var lastKnownSalt [2]uint32
	if len(knownSalts) > 0 {
		lastKnownSalt = knownSalts[len(knownSalts)-1]
	}

	rd, err := NewWALReader(walFile, db.Logger)
	if err != nil {
		return false, fmt.Errorf("new wal reader: %w", err)
	}
	m, err := rd.FrameSaltsUntil(ctx, lastKnownSalt)
	if err != nil {
		return false, fmt.Errorf("frame salts until: %w", err)
	}

	// Remove known salts from the map.
	for _, salt := range knownSalts {
		delete(m, salt)
	}

	// If we have more than one unknown salt, then we have a FULL or RESTART checkpoint.
	return len(m) >= 1, nil
}

type syncInfo struct {
	offset       int64 // end of the previous LTX read
	salt1        uint32
	salt2        uint32
	snapshotting bool   // if true, a full snapshot is required
	reason       string // reason for snapshot
}

// sync copies pending bytes from the real WAL to LTX.
// Returns synced=true if an LTX file was created (i.e., there were new pages to sync).
func (db *DB) sync(ctx context.Context, checkpointing bool, info syncInfo) (synced bool, err error) {
	// Determine the next sequential transaction ID.
	pos, err := db.Pos()
	if err != nil {
		return false, fmt.Errorf("pos: %w", err)
	}
	txID := pos.TXID + 1

	filename := db.LTXPath(0, txID, txID)

	logArgs := []any{
		"txid", txID.String(),
		"offset", info.offset,
	}
	if checkpointing {
		logArgs = append(logArgs, "chkpt", "true")
	}
	if info.snapshotting {
		logArgs = append(logArgs, "snap", "true")
	}
	if info.reason != "" {
		logArgs = append(logArgs, "reason", info.reason)
	}
	db.Logger.Debug("sync", logArgs...)

	// Prevent internal checkpoints during sync. Ignore if already in a checkpoint.
	if !checkpointing {
		db.chkMu.RLock()
		defer db.chkMu.RUnlock()
	}

	fi, err := db.f.Stat()
	if err != nil {
		return false, err
	}
	mode := fi.Mode()
	commit := uint32(fi.Size() / int64(db.pageSize))

	walFile, err := os.Open(db.WALPath())
	if err != nil {
		return false, err
	}
	defer walFile.Close()

	var rd *WALReader
	if info.offset == WALHeaderSize {
		if rd, err = NewWALReader(walFile, db.Logger); err != nil {
			return false, fmt.Errorf("new wal reader: %w", err)
		}
	} else {
		// If we cannot verify the previous frame
		var pfmError *PrevFrameMismatchError
		if rd, err = NewWALReaderWithOffset(ctx, walFile, info.offset, info.salt1, info.salt2, db.Logger); errors.As(err, &pfmError) {
			db.Logger.Log(ctx, internal.LevelTrace, "prev frame mismatch, snapshotting", "err", pfmError.Err)
			info.offset = WALHeaderSize
			if rd, err = NewWALReader(walFile, db.Logger); err != nil {
				return false, fmt.Errorf("new wal reader, after reset")
			}
		} else if err != nil {
			return false, fmt.Errorf("new wal reader with offset: %w", err)
		}
	}

	// Build a mapping of changed page numbers and their latest content.
	pageMap, maxOffset, walCommit, err := rd.PageMap(ctx)
	if err != nil {
		return false, fmt.Errorf("page map: %w", err)
	}
	if walCommit > 0 {
		commit = walCommit
	}

	var sz int64
	if maxOffset > 0 {
		sz = maxOffset - info.offset
	}
	assert(sz >= 0, fmt.Sprintf("wal size must be positive: sz=%d, maxOffset=%d, info.offset=%d", sz, maxOffset, info.offset))

	// Track total WAL bytes synced.
	if sz > 0 {
		db.totalWALBytesCounter.Add(float64(sz))
	}

	// Exit if we have no new WAL pages and we aren't snapshotting.
	if !info.snapshotting && sz == 0 {
		db.Logger.Log(ctx, internal.LevelTrace, "sync: skip", "reason", "no new wal pages")
		return false, nil
	}

	tmpFilename := filename + ".tmp"
	if err := internal.MkdirAll(filepath.Dir(tmpFilename), db.dirInfo); err != nil {
		return false, err
	}

	ltxFile, err := os.OpenFile(tmpFilename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return false, fmt.Errorf("open temp ltx file: %w", err)
	}
	defer func() { _ = os.Remove(tmpFilename) }()
	defer func() { _ = ltxFile.Close() }()

	uid, gid := internal.Fileinfo(db.fileInfo)
	_ = os.Chown(tmpFilename, uid, gid)

	db.Logger.Log(ctx, internal.LevelTrace, "encode header",
		"txid", txID.String(),
		"commit", commit,
		"walOffset", info.offset,
		"walSize", sz,
		"salt1", rd.salt1,
		"salt2", rd.salt2)

	timestamp := time.Now()
	enc, err := ltx.NewEncoder(ltxFile)
	if err != nil {
		return false, fmt.Errorf("new ltx encoder: %w", err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  uint32(db.pageSize),
		Commit:    commit,
		MinTXID:   txID,
		MaxTXID:   txID,
		Timestamp: timestamp.UnixMilli(),
		WALOffset: info.offset,
		WALSize:   sz,
		WALSalt1:  rd.salt1,
		WALSalt2:  rd.salt2,
	}); err != nil {
		return false, fmt.Errorf("encode ltx header: %w", err)
	}

	// If we need a full snapshot, then copy from the database & WAL.
	// Otherwise, just copy incrementally from the WAL.
	if info.snapshotting {
		if err := db.writeLTXFromDB(ctx, enc, walFile, commit, pageMap); err != nil {
			return false, fmt.Errorf("write ltx from db: %w", err)
		}
	} else {
		if err := db.writeLTXFromWAL(ctx, enc, walFile, pageMap); err != nil {
			return false, fmt.Errorf("write ltx from wal: %w", err)
		}
	}

	// Encode final trailer to the end of the LTX file.
	if err := enc.Close(); err != nil {
		return false, fmt.Errorf("close ltx encoder: %w", err)
	}

	// Sync & close LTX file.
	if err := ltxFile.Sync(); err != nil {
		return false, fmt.Errorf("sync ltx file: %w", err)
	}
	if err := ltxFile.Close(); err != nil {
		return false, fmt.Errorf("close ltx file: %w", err)
	}

	// Atomically rename file to final path.
	if err := os.Rename(tmpFilename, filename); err != nil {
		db.maxLTXFileInfos.Lock()
		delete(db.maxLTXFileInfos.m, 0) // clear cache if in unknown state
		db.maxLTXFileInfos.Unlock()
		return false, fmt.Errorf("rename ltx file: %w", err)
	}

	// Update file info cache for L0.
	db.maxLTXFileInfos.Lock()
	db.maxLTXFileInfos.m[0] = &ltx.FileInfo{
		Level:     0,
		MinTXID:   txID,
		MaxTXID:   txID,
		CreatedAt: time.Now(),
		Size:      enc.N(),
	}
	db.maxLTXFileInfos.Unlock()

	db.Logger.Debug("db sync", "status", "ok")

	return true, nil
}

func (db *DB) writeLTXFromDB(ctx context.Context, enc *ltx.Encoder, walFile *os.File, commit uint32, pageMap map[uint32]int64) error {
	lockPgno := ltx.LockPgno(uint32(db.pageSize))
	data := make([]byte, db.pageSize)

	for pgno := uint32(1); pgno <= commit; pgno++ {
		if pgno == lockPgno {
			continue
		}

		// Check if the caller has canceled during processing.
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}

		// If page exists in the WAL, read from there.
		if offset, ok := pageMap[pgno]; ok {
			db.Logger.Log(ctx, internal.LevelTrace, "encode page from wal", "txid", enc.Header().MinTXID, "offset", offset, "pgno", pgno, "type", "db+wal")

			if n, err := walFile.ReadAt(data, offset+WALFrameHeaderSize); err != nil {
				return fmt.Errorf("read page %d @ %d: %w", pgno, offset, err)
			} else if n != len(data) {
				return fmt.Errorf("short read page %d @ %d", pgno, offset)
			}

			if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, data); err != nil {
				return fmt.Errorf("encode ltx frame (pgno=%d): %w", pgno, err)
			}
			continue
		}

		offset := int64(pgno-1) * int64(db.pageSize)
		db.Logger.Log(ctx, internal.LevelTrace, "encode page from database", "offset", offset, "pgno", pgno)

		// Otherwise read directly from the database file.
		if _, err := db.f.ReadAt(data, offset); err != nil {
			return fmt.Errorf("read database page %d: %w", pgno, err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, data); err != nil {
			return fmt.Errorf("encode ltx frame (pgno=%d): %w", pgno, err)
		}
	}

	return nil
}

func (db *DB) writeLTXFromWAL(ctx context.Context, enc *ltx.Encoder, walFile *os.File, pageMap map[uint32]int64) error {
	// Create an ordered list of page numbers since the LTX encoder requires it.
	pgnos := make([]uint32, 0, len(pageMap))
	for pgno := range pageMap {
		pgnos = append(pgnos, pgno)
	}
	slices.Sort(pgnos)

	data := make([]byte, db.pageSize)
	for _, pgno := range pgnos {
		offset := pageMap[pgno]

		db.Logger.Log(ctx, internal.LevelTrace, "encode page from wal", "txid", enc.Header().MinTXID, "offset", offset, "pgno", pgno, "type", "walonly")

		// Read source page using page map.
		if n, err := walFile.ReadAt(data, offset+WALFrameHeaderSize); err != nil {
			return fmt.Errorf("read page %d @ %d: %w", pgno, offset, err)
		} else if n != len(data) {
			return fmt.Errorf("short read page %d @ %d", pgno, offset)
		}

		// Write page to LTX encoder.
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, data); err != nil {
			return fmt.Errorf("encode ltx frame (pgno=%d): %w", pgno, err)
		}
	}
	return nil
}

// Checkpoint performs a checkpoint on the WAL file.
func (db *DB) Checkpoint(ctx context.Context, mode string) (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.checkpoint(ctx, mode)
}

// checkpoint performs a checkpoint on the WAL file and initializes a
// new shadow WAL file.
func (db *DB) checkpoint(ctx context.Context, mode string) error {
	// Try getting a checkpoint lock, will fail during snapshots.
	if !db.chkMu.TryLock() {
		return nil
	}
	defer db.chkMu.Unlock()

	// Read WAL header before checkpoint to check if it has been restarted.
	hdr, err := readWALHeader(db.WALPath())
	if err != nil {
		return err
	}

	// Copy end of WAL before checkpoint to copy as much as possible.
	if _, _, _, err := db.verifyAndSync(ctx, true); err != nil {
		return fmt.Errorf("cannot copy wal before checkpoint: %w", err)
	}

	// Execute checkpoint and immediately issue a write to the WAL to ensure
	// a new page is written.
	if err := db.execCheckpoint(ctx, mode); err != nil {
		return err
	} else if _, err = db.db.ExecContext(ctx, `INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1`); err != nil {
		return err
	}

	// If WAL hasn't been restarted, exit.
	if other, err := readWALHeader(db.WALPath()); err != nil {
		return err
	} else if bytes.Equal(hdr, other) {
		db.syncedSinceCheckpoint = false
		return nil
	}

	// Start a transaction. This will be promoted immediately after.
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = rollback(tx) }()

	// Insert into the lock table to promote to a write tx. The lock table
	// insert will never actually occur because our tx will be rolled back,
	// however, it will ensure our tx grabs the write lock. Unfortunately,
	// we can't call "BEGIN IMMEDIATE" as we are already in a transaction.
	if _, err := tx.ExecContext(ctx, `INSERT INTO _litestream_lock (id) VALUES (1);`); err != nil {
		return fmt.Errorf("_litestream_lock: %w", err)
	}

	// Copy anything that may have occurred after the checkpoint.
	if _, _, _, err := db.verifyAndSync(ctx, true); err != nil {
		return fmt.Errorf("cannot copy wal after checkpoint: %w", err)
	}

	// Release write lock before exiting.
	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("rollback post-checkpoint tx: %w", err)
	}

	db.syncedSinceCheckpoint = false
	return nil
}

func (db *DB) execCheckpoint(ctx context.Context, mode string) (err error) {
	// Ignore if there is no underlying database.
	if db.db == nil {
		return nil
	}

	// Track checkpoint metrics.
	t := time.Now()
	defer func() {
		labels := prometheus.Labels{"mode": mode}
		db.checkpointNCounterVec.With(labels).Inc()
		if err != nil {
			db.checkpointErrorNCounterVec.With(labels).Inc()
		}
		db.checkpointSecondsCounterVec.With(labels).Add(float64(time.Since(t).Seconds()))
	}()

	// Ensure the read lock has been removed before issuing a checkpoint.
	// We defer the re-acquire to ensure it occurs even on an early return.
	if err := db.releaseReadLock(); err != nil {
		return fmt.Errorf("release read lock: %w", err)
	}
	defer func() { _ = db.acquireReadLock(ctx) }()

	// A non-forced checkpoint is issued as "PASSIVE". This will only checkpoint
	// if there are not pending transactions. A forced checkpoint ("RESTART")
	// will wait for pending transactions to end & block new transactions before
	// forcing the checkpoint and restarting the WAL.
	//
	// See: https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
	rawsql := `PRAGMA wal_checkpoint(` + mode + `);`

	var row [3]int
	if err := db.db.QueryRowContext(ctx, rawsql).Scan(&row[0], &row[1], &row[2]); err != nil {
		return err
	}
	db.Logger.Debug("checkpoint", "mode", mode, "result", fmt.Sprintf("%d,%d,%d", row[0], row[1], row[2]))

	// Reacquire the read lock immediately after the checkpoint.
	if err := db.acquireReadLock(ctx); err != nil {
		return fmt.Errorf("reacquire read lock: %w", err)
	}

	return nil
}

// SnapshotReader returns the current position of the database & a reader that contains a full database snapshot.
func (db *DB) SnapshotReader(ctx context.Context) (ltx.Pos, io.Reader, error) {
	if db.PageSize() == 0 {
		return ltx.Pos{}, nil, fmt.Errorf("page size not initialized yet")
	}

	pos, err := db.Pos()
	if err != nil {
		return pos, nil, fmt.Errorf("pos: %w", err)
	}

	db.Logger.Debug("snapshot", "txid", pos.TXID.String())

	// Prevent internal checkpoints during sync.
	db.chkMu.RLock()
	defer db.chkMu.RUnlock()

	// TODO(ltx): Read database size from database header.

	fi, err := db.f.Stat()
	if err != nil {
		return pos, nil, err
	}
	commit := uint32(fi.Size() / int64(db.pageSize))

	// Execute encoding in a separate goroutine so the caller can initialize before reading.
	pr, pw := io.Pipe()
	go func() {
		walFile, err := os.Open(db.WALPath())
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		defer walFile.Close()

		rd, err := NewWALReader(walFile, db.Logger)
		if err != nil {
			pw.CloseWithError(fmt.Errorf("new wal reader: %w", err))
			return
		}

		// Build a mapping of changed page numbers and their latest content.
		pageMap, maxOffset, walCommit, err := rd.PageMap(ctx)
		if err != nil {
			pw.CloseWithError(fmt.Errorf("page map: %w", err))
			return
		}
		if walCommit > 0 {
			commit = walCommit
		}

		var sz int64
		if maxOffset > 0 {
			sz = maxOffset - rd.Offset()
		}

		db.Logger.Debug("encode snapshot header",
			"txid", pos.TXID.String(),
			"commit", commit,
			"walOffset", rd.Offset(),
			"walSize", sz,
			"salt1", rd.salt1,
			"salt2", rd.salt2)

		enc, err := ltx.NewEncoder(pw)
		if err != nil {
			pw.CloseWithError(fmt.Errorf("new ltx encoder: %w", err))
			return
		}
		if err := enc.EncodeHeader(ltx.Header{
			Version:   ltx.Version,
			Flags:     ltx.HeaderFlagNoChecksum,
			PageSize:  uint32(db.pageSize),
			Commit:    commit,
			MinTXID:   1,
			MaxTXID:   pos.TXID,
			Timestamp: time.Now().UnixMilli(),
			WALOffset: rd.Offset(),
			WALSize:   sz,
			WALSalt1:  rd.salt1,
			WALSalt2:  rd.salt2,
		}); err != nil {
			pw.CloseWithError(fmt.Errorf("encode ltx snapshot header: %w", err))
			return
		}

		if err := db.writeLTXFromDB(ctx, enc, walFile, commit, pageMap); err != nil {
			pw.CloseWithError(fmt.Errorf("write snapshot ltx: %w", err))
			return
		}

		if err := enc.Close(); err != nil {
			pw.CloseWithError(fmt.Errorf("close ltx snapshot encoder: %w", err))
			return
		}
		_ = pw.Close()
	}()

	return pos, pr, nil
}

// Compact performs a compaction of the LTX file at the previous level into dstLevel.
// Returns metadata for the newly written compaction file. Returns ErrNoCompaction
// if no new files are available to be compacted.
func (db *DB) Compact(ctx context.Context, dstLevel int) (*ltx.FileInfo, error) {
	srcLevel := dstLevel - 1

	prevMaxInfo, err := db.Replica.MaxLTXFileInfo(ctx, dstLevel)
	if err != nil {
		return nil, fmt.Errorf("cannot determine max ltx file for destination level: %w", err)
	}
	seekTXID := prevMaxInfo.MaxTXID + 1

	// Collect files after last compaction.
	// Normal operation - use fast timestamps
	itr, err := db.Replica.Client.LTXFiles(ctx, srcLevel, seekTXID, false)
	if err != nil {
		return nil, fmt.Errorf("source ltx files after %s: %w", seekTXID, err)
	}
	defer itr.Close()

	// Ensure all readers are closed by the end, even if an error occurs.
	var rdrs []io.Reader
	defer func() {
		for _, rd := range rdrs {
			if closer, ok := rd.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	}()

	// Build a list of input files to compact from.
	var minTXID, maxTXID ltx.TXID
	for itr.Next() {
		info := itr.Item()

		// Track TXID bounds of all files being compacted.
		if minTXID == 0 || info.MinTXID < minTXID {
			minTXID = info.MinTXID
		}
		if maxTXID == 0 || info.MaxTXID > maxTXID {
			maxTXID = info.MaxTXID
		}

		// Prefer the on-disk LTX file when available so compaction is not subject
		// to eventual consistency quirks of remote storage providers. Fallback to
		// downloading the file from the replica client if the local copy has been
		// removed.
		if f, err := os.Open(db.LTXPath(srcLevel, info.MinTXID, info.MaxTXID)); err == nil {
			rdrs = append(rdrs, f)
			continue
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("open local ltx file: %w", err)
		}

		f, err := db.Replica.Client.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
		if err != nil {
			return nil, fmt.Errorf("open ltx file: %w", err)
		}
		rdrs = append(rdrs, f)
	}
	if len(rdrs) == 0 {
		return nil, ErrNoCompaction
	}

	// Stream compaction to destination in level.
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

	info, err := db.Replica.Client.WriteLTXFile(ctx, dstLevel, minTXID, maxTXID, pr)
	if err != nil {
		return nil, fmt.Errorf("write ltx file: %w", err)
	}

	// Cache last metadata for the level.
	db.maxLTXFileInfos.Lock()
	db.maxLTXFileInfos.m[dstLevel] = info
	db.maxLTXFileInfos.Unlock()

	// If this is L1, clean up L0 files using the time-based retention policy.
	if dstLevel == 1 {
		if err := db.EnforceL0RetentionByTime(ctx); err != nil {
			// Don't log context cancellation errors during shutdown
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				db.Logger.Error("enforce L0 time retention", "error", err)
			}
		}
	}

	return info, nil
}

// SnapshotDB writes a snapshot to the replica for the current position of the database.
func (db *DB) Snapshot(ctx context.Context) (*ltx.FileInfo, error) {
	pos, r, err := db.SnapshotReader(ctx)
	if err != nil {
		return nil, err
	}
	info, err := db.Replica.Client.WriteLTXFile(ctx, SnapshotLevel, 1, pos.TXID, r)
	if err != nil {
		return info, err
	}

	db.maxLTXFileInfos.Lock()
	db.maxLTXFileInfos.m[SnapshotLevel] = info
	db.maxLTXFileInfos.Unlock()

	return info, nil
}

// EnforceSnapshotRetention enforces retention of the snapshot level in the database by timestamp.
func (db *DB) EnforceSnapshotRetention(ctx context.Context, timestamp time.Time) (minSnapshotTXID ltx.TXID, err error) {
	db.Logger.Debug("enforcing snapshot retention", "timestamp", timestamp)

	// Normal operation - use fast timestamps
	itr, err := db.Replica.Client.LTXFiles(ctx, SnapshotLevel, 0, false)
	if err != nil {
		return 0, fmt.Errorf("fetch ltx files: %w", err)
	}
	defer itr.Close()

	var deleted []*ltx.FileInfo
	var lastInfo *ltx.FileInfo
	for itr.Next() {
		info := itr.Item()
		lastInfo = info

		// If this snapshot is before the retention timestamp, mark it for deletion.
		if info.CreatedAt.Before(timestamp) {
			deleted = append(deleted, info)
			continue
		}

		// Track the lowest snapshot TXID so we can enforce retention in lower levels.
		// This is only tracked for snapshots not marked for deletion.
		if minSnapshotTXID == 0 || info.MaxTXID < minSnapshotTXID {
			minSnapshotTXID = info.MaxTXID
		}
	}

	// If this is the snapshot level, we need to ensure that at least one snapshot exists.
	if len(deleted) > 0 && deleted[len(deleted)-1] == lastInfo {
		deleted = deleted[:len(deleted)-1]
	}

	// Remove all files marked for deletion from both remote and local storage.
	if err := db.Replica.Client.DeleteLTXFiles(ctx, deleted); err != nil {
		return 0, fmt.Errorf("remove ltx files: %w", err)
	}

	for _, info := range deleted {
		localPath := db.LTXPath(SnapshotLevel, info.MinTXID, info.MaxTXID)
		db.Logger.Debug("deleting local ltx file", "level", SnapshotLevel, "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "path", localPath)

		if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
			db.Logger.Error("failed to remove local ltx file", "path", localPath, "error", err)
		}
	}

	return minSnapshotTXID, nil
}

// EnforceL0RetentionByTime retains L0 files until they have been compacted into
// L1 and have existed for at least L0Retention.
func (db *DB) EnforceL0RetentionByTime(ctx context.Context) error {
	if db.L0Retention <= 0 {
		return nil
	}

	db.Logger.Debug("enforcing l0 retention", "retention", db.L0Retention)

	// Determine the highest TXID that has been compacted into L1.
	itr, err := db.Replica.Client.LTXFiles(ctx, 1, 0, false)
	if err != nil {
		return fmt.Errorf("fetch l1 files: %w", err)
	}
	var maxL1TXID ltx.TXID
	for itr.Next() {
		info := itr.Item()
		if info.MaxTXID > maxL1TXID {
			maxL1TXID = info.MaxTXID
		}
	}
	if err := itr.Close(); err != nil {
		return fmt.Errorf("close l1 iterator: %w", err)
	}
	if maxL1TXID == 0 {
		return nil
	}

	threshold := time.Now().Add(-db.L0Retention)
	itr, err = db.Replica.Client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		return fmt.Errorf("fetch l0 files: %w", err)
	}
	defer itr.Close()

	var (
		deleted      []*ltx.FileInfo
		lastInfo     *ltx.FileInfo
		processedAll = true
	)
	for itr.Next() {
		info := itr.Item()
		lastInfo = info

		createdAt := info.CreatedAt
		if createdAt.IsZero() {
			if fi, err := os.Stat(db.LTXPath(0, info.MinTXID, info.MaxTXID)); err == nil {
				createdAt = fi.ModTime().UTC()
			} else {
				createdAt = threshold
			}
		}

		if createdAt.After(threshold) {
			// L0 entries are ordered; once we reach a newer file we stop so we don't
			// create gaps between retained files. VFS expects contiguous coverage.
			processedAll = false
			break
		}

		if info.MaxTXID <= maxL1TXID {
			deleted = append(deleted, info)
		}
	}

	// Ensure we do not delete the newest L0 file only if we processed the entire level.
	if processedAll && len(deleted) > 0 && lastInfo != nil && deleted[len(deleted)-1] == lastInfo {
		deleted = deleted[:len(deleted)-1]
	}

	if len(deleted) == 0 {
		return nil
	}

	if err := db.Replica.Client.DeleteLTXFiles(ctx, deleted); err != nil {
		return fmt.Errorf("remove expired l0 files: %w", err)
	}

	for _, info := range deleted {
		localPath := db.LTXPath(0, info.MinTXID, info.MaxTXID)
		db.Logger.Debug("deleting expired local l0 file", "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "path", localPath)
		if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
			db.Logger.Error("failed to remove local l0 file", "path", localPath, "error", err)
		}
	}

	return nil
}

// EnforceRetentionByTXID enforces retention so that any LTX files below
// the target TXID are deleted. Always keep at least one file.
func (db *DB) EnforceRetentionByTXID(ctx context.Context, level int, txID ltx.TXID) (err error) {
	db.Logger.Debug("enforcing retention", "level", level, "txid", txID)

	// Normal operation - use fast timestamps
	itr, err := db.Replica.Client.LTXFiles(ctx, level, 0, false)
	if err != nil {
		return fmt.Errorf("fetch ltx files: %w", err)
	}
	defer itr.Close()

	var deleted []*ltx.FileInfo
	var lastInfo *ltx.FileInfo
	for itr.Next() {
		info := itr.Item()
		lastInfo = info

		// If this file's maxTXID is below the target TXID, mark it for deletion.
		if info.MaxTXID < txID {
			deleted = append(deleted, info)
			continue
		}
	}

	// Ensure we don't delete the last file.
	if len(deleted) > 0 && deleted[len(deleted)-1] == lastInfo {
		deleted = deleted[:len(deleted)-1]
	}

	// Remove all files marked for deletion from both remote and local storage.
	if err := db.Replica.Client.DeleteLTXFiles(ctx, deleted); err != nil {
		return fmt.Errorf("remove ltx files: %w", err)
	}

	for _, info := range deleted {
		localPath := db.LTXPath(level, info.MinTXID, info.MaxTXID)
		db.Logger.Debug("deleting local ltx file", "level", level, "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "path", localPath)

		if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
			db.Logger.Error("failed to remove local ltx file", "path", localPath, "error", err)
		}
	}

	return nil
}

// monitor runs in a separate goroutine and monitors the database & WAL.
func (db *DB) monitor() {
	ticker := time.NewTicker(db.MonitorInterval)
	defer ticker.Stop()

	for {
		// Wait for ticker or context close.
		select {
		case <-db.ctx.Done():
			return
		case <-ticker.C:
		}

		// Sync the database to the shadow WAL.
		if err := db.Sync(db.ctx); err != nil && !errors.Is(err, context.Canceled) {
			db.Logger.Error("sync error", "error", err)
		}
	}
}

// CRC64 returns a CRC-64 ISO checksum of the database and its current position.
//
// This function obtains a read lock so it prevents syncs from occurring until
// the operation is complete. The database will still be usable but it will be
// unable to checkpoint during this time.
//
// If dst is set, the database file is copied to that location before checksum.
func (db *DB) CRC64(ctx context.Context) (uint64, ltx.Pos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.init(ctx); err != nil {
		return 0, ltx.Pos{}, err
	} else if db.db == nil {
		return 0, ltx.Pos{}, os.ErrNotExist
	}

	// Force a RESTART checkpoint to ensure the database is at the start of the WAL.
	if err := db.checkpoint(ctx, CheckpointModeRestart); err != nil {
		return 0, ltx.Pos{}, err
	}

	// Obtain current position. Clear the offset since we are only reading the
	// DB and not applying the current WAL.
	pos, err := db.Pos()
	if err != nil {
		return 0, pos, err
	}

	// Seek to the beginning of the db file descriptor and checksum whole file.
	h := crc64.New(crc64.MakeTable(crc64.ISO))
	if _, err := db.f.Seek(0, io.SeekStart); err != nil {
		return 0, pos, err
	} else if _, err := io.Copy(h, db.f); err != nil {
		return 0, pos, err
	}
	return h.Sum64(), pos, nil
}

// MaxLTXFileInfo returns the metadata for the last LTX file in a level.
// If cached, it will returned the local copy. Otherwise, it fetches from the replica.
func (db *DB) MaxLTXFileInfo(ctx context.Context, level int) (ltx.FileInfo, error) {
	db.maxLTXFileInfos.Lock()
	defer db.maxLTXFileInfos.Unlock()

	info, ok := db.maxLTXFileInfos.m[level]
	if ok {
		return *info, nil
	}

	remoteInfo, err := db.Replica.MaxLTXFileInfo(ctx, level)
	if err != nil {
		return ltx.FileInfo{}, fmt.Errorf("cannot determine L%d max ltx file for %q: %w", level, db.Path(), err)
	}

	db.maxLTXFileInfos.m[level] = &remoteInfo
	return remoteInfo, nil
}

// DefaultRestoreParallelism is the default parallelism when downloading WAL files.
const DefaultRestoreParallelism = 8

// RestoreOptions represents options for DB.Restore().
type RestoreOptions struct {
	// Target path to restore into.
	// If blank, the original DB path is used.
	OutputPath string

	// Specific transaction to restore to.
	// If zero, TXID is ignored.
	TXID ltx.TXID

	// Point-in-time to restore database.
	// If zero, database restore to most recent state available.
	Timestamp time.Time

	// Specifies how many WAL files are downloaded in parallel during restore.
	Parallelism int
}

// NewRestoreOptions returns a new instance of RestoreOptions with defaults.
func NewRestoreOptions() RestoreOptions {
	return RestoreOptions{
		Parallelism: DefaultRestoreParallelism,
	}
}

// Database metrics.
var (
	dbSizeGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litestream_db_size",
		Help: "The current size of the real DB",
	}, []string{"db"})

	walSizeGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litestream_wal_size",
		Help: "The current size of the real WAL",
	}, []string{"db"})

	totalWALBytesCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litestream_total_wal_bytes",
		Help: "Total number of bytes written to shadow WAL",
	}, []string{"db"})

	txIDIndexGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litestream_txid",
		Help: "The current transaction ID",
	}, []string{"db"})

	syncNCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litestream_sync_count",
		Help: "Number of sync operations performed",
	}, []string{"db"})

	syncErrorNCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litestream_sync_error_count",
		Help: "Number of sync errors that have occurred",
	}, []string{"db"})

	syncSecondsCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litestream_sync_seconds",
		Help: "Time spent syncing shadow WAL, in seconds",
	}, []string{"db"})

	checkpointNCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litestream_checkpoint_count",
		Help: "Number of checkpoint operations performed",
	}, []string{"db", "mode"})

	checkpointErrorNCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litestream_checkpoint_error_count",
		Help: "Number of checkpoint errors that have occurred",
	}, []string{"db", "mode"})

	checkpointSecondsCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litestream_checkpoint_seconds",
		Help: "Time spent checkpointing WAL, in seconds",
	}, []string{"db", "mode"})
)
