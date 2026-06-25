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
	"golang.org/x/sync/semaphore"
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
	DefaultMaxSyncWALBytes      = 64 << 20
	DefaultShutdownSyncTimeout  = 30 * time.Second
	DefaultShutdownSyncInterval = 500 * time.Millisecond

	// Sync error backoff configuration.
	// When sync errors occur repeatedly (e.g., disk full), backoff doubles each time.
	DefaultSyncBackoffMax = 5 * time.Minute  // Maximum backoff between retries
	SyncErrorLogInterval  = 30 * time.Second // Rate-limit repeated error logging
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
	mu        sync.RWMutex
	execSem   *semaphore.Weighted
	path      string        // part to database
	metaPath  string        // Path to the database metadata.
	db        *sql.DB       // target database
	f         *os.File      // long-running db file descriptor
	rtx       *sql.Tx       // long running read transaction
	pageSize  int           // page size, in bytes
	notify    chan struct{} // closes on WAL change
	chkMu     sync.RWMutex  // checkpoint lock
	opened    bool          // true if Open() was called and Close() not yet called
	syncState syncState
	syncDiag  diagState

	// last file info for each level
	maxLTXFileInfos struct {
		sync.Mutex
		m map[int]*ltx.FileInfo
	}

	// Cached position from the latest L0 LTX file.
	// nil means cache is invalid; non-nil is the cached position.
	pos struct {
		sync.Mutex
		value *ltx.Pos
	}

	fileInfo os.FileInfo // db info cached during init
	dirInfo  os.FileInfo // parent dir info cached during init

	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup
	Done   <-chan struct{}

	// Metrics
	dbSizeGauge                 prometheus.Gauge
	walSizeGauge                prometheus.Gauge
	totalWALBytesCounter        prometheus.Counter
	txIDGauge                   prometheus.Gauge
	syncNCounter                prometheus.Counter
	syncErrorNCounter           prometheus.Counter
	syncSecondsCounter          prometheus.Counter
	diskFullGauge               prometheus.Gauge
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

	// Maximum WAL bytes to process in a single sync executor run.
	// The limit is checked at commit boundaries so a single transaction
	// larger than this may exceed it. Set to zero to process all
	// committed WAL frames in one run.
	MaxSyncWALBytes int64

	// The timeout to wait for EBUSY from SQLite.
	BusyTimeout time.Duration

	// Minimum time to retain L0 files after they have been compacted into L1.
	L0Retention time.Duration

	// VerifyCompaction enables post-compaction TXID consistency verification.
	// When enabled, verifies that files at the destination level have
	// contiguous TXID ranges after each compaction.
	VerifyCompaction bool

	// RetentionEnabled controls whether Litestream actively deletes old files
	// during retention enforcement. When false, cloud provider lifecycle
	// policies handle retention instead. Local file cleanup still occurs.
	RetentionEnabled bool

	// Remote replica for the database.
	// Must be set before calling Open().
	Replica *Replica

	// Compactor handles shared compaction logic.
	// Created in NewDB with nil client; client set once in Open() from Replica.Client.
	compactor *Compactor

	// Shutdown sync retry settings.
	// ShutdownSyncTimeout is the total time to retry syncing on shutdown.
	// ShutdownSyncInterval is the time between retry attempts.
	ShutdownSyncTimeout  time.Duration
	ShutdownSyncInterval time.Duration

	// lastSuccessfulSyncAt tracks when replication last succeeded.
	// Used by heartbeat monitoring to determine if a ping should be sent.
	lastSuccessfulSyncMu sync.RWMutex
	lastSuccessfulSyncAt time.Time

	// Where to send log messages, defaults to global slog with database epath.
	Logger *slog.Logger
}

// syncState holds mutable sync-tracking fields extracted from DB.
// These fields are threaded through sync/checkpoint methods via pointer.
type syncState struct {
	// syncedSinceCheckpoint tracks whether any data has been synced since
	// the last checkpoint. Used to prevent time-based checkpoints from
	// triggering when there are no actual database changes, which would
	// otherwise create unnecessary LTX files. See issue #896.
	syncedSinceCheckpoint bool

	// syncedToWALEnd tracks whether the last successful sync reached the
	// exact end of the WAL file. When true, a subsequent WAL truncation
	// (from checkpoint) is expected and should NOT trigger a full snapshot.
	// This prevents issue #927 where every checkpoint triggers unnecessary
	// full snapshots because verify() sees the old LTX position exceeds
	// the new (truncated) WAL size.
	syncedToWALEnd bool

	// lastSyncedWALOffset tracks the logical end of the WAL content after
	// the last successful sync. This is the WALOffset + WALSize from the
	// last LTX file. Used for checkpoint threshold decisions instead of
	// file size, which may include stale frames with old salt values after
	// a checkpoint. This prevents issue #997 where PASSIVE checkpoints
	// trigger a feedback loop because stale file size exceeds threshold.
	lastSyncedWALOffset int64
}

type syncExecutor struct {
	state      syncState
	pos        ltx.Pos
	posChanged bool
	l0FileInfo *ltx.FileInfo
	synced     bool
}

type diagOp string

const (
	diagOpSync       diagOp = "sync"
	diagOpCheckpoint diagOp = "checkpoint"
)

type diagPhase string

const (
	diagPhaseStarting                       diagPhase = "starting"
	diagPhaseEnsureWAL                      diagPhase = "ensure_wal"
	diagPhaseVerifyAndSync                  diagPhase = "verify_and_sync"
	diagPhaseCheckpointIfNeeded             diagPhase = "checkpoint_if_needed"
	diagPhaseUpdateMetrics                  diagPhase = "update_metrics"
	diagPhaseStatWAL                        diagPhase = "stat_wal"
	diagPhaseVerify                         diagPhase = "verify"
	diagPhaseSyncLTX                        diagPhase = "sync_ltx"
	diagPhaseSyncOpenLTX                    diagPhase = "sync_open_ltx"
	diagPhaseSyncPageMap                    diagPhase = "sync_page_map"
	diagPhaseSyncPrepareLTX                 diagPhase = "sync_prepare_ltx"
	diagPhaseWriteLTXFromDB                 diagPhase = "write_ltx_from_db"
	diagPhaseWriteLTXFromWAL                diagPhase = "write_ltx_from_wal"
	diagPhaseCloseLTX                       diagPhase = "close_ltx"
	diagPhaseFsyncLTX                       diagPhase = "fsync_ltx"
	diagPhaseRenameLTX                      diagPhase = "rename_ltx"
	diagPhaseSyncComplete                   diagPhase = "sync_complete"
	diagPhaseCheckpointLock                 diagPhase = "checkpoint_lock"
	diagPhaseCheckpointReadWALHeader        diagPhase = "checkpoint_read_wal_header"
	diagPhaseCheckpointCopyBefore           diagPhase = "checkpoint_copy_before"
	diagPhaseCheckpointExec                 diagPhase = "checkpoint_exec"
	diagPhaseCheckpointVerifyRestart        diagPhase = "checkpoint_verify_restart"
	diagPhaseCheckpointSnapshotBoundaryLock diagPhase = "checkpoint_snapshot_boundary_lock"
	diagPhaseCheckpointSnapshotBoundary     diagPhase = "checkpoint_snapshot_boundary"
)

type diagState struct {
	sync.RWMutex
	active              bool
	operation           diagOp
	phase               diagPhase
	startedAt           time.Time
	updatedAt           time.Time
	txID                ltx.TXID
	walSize             int64
	lastSyncedWALOffset int64
	snapshotting        bool
	checkpointMode      string
	reason              string
	err                 string
	executorWaiterCount int
	executorWaitStarted time.Time
}

// SyncDiagnostic reports the latest DB sync/checkpoint activity.
type SyncDiagnostic struct {
	Path                string     `json:"path"`
	Active              bool       `json:"active"`
	Operation           string     `json:"operation,omitempty"`
	Phase               string     `json:"phase,omitempty"`
	StartedAt           *time.Time `json:"started_at,omitempty"`
	UpdatedAt           *time.Time `json:"updated_at,omitempty"`
	ElapsedSeconds      float64    `json:"elapsed_seconds,omitempty"`
	TXID                uint64     `json:"txid,omitempty"`
	WALSize             int64      `json:"wal_size,omitempty"`
	LastSyncedWALOffset int64      `json:"last_synced_wal_offset,omitempty"`
	Snapshotting        bool       `json:"snapshotting,omitempty"`
	CheckpointMode      string     `json:"checkpoint_mode,omitempty"`
	Reason              string     `json:"reason,omitempty"`
	Error               string     `json:"error,omitempty"`
	ExecutorWaiterCount int        `json:"executor_waiter_count,omitempty"`
	ExecutorWaitStarted *time.Time `json:"executor_wait_started_at,omitempty"`
	ExecutorWaitSeconds float64    `json:"executor_wait_seconds,omitempty"`
}

// NewDB returns a new instance of DB for a given path.
func NewDB(path string) *DB {
	dir, file := filepath.Split(path)

	db := &DB{
		path:     path,
		metaPath: filepath.Join(dir, "."+file+MetaDirSuffix),
		execSem:  semaphore.NewWeighted(1),
		notify:   make(chan struct{}),

		MinCheckpointPageN:   DefaultMinCheckpointPageN,
		TruncatePageN:        DefaultTruncatePageN,
		CheckpointInterval:   DefaultCheckpointInterval,
		MonitorInterval:      DefaultMonitorInterval,
		MaxSyncWALBytes:      DefaultMaxSyncWALBytes,
		BusyTimeout:          DefaultBusyTimeout,
		L0Retention:          DefaultL0Retention,
		RetentionEnabled:     true,
		ShutdownSyncTimeout:  DefaultShutdownSyncTimeout,
		ShutdownSyncInterval: DefaultShutdownSyncInterval,
		Logger:               slog.With(LogKeyDB, filepath.Base(path)),
	}
	db.maxLTXFileInfos.m = make(map[int]*ltx.FileInfo)

	db.dbSizeGauge = dbSizeGaugeVec.WithLabelValues(db.path)
	db.walSizeGauge = walSizeGaugeVec.WithLabelValues(db.path)
	db.totalWALBytesCounter = totalWALBytesCounterVec.WithLabelValues(db.path)
	db.txIDGauge = txIDIndexGaugeVec.WithLabelValues(db.path)
	db.syncNCounter = syncNCounterVec.WithLabelValues(db.path)
	db.syncErrorNCounter = syncErrorNCounterVec.WithLabelValues(db.path)
	db.syncSecondsCounter = syncSecondsCounterVec.WithLabelValues(db.path)
	db.diskFullGauge = diskFullGaugeVec.WithLabelValues(db.path)
	db.checkpointNCounterVec = checkpointNCounterVec.MustCurryWith(prometheus.Labels{"db": db.path})
	db.checkpointErrorNCounterVec = checkpointErrorNCounterVec.MustCurryWith(prometheus.Labels{"db": db.path})
	db.checkpointSecondsCounterVec = checkpointSecondsCounterVec.MustCurryWith(prometheus.Labels{"db": db.path})

	db.ctx, db.cancel = context.WithCancel(context.Background())

	// Initialize compactor with nil client (set once in Open() from Replica.Client).
	db.compactor = NewCompactor(nil, db.Logger.With(LogKeySubsystem, LogSubsystemCompactor))
	db.compactor.LocalFileOpener = db.openLocalLTXFile
	db.compactor.LocalFileDeleter = db.deleteLocalLTXFile
	db.compactor.CompactionVerifyErrorCounter = compactionVerifyErrorCounterVec.WithLabelValues(db.path)
	db.compactor.CacheGetter = func(level int) (*ltx.FileInfo, bool) {
		db.maxLTXFileInfos.Lock()
		defer db.maxLTXFileInfos.Unlock()
		info, ok := db.maxLTXFileInfos.m[level]
		return info, ok
	}
	db.compactor.CacheSetter = func(level int, info *ltx.FileInfo) {
		db.maxLTXFileInfos.Lock()
		defer db.maxLTXFileInfos.Unlock()
		db.maxLTXFileInfos.m[level] = info
	}

	return db
}

// SetLogger updates the database logger and propagates to subsystems.
func (db *DB) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	db.Logger = logger
	if db.compactor != nil {
		db.compactor.setLogger(logger.With(LogKeySubsystem, LogSubsystemCompactor))
	}
	if db.Replica != nil && db.Replica.Client != nil {
		db.Replica.Client.SetLogger(db.Replica.Logger())
	}
}

// SQLDB returns a reference to the underlying sql.DB connection.
func (db *DB) SQLDB() *sql.DB {
	return db.db
}

// Path returns the path to the database.
func (db *DB) Path() string {
	return db.path
}

// SyncDiagnostic returns the latest sync/checkpoint diagnostic snapshot.
func (db *DB) SyncDiagnostic() SyncDiagnostic {
	db.syncDiag.RLock()
	defer db.syncDiag.RUnlock()

	diag := SyncDiagnostic{
		Path:                db.path,
		Active:              db.syncDiag.active,
		Operation:           string(db.syncDiag.operation),
		Phase:               string(db.syncDiag.phase),
		TXID:                uint64(db.syncDiag.txID),
		WALSize:             db.syncDiag.walSize,
		LastSyncedWALOffset: db.syncDiag.lastSyncedWALOffset,
		Snapshotting:        db.syncDiag.snapshotting,
		CheckpointMode:      db.syncDiag.checkpointMode,
		Reason:              db.syncDiag.reason,
		Error:               db.syncDiag.err,
		ExecutorWaiterCount: db.syncDiag.executorWaiterCount,
	}
	if !db.syncDiag.executorWaitStarted.IsZero() {
		startedAt := db.syncDiag.executorWaitStarted
		diag.ExecutorWaitStarted = &startedAt
		diag.ExecutorWaitSeconds = time.Since(db.syncDiag.executorWaitStarted).Seconds()
	}
	if !db.syncDiag.startedAt.IsZero() {
		startedAt := db.syncDiag.startedAt
		diag.StartedAt = &startedAt
	}
	if !db.syncDiag.updatedAt.IsZero() {
		updatedAt := db.syncDiag.updatedAt
		diag.UpdatedAt = &updatedAt
	}
	if !db.syncDiag.startedAt.IsZero() {
		end := db.syncDiag.updatedAt
		if db.syncDiag.active || end.IsZero() {
			end = time.Now()
		}
		diag.ElapsedSeconds = end.Sub(db.syncDiag.startedAt).Seconds()
	}
	return diag
}

func (db *DB) beginSyncDiag(operation diagOp) {
	now := time.Now()
	walSize, _ := db.walFileSize()

	db.syncDiag.Lock()
	db.syncDiag.active = true
	db.syncDiag.operation = operation
	db.syncDiag.phase = diagPhaseStarting
	db.syncDiag.startedAt = now
	db.syncDiag.updatedAt = now
	db.syncDiag.txID = 0
	db.syncDiag.walSize = walSize
	db.syncDiag.lastSyncedWALOffset = db.syncState.lastSyncedWALOffset
	db.syncDiag.snapshotting = false
	db.syncDiag.checkpointMode = ""
	db.syncDiag.reason = ""
	db.syncDiag.err = ""
	db.syncDiag.Unlock()
}

func (db *DB) setSyncDiagPhase(phase diagPhase, updates ...func(*diagState)) {
	db.syncDiag.Lock()
	defer db.syncDiag.Unlock()
	if !db.syncDiag.active {
		return
	}
	db.syncDiag.phase = phase
	db.syncDiag.updatedAt = time.Now()
	for _, update := range updates {
		update(&db.syncDiag)
	}
}

func (db *DB) finishSyncDiag(err error) {
	db.syncDiag.Lock()
	defer db.syncDiag.Unlock()
	if !db.syncDiag.active {
		return
	}
	db.syncDiag.active = false
	db.syncDiag.updatedAt = time.Now()
	if err != nil {
		db.syncDiag.err = err.Error()
	}
}

func (db *DB) beginSyncExecutorWait() {
	db.syncDiag.Lock()
	defer db.syncDiag.Unlock()
	if db.syncDiag.executorWaiterCount == 0 {
		db.syncDiag.executorWaitStarted = time.Now()
	}
	db.syncDiag.executorWaiterCount++
}

func (db *DB) finishSyncExecutorWait() {
	db.syncDiag.Lock()
	defer db.syncDiag.Unlock()
	db.syncDiag.executorWaiterCount--
	if db.syncDiag.executorWaiterCount == 0 {
		db.syncDiag.executorWaitStarted = time.Time{}
	}
}

// IsOpen returns true if the database has been opened.
func (db *DB) IsOpen() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.opened
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

// ResetLocalState removes local LTX files, forcing a fresh snapshot on next sync.
// This is useful for recovering from corrupted or missing LTX files.
// The database file itself is not modified.
func (db *DB) ResetLocalState(ctx context.Context) error {
	db.Logger.Info("resetting local litestream state",
		"meta_path", db.metaPath,
		"ltx_dir", db.LTXDir())

	// Remove all LTX files
	if err := os.RemoveAll(db.LTXDir()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove ltx directory: %w", err)
	}

	// Clear cached LTX file info
	db.maxLTXFileInfos.Lock()
	db.maxLTXFileInfos.m = make(map[int]*ltx.FileInfo)
	db.maxLTXFileInfos.Unlock()

	db.invalidatePosCache()

	db.Logger.Info("local state reset complete, next sync will create fresh snapshot")
	return nil
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

// openLocalLTXFile opens a local LTX file for reading.
// Used by the Compactor to prefer local files over remote.
func (db *DB) openLocalLTXFile(level int, minTXID, maxTXID ltx.TXID) (io.ReadCloser, error) {
	return os.Open(db.LTXPath(level, minTXID, maxTXID))
}

// deleteLocalLTXFile deletes a local LTX file.
// Used by the Compactor for retention enforcement.
func (db *DB) deleteLocalLTXFile(level int, minTXID, maxTXID ltx.TXID) error {
	path := db.LTXPath(level, minTXID, maxTXID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if level == 0 {
		db.invalidatePosCache()
	}
	return nil
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
// The result is cached and invalidated when L0 LTX files change.
func (db *DB) Pos() (ltx.Pos, error) {
	db.pos.Lock()
	defer db.pos.Unlock()

	if db.pos.value != nil {
		return *db.pos.value, nil
	}

	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		return ltx.Pos{}, err
	} else if minTXID == 0 {
		return ltx.Pos{}, nil // no replication yet
	}

	ltxPath := db.LTXPath(0, minTXID, maxTXID)
	f, err := os.Open(ltxPath)
	if err != nil {
		return ltx.Pos{}, NewLTXError("open", ltxPath, 0, uint64(minTXID), uint64(maxTXID), err)
	}
	defer func() { _ = f.Close() }()

	dec := ltx.NewDecoder(f)
	if err := dec.Verify(); err != nil {
		return ltx.Pos{}, NewLTXError("verify", ltxPath, 0, uint64(minTXID), uint64(maxTXID), fmt.Errorf("%w: %w", ErrLTXCorrupted, err))
	}

	pos := dec.PostApplyPos()
	db.pos.value = &pos

	return pos, nil
}

// invalidatePosCache clears the cached position so the next call to Pos()
// recomputes it from disk. Call this when L0 LTX files are deleted or
// when the L0 directory is cleared.
func (db *DB) invalidatePosCache() {
	db.pos.Lock()
	db.pos.value = nil
	db.pos.Unlock()
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

// RecordSuccessfulSync marks the current time as a successful sync.
// Used by heartbeat monitoring to determine if a ping should be sent.
func (db *DB) RecordSuccessfulSync() {
	db.lastSuccessfulSyncMu.Lock()
	defer db.lastSuccessfulSyncMu.Unlock()
	db.lastSuccessfulSyncAt = time.Now()
}

// LastSuccessfulSyncAt returns the time of the last successful sync.
func (db *DB) LastSuccessfulSyncAt() time.Time {
	db.lastSuccessfulSyncMu.RLock()
	defer db.lastSuccessfulSyncMu.RUnlock()
	return db.lastSuccessfulSyncAt
}

// SyncStatus represents the current replication state of the database.
type SyncStatus struct {
	LocalTXID  ltx.TXID
	RemoteTXID ltx.TXID
	InSync     bool
}

// SyncStatus returns the current replication status of the database, comparing
// the local transaction position against the remote replica position. The remote
// position is queried from the replica storage, so this method may perform I/O.
func (db *DB) SyncStatus(ctx context.Context) (SyncStatus, error) {
	if db.Replica == nil {
		return SyncStatus{}, fmt.Errorf("no replica configured")
	}

	localPos, err := db.Pos()
	if err != nil {
		return SyncStatus{}, fmt.Errorf("local position: %w", err)
	}

	remotePos, err := db.Replica.calcPos(ctx)
	if err != nil {
		return SyncStatus{}, fmt.Errorf("remote position: %w", err)
	}

	return SyncStatus{
		LocalTXID:  localPos.TXID,
		RemoteTXID: remotePos.TXID,
		InSync:     localPos.TXID > 0 && localPos.TXID == remotePos.TXID,
	}, nil
}

// SyncAndWait performs a full sync: WAL to LTX files, then LTX files to remote
// replica. Blocks until both stages complete.
func (db *DB) SyncAndWait(ctx context.Context) error {
	if db.Replica == nil {
		return fmt.Errorf("no replica configured")
	}

	if err := db.Sync(ctx); err != nil {
		return fmt.Errorf("db sync: %w", err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		return fmt.Errorf("replica sync: %w", err)
	}
	return nil
}

// EnsureExists restores the database from the configured replica if the local
// database file does not exist. If no backup is available, it returns nil and
// a fresh database will be created on Open(). Must be called before Open().
func (db *DB) EnsureExists(ctx context.Context) error {
	if db.Replica == nil {
		return fmt.Errorf("no replica configured")
	}
	if db.Replica.Client == nil {
		return fmt.Errorf("no replica client configured")
	}

	if _, err := os.Stat(db.Path()); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat database: %w", err)
	}

	if dir := filepath.Dir(db.Path()); dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("create parent directory: %w", err)
		}
	}

	opt := NewRestoreOptions()
	opt.OutputPath = db.Path()
	opt.IntegrityCheck = IntegrityCheckQuick

	if err := db.Replica.Restore(ctx, opt); err != nil {
		if errors.Is(err, ErrTxNotAvailable) || errors.Is(err, ErrNoSnapshots) {
			db.Logger.Debug("no backup found, will create fresh database")
			return nil
		}
		return fmt.Errorf("restore from backup: %w", err)
	}

	db.Logger.Info("database restored from backup", "path", db.Path())
	return nil
}

// Open initializes the background monitoring goroutine.
func (db *DB) Open() (err error) {
	db.mu.Lock()
	if db.opened {
		db.mu.Unlock()
		return nil // already open
	}
	// Recreate context for fresh start (handles reopen after close)
	db.ctx, db.cancel = context.WithCancel(context.Background())
	db.mu.Unlock()

	// Validate fields on database.
	if db.Replica == nil {
		return fmt.Errorf("replica required before opening database")
	}
	if db.Replica.Client == nil {
		return fmt.Errorf("replica client required before opening database")
	}
	if db.MinCheckpointPageN <= 0 {
		return fmt.Errorf("minimum checkpoint page count required")
	}

	// Clear old temporary files that my have been left from a crash.
	if err := removeTmpFiles(db.metaPath); err != nil {
		return fmt.Errorf("cannot remove tmp files: %w", err)
	}

	// Set the compactor client once before starting any goroutines.
	db.compactor.VerifyCompaction = db.VerifyCompaction
	db.compactor.RetentionEnabled = db.RetentionEnabled
	db.compactor.client = db.Replica.Client

	// Start monitoring SQLite database in a separate goroutine.
	if db.MonitorInterval > 0 {
		db.wg.Add(1)
		go func() { defer db.wg.Done(); db.monitor() }()
	}

	// Mark as opened only after successful initialization.
	db.mu.Lock()
	db.opened = true
	db.mu.Unlock()

	return nil
}

// Close flushes outstanding WAL writes to replicas, releases the read lock,
// and closes the database. If Done is set, closing it interrupts the shutdown
// sync retry loop and cancels any in-flight sync attempt.
func (db *DB) Close(ctx context.Context) (err error) {
	db.cancel()
	db.wg.Wait()

	// Acquire without honoring caller cancellation: the cleanup below
	// (read lock release, handle closes, state reset) must always run or
	// the DB is left half-closed with its read lock held. Semaphore
	// acquisition fails immediately on an already-done context even when
	// the semaphore is free.
	if err := db.execSem.Acquire(context.WithoutCancel(ctx), 1); err != nil {
		return err
	}
	defer db.execSem.Release(1)

	// Perform a final db sync, if initialized.
	if db.db != nil {
		if _, e := db.syncLocked(ctx, 0); e != nil {
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

	db.mu.Lock()
	sqlDB := db.db
	f := db.f
	db.db = nil
	db.f = nil
	db.opened = false
	db.rtx = nil
	db.mu.Unlock()

	if sqlDB != nil {
		if e := sqlDB.Close(); e != nil && err == nil {
			err = e
		}
	}

	if f != nil {
		if e := f.Close(); e != nil && err == nil {
			err = e
		}
	}

	return err
}

// syncReplicaWithRetry attempts to sync the replica with retry logic for shutdown.
// It retries until success, timeout, or context cancellation. If db.Done is non-nil,
// closing it cancels any in-flight sync attempt and exits the retry loop.
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

	// Create deadline context for total retry duration.
	deadlineCtx, deadlineCancel := context.WithTimeout(ctx, timeout)
	defer deadlineCancel()

	// If db.Done is set, derive a context that cancels when done is closed
	// so that in-flight Replica.Sync calls are interrupted immediately.
	syncCtx := deadlineCtx
	if db.Done != nil {
		var syncCancel context.CancelFunc
		syncCtx, syncCancel = context.WithCancel(deadlineCtx)
		go func() {
			select {
			case <-db.Done:
				syncCancel()
			case <-deadlineCtx.Done():
				syncCancel()
			}
		}()
	}

	var lastErr error
	attempt := 0
	startTime := time.Now()

	for {
		// Check if done is already closed before attempting sync
		if db.Done != nil {
			select {
			case <-db.Done:
				db.Logger.Warn("shutdown sync skipped, interrupted by signal",
					"attempts", attempt,
					"duration", time.Since(startTime))
				return fmt.Errorf("after %d attempts: %w", attempt, ErrShutdownInterrupted)
			default:
			}
		}

		attempt++

		// Try sync
		if err := db.Replica.Sync(syncCtx); err == nil {
			if attempt > 1 {
				db.Logger.Info("shutdown sync succeeded after retry",
					"attempts", attempt,
					"duration", time.Since(startTime))
			}
			return nil
		} else {
			lastErr = err
		}

		// Check if we should stop retrying (done signal or timeout)
		select {
		case <-deadlineCtx.Done():
			db.Logger.Error("shutdown sync failed after timeout",
				"attempts", attempt,
				"duration", time.Since(startTime),
				"error", lastErr)
			return fmt.Errorf("shutdown sync timeout after %d attempts: %w", attempt, lastErr)
		case <-db.Done:
			db.Logger.Warn("shutdown sync interrupted by signal",
				"attempts", attempt,
				"duration", time.Since(startTime),
				"error", lastErr)
			return fmt.Errorf("after %d attempts: %w", attempt, ErrShutdownInterrupted)
		default:
		}

		// Log retry with hint about second signal if interruptible
		if db.Done != nil {
			db.Logger.Warn("shutdown sync failed, retrying (press Ctrl+C again to skip)",
				"attempts", attempt,
				"error", lastErr,
				"elapsed", time.Since(startTime),
				"remaining", time.Until(startTime.Add(timeout)))
		} else {
			db.Logger.Warn("shutdown sync failed, retrying",
				"attempts", attempt,
				"error", lastErr,
				"elapsed", time.Since(startTime),
				"remaining", time.Until(startTime.Add(timeout)))
		}

		// Wait before retry, but also listen for done signal
		select {
		case <-time.After(interval):
		case <-deadlineCtx.Done():
			return fmt.Errorf("shutdown sync timeout after %d attempts: %w", attempt, lastErr)
		case <-db.Done:
			db.Logger.Warn("shutdown sync interrupted by signal",
				"attempts", attempt,
				"duration", time.Since(startTime))
			return fmt.Errorf("after %d attempts: %w", attempt, ErrShutdownInterrupted)
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

	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(%d)&_pragma=wal_autocheckpoint(0)",
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
	// Use rollback() helper to suppress "already rolled back" errors that can
	// occur during shutdown when concurrent checkpoint and close operations
	// both attempt to release the read lock. See issue #934.
	err := rollback(db.rtx)
	db.rtx = nil
	return err
}

// Sync copies pending data from the WAL to the shadow WAL.
func (db *DB) Sync(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}

		result, err := db.syncOnce(ctx, db.MaxSyncWALBytes)
		if err != nil {
			return err
		} else if !result.synced || !result.limited || result.syncedToWALEnd {
			return nil
		}
	}
}

func (db *DB) syncOnce(ctx context.Context, maxSyncWALBytes int64) (syncResult, error) {
	if err := db.lockExec(ctx); err != nil {
		return syncResult{}, err
	}
	defer db.execSem.Release(1)

	return db.syncLocked(ctx, maxSyncWALBytes)
}

func (db *DB) lockExec(ctx context.Context) error {
	if db.execSem.TryAcquire(1) {
		return nil
	}
	db.beginSyncExecutorWait()
	defer db.finishSyncExecutorWait()

	if err := db.execSem.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("wait for db sync executor: %w", context.Cause(ctx))
	}
	return nil
}

func (db *DB) syncLocked(ctx context.Context, maxSyncWALBytes int64) (result syncResult, err error) {
	db.beginSyncDiag(diagOpSync)
	defer func() { db.finishSyncDiag(err) }()

	// Track total sync metrics.
	t := time.Now()
	defer func() {
		db.syncNCounter.Inc()
		if err != nil {
			db.syncErrorNCounter.Inc()
			var diskFullErr *LTXStagingDiskFullError
			if errors.As(err, &diskFullErr) {
				db.diskFullGauge.Set(1)
			} else {
				db.diskFullGauge.Set(0)
			}
		} else {
			db.diskFullGauge.Set(0)
		}
		db.syncSecondsCounter.Add(float64(time.Since(t).Seconds()))
	}()

	exec, err := db.newSyncExecutor(ctx)
	if err != nil {
		return result, err
	} else if exec == nil {
		db.Logger.Debug("sync: no database found")
		return result, nil
	}
	defer db.applySyncExecutor(exec, true)

	// Ensure WAL has at least one frame in it.
	db.setSyncDiagPhase(diagPhaseEnsureWAL, func(s *diagState) {
		s.lastSyncedWALOffset = exec.state.lastSyncedWALOffset
	})
	if err := db.ensureWALExists(ctx); err != nil {
		return result, fmt.Errorf("ensure wal exists: %w", err)
	}

	db.setSyncDiagPhase(diagPhaseVerifyAndSync, func(s *diagState) {
		s.txID = exec.pos.TXID + 1
	})
	result, err = db.verifyAndSyncWithExecutor(ctx, false, exec, maxSyncWALBytes)
	if err != nil {
		return result, err
	}
	exec.applySyncResult(result)

	// Track that data was synced for time-based checkpoint decisions.
	if result.synced {
		exec.state.syncedSinceCheckpoint = true
	}

	// Checkpoint checks normally wait until the WAL is fully synced, but the
	// emergency truncate threshold must be honored even mid catch-up:
	// sustained writes could otherwise keep every chunk limited and defer the
	// truncate checkpoint indefinitely, growing the WAL without bound.
	if !result.limited || result.syncedToWALEnd || db.exceedsTruncateThreshold(result.origWALSize) {
		db.setSyncDiagPhase(diagPhaseCheckpointIfNeeded, func(s *diagState) {
			s.txID = exec.pos.TXID
			s.lastSyncedWALOffset = exec.state.lastSyncedWALOffset
		})
		if err := db.checkpointIfNeeded(ctx, exec, result.origWALSize, result.newWALSize); err != nil {
			return result, fmt.Errorf("checkpoint: %w", err)
		}
	}

	db.setSyncDiagPhase(diagPhaseUpdateMetrics, func(s *diagState) {
		s.txID = exec.pos.TXID
		s.lastSyncedWALOffset = exec.state.lastSyncedWALOffset
	})

	// Compute current index and total shadow WAL size.
	db.txIDGauge.Set(float64(exec.pos.TXID))

	// Update file size metrics.
	if fi, err := os.Stat(db.path); err == nil {
		db.dbSizeGauge.Set(float64(fi.Size()))
	}
	db.walSizeGauge.Set(float64(exec.state.lastSyncedWALOffset))

	return result, nil
}

func (db *DB) verifyAndSync(ctx context.Context, checkpointing bool, state *syncState) (syncResult, error) {
	pos, err := db.Pos()
	if err != nil {
		return syncResult{}, fmt.Errorf("pos: %w", err)
	}

	return db.verifyAndSyncWithExecutor(ctx, checkpointing, &syncExecutor{
		state: *state,
		pos:   pos,
	}, 0)
}

func (db *DB) verifyAndSyncWithExecutor(ctx context.Context, checkpointing bool, exec *syncExecutor, maxSyncWALBytes int64) (syncResult, error) {
	db.setSyncDiagPhase(diagPhaseStatWAL, func(s *diagState) {
		s.txID = exec.pos.TXID + 1
		s.lastSyncedWALOffset = exec.state.lastSyncedWALOffset
	})

	// Use the last synced WAL offset as the logical size for checkpoint decisions.
	// This avoids using file size which may include stale frames with old salt
	// values after a checkpoint. See issue #997.
	origWALSize := exec.state.lastSyncedWALOffset
	if origWALSize == 0 {
		// First sync - use file size as fallback
		var err error
		origWALSize, err = db.walFileSize()
		if err != nil {
			return syncResult{}, fmt.Errorf("stat wal before sync: %w", err)
		}
	}

	// Verify our last sync matches the current state of the WAL.
	// This ensures that the last sync position of the real WAL hasn't
	// been overwritten by another process.
	db.setSyncDiagPhase(diagPhaseVerify)
	info, err := db.verifyWithExecutor(ctx, exec)
	if err != nil {
		return syncResult{}, fmt.Errorf("cannot verify wal state: %w", err)
	}

	db.setSyncDiagPhase(diagPhaseSyncLTX, func(s *diagState) {
		s.txID = exec.pos.TXID + 1
		s.snapshotting = info.snapshotting
		s.reason = info.reason
		s.lastSyncedWALOffset = exec.state.lastSyncedWALOffset
	})
	result, err := db.sync(ctx, checkpointing, exec, info, maxSyncWALBytes)
	if err != nil {
		return syncResult{}, fmt.Errorf("sync: %w", err)
	}

	result.origWALSize = origWALSize
	return result, nil
}

// checkpointIfNeeded performs a checkpoint based on configured thresholds.
// Checks thresholds in priority order: TruncatePageN → MinCheckpointPageN → CheckpointInterval.
//
// TruncatePageN uses TRUNCATE mode (blocking). Others use PASSIVE mode, which
// briefly holds the write lock to seal the WAL and can be skipped when the
// database is busy.
//
// Time-based checkpoints only trigger if exec.state.syncedSinceCheckpoint is true, indicating
// that data has been synced since the last checkpoint. This prevents creating unnecessary
// LTX files when the only WAL data is from internal bookkeeping (like _litestream_seq
// updates from previous checkpoints). See issue #896.
func (db *DB) checkpointIfNeeded(ctx context.Context, exec *syncExecutor, origWALSize, newWALSize int64) error {
	if db.pageSize == 0 {
		return nil
	}

	// Priority 1: Emergency truncate checkpoint (TRUNCATE mode, blocking)
	// This prevents unbounded WAL growth from long-lived read transactions.
	if db.exceedsTruncateThreshold(origWALSize) {
		truncateThreshold := calcWALSize(uint32(db.pageSize), uint32(db.TruncatePageN))

		// Try a PASSIVE checkpoint first: if it restarts the WAL and brings
		// the synced offset below the threshold, the blocking TRUNCATE and
		// its mandatory boundary snapshot are skipped.
		if restarted, err := db.checkpointWithExecutor(ctx, CheckpointModePassive, exec); err != nil {
			if !isSQLiteBusyError(err) {
				return err
			}
			db.Logger.Log(ctx, internal.LevelTrace, "passive checkpoint skipped", "reason", "database busy")
		} else if restarted && !db.exceedsTruncateThreshold(exec.state.lastSyncedWALOffset) {
			db.Logger.Info("wal restarted by passive checkpoint, skipping truncate checkpoint",
				"wal_size", origWALSize,
				"threshold", truncateThreshold)
			return nil
		}

		db.Logger.Info("forcing truncate checkpoint",
			"wal_size", origWALSize,
			"threshold", truncateThreshold)
		_, err := db.checkpointWithExecutor(ctx, CheckpointModeTruncate, exec)
		return err
	}

	// Priority 2: Regular checkpoint at min threshold (PASSIVE mode)
	if newWALSize >= calcWALSize(uint32(db.pageSize), uint32(db.MinCheckpointPageN)) {
		if _, err := db.checkpointWithExecutor(ctx, CheckpointModePassive, exec); err != nil {
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

	// Priority 3: Time-based checkpoint (PASSIVE mode)
	// Only trigger if there have been actual changes synced since the last
	// checkpoint. This prevents creating unnecessary LTX files when the only
	// WAL data is from internal bookkeeping (like _litestream_seq updates).
	if db.CheckpointInterval > 0 && exec.state.syncedSinceCheckpoint {
		// Get database file modification time
		fi, err := db.f.Stat()
		if err != nil {
			return fmt.Errorf("stat database: %w", err)
		}

		// Only checkpoint if enough time has passed and WAL has data
		if time.Since(fi.ModTime()) > db.CheckpointInterval && newWALSize > calcWALSize(uint32(db.pageSize), 1) {
			if _, err := db.checkpointWithExecutor(ctx, CheckpointModePassive, exec); err != nil {
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

// exceedsTruncateThreshold returns true once walSize has grown past the
// emergency truncate checkpoint threshold.
func (db *DB) exceedsTruncateThreshold(walSize int64) bool {
	return db.TruncatePageN > 0 && db.pageSize != 0 &&
		walSize >= calcWALSize(uint32(db.pageSize), uint32(db.TruncatePageN))
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

// isDiskFullError returns true if the error indicates disk space issues.
// This includes "no space left on device" (ENOSPC) and "disk quota exceeded" (EDQUOT).
func isDiskFullError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrDiskFull) {
		return true
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "no space left on device") ||
		strings.Contains(errStr, "disk quota exceeded") ||
		strings.Contains(errStr, "enospc") ||
		strings.Contains(errStr, "edquot")
}

type ltxStagingFile interface {
	io.Writer
	Sync() error
	Close() error
}

var openLTXFile = func(name string, flag int, perm os.FileMode) (ltxStagingFile, error) {
	return os.OpenFile(name, flag, perm)
}

func newLTXStagingDiskFullError(op, path string, level int, minTXID, maxTXID ltx.TXID, err error) *LTXStagingDiskFullError {
	return &LTXStagingDiskFullError{
		Op:      op,
		Path:    path,
		Level:   level,
		MinTXID: uint64(minTXID),
		MaxTXID: uint64(maxTXID),
		Err:     err,
	}
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

func (db *DB) bumpLitestreamSeq(ctx context.Context) error {
	_, err := db.db.ExecContext(ctx, `INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1`)
	return err
}

// ensureWALExists checks that the real WAL exists and has a header.
func (db *DB) ensureWALExists(ctx context.Context) (err error) {
	// Exit early if WAL header exists.
	if fi, err := os.Stat(db.WALPath()); err == nil && fi.Size() >= WALHeaderSize {
		return nil
	}

	// Otherwise create transaction that updates the internal litestream table.
	return db.bumpLitestreamSeq(ctx)
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
	db.invalidatePosCache()
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
	db.invalidatePosCache()

	db.Logger.Info("fetched latest L0 file from replica",
		"min_txid", minTXID,
		"max_txid", maxTXID)

	return nil
}

// verify ensures the current LTX state matches where it left off from
// the real WAL. Check info.ok if verification was successful.
func (db *DB) verify(ctx context.Context, state *syncState) (info syncInfo, err error) {
	pos, err := db.Pos()
	if err != nil {
		return info, fmt.Errorf("pos: %w", err)
	}

	return db.verifyWithExecutor(ctx, &syncExecutor{
		state: *state,
		pos:   pos,
	})
}

func (db *DB) verifyWithExecutor(ctx context.Context, exec *syncExecutor) (info syncInfo, err error) {
	frameSize := int64(db.pageSize + WALFrameHeaderSize)
	info.snapshotting = true

	if exec.pos.TXID == 0 {
		info.offset = WALHeaderSize
		return info, nil // first sync
	}

	// Determine last WAL offset we save from.
	ltxPath := db.LTXPath(0, exec.pos.TXID, exec.pos.TXID)
	ltxFile, err := os.Open(ltxPath)
	if err != nil {
		return info, NewLTXError("open", ltxPath, 0, uint64(exec.pos.TXID), uint64(exec.pos.TXID), err)
	}
	defer func() { _ = ltxFile.Close() }()

	dec := ltx.NewDecoder(ltxFile)
	if err := dec.DecodeHeader(); err != nil {
		// Decode failure indicates corruption
		ltxErr := NewLTXError("decode", ltxPath, 0, uint64(exec.pos.TXID), uint64(exec.pos.TXID), fmt.Errorf("%w: %w", ErrLTXCorrupted, err))
		return info, ltxErr
	}
	info.offset = dec.Header().WALOffset + dec.Header().WALSize
	info.salt1 = dec.Header().WALSalt1
	info.salt2 = dec.Header().WALSalt2
	info.prevCommit = dec.Header().Commit

	// If LTX WAL offset is larger than real WAL then the WAL has been truncated.
	if fi, err := os.Stat(db.WALPath()); err != nil {
		return info, fmt.Errorf("open wal file: %w", err)
	} else if info.offset > fi.Size() {
		// If we previously synced to the exact end of the WAL, this truncation
		// is expected (normal checkpoint behavior). Reset position and continue
		// incrementally rather than triggering a full snapshot. See issue #927.
		if exec.state.syncedToWALEnd {
			// Read new WAL header to get current salt values
			hdr, err := readWALHeader(db.WALPath())
			if err != nil {
				return info, fmt.Errorf("read wal header after expected truncation: %w", err)
			}

			info.offset = WALHeaderSize
			info.salt1 = binary.BigEndian.Uint32(hdr[16:])
			info.salt2 = binary.BigEndian.Uint32(hdr[20:])
			info.snapshotting = false
			info.reason = ""
			info.clearSyncedToWALEnd = true

			db.Logger.Log(ctx, internal.LevelTrace, "wal truncated after sync to end (expected checkpoint)",
				"new_salt1", info.salt1,
				"new_salt2", info.salt2)

			return info, nil
		}

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
		db.Logger.Debug("verify", "saltMatch", saltMatch, "atWALHeader", true)
		if saltMatch {
			info.snapshotting = false
			return info, nil
		}
		info.reason = "wal header salt reset, snapshotting"
		return info, nil
	}

	// If offset is at the beginning of the first page, we can't check for previous page.
	prevWALOffset := info.offset - frameSize
	db.Logger.Debug("verify", "saltMatch", saltMatch, "prevWALOffset", prevWALOffset)

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

	db.Logger.Debug("verify.2", "lastPageMatch", lastPageMatch)

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

	rd, err := NewWALReader(walFile, db.Logger.With(LogKeySubsystem, LogSubsystemWALReader))
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
	offset              int64 // end of the previous LTX read
	salt1               uint32
	salt2               uint32
	prevCommit          uint32
	snapshotting        bool   // if true, a full snapshot is required
	reason              string // reason for snapshot
	clearSyncedToWALEnd bool
}

type syncResult struct {
	origWALSize    int64
	newWALSize     int64
	synced         bool
	limited        bool
	syncedToWALEnd bool
	pos            *ltx.Pos
	l0FileInfo     *ltx.FileInfo
}

func (db *DB) applySyncResult(state *syncState, result syncResult) {
	state.lastSyncedWALOffset = result.newWALSize
	state.syncedToWALEnd = result.syncedToWALEnd
	if result.pos != nil {
		db.pos.Lock()
		db.pos.value = result.pos
		db.pos.Unlock()
	}
	if result.l0FileInfo != nil {
		db.maxLTXFileInfos.Lock()
		db.maxLTXFileInfos.m[0] = result.l0FileInfo
		db.maxLTXFileInfos.Unlock()
	}
}

func (db *DB) newSyncExecutor(ctx context.Context) (*syncExecutor, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.init(ctx); err != nil {
		return nil, err
	} else if db.db == nil {
		return nil, nil
	}

	pos, err := db.Pos()
	if err != nil {
		return nil, fmt.Errorf("pos: %w", err)
	}

	return &syncExecutor{
		state: db.syncState,
		pos:   pos,
	}, nil
}

func (db *DB) applySyncExecutor(exec *syncExecutor, notify bool) {
	if exec == nil {
		return
	}

	// Only publish the position if this executor advanced it. Republishing
	// the snapshot taken at executor start would clobber concurrent cache
	// invalidation (e.g. ResetLocalState) with a stale position.
	if exec.posChanged {
		db.pos.Lock()
		pos := exec.pos
		db.pos.value = &pos
		db.pos.Unlock()
	}

	if exec.l0FileInfo != nil {
		db.maxLTXFileInfos.Lock()
		info := *exec.l0FileInfo
		db.maxLTXFileInfos.m[0] = &info
		db.maxLTXFileInfos.Unlock()
	}

	db.mu.Lock()
	db.syncState = exec.state
	if notify && exec.synced {
		close(db.notify)
		db.notify = make(chan struct{})
	}
	db.mu.Unlock()
}

func (exec *syncExecutor) applySyncResult(result syncResult) {
	exec.state.lastSyncedWALOffset = result.newWALSize
	exec.state.syncedToWALEnd = result.syncedToWALEnd
	if result.pos != nil {
		exec.pos = *result.pos
		exec.posChanged = true
	}
	if result.l0FileInfo != nil {
		info := *result.l0FileInfo
		exec.l0FileInfo = &info
	}
	exec.synced = exec.synced || result.synced
}

// sync copies pending bytes from the real WAL to LTX.
// Returns synced=true if an LTX file was created (i.e., there were new pages to sync).
func (db *DB) sync(ctx context.Context, checkpointing bool, exec *syncExecutor, info syncInfo, maxSyncWALBytes int64) (result syncResult, err error) {
	result.newWALSize = exec.state.lastSyncedWALOffset
	result.syncedToWALEnd = exec.state.syncedToWALEnd
	if info.clearSyncedToWALEnd {
		result.syncedToWALEnd = false
	}

	// Determine the next sequential transaction ID.
	txID := exec.pos.TXID + 1
	db.setSyncDiagPhase(diagPhaseSyncOpenLTX,
		func(s *diagState) {
			s.txID = txID
			s.snapshotting = info.snapshotting
			s.reason = info.reason
		})

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
		return result, err
	}
	mode := fi.Mode()
	commit := uint32(fi.Size() / int64(db.pageSize))

	walFile, err := os.Open(db.WALPath())
	if err != nil {
		return result, err
	}
	defer walFile.Close()

	walReaderLogger := db.Logger.With(LogKeySubsystem, LogSubsystemWALReader)
	var rd *WALReader
	if info.offset == WALHeaderSize {
		if rd, err = NewWALReader(walFile, walReaderLogger); err != nil {
			return result, fmt.Errorf("new wal reader: %w", err)
		}
	} else {
		// If we cannot verify the previous frame
		var pfmError *PrevFrameMismatchError
		if rd, err = NewWALReaderWithOffset(ctx, walFile, info.offset, info.salt1, info.salt2, walReaderLogger); errors.As(err, &pfmError) {
			db.Logger.Log(ctx, internal.LevelTrace, "prev frame mismatch, snapshotting", "err", pfmError.Err)
			info.offset = WALHeaderSize
			if rd, err = NewWALReader(walFile, walReaderLogger); err != nil {
				return result, fmt.Errorf("new wal reader, after reset")
			}
		} else if err != nil {
			return result, fmt.Errorf("new wal reader with offset: %w", err)
		}
	}

	// Build a mapping of changed page numbers and their latest content.
	db.setSyncDiagPhase(diagPhaseSyncPageMap,
		func(s *diagState) {
			s.txID = txID
			s.snapshotting = info.snapshotting
			s.reason = info.reason
		})
	if info.snapshotting {
		maxSyncWALBytes = 0
	}
	pageMap, maxOffset, walCommit, limited, err := rd.pageMap(ctx, maxSyncWALBytes)
	if err != nil {
		return result, fmt.Errorf("page map: %w", err)
	}
	result.limited = limited
	if walCommit > 0 {
		commit = walCommit
	}
	var sz int64
	if maxOffset > 0 {
		sz = maxOffset - info.offset
	}
	assert(sz >= 0, fmt.Sprintf("wal size must be positive: sz=%d, maxOffset=%d, info.offset=%d", sz, maxOffset, info.offset))
	db.setSyncDiagPhase(diagPhaseSyncPrepareLTX,
		func(s *diagState) {
			s.txID = txID
			s.walSize = sz
			s.snapshotting = info.snapshotting
			s.reason = info.reason
		})

	// Track total WAL bytes synced.
	if sz > 0 {
		db.totalWALBytesCounter.Add(float64(sz))
	}

	// Exit if we have no new WAL pages and we aren't snapshotting.
	if !info.snapshotting && sz == 0 {
		db.Logger.Log(ctx, internal.LevelTrace, "sync: skip", "reason", "no new wal pages")
		return result, nil
	}

	tmpFilename := filename + ".tmp"
	if err := internal.MkdirAll(filepath.Dir(tmpFilename), db.dirInfo); err != nil {
		return result, err
	}

	ltxFile, err := openLTXFile(tmpFilename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		if isDiskFullError(err) {
			return result, newLTXStagingDiskFullError("open", tmpFilename, 0, txID, txID, err)
		}
		return result, fmt.Errorf("open temp ltx file: %w", err)
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
		return result, fmt.Errorf("new ltx encoder: %w", err)
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
		if isDiskFullError(err) {
			return result, newLTXStagingDiskFullError("write", tmpFilename, 0, txID, txID, err)
		}
		return result, fmt.Errorf("encode ltx header: %w", err)
	}

	// If we need a full snapshot, then copy from the database & WAL.
	// Otherwise, just copy incrementally from the WAL.
	if info.snapshotting {
		db.setSyncDiagPhase(diagPhaseWriteLTXFromDB,
			func(s *diagState) {
				s.txID = txID
				s.walSize = sz
				s.snapshotting = true
				s.reason = info.reason
			})
		if err := db.writeLTXFromDB(ctx, enc, walFile, commit, pageMap); err != nil {
			if isDiskFullError(err) {
				return result, newLTXStagingDiskFullError("write", tmpFilename, 0, txID, txID, err)
			}
			return result, fmt.Errorf("write ltx from db: %w", err)
		}
	} else {
		db.setSyncDiagPhase(diagPhaseWriteLTXFromWAL,
			func(s *diagState) {
				s.txID = txID
				s.walSize = sz
				s.snapshotting = false
				s.reason = info.reason
			})
		if err := db.writeLTXFromWAL(ctx, enc, walFile, info.prevCommit, commit, pageMap); err != nil {
			if isDiskFullError(err) {
				return result, newLTXStagingDiskFullError("write", tmpFilename, 0, txID, txID, err)
			}
			return result, fmt.Errorf("write ltx from wal: %w", err)
		}
	}

	// Encode final trailer to the end of the LTX file.
	db.setSyncDiagPhase(diagPhaseCloseLTX, func(s *diagState) {
		s.txID = txID
		s.walSize = sz
	})
	if err := enc.Close(); err != nil {
		if isDiskFullError(err) {
			return result, newLTXStagingDiskFullError("write", tmpFilename, 0, txID, txID, err)
		}
		return result, fmt.Errorf("close ltx encoder: %w", err)
	}

	// Sync & close LTX file.
	db.setSyncDiagPhase(diagPhaseFsyncLTX, func(s *diagState) {
		s.txID = txID
		s.walSize = sz
	})
	if err := ltxFile.Sync(); err != nil {
		if isDiskFullError(err) {
			return result, newLTXStagingDiskFullError("sync", tmpFilename, 0, txID, txID, err)
		}
		return result, fmt.Errorf("sync ltx file: %w", err)
	}
	if err := ltxFile.Close(); err != nil {
		if isDiskFullError(err) {
			return result, newLTXStagingDiskFullError("close", tmpFilename, 0, txID, txID, err)
		}
		return result, fmt.Errorf("close ltx file: %w", err)
	}

	// Atomically rename file to final path.
	db.setSyncDiagPhase(diagPhaseRenameLTX, func(s *diagState) {
		s.txID = txID
		s.walSize = sz
	})
	if err := os.Rename(tmpFilename, filename); err != nil {
		db.maxLTXFileInfos.Lock()
		delete(db.maxLTXFileInfos.m, 0) // clear cache if in unknown state
		db.maxLTXFileInfos.Unlock()
		db.invalidatePosCache()
		return result, fmt.Errorf("rename ltx file: %w", err)
	}

	result.synced = true
	result.l0FileInfo = &ltx.FileInfo{
		Level:     0,
		MinTXID:   txID,
		MaxTXID:   txID,
		CreatedAt: time.Now(),
		Size:      enc.N(),
	}

	encPos := enc.PostApplyPos()
	result.pos = &encPos

	// Track the logical end of WAL content for checkpoint decisions.
	// This is the WALOffset + WALSize from the LTX we just created.
	// Using this instead of file size prevents issue #997 where stale
	// frames with old salt values cause perpetual checkpoint triggering.
	finalOffset := info.offset + sz
	result.newWALSize = finalOffset

	// Track if we synced to the exact end of the WAL file.
	// This is used by verify() to distinguish expected checkpoint truncation
	// from unexpected external WAL modifications. See issue #927.
	if walSize, err := db.walFileSize(); err == nil {
		result.syncedToWALEnd = finalOffset == walSize
	} else {
		result.syncedToWALEnd = false
	}
	db.setSyncDiagPhase(diagPhaseSyncComplete,
		func(s *diagState) {
			s.txID = txID
			s.walSize = sz
			s.lastSyncedWALOffset = finalOffset
			s.snapshotting = info.snapshotting
			s.reason = info.reason
		})

	db.Logger.Debug("db sync", "status", "ok")

	return result, nil
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

func (db *DB) writeLTXFromWAL(ctx context.Context, enc *ltx.Encoder, walFile *os.File, prevCommit, commit uint32, pageMap map[uint32]int64) error {
	// Create an ordered list of page numbers since the LTX encoder requires it.
	pgnos := make([]uint32, 0, len(pageMap))
	for pgno := range pageMap {
		pgnos = append(pgnos, pgno)
	}
	lockPgno := ltx.LockPgno(uint32(db.pageSize))
	if commit > prevCommit {
		walPgnoN := len(pgnos)
		for pgno := prevCommit + 1; pgno <= commit; pgno++ {
			if pgno == lockPgno {
				continue
			}
			if _, ok := pageMap[pgno]; ok {
				continue
			}
			pgnos = append(pgnos, pgno)
		}
		if growthPgnoN := len(pgnos) - walPgnoN; growthPgnoN > 0 {
			db.Logger.Debug("filling wal growth pages from database",
				"txid", enc.Header().MinTXID,
				"n", growthPgnoN,
				"prev_commit", prevCommit,
				"commit", commit)
		}
	}
	slices.Sort(pgnos)

	data := make([]byte, db.pageSize)
	for _, pgno := range pgnos {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}
		if offset, ok := pageMap[pgno]; ok {
			db.Logger.Log(ctx, internal.LevelTrace, "encode page from wal", "txid", enc.Header().MinTXID, "offset", offset, "pgno", pgno, "type", "walonly")

			if n, err := walFile.ReadAt(data, offset+WALFrameHeaderSize); err != nil {
				return fmt.Errorf("read page %d @ %d: %w", pgno, offset, err)
			} else if n != len(data) {
				return fmt.Errorf("short read page %d @ %d", pgno, offset)
			}
		} else {
			offset := int64(pgno-1) * int64(db.pageSize)
			db.Logger.Log(ctx, internal.LevelTrace, "encode page from database", "txid", enc.Header().MinTXID, "offset", offset, "pgno", pgno, "type", "walgrowth")

			if _, err := db.f.ReadAt(data, offset); err != nil {
				return fmt.Errorf("read database page %d: %w", pgno, err)
			}
		}

		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, data); err != nil {
			return fmt.Errorf("encode ltx frame (pgno=%d): %w", pgno, err)
		}
	}
	return nil
}

// Checkpoint performs a checkpoint on the WAL file.
func (db *DB) Checkpoint(ctx context.Context, mode string) (err error) {
	if err := db.lockExec(ctx); err != nil {
		return err
	}
	defer db.execSem.Release(1)
	db.beginSyncDiag(diagOpCheckpoint)
	defer func() { db.finishSyncDiag(err) }()

	exec, err := db.newSyncExecutor(ctx)
	if err != nil {
		return err
	} else if exec == nil {
		return nil
	}
	defer db.applySyncExecutor(exec, true)

	_, err = db.checkpointWithExecutor(ctx, mode, exec)
	return err
}

// checkpoint performs a checkpoint on the WAL file and initializes a
// new shadow WAL file.
func (db *DB) checkpoint(ctx context.Context, mode string, state *syncState) error {
	pos, err := db.Pos()
	if err != nil {
		return fmt.Errorf("pos: %w", err)
	}

	exec := &syncExecutor{
		state: *state,
		pos:   pos,
	}
	if _, err := db.checkpointWithExecutor(ctx, mode, exec); err != nil {
		return err
	}

	*state = exec.state
	db.pos.Lock()
	pos = exec.pos
	db.pos.value = &pos
	db.pos.Unlock()
	if exec.l0FileInfo != nil {
		db.maxLTXFileInfos.Lock()
		info := *exec.l0FileInfo
		db.maxLTXFileInfos.m[0] = &info
		db.maxLTXFileInfos.Unlock()
	}
	return nil
}

// checkpointWithExecutor performs a checkpoint in the given mode and reports
// whether the checkpoint restarted the WAL. It returns false without
// checkpointing when the checkpoint lock is held by an in-progress snapshot.
func (db *DB) checkpointWithExecutor(ctx context.Context, mode string, exec *syncExecutor) (walRestarted bool, err error) {
	db.setSyncDiagPhase(diagPhaseCheckpointLock,
		func(s *diagState) {
			s.checkpointMode = mode
			s.lastSyncedWALOffset = exec.state.lastSyncedWALOffset
		})
	// Try getting a checkpoint lock, will fail during snapshots. Skipping is
	// safe since the next sync retries.
	if !db.chkMu.TryLock() {
		if mode == CheckpointModeTruncate {
			db.Logger.Info("checkpoint skipped, snapshot in progress", "mode", mode)
		} else {
			db.Logger.Log(ctx, internal.LevelTrace, "checkpoint skipped, snapshot in progress", "mode", mode)
		}
		return false, nil
	}
	defer db.chkMu.Unlock()

	// Read WAL header before checkpoint to check if it has been restarted.
	db.setSyncDiagPhase(diagPhaseCheckpointReadWALHeader,
		func(s *diagState) {
			s.checkpointMode = mode
			s.lastSyncedWALOffset = exec.state.lastSyncedWALOffset
		})
	hdr, err := readWALHeader(db.WALPath())
	if err != nil {
		return false, err
	}

	// Copy end of WAL before checkpoint to copy as much as possible.
	db.setSyncDiagPhase(diagPhaseCheckpointCopyBefore,
		func(s *diagState) {
			s.checkpointMode = mode
			s.lastSyncedWALOffset = exec.state.lastSyncedWALOffset
		})
	result, err := db.verifyAndSyncWithExecutor(ctx, true, exec, 0)
	if err != nil {
		return false, fmt.Errorf("cannot copy wal before checkpoint: %w", err)
	}
	exec.applySyncResult(result)

	var barrierTx *sql.Tx
	if mode == CheckpointModePassive {
		barrierTx, err = db.db.BeginTx(ctx, nil)
		if err != nil {
			return false, fmt.Errorf("begin passive checkpoint barrier: %w", err)
		}
		defer func() {
			if barrierTx != nil {
				_ = rollback(barrierTx)
			}
		}()

		if _, err := barrierTx.ExecContext(ctx, `INSERT INTO _litestream_lock (id) VALUES (1);`); err != nil {
			return false, fmt.Errorf("_litestream_lock: %w", err)
		}

		result, err = db.verifyAndSyncWithExecutor(ctx, true, exec, 0)
		if err != nil {
			return false, fmt.Errorf("cannot seal wal before passive checkpoint: %w", err)
		}
		exec.applySyncResult(result)
	}

	frameSize := int64(db.pageSize + WALFrameHeaderSize)
	preCheckpointFrameN := 0
	if exec.state.lastSyncedWALOffset > WALHeaderSize {
		preCheckpointFrameN = int((exec.state.lastSyncedWALOffset - WALHeaderSize) / frameSize)
	}

	// Execute checkpoint and immediately issue a write to the WAL to ensure
	// a new page is written.
	db.setSyncDiagPhase(diagPhaseCheckpointExec,
		func(s *diagState) {
			s.checkpointMode = mode
			s.lastSyncedWALOffset = exec.state.lastSyncedWALOffset
		})
	walFrameN, err := db.execCheckpoint(ctx, mode)
	if err != nil {
		return false, err
	}

	if barrierTx != nil {
		if err = rollback(barrierTx); err != nil {
			return false, fmt.Errorf("rollback passive checkpoint barrier: %w", err)
		}
		barrierTx = nil
	}

	if err = db.bumpLitestreamSeq(ctx); err != nil {
		return false, fmt.Errorf("bump litestream seq: %w", err)
	}

	// If WAL hasn't been restarted, exit.
	db.setSyncDiagPhase(diagPhaseCheckpointVerifyRestart,
		func(s *diagState) {
			s.checkpointMode = mode
			s.lastSyncedWALOffset = exec.state.lastSyncedWALOffset
		})
	other, err := readWALHeader(db.WALPath())
	if err != nil {
		return false, err
	} else if bytes.Equal(hdr, other) {
		exec.state.syncedSinceCheckpoint = false
		return false, nil
	}

	if mode == CheckpointModePassive {
		result, err = db.verifyAndSyncWithExecutor(ctx, true, exec, 0)
		if err != nil {
			return false, fmt.Errorf("cannot copy wal after passive checkpoint: %w", err)
		}
		exec.applySyncResult(result)
		exec.state.syncedSinceCheckpoint = false
		return true, nil
	}

	// A successful TRUNCATE checkpoint always reports zero frames because
	// the WAL is reset before the counters are read, so the comparison
	// below can never prove that no commits landed between the sealed
	// sync and the checkpoint taking the writer lock. Those commits are
	// backfilled and truncated unseen, so TRUNCATE must take the boundary
	// snapshot unconditionally.
	if mode != CheckpointModeTruncate && walFrameN <= preCheckpointFrameN {
		result, err = db.verifyAndSyncWithExecutor(ctx, true, exec, 0)
		if err != nil {
			return false, fmt.Errorf("cannot copy wal after checkpoint: %w", err)
		}
		exec.applySyncResult(result)
		exec.state.syncedSinceCheckpoint = false
		return true, nil
	}

	// Start a transaction. This will be promoted immediately after.
	db.setSyncDiagPhase(diagPhaseCheckpointSnapshotBoundaryLock,
		func(s *diagState) {
			s.checkpointMode = mode
			s.lastSyncedWALOffset = exec.state.lastSyncedWALOffset
		})
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = rollback(tx) }()

	// Insert into the lock table to promote to a write tx. The lock table
	// insert will never actually occur because our tx will be rolled back,
	// however, it will ensure our tx grabs the write lock. Unfortunately,
	// we can't call "BEGIN IMMEDIATE" as we are already in a transaction.
	if _, err := tx.ExecContext(ctx, `INSERT INTO _litestream_lock (id) VALUES (1);`); err != nil {
		return false, fmt.Errorf("_litestream_lock: %w", err)
	}

	// Copy anything that may have occurred after the checkpoint.
	db.setSyncDiagPhase(diagPhaseCheckpointSnapshotBoundary,
		func(s *diagState) {
			s.checkpointMode = mode
			s.lastSyncedWALOffset = exec.state.lastSyncedWALOffset
		})
	snapshotInfo := syncInfo{
		offset:       WALHeaderSize,
		salt1:        binary.BigEndian.Uint32(other[16:]),
		salt2:        binary.BigEndian.Uint32(other[20:]),
		snapshotting: true,
		reason:       "checkpoint boundary snapshot",
	}
	result, err = db.sync(ctx, true, exec, snapshotInfo, 0)
	if err != nil {
		return false, fmt.Errorf("cannot snapshot after checkpoint: %w", err)
	}
	exec.applySyncResult(result)

	// Release write lock before exiting.
	// Use rollback() helper for consistency with releaseReadLock() and the
	// defer above. See issue #934.
	if err := rollback(tx); err != nil {
		return false, fmt.Errorf("rollback post-checkpoint tx: %w", err)
	}

	exec.state.syncedSinceCheckpoint = false
	return true, nil
}

// execCheckpoint issues a wal_checkpoint PRAGMA in the given mode and returns
// the number of frames in the WAL as reported by the checkpoint.
func (db *DB) execCheckpoint(ctx context.Context, mode string) (walFrameN int, err error) {
	// Ignore if there is no underlying database.
	if db.db == nil {
		return 0, nil
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
		return 0, fmt.Errorf("release read lock: %w", err)
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
		return 0, err
	}
	db.Logger.Debug("checkpoint", "mode", mode, "result", fmt.Sprintf("%d,%d,%d", row[0], row[1], row[2]))

	// Reacquire the read lock immediately after the checkpoint.
	if err := db.acquireReadLock(ctx); err != nil {
		return 0, fmt.Errorf("reacquire read lock: %w", err)
	}

	return row[1], nil
}

type snapshotReadPosition struct {
	pos          ltx.Pos
	pageSize     int
	walEndOffset int64
	db           *DB
}

func (p *snapshotReadPosition) close() {
	if p.db != nil {
		p.db.chkMu.RUnlock()
		p.db = nil
	}
}

// SnapshotReader returns the current position of the database & a reader that contains a full database snapshot.
// Internal checkpoints are disabled until the reader is fully drained or
// closed (it implements io.Closer). An abandoned reader blocks checkpoints
// for the life of the process.
func (db *DB) SnapshotReader(ctx context.Context) (ltx.Pos, io.Reader, error) {
	pos, err := db.snapshotPosition(ctx)
	if err != nil {
		return ltx.Pos{}, nil, err
	}

	r, err := db.snapshotReader(ctx, pos)
	if err != nil {
		pos.close()
		return pos.pos, nil, err
	}
	return pos.pos, r, nil
}

func (db *DB) snapshotPosition(ctx context.Context) (*snapshotReadPosition, error) {
	if err := db.lockExec(ctx); err != nil {
		return nil, err
	}
	defer db.execSem.Release(1)

	pageSize := db.PageSize()
	pos, err := db.Pos()

	if pageSize == 0 {
		db.Logger.Debug("page size not initialized yet", "pageSize", 0)
		return nil, &DBNotReadyError{Reason: "page size not initialized"}
	}
	if err != nil {
		return nil, fmt.Errorf("pos: %w", err)
	}

	walEndOffset, err := db.snapshotWALEndOffset(pos)
	if err != nil {
		return nil, err
	}
	if walEndOffset < WALHeaderSize {
		walEndOffset = WALHeaderSize
	}

	// Acquire the checkpoint read lock while the executor semaphore is still
	// held (the deferred release runs after this function returns). Every
	// checkpoint takes chkMu under execSem, so this handoff guarantees no
	// checkpoint can run between capturing pos and locking chkMu — the
	// snapshot always matches the advertised position.
	db.chkMu.RLock()
	return &snapshotReadPosition{
		pos:          pos,
		pageSize:     pageSize,
		walEndOffset: walEndOffset,
		db:           db,
	}, nil
}

// snapshotWALEndOffset returns the WAL offset a snapshot may read up to for
// the given position. db.syncState is read without db.mu because every writer
// mutates it while holding execSem, which the caller also holds.
func (db *DB) snapshotWALEndOffset(pos ltx.Pos) (int64, error) {
	if db.syncState.lastSyncedWALOffset > 0 {
		return db.syncState.lastSyncedWALOffset, nil
	}
	if pos.TXID == 0 {
		return WALHeaderSize, nil
	}

	ltxPath := db.LTXPath(0, pos.TXID, pos.TXID)
	f, err := os.Open(ltxPath)
	if err != nil {
		return 0, NewLTXError("open", ltxPath, 0, uint64(pos.TXID), uint64(pos.TXID), err)
	}
	defer func() { _ = f.Close() }()

	dec := ltx.NewDecoder(f)
	if err := dec.DecodeHeader(); err != nil {
		return 0, NewLTXError("decode", ltxPath, 0, uint64(pos.TXID), uint64(pos.TXID), fmt.Errorf("%w: %w", ErrLTXCorrupted, err))
	}

	// Compare WAL headers. If the WAL was restarted since this LTX file was
	// written, its salts no longer match and its recorded extent does not
	// apply to the current WAL.
	hdr, err := readWALHeader(db.WALPath())
	if os.IsNotExist(err) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return WALHeaderSize, nil
	} else if err != nil {
		return 0, fmt.Errorf("cannot read wal header: %w", err)
	}
	salt1 := binary.BigEndian.Uint32(hdr[16:])
	salt2 := binary.BigEndian.Uint32(hdr[20:])
	if salt1 != dec.Header().WALSalt1 || salt2 != dec.Header().WALSalt2 {
		return WALHeaderSize, nil
	}

	return dec.Header().WALOffset + dec.Header().WALSize, nil
}

func (db *DB) snapshotReader(ctx context.Context, pos *snapshotReadPosition) (io.Reader, error) {
	db.Logger.Debug("snapshot", "txid", pos.pos.TXID.String(), "walEndOffset", pos.walEndOffset)

	// TODO(ltx): Read database size from database header.

	fi, err := db.f.Stat()
	if err != nil {
		return nil, err
	}
	commit := uint32(fi.Size() / int64(pos.pageSize))

	// Execute encoding in a separate goroutine so the caller can initialize before reading.
	pr, pw := io.Pipe()
	go func() {
		defer pos.close()

		walFile, err := os.Open(db.WALPath())
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		defer walFile.Close()

		rd, err := NewWALReader(walFile, db.Logger.With(LogKeySubsystem, LogSubsystemWALReader))
		if err != nil {
			pw.CloseWithError(fmt.Errorf("new wal reader: %w", err))
			return
		}

		// Build a mapping of changed page numbers and their latest content.
		maxBytes := pos.walEndOffset - WALHeaderSize
		pageMap := make(map[uint32]int64)
		var maxOffset int64
		var walCommit uint32
		if maxBytes > 0 {
			pageMap, maxOffset, walCommit, _, err = rd.pageMap(ctx, maxBytes)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("page map: %w", err))
				return
			}
		}
		if walCommit > 0 {
			commit = walCommit
		}

		if maxOffset > pos.walEndOffset {
			pw.CloseWithError(fmt.Errorf("snapshot wal read exceeded bound: max offset %d > end offset %d", maxOffset, pos.walEndOffset))
			return
		}
		walOffset, walSize := snapshotHeaderWALRange(maxOffset, int64(WALFrameHeaderSize+pos.pageSize))

		db.Logger.Debug("encode snapshot header",
			"txid", pos.pos.TXID.String(),
			"commit", commit,
			"walOffset", walOffset,
			"walSize", walSize,
			"walEndOffset", pos.walEndOffset,
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
			PageSize:  uint32(pos.pageSize),
			Commit:    commit,
			MinTXID:   1,
			MaxTXID:   pos.pos.TXID,
			Timestamp: time.Now().UnixMilli(),
			WALOffset: walOffset,
			WALSize:   walSize,
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

	return pr, nil
}

func snapshotHeaderWALRange(maxOffset, frameSize int64) (offset, size int64) {
	if maxOffset <= WALHeaderSize || frameSize <= 0 {
		return WALHeaderSize, 0
	}
	offset = max(maxOffset-frameSize, WALHeaderSize)
	return offset, maxOffset - offset
}

// Compact performs a compaction of the LTX file at the previous level into dstLevel.
// Returns metadata for the newly written compaction file. Returns ErrNoCompaction
// if no new files are available to be compacted.
func (db *DB) Compact(ctx context.Context, dstLevel int) (*ltx.FileInfo, error) {
	info, err := db.compactor.Compact(ctx, dstLevel)
	if err != nil {
		return nil, err
	}

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

// Snapshot writes a snapshot to the replica for the current database position.
// It always writes, even when a snapshot already exists at the current
// position, so operators can force a re-upload (replicate -force-snapshot).
// Callers that want to skip duplicates check first, as Store.CompactDB does.
func (db *DB) Snapshot(ctx context.Context) (*ltx.FileInfo, error) {
	pos, r, err := db.SnapshotReader(ctx)
	if err != nil {
		return nil, err
	}

	info, err := db.Replica.Client.WriteLTXFile(ctx, SnapshotLevel, 1, pos.TXID, r)
	if err != nil {
		if closer, ok := r.(io.Closer); ok {
			_ = closer.Close()
		}
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
	var snapshots []*ltx.FileInfo
	var lastInfo *ltx.FileInfo
	for itr.Next() {
		info := itr.Item()
		snapshots = append(snapshots, info)
		lastInfo = info

		// If this snapshot is before the retention timestamp, mark it for deletion.
		if info.CreatedAt.Before(timestamp) {
			deleted = append(deleted, info)
			continue
		}
	}

	// If this is the snapshot level, we need to ensure that at least one snapshot exists.
	if len(deleted) > 0 && deleted[len(deleted)-1] == lastInfo {
		deleted = deleted[:len(deleted)-1]
	}

	for i, info := range snapshots {
		if slices.Contains(deleted, info) {
			continue
		}
		if i > 0 {
			minSnapshotTXID = snapshots[i-1].MaxTXID
		}
		break
	}

	// Remove files marked for deletion from remote storage (unless retention disabled).
	if !db.RetentionEnabled {
		db.Logger.Debug("skipping remote deletion (retention disabled)", "level", SnapshotLevel, "count", len(deleted))
	} else if err := db.Replica.Client.DeleteLTXFiles(ctx, deleted); err != nil {
		return 0, fmt.Errorf("remove ltx files: %w", err)
	}

	// Always clean up local files.
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

	db.Logger.Debug("starting l0 retention enforcement", "retention", db.L0Retention)

	dbName := filepath.Base(db.Path())

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
		internal.L0RetentionGaugeVec.WithLabelValues(dbName, "eligible").Set(0)
		internal.L0RetentionGaugeVec.WithLabelValues(dbName, "not_compacted").Set(0)
		internal.L0RetentionGaugeVec.WithLabelValues(dbName, "too_recent").Set(0)
		return nil
	}

	threshold := time.Now().Add(-db.L0Retention)
	itr, err = db.Replica.Client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		return fmt.Errorf("fetch l0 files: %w", err)
	}
	defer itr.Close()

	var (
		deleted           []*ltx.FileInfo
		lastInfo          *ltx.FileInfo
		processedAll      = true
		totalFiles        int
		notCompactedCount int
		tooRecentCount    int
	)
	for itr.Next() {
		info := itr.Item()
		lastInfo = info
		totalFiles++

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
			tooRecentCount++
			break
		}

		if info.MaxTXID <= maxL1TXID {
			deleted = append(deleted, info)
		} else {
			notCompactedCount++
		}
	}

	// Count remaining files as too_recent if we stopped early
	if !processedAll {
		for itr.Next() {
			tooRecentCount++
		}
	}

	// Ensure we do not delete the newest L0 file only if we processed the entire level.
	if processedAll && len(deleted) > 0 && lastInfo != nil && deleted[len(deleted)-1] == lastInfo {
		deleted = deleted[:len(deleted)-1]
	}

	internal.L0RetentionGaugeVec.WithLabelValues(dbName, "eligible").Set(float64(len(deleted)))
	internal.L0RetentionGaugeVec.WithLabelValues(dbName, "not_compacted").Set(float64(notCompactedCount))
	internal.L0RetentionGaugeVec.WithLabelValues(dbName, "too_recent").Set(float64(tooRecentCount))

	db.Logger.Debug("l0 retention scan complete",
		"total_l0_files", totalFiles,
		"eligible_for_deletion", len(deleted),
		"not_compacted_yet", notCompactedCount,
		"too_recent", tooRecentCount,
		"max_l1_txid", maxL1TXID)

	if len(deleted) == 0 {
		return nil
	}

	if !db.RetentionEnabled {
		db.Logger.Debug("skipping remote deletion (retention disabled)", "level", 0, "count", len(deleted))
	} else if err := db.Replica.Client.DeleteLTXFiles(ctx, deleted); err != nil {
		return fmt.Errorf("remove expired l0 files: %w", err)
	}

	for _, info := range deleted {
		localPath := db.LTXPath(0, info.MinTXID, info.MaxTXID)
		db.Logger.Debug("deleting expired local l0 file", "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "path", localPath)
		if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
			db.Logger.Error("failed to remove local l0 file", "path", localPath, "error", err)
		}
	}
	if len(deleted) > 0 {
		db.invalidatePosCache()
	}

	db.Logger.Info("l0 retention enforced", "deleted_count", len(deleted), "max_l1_txid", maxL1TXID)

	return nil
}

// EnforceRetentionByTXID enforces retention so that any LTX files below
// the target TXID are deleted. Always keep at least one file.
func (db *DB) EnforceRetentionByTXID(ctx context.Context, level int, txID ltx.TXID) (err error) {
	return db.compactor.EnforceRetentionByTXID(ctx, level, txID)
}

// monitor runs in a separate goroutine and monitors the database & WAL.
//
// Implements exponential backoff on repeated sync errors to prevent disk churn
// when persistent errors (like disk full) occur. See issue #927.
func (db *DB) monitor() {
	ticker := time.NewTicker(db.MonitorInterval)
	defer ticker.Stop()

	// Backoff state for error handling.
	var backoff time.Duration
	var lastLogTime time.Time
	var consecutiveErrs int

	for {
		// Wait for ticker or context close.
		select {
		case <-db.ctx.Done():
			return
		case <-ticker.C:
		}

		// If in backoff mode, wait additional time before retrying.
		if backoff > 0 {
			select {
			case <-db.ctx.Done():
				return
			case <-time.After(backoff):
			}
		}

		// Sync the database to the shadow WAL. Sync() loops over bounded
		// chunks, releasing the executor lock between each so checkpoints
		// and snapshots can interleave, but always catches up to the WAL
		// end so checkpointIfNeeded() runs. A single bounded chunk per
		// tick would cap drain throughput and starve the TruncatePageN
		// emergency checkpoint while behind, growing the WAL unbounded.
		if err := db.Sync(db.ctx); err != nil && !errors.Is(err, context.Canceled) {
			consecutiveErrs++

			// Exponential backoff: MonitorInterval -> 2x -> 4x -> ... -> max
			if backoff == 0 {
				backoff = db.MonitorInterval
			} else {
				backoff *= 2
				if backoff > DefaultSyncBackoffMax {
					backoff = DefaultSyncBackoffMax
				}
			}

			var diskFullErr *LTXStagingDiskFullError
			if errors.As(err, &diskFullErr) {
				if time.Since(lastLogTime) >= SyncErrorLogInterval {
					db.Logger.Error(fmt.Sprintf("disk full while staging LTX file %s: replication paused, will resume automatically when space is freed", diskFullErr.Path),
						"error", err,
						"path", diskFullErr.Path,
						"level", diskFullErr.Level,
						"min_txid", diskFullErr.MinTXID,
						"max_txid", diskFullErr.MaxTXID,
						"consecutive_errors", consecutiveErrs,
						"backoff", backoff)
					lastLogTime = time.Now()
				}
			} else {
				// Log with rate limiting to avoid log spam during persistent errors.
				if time.Since(lastLogTime) >= SyncErrorLogInterval {
					db.Logger.Error("sync error",
						"error", err,
						"consecutive_errors", consecutiveErrs,
						"backoff", backoff)
					lastLogTime = time.Now()
				}
			}

			// Try to clean up stale temp files after persistent disk errors.
			if isDiskFullError(err) && consecutiveErrs >= 3 {
				db.Logger.Warn("attempting temp file cleanup due to persistent disk errors")
				if cleanupErr := removeTmpFiles(db.metaPath); cleanupErr != nil {
					db.Logger.Error("temp file cleanup failed", "error", cleanupErr)
				}
			}
			continue
		}

		// Success - reset backoff and error counter.
		if consecutiveErrs > 0 {
			db.Logger.Info("sync recovered", "previous_errors", consecutiveErrs)
		}
		backoff = 0
		consecutiveErrs = 0
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
	if err := db.lockExec(ctx); err != nil {
		return 0, ltx.Pos{}, err
	}
	defer db.execSem.Release(1)

	exec, err := db.newSyncExecutor(ctx)
	if err != nil {
		return 0, ltx.Pos{}, err
	} else if exec == nil {
		return 0, ltx.Pos{}, os.ErrNotExist
	}
	defer db.applySyncExecutor(exec, true)

	// Force a RESTART checkpoint to ensure the database is at the start of the WAL.
	if _, err := db.checkpointWithExecutor(ctx, CheckpointModeRestart, exec); err != nil {
		return 0, ltx.Pos{}, err
	}

	// Seek to the beginning of the db file descriptor and checksum whole file.
	h := crc64.New(crc64.MakeTable(crc64.ISO))
	if _, err := db.f.Seek(0, io.SeekStart); err != nil {
		return 0, exec.pos, err
	} else if _, err := io.Copy(h, db.f); err != nil {
		return 0, exec.pos, err
	}
	return h.Sum64(), exec.pos, nil
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

// DefaultFollowInterval is the default polling interval for follow mode.
const DefaultFollowInterval = 1 * time.Second

// IntegrityCheckMode specifies the level of integrity checking after restore.
type IntegrityCheckMode int

const (
	IntegrityCheckNone IntegrityCheckMode = iota
	IntegrityCheckQuick
	IntegrityCheckFull
)

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

	// Follow enables continuous restore mode, polling for new LTX files
	// and applying them to the restored database. Similar to tail -f.
	Follow bool

	// FollowInterval specifies how often to poll for new LTX files in follow mode.
	FollowInterval time.Duration

	// IntegrityCheck specifies the level of integrity checking after restore.
	// Zero value (IntegrityCheckNone) skips the check for backward compatibility.
	IntegrityCheck IntegrityCheckMode
}

// NewRestoreOptions returns a new instance of RestoreOptions with defaults.
func NewRestoreOptions() RestoreOptions {
	return RestoreOptions{
		Parallelism:    DefaultRestoreParallelism,
		FollowInterval: DefaultFollowInterval,
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

	diskFullGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litestream_disk_full",
		Help: "Whether replication is paused because the local disk is full",
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

	compactionVerifyErrorCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litestream_compaction_verify_error_count",
		Help: "Number of post-compaction verification failures",
	}, []string{"db"})
)
