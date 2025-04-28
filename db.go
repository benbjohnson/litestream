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
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/benbjohnson/litestream/internal"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/superfly/ltx"
)

// Default DB settings.
const (
	DefaultMonitorInterval    = 1 * time.Second
	DefaultCheckpointInterval = 1 * time.Minute
	DefaultBusyTimeout        = 1 * time.Second
	DefaultMinCheckpointPageN = 1000
	DefaultMaxCheckpointPageN = 10000
	DefaultTruncatePageN      = 500000
)

// DB represents a managed instance of a SQLite database in the file system.
type DB struct {
	mu       sync.RWMutex
	path     string        // part to database
	metaPath string        // Path to the database metadata.
	db       *sql.DB       // target database
	f        *os.File      // long-running db file descriptor
	rtx      *sql.Tx       // long running read transaction
	pageSize int           // page size, in bytes
	notify   chan struct{} // closes on WAL change
	chkMu    sync.Mutex    // checkpoint lock

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
	ltxSizeGauge                prometheus.Gauge
	syncNCounter                prometheus.Counter
	syncErrorNCounter           prometheus.Counter
	syncSecondsCounter          prometheus.Counter
	checkpointNCounterVec       *prometheus.CounterVec
	checkpointErrorNCounterVec  *prometheus.CounterVec
	checkpointSecondsCounterVec *prometheus.CounterVec

	// Minimum threshold of WAL size, in pages, before a passive checkpoint.
	// A passive checkpoint will attempt a checkpoint but fail if there are
	// active transactions occurring at the same time.
	MinCheckpointPageN int

	// Maximum threshold of WAL size, in pages, before a forced checkpoint.
	// A forced checkpoint will block new transactions and wait for existing
	// transactions to finish before issuing a checkpoint and resetting the WAL.
	//
	// If zero, no checkpoints are forced. This can cause the WAL to grow
	// unbounded if there are always read transactions occurring.
	MaxCheckpointPageN int

	// Threshold of WAL size, in pages, before a forced truncation checkpoint.
	// A forced truncation checkpoint will block new transactions and wait for
	// existing transactions to finish before issuing a checkpoint and
	// truncating the WAL.
	//
	// If zero, no truncates are forced. This can cause the WAL to grow
	// unbounded if there's a sudden spike of changes between other
	// checkpoints.
	TruncatePageN int

	// Time between automatic checkpoints in the WAL. This is done to allow
	// more fine-grained WAL files so that restores can be performed with
	// better precision.
	CheckpointInterval time.Duration

	// Frequency at which to perform db sync.
	MonitorInterval time.Duration

	// The timeout to wait for EBUSY from SQLite.
	BusyTimeout time.Duration

	// List of replicas for the database.
	// Must be set before calling Open().
	Replicas []*Replica

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

		MinCheckpointPageN: DefaultMinCheckpointPageN,
		MaxCheckpointPageN: DefaultMaxCheckpointPageN,
		TruncatePageN:      DefaultTruncatePageN,
		CheckpointInterval: DefaultCheckpointInterval,
		MonitorInterval:    DefaultMonitorInterval,
		BusyTimeout:        DefaultBusyTimeout,
		Logger:             slog.With("db", path),
	}

	db.dbSizeGauge = dbSizeGaugeVec.WithLabelValues(db.path)
	db.walSizeGauge = walSizeGaugeVec.WithLabelValues(db.path)
	db.totalWALBytesCounter = totalWALBytesCounterVec.WithLabelValues(db.path)
	db.txIDGauge = txIDIndexGaugeVec.WithLabelValues(db.path)
	db.ltxSizeGauge = ltxSizeGaugeVec.WithLabelValues(db.path)
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
	assert(minTXID >= 0, "min txid cannot be negative")
	assert(maxTXID >= 0, "max txid cannot be negative")
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

// Replica returns a replica by name.
func (db *DB) Replica(name string) *Replica {
	for _, r := range db.Replicas {
		if r.Name() == name {
			return r
		}
	}
	return nil
}

// Pos returns the current replication position of the database.
func (db *DB) Pos() (ltx.Pos, error) {
	minTXID, maxTXID, err := db.MaxLTX()
	if err != nil {
		return ltx.Pos{}, err
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

	// Validate that all replica names are unique.
	m := make(map[string]struct{})
	for _, r := range db.Replicas {
		if _, ok := m[r.Name()]; ok {
			return fmt.Errorf("duplicate replica name: %q", r.Name())
		}
		m[r.Name()] = struct{}{}
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
	for _, r := range db.Replicas {
		if db.db != nil {
			if e := r.Sync(ctx); e != nil && err == nil {
				err = e
			}
		}
		r.Stop(true)
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

// init initializes the connection to the database.
// Skipped if already initialized or if the database file does not exist.
func (db *DB) init() (err error) {
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

	dsn := db.path
	dsn += fmt.Sprintf("?_busy_timeout=%d", db.BusyTimeout.Milliseconds())

	// Connect to SQLite database. Use the driver registered with a hook to
	// prevent WAL files from being removed.
	if db.db, err = sql.Open("litestream-sqlite3", dsn); err != nil {
		return err
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
	if err := db.db.QueryRow(`PRAGMA journal_mode = wal;`).Scan(&mode); err != nil {
		return err
	} else if mode != "wal" {
		return fmt.Errorf("enable wal failed, mode=%q", mode)
	}

	// Disable autocheckpoint for litestream's connection.
	if _, err := db.db.ExecContext(db.ctx, `PRAGMA wal_autocheckpoint = 0;`); err != nil {
		return fmt.Errorf("disable autocheckpoint: %w", err)
	}

	// Create a table to force writes to the WAL when empty.
	// There should only ever be one row with id=1.
	if _, err := db.db.Exec(`CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);`); err != nil {
		return fmt.Errorf("create _litestream_seq table: %w", err)
	}

	// Create a lock table to force write locks during sync.
	// The sync write transaction always rolls back so no data should be in this table.
	if _, err := db.db.Exec(`CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);`); err != nil {
		return fmt.Errorf("create _litestream_lock table: %w", err)
	}

	// Start a long-running read transaction to prevent other transactions
	// from checkpointing.
	if err := db.acquireReadLock(); err != nil {
		return fmt.Errorf("acquire read lock: %w", err)
	}

	// Read page size.
	if err := db.db.QueryRow(`PRAGMA page_size;`).Scan(&db.pageSize); err != nil {
		return fmt.Errorf("read page size: %w", err)
	} else if db.pageSize <= 0 {
		return fmt.Errorf("invalid db page size: %d", db.pageSize)
	}

	// Ensure meta directory structure exists.
	if err := internal.MkdirAll(db.metaPath, db.dirInfo); err != nil {
		return err
	}

	// If we have an existing shadow WAL, ensure the headers match.
	if err := db.verifyHeadersMatch(); err != nil {
		panic(fmt.Sprintf("init: cannot determine last wal position, clearing generation: %s", err))
	}

	// TODO(gen): Generate diff of current LTX snapshot and save as next LTX file.

	// Clean up replicated WAL files.
	if err := db.clean(); err != nil {
		return fmt.Errorf("clean: %w", err)
	}

	// Start replication.
	for _, r := range db.Replicas {
		r.Start(db.ctx)
	}

	return nil
}

// verifyHeadersMatch returns true if the primary WAL and last shadow WAL header match.
func (db *DB) verifyHeadersMatch() error {
	pos, err := db.Pos()
	if err != nil {
		return fmt.Errorf("cannot determine position: %w", err)
	}

	hdr0, err := readWALHeader(db.WALPath())
	if os.IsNotExist(err) {
		return fmt.Errorf("no primary wal: %w", err)
	} else if err != nil {
		return fmt.Errorf("primary wal header: %w", err)
	}
	salt1 := binary.BigEndian.Uint32(hdr0[16:])
	salt2 := binary.BigEndian.Uint32(hdr0[20:])

	ltxPath := db.LTXPath(0, pos.TXID, pos.TXID)
	f, err := os.Open()
	if err != nil {
		return fmt.Errorf("open ltx path: %w", err)
	}
	defer func() { _ = f.Close() }()

	dec := ltx.NewDecoder(f)
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("decode ltx header: %w", err)
	}
	hdr1 := dec.Header()
	if salt1 != hdr1.WALSalt1 || salt2 != hdr1.WALSalt2 {
		return fmt.Errorf("salt mismatch (%x,%x) <> (%x,%x) on %s", salt1, salt2, hdr1.WALSalt1, hdr1.WALSalt2, ltxPath)
	}
	return nil
}

// clean removes old WAL files.
func (db *DB) clean() error {
	return db.cleanWAL()
}

// cleanWAL removes WAL files that have been replicated.
func (db *DB) cleanWAL() error {
	// Determine lowest txn that's been replicated to all replicas.
	var min ltx.TXID
	for _, r := range db.Replicas {
		pos := r.Pos()
		if min == 0 || pos.TXID < min {
			min = pos.TXID
		}
	}

	// Skip if our lowest index is too small.
	if min <= 0 {
		return nil
	}
	min-- // Always keep an extra WAL file.

	// Remove all WAL files before the lowest index.
	dir := db.LTXLevelDir(0)
	fis, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	for _, fi := range fis {
		if _, maxTXID, err := ltx.ParseFilename(fi.Name()); err != nil {
			continue
		} else if maxTXID >= min {
			continue
		}
		if err := os.Remove(filepath.Join(dir, fi.Name())); err != nil {
			return err
		}
	}
	return nil
}

// acquireReadLock begins a read transaction on the database to prevent checkpointing.
func (db *DB) acquireReadLock() error {
	if db.rtx != nil {
		return nil
	}

	// Start long running read-transaction to prevent checkpoints.
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}

	// Execute read query to obtain read lock.
	if _, err := tx.ExecContext(db.ctx, `SELECT COUNT(1) FROM _litestream_seq;`); err != nil {
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
	if err := db.init(); err != nil {
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
	if err := db.ensureWALExists(); err != nil {
		return fmt.Errorf("ensure wal exists: %w", err)
	}

	// Verify our last sync matches the current state of the WAL.
	// This ensures that the last sync position of the real WAL hasn't
	// been overwritten by another process.
	info, err := db.verify()
	if err != nil {
		return fmt.Errorf("cannot verify wal state: %w", err)
	}
	db.Logger.Debug("sync", "info", &info)

	// Track if anything in the shadow WAL changes and then notify at the end.
	changed := info.walSize != info.shadowWALSize || info.restart || info.reason != ""

	/*
		// TODO(gen): Handle sync issue that previously restarted generation

		// If we are unable to verify the WAL state then we start a new generation.
		if info.reason != "" {
			panic(fmt.Sprintf("unable to verify wal state: %q", info.reason))

			// Clear shadow wal info.
			info.shadowWALPath = db.ShadowWALPath(0)
			info.shadowWALSize = WALHeaderSize
			info.restart = false
			info.reason = ""
		}
	*/

	// Synchronize real WAL with current shadow WAL.
	origWALSize, newWALSize, err := db.syncWAL(info)
	if err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}

	// If WAL size is great than max threshold, force checkpoint.
	// If WAL size is greater than min threshold, attempt checkpoint.
	var checkpoint bool
	checkpointMode := CheckpointModePassive
	if db.TruncatePageN > 0 && origWALSize >= calcWALSize(db.pageSize, db.TruncatePageN) {
		checkpoint, checkpointMode = true, CheckpointModeTruncate
	} else if db.MaxCheckpointPageN > 0 && newWALSize >= calcWALSize(db.pageSize, db.MaxCheckpointPageN) {
		checkpoint, checkpointMode = true, CheckpointModeRestart
	} else if newWALSize >= calcWALSize(db.pageSize, db.MinCheckpointPageN) {
		checkpoint = true
	} else if db.CheckpointInterval > 0 && !info.dbModTime.IsZero() && time.Since(info.dbModTime) > db.CheckpointInterval && newWALSize > calcWALSize(db.pageSize, 1) {
		checkpoint = true
	}

	// Issue the checkpoint.
	if checkpoint {
		changed = true

		if err := db.checkpoint(ctx, checkpointMode); err != nil {
			return fmt.Errorf("checkpoint: mode=%v err=%w", checkpointMode, err)
		}
	}

	// Clean up any old files.
	if err := db.clean(); err != nil {
		return fmt.Errorf("cannot clean: %w", err)
	}

	// Compute current index and total shadow WAL size.
	pos, err := db.Pos()
	if err != nil {
		return fmt.Errorf("pos: %w", err)
	}
	db.txIDGauge.Set(float64(pos.TXID))
	// db.shadowWALSizeGauge.Set(float64(size))

	// Notify replicas of WAL changes.
	if changed {
		close(db.notify)
		db.notify = make(chan struct{})
	}

	db.Logger.Debug("sync: ok")

	return nil
}

// ensureWALExists checks that the real WAL exists and has a header.
func (db *DB) ensureWALExists() (err error) {
	// Exit early if WAL header exists.
	if fi, err := os.Stat(db.WALPath()); err == nil && fi.Size() >= WALHeaderSize {
		return nil
	}

	// Otherwise create transaction that updates the internal litestream table.
	_, err = db.db.Exec(`INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1`)
	return err
}

// verify ensures the current LTX state matches where it left off from
// the real WAL. Returns WAL sync information. If info.reason is not blank,
// verification failed.
func (db *DB) verify() (info syncInfo, err error) {
	// Determine total bytes of real DB for metrics.
	fi, err := os.Stat(db.Path())
	if err != nil {
		return info, err
	}
	info.dbModTime = fi.ModTime()
	db.dbSizeGauge.Set(float64(fi.Size()))

	// Determine total bytes of real WAL.
	fi, err = os.Stat(db.WALPath())
	if err != nil {
		return info, err
	}
	info.walSize = frameAlign(fi.Size(), db.pageSize)
	info.walModTime = fi.ModTime()
	db.walSizeGauge.Set(float64(fi.Size()))

	// Open shadow WAL to copy append to.
	pos, err := db.Pos()
	if err != nil {
		return info, fmt.Errorf("cannot determine current position: %w", err)
	}
	info.ltxPath = db.LTXPath(0, pos.TXID, pos.TXID)

	// Determine shadow WAL current size.
	f, err := os.Open(info.ltxPath)
	if os.IsNotExist(err) {
		info.reason = "no shadow wal"
		return info, nil
	} else if err != nil {
		return info, fmt.Errorf("open ltx file: %w", err)
	}
	defer func() { _ = f.Close() }()

	dec := ltx.NewDecoder(f)
	if err := dec.DecodeHeader(); err != nil {
		return info, fmt.Errorf("deocde ltx file: %w", err)
	}
	info.ltxWALSize = dec.Header().WALSize

	// If LTX WAL offset is larger than real WAL then the WAL has been truncated
	// so we cannot determine our last state.
	if info.ltxWALSize > info.walSize {
		info.reason = "wal truncated by another process"
		return info, nil
	}

	// Compare WAL headers. Start a new shadow WAL if they are mismatched.
	hdr0, err := readWALHeader(db.WALPath())
	if err != nil {
		return info, fmt.Errorf("cannot read wal header: %w", err)
	}
	salt1 := binary.BigEndian.Uint32(hdr0[16:])
	salt2 := binary.BigEndian.Uint32(hdr0[20:])

	if salt1 != dec.Header().WALSalt1 || salt2 != dec.Header().WALSalt2 {
		info.restart = true
	}

	// If we only have a header then ensure header matches.
	if /*info.ltxWALSize == WALHeaderSize &&*/ info.restart {
		info.reason = "wal header only, mismatched"
		return info, nil
	}

	// Verify last page exists in latest LTX file.
	/*if info.ltxWALSize > WALHeaderSize {*/
	/*offset := info.ltxWALSize - int64(db.pageSize+WALFrameHeaderSize)*/
	frameSize := int64(db.pageSize + WALFrameHeaderSize)
	buf, err := readWALFileAt(db.WALPath(), dec.Header().WALSize-frameSize, frameSize)
	if err != nil {
		return info, fmt.Errorf("cannot read last synced wal page: %w", err)
	}
	pgno := binary.BigEndian.Uint32(buf[0:])
	fsalt1 := binary.BigEndian.Uint32(buf[8:])
	fsalt2 := binary.BigEndian.Uint32(buf[12:])

	pageData := make([]byte, db.pageSize)
	for {
		var pageHdr ltx.PageHeader
		if err := dec.DecodePage(&pageHdr, pageData); err != nil {
			return info, fmt.Errorf("decode ltx page: %w", err)
		}
		if fsalt1 != dec.Header().WALSalt1 || fsalt2 != dec.Header().WALSalt2 {
			info.reason = "frame salt mismatch, wal overwritten by another process"
			return info, nil
		}
		if pgno != pageHdr.Pgno {
			info.reason = "pgno mismatch, wal overwritten by another process"
			return info, nil
		}
		if !bytes.Equal(buf[WALFrameHeaderSize:], pageData) {
			info.reason = "page data mismatch, wal overwritten by another process"
			return info, nil
		}
	}
	return info, nil
}

type syncInfo struct {
	dbModTime  time.Time // last modified date of real DB file
	walSize    int64     // size of real WAL file
	walModTime time.Time // last modified date of real WAL file
	ltxPath    string    // name of latest LTX file
	ltxWALSize int64     // WAL offset from latest LTX file
	restart    bool      // if true, real WAL salt does not match LTX
	reason     string    // if non-blank, reason for sync failure
}

// syncWAL copies pending bytes from the real WAL to LTX.
func (db *DB) syncWAL(info syncInfo) (origSize int64, newSize int64, err error) {
	// TODO: Read WAL transactions since last LTX offset.
	// TODO: Check if the beginning has

	filename := db.LTXPath(0, info.txid, info.txid)
	logger := db.Logger.With("filename", filename)
	logger.Debug("syncWAL")

	walFile, err := os.Open(db.WALPath())
	if err != nil {
		return 0, 0, err
	}
	defer walFile.Close()

	fi, err := walFile.Stat()
	if err != nil {
		return 0, 0, err
	}
	origWALSize = frameAlign(fi.Size(), db.pageSize)

	rd := NewWALReader(walFile)
	if err := rd.ReadHeader(); err != nil {
		return 0, 0, fmt.Errorf("read wal header: %w", err)
	}
	pageMap, sz, commit, err := rd.PageMap()
	if err != nil {
		return 0, 0, fmt.Errorf("page map: %w", err)
	}

	mode := os.FileMode(0600)
	if fi := db.fileInfo; fi != nil {
		mode = fi.Mode()
	}

	tmpFilename := filename + ".tmp"
	w, err := os.OpenFile(tmpFilename, os.O_RDWR, mode)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = os.Remove(tmpFilename) }()
	defer func() { _ = w.Close() }()

	if err := internal.MkdirAll(filepath.Dir(tmpFilename), db.dirInfo); err != nil {
		return 0, 0, err
	}
	uid, gid := internal.Fileinfo(db.fileInfo)
	_ = os.Chown(tmpFilename, uid, gid)

	enc := ltx.NewEncoder(w)
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum | ltx.HeaderFlagCompressLZ4,
		PageSize:  pageSize,
		Commit:    commit,
		MinTXID:   info.txid,
		MaxTXID:   info.txid,
		Timestamp: time.Now().UnixMilli(),
		WALOffset: info.walOffset,
		WALSize:   sz,
		WALSalt1:  rd.salt1,
		WALSalt2:  rd.salt2,
	}); err != nil {
		return 0, 0, fmt.Errorf("encode ltx header: %w", err)
	}

	data := make([]byte, pageSize)
	for {
		// TODO: Read source page using page map
		// TODO: Write page to LTX file.
	}

	// TODO: Sync & close LTX file, atomically write to final destination.

	return origSize, newSize, nil
}

func (db *DB) copyToLTX(filename string) (origWalSize int64, newSize int64, err error) {
	logger := db.Logger.With("filename", filename)
	logger.Debug("copy-shadow")

	r, err := os.Open(db.WALPath())
	if err != nil {
		return 0, 0, err
	}
	defer r.Close()

	fi, err := r.Stat()
	if err != nil {
		return 0, 0, err
	}
	origWalSize = frameAlign(fi.Size(), db.pageSize)

	w, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		return 0, 0, err
	}
	defer w.Close()

	fi, err = w.Stat()
	if err != nil {
		return 0, 0, err
	}
	origSize := frameAlign(fi.Size(), db.pageSize)

	// TODO(ltx): Read salt & offset from previous LTX file.
	// TODO(ltx): If there is another transaction to read, continue reading.
	// TODO(ltx): If are no additional transactions, check if salt has changed at beginning and read transactions from there.

	// Read LTX salt to determine byte order for checksum & salt.
	/*
		hdr := make([]byte, WALHeaderSize)
		if _, err := io.ReadFull(w, hdr); err != nil {
			return 0, 0, fmt.Errorf("read header: %w", err)
		}
		hsalt0 := binary.BigEndian.Uint32(hdr[16:])
		hsalt1 := binary.BigEndian.Uint32(hdr[20:])
	*/

	bo, err := headerByteOrder(hdr)
	if err != nil {
		return 0, 0, err
	}

	// Read previous checksum.
	chksum0, chksum1, err := readLastChecksumFrom(w, db.pageSize)
	if err != nil {
		return 0, 0, fmt.Errorf("last checksum: %w", err)
	}

	// Write to a temporary LTX file.
	tempFilename := filename + ".tmp"
	defer os.Remove(tempFilename)

	f, err := internal.CreateFile(tempFilename, db.fileInfo)
	if err != nil {
		return 0, 0, fmt.Errorf("create temp file: %w", err)
	}
	defer f.Close()

	// Seek to correct position on real wal.
	if _, err := r.Seek(origSize, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("real wal seek: %w", err)
	}
	//else if _, err := w.Seek(origSize, io.SeekStart); err != nil {
	//	return 0, 0, fmt.Errorf("shadow wal seek: %w", err)
	//}

	// Read through WAL from last position to find the page of the last
	// committed transaction.
	frame := make([]byte, db.pageSize+WALFrameHeaderSize)
	offset := origSize
	lastCommitSize := origSize
	for {
		// Read next page from WAL file.
		if _, err := io.ReadFull(r, frame); err == io.EOF || err == io.ErrUnexpectedEOF {
			logger.Debug("copy-shadow: break", "offset", offset, "error", err)
			break // end of file or partial page
		} else if err != nil {
			return 0, 0, fmt.Errorf("read wal: %w", err)
		}

		// Read frame salt & compare to header salt. Stop reading on mismatch.
		salt0 := binary.BigEndian.Uint32(frame[8:])
		salt1 := binary.BigEndian.Uint32(frame[12:])
		if salt0 != hsalt0 || salt1 != hsalt1 {
			logger.Debug("copy-shadow: break: salt mismatch")
			break
		}

		// Verify checksum of page is valid.
		fchksum0 := binary.BigEndian.Uint32(frame[16:])
		fchksum1 := binary.BigEndian.Uint32(frame[20:])
		chksum0, chksum1 = Checksum(bo, chksum0, chksum1, frame[:8])  // frame header
		chksum0, chksum1 = Checksum(bo, chksum0, chksum1, frame[24:]) // frame data
		if chksum0 != fchksum0 || chksum1 != fchksum1 {
			logger.Debug("copy shadow: checksum mismatch, skipping", "offset", offset, "check", fmt.Sprintf("(%x,%x) != (%x,%x)", chksum0, chksum1, fchksum0, fchksum1))
			break
		}

		// Write page to temporary WAL file.
		if _, err := f.Write(frame); err != nil {
			return 0, 0, fmt.Errorf("write temp shadow wal: %w", err)
		}

		logger.Debug("copy-shadow: ok", "offset", offset, "salt", fmt.Sprintf("%x %x", salt0, salt1))
		offset += int64(len(frame))

		// Update new size if written frame was a commit record.
		newDBSize := binary.BigEndian.Uint32(frame[4:])
		if newDBSize != 0 {
			lastCommitSize = offset
		}
	}

	// If no WAL writes found, exit.
	if origSize == lastCommitSize {
		return origSize, lastCommitSize, nil
	}

	walByteN := lastCommitSize - origSize

	// Move to beginning of temporary file.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, fmt.Errorf("temp file seek: %w", err)
	}

	// Copy from temporary file to shadow WAL.
	if _, err := io.Copy(w, &io.LimitedReader{R: f, N: walByteN}); err != nil {
		return 0, 0, fmt.Errorf("write shadow file: %w", err)
	}

	// Close & remove temporary file.
	if err := f.Close(); err != nil {
		return 0, 0, err
	} else if err := os.Remove(tempFilename); err != nil {
		return 0, 0, err
	}

	// Sync & close shadow WAL.
	if err := w.Sync(); err != nil {
		return 0, 0, err
	} else if err := w.Close(); err != nil {
		return 0, 0, err
	}

	// Track total number of bytes written to WAL.
	db.totalWALBytesCounter.Add(float64(walByteN))

	return origWalSize, lastCommitSize, nil
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

	shadowWALPath, err := db.CurrentShadowWALPath()
	if err != nil {
		return err
	}

	// Read WAL header before checkpoint to check if it has been restarted.
	hdr, err := readWALHeader(db.WALPath())
	if err != nil {
		return err
	}

	// Copy shadow WAL before checkpoint to copy as much as possible.
	if _, _, err := db.copyToShadowWAL(shadowWALPath); err != nil {
		return fmt.Errorf("cannot copy to end of shadow wal before checkpoint: %w", err)
	}

	// Execute checkpoint and immediately issue a write to the WAL to ensure
	// a new page is written.
	if err := db.execCheckpoint(mode); err != nil {
		return err
	} else if _, err = db.db.Exec(`INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1`); err != nil {
		return err
	}

	// If WAL hasn't been restarted, exit.
	if other, err := readWALHeader(db.WALPath()); err != nil {
		return err
	} else if bytes.Equal(hdr, other) {
		return nil
	}

	// Start a transaction. This will be promoted immediately after.
	tx, err := db.db.Begin()
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

	// Copy the end of the previous WAL before starting a new shadow WAL.
	if _, _, err := db.copyToShadowWAL(shadowWALPath); err != nil {
		return fmt.Errorf("cannot copy to end of shadow wal: %w", err)
	}

	// Parse index of current shadow WAL file.
	index, err := ParseWALPath(shadowWALPath)
	if err != nil {
		return fmt.Errorf("cannot parse shadow wal filename: %s", shadowWALPath)
	}

	// Start a new shadow WAL file with next index.
	newShadowWALPath := filepath.Join(filepath.Dir(shadowWALPath), FormatWALPath(index+1))
	if _, err := db.initShadowWALFile(newShadowWALPath); err != nil {
		return fmt.Errorf("cannot init shadow wal file: name=%s err=%w", newShadowWALPath, err)
	}

	// Release write lock before checkpointing & exiting.
	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("rollback post-checkpoint tx: %w", err)
	}
	return nil
}

func (db *DB) execCheckpoint(mode string) (err error) {
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
	defer func() { _ = db.acquireReadLock() }()

	// A non-forced checkpoint is issued as "PASSIVE". This will only checkpoint
	// if there are not pending transactions. A forced checkpoint ("RESTART")
	// will wait for pending transactions to end & block new transactions before
	// forcing the checkpoint and restarting the WAL.
	//
	// See: https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
	rawsql := `PRAGMA wal_checkpoint(` + mode + `);`

	var row [3]int
	if err := db.db.QueryRow(rawsql).Scan(&row[0], &row[1], &row[2]); err != nil {
		return err
	}
	db.Logger.Debug("checkpoint", "mode", mode, "result", fmt.Sprintf("%d,%d,%d", row[0], row[1], row[2]))

	// Reacquire the read lock immediately after the checkpoint.
	if err := db.acquireReadLock(); err != nil {
		return fmt.Errorf("release read lock: %w", err)
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

// CalcRestoreTarget returns a replica to restore from based on opt criteria.
func (db *DB) CalcRestoreTarget(ctx context.Context, opt RestoreOptions) (*Replica, error) {
	var target struct {
		replica   *Replica
		updatedAt time.Time
	}

	for _, r := range db.Replicas {
		// Skip replica if it does not match filter.
		if opt.ReplicaName != "" && r.Name() != opt.ReplicaName {
			continue
		}

		updatedAt, err := r.CalcRestoreTarget(ctx, opt)
		if err != nil {
			return nil, err
		}

		// Use the latest replica if we have multiple candidates.
		if !updatedAt.After(target.updatedAt) {
			continue
		}

		target.replica, target.updatedAt = r, updatedAt
	}
	return target.replica, nil
}

// applyWAL performs a truncating checkpoint on the given database.
func applyWAL(_ context.Context, index int, dbPath string) error {
	// Copy WAL file from it's staging path to the correct "-wal" location.
	if err := os.Rename(fmt.Sprintf("%s-%08x-wal", dbPath, index), dbPath+"-wal"); err != nil {
		return err
	}

	// Open SQLite database and force a truncating checkpoint.
	d, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer d.Close()

	var row [3]int
	if err := d.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE);`).Scan(&row[0], &row[1], &row[2]); err != nil {
		return err
	} else if row[0] != 0 {
		return fmt.Errorf("truncation checkpoint failed during restore (%d,%d,%d)", row[0], row[1], row[2])
	}
	return d.Close()
}

// CRC64 returns a CRC-64 ISO checksum of the database and its current position.
//
// This function obtains a read lock so it prevents syncs from occurring until
// the operation is complete. The database will still be usable but it will be
// unable to checkpoint during this time.
//
// If dst is set, the database file is copied to that location before checksum.
func (db *DB) CRC64(ctx context.Context) (uint64, Pos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.init(); err != nil {
		return 0, Pos{}, err
	} else if db.db == nil {
		return 0, Pos{}, os.ErrNotExist
	}

	// Force a RESTART checkpoint to ensure the database is at the start of the WAL.
	if err := db.checkpoint(ctx, CheckpointModeRestart); err != nil {
		return 0, Pos{}, err
	}

	// Obtain current position. Clear the offset since we are only reading the
	// DB and not applying the current WAL.
	pos, err := db.Pos()
	if err != nil {
		return 0, pos, err
	}
	pos.Offset = 0

	// Seek to the beginning of the db file descriptor and checksum whole file.
	h := crc64.New(crc64.MakeTable(crc64.ISO))
	if _, err := db.f.Seek(0, io.SeekStart); err != nil {
		return 0, pos, err
	} else if _, err := io.Copy(h, db.f); err != nil {
		return 0, pos, err
	}
	return h.Sum64(), pos, nil
}

// BeginSnapshot takes an internal snapshot lock preventing checkpoints.
//
// When calling this the caller must also call EndSnapshot() once the snapshot
// is finished.
func (db *DB) BeginSnapshot() {
	db.chkMu.Lock()
}

// EndSnapshot releases the internal snapshot lock that prevents checkpoints.
func (db *DB) EndSnapshot() {
	db.chkMu.Unlock()
}

// frameAlign returns a frame-aligned offset.
// Returns zero if offset is less than the WAL header size.
func frameAlign(offset int64, pageSize int) int64 {
	assert(offset >= 0, "frameAlign(): offset must be non-negative")
	assert(pageSize >= 0, "frameAlign(): page size must be non-negative")

	if offset < WALHeaderSize {
		return 0
	}

	frameSize := WALFrameHeaderSize + int64(pageSize)
	frameN := (offset - WALHeaderSize) / frameSize
	return (frameN * frameSize) + WALHeaderSize
}

func readLastChecksumFrom(f *os.File, pageSize int) (uint32, uint32, error) {
	// Determine the byte offset of the checksum for the header (if no pages
	// exist) or for the last page (if at least one page exists).
	offset := int64(WALHeaderChecksumOffset)
	if fi, err := f.Stat(); err != nil {
		return 0, 0, err
	} else if sz := frameAlign(fi.Size(), pageSize); fi.Size() > WALHeaderSize {
		offset = sz - int64(pageSize) - WALFrameHeaderSize + WALFrameHeaderChecksumOffset
	}

	// Read big endian checksum.
	b := make([]byte, 8)
	if n, err := f.ReadAt(b, offset); err != nil {
		return 0, 0, err
	} else if n != len(b) {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return binary.BigEndian.Uint32(b[0:]), binary.BigEndian.Uint32(b[4:]), nil
}

// DefaultRestoreParallelism is the default parallelism when downloading WAL files.
const DefaultRestoreParallelism = 8

// RestoreOptions represents options for DB.Restore().
type RestoreOptions struct {
	// Target path to restore into.
	// If blank, the original DB path is used.
	OutputPath string

	// Specific replica to restore from.
	// If blank, all replicas are considered.
	ReplicaName string

	// Specific index to restore from.
	// Set to math.MaxInt32 to ignore index.
	Index int

	// Point-in-time to restore database.
	// If zero, database restore to most recent state available.
	Timestamp time.Time

	// Specifies how many WAL files are downloaded in parallel during restore.
	Parallelism int
}

// NewRestoreOptions returns a new instance of RestoreOptions with defaults.
func NewRestoreOptions() RestoreOptions {
	return RestoreOptions{
		Index:       math.MaxInt32,
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

	shadowWALIndexGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litestream_shadow_wal_index",
		Help: "The current index of the shadow WAL",
	}, []string{"db"})

	shadowWALSizeGaugeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litestream_shadow_wal_size",
		Help: "Current size of shadow WAL, in bytes",
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

func headerByteOrder(hdr []byte) (binary.ByteOrder, error) {
	magic := binary.BigEndian.Uint32(hdr[0:])
	switch magic {
	case 0x377f0682:
		return binary.LittleEndian, nil
	case 0x377f0683:
		return binary.BigEndian, nil
	default:
		return nil, fmt.Errorf("invalid wal header magic: %x", magic)
	}
}
