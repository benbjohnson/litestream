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

	// Remote replica for the database.
	// Must be set before calling Open().
	Replica *Replica

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
		Logger:             slog.With("db", filepath.Base(path)),
	}

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
			if e := db.Replica.Sync(ctx); e != nil && err == nil {
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

	// Ensure WAL has at least one frame in it.
	if err := db.ensureWALExists(); err != nil {
		return fmt.Errorf("ensure wal exists: %w", err)
	}

	// If we have an existing replication files, ensure the headers match.
	//if err := db.verifyHeadersMatch(); err != nil {
	//	return fmt.Errorf("cannot determine last wal position: %w", err)
	//}

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
		db.Logger.Debug("salt mismatch",
			"path", ltxPath,
			"wal", [2]uint32{salt1, salt2},
			"ltx", [2]uint32{hdr1.WALSalt1, hdr1.WALSalt2})
		return false, nil
	}
	return true, nil
}
*/

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

	if err := db.verifyAndSync(ctx, false); err != nil {
		return err
	}

	/*
		// TODO(ltx): Move checkpointing into its own goroutine?

		// If WAL size is greater than max threshold, force checkpoint.
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
	*/

	// Compute current index and total shadow WAL size.
	pos, err := db.Pos()
	if err != nil {
		return fmt.Errorf("pos: %w", err)
	}
	db.txIDGauge.Set(float64(pos.TXID))
	// db.shadowWALSizeGauge.Set(float64(size))

	// Notify replicas of WAL changes.
	// if changed {
	close(db.notify)
	db.notify = make(chan struct{})
	// }

	return nil
}

func (db *DB) verifyAndSync(ctx context.Context, checkpointing bool) error {
	// Verify our last sync matches the current state of the WAL.
	// This ensures that the last sync position of the real WAL hasn't
	// been overwritten by another process.
	info, err := db.verify()
	if err != nil {
		return fmt.Errorf("cannot verify wal state: %w", err)
	}

	if err := db.sync(ctx, checkpointing, info); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
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
// the real WAL. Check info.ok if verification was successful.
func (db *DB) verify() (info syncInfo, err error) {
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

	if salt1 != dec.Header().WALSalt1 || salt2 != dec.Header().WALSalt2 {
		db.Logger.Debug("wal restarted",
			"salt1", salt1,
			"salt2", salt2)

		info.offset = WALHeaderSize
		info.salt1, info.salt2 = salt1, salt2
		info.snapshotting = false
		return info, nil
	}

	// If offset is at the beginning of the first page, we can't check for previous page.
	frameSize := int64(db.pageSize + WALFrameHeaderSize)
	prevWALOffset := info.offset - frameSize
	if prevWALOffset <= 0 {
		info.snapshotting = false
		return info, nil
	}

	// Verify last page exists in latest LTX file.
	buf, err := readWALFileAt(db.WALPath(), prevWALOffset, frameSize)
	if err != nil {
		return info, fmt.Errorf("cannot read last synced wal page: %w", err)
	}
	pgno := binary.BigEndian.Uint32(buf[0:])
	fsalt1 := binary.BigEndian.Uint32(buf[8:])
	fsalt2 := binary.BigEndian.Uint32(buf[12:])

	if fsalt1 != dec.Header().WALSalt1 || fsalt2 != dec.Header().WALSalt2 {
		info.reason = "frame salt mismatch, wal overwritten by another process"
		return info, nil
	}

	// Verify that the last page in the WAL exists in the last LTX file.
	if ok, err := db.ltxDecoderContains(dec, pgno, buf[WALFrameHeaderSize:]); err != nil {
		return info, fmt.Errorf("ltx contains: %w", err)
	} else if !ok {
		db.Logger.Debug("cannot find last page in last ltx file", "pgno", pgno)
		info.reason = "last page does not exist in last ltx file, wal overwritten by another process"
		return info, nil
	}

	info.snapshotting = false

	return info, nil
}

func (db *DB) ltxDecoderContains(dec *ltx.Decoder, pgno uint32, data []byte) (bool, error) {
	buf := make([]byte, dec.Header().PageSize)
	for {
		var hdr ltx.PageHeader
		if err := dec.DecodePage(&hdr, buf); err == io.EOF {
			return false, nil
		} else if err != nil {
			return false, fmt.Errorf("decode ltx page: %w", err)
		}

		if pgno != hdr.Pgno {
			continue
		}
		if !bytes.Equal(data, buf) {
			continue
		}
		return true, nil
	}
}

type syncInfo struct {
	offset       int64 // end of the previous LTX read
	salt1        uint32
	salt2        uint32
	snapshotting bool   // if true, a full snapshot is required
	reason       string // reason for snapshot
}

// sync copies pending bytes from the real WAL to LTX.
func (db *DB) sync(ctx context.Context, checkpointing bool, info syncInfo) error {
	// Determine the next sequential transaction ID.
	pos, err := db.Pos()
	if err != nil {
		return fmt.Errorf("pos: %w", err)
	}
	txID := pos.TXID + 1

	filename := db.LTXPath(0, txID, txID)
	db.Logger.Debug("sync",
		"txid", txID.String(),
		"chkpt", checkpointing,
		"snap", info.snapshotting,
		"offset", info.offset,
		"reason", info.reason)

	// Prevent internal checkpoints during sync. Ignore if already in a checkpoint.
	if !checkpointing {
		db.chkMu.Lock()
		defer db.chkMu.Unlock()
	}

	fi, err := db.f.Stat()
	if err != nil {
		return err
	}
	mode := fi.Mode()
	commit := uint32(fi.Size() / int64(db.pageSize))

	walFile, err := os.Open(db.WALPath())
	if err != nil {
		return err
	}
	defer walFile.Close()

	var rd *WALReader
	if info.offset == WALHeaderSize {
		if rd, err = NewWALReader(walFile, db.Logger); err != nil {
			return fmt.Errorf("new wal reader: %w", err)
		}
	} else {
		// If we cannot verify the previous frame
		var pfmError *PrevFrameMismatchError
		if rd, err = NewWALReaderWithOffset(walFile, info.offset, info.salt1, info.salt2, db.Logger); errors.As(err, &pfmError) {
			db.Logger.Debug("prev frame mismatch, snapshotting", "err", pfmError.Err)
			if rd, err = NewWALReader(walFile, db.Logger); err != nil {
				return fmt.Errorf("new wal reader, after reset")
			}
		} else if err != nil {
			return fmt.Errorf("new wal reader with offset: %w", err)
		}
	}

	// Build a mapping of changed page numbers and their latest content.
	pageMap, sz, walCommit, err := rd.PageMap()
	if err != nil {
		return fmt.Errorf("page map: %w", err)
	}
	if walCommit > 0 {
		commit = walCommit
	}

	// Exit if we have no new WAL pages and we aren't snapshotting.
	if !info.snapshotting && sz == 0 {
		db.Logger.Debug("sync: skip", "reason", "no new wal pages")
		return nil
	}

	tmpFilename := filename + ".tmp"
	if err := internal.MkdirAll(filepath.Dir(tmpFilename), db.dirInfo); err != nil {
		return err
	}

	ltxFile, err := os.OpenFile(tmpFilename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("open temp ltx file: %w", err)
	}
	defer func() { _ = os.Remove(tmpFilename) }()
	defer func() { _ = ltxFile.Close() }()

	uid, gid := internal.Fileinfo(db.fileInfo)
	_ = os.Chown(tmpFilename, uid, gid)

	db.Logger.Debug("encode header",
		"txid", txID.String(),
		"commit", commit,
		"walOffset", info.offset,
		"walSize", sz,
		"salt1", rd.salt1,
		"salt2", rd.salt2)

	enc := ltx.NewEncoder(ltxFile)
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum | ltx.HeaderFlagCompressLZ4,
		PageSize:  uint32(db.pageSize),
		Commit:    commit,
		MinTXID:   txID,
		MaxTXID:   txID,
		Timestamp: time.Now().UnixMilli(),
		WALOffset: info.offset,
		WALSize:   sz,
		WALSalt1:  rd.salt1,
		WALSalt2:  rd.salt2,
	}); err != nil {
		return fmt.Errorf("encode ltx header: %w", err)
	}

	// If we need a full snapshot, then copy from the database & WAL.
	// Otherwise, just copy incrementally from the WAL.
	if info.snapshotting {
		if err := db.writeLTXFromDB(ctx, enc, walFile, commit, pageMap); err != nil {
			return fmt.Errorf("write ltx from db: %w", err)
		}
	} else {
		if err := db.writeLTXFromWAL(ctx, enc, walFile, pageMap); err != nil {
			return fmt.Errorf("write ltx from db: %w", err)
		}
	}

	// Encode final trailer to the end of the LTX file.
	if err := enc.Close(); err != nil {
		return fmt.Errorf("close ltx encoder: %w", err)
	}

	// Sync & close LTX file.
	if err := ltxFile.Sync(); err != nil {
		return fmt.Errorf("sync ltx file: %w", err)
	}
	if err := ltxFile.Close(); err != nil {
		return fmt.Errorf("close ltx file: %w", err)
	}

	// Atomically rename file to final path.
	if err := os.Rename(tmpFilename, filename); err != nil {
		return fmt.Errorf("rename ltx file: %w", err)
	}

	db.Logger.Debug("sync: ok")

	return nil
}

func (db *DB) writeLTXFromDB(_ context.Context, enc *ltx.Encoder, walFile *os.File, commit uint32, pageMap map[uint32]int64) error {
	lockPgno := ltx.LockPgno(uint32(db.pageSize))
	data := make([]byte, db.pageSize)

	for pgno := uint32(1); pgno <= commit; pgno++ {
		if pgno == lockPgno {
			continue
		}

		// If page exists in the WAL, read from there.
		if offset, ok := pageMap[pgno]; ok {
			db.Logger.Debug("encode page from wal", "offset", offset, "pgno", pgno)

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
		db.Logger.Debug("encode page from database", "offset", offset, "pgno", pgno)

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

func (db *DB) writeLTXFromWAL(_ context.Context, enc *ltx.Encoder, walFile *os.File, pageMap map[uint32]int64) error {
	// Create an ordered list of page numbers since the LTX encoder requires it.
	pgnos := make([]uint32, 0, len(pageMap))
	for pgno := range pageMap {
		pgnos = append(pgnos, pgno)
	}
	slices.Sort(pgnos)

	data := make([]byte, db.pageSize)
	for _, pgno := range pgnos {
		offset := pageMap[pgno]

		db.Logger.Debug("encode page from wal", "offset", offset, "pgno", pgno)

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
	if err := db.verifyAndSync(ctx, true); err != nil {
		return fmt.Errorf("cannot copy wal before checkpoint: %w", err)
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

	// Copy anything that may have occurred after the checkpoint.
	if err := db.verifyAndSync(ctx, true); err != nil {
		return fmt.Errorf("cannot copy wal after checkpoint: %w", err)
	}

	// Release write lock before exiting.
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

	if err := db.init(); err != nil {
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
