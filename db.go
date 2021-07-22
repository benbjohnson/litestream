package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/litestream/internal"
	"github.com/pierrec/lz4/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Default DB settings.
const (
	DefaultMonitorInterval    = 1 * time.Second
	DefaultCheckpointInterval = 1 * time.Minute
	DefaultMinCheckpointPageN = 1000
	DefaultMaxCheckpointPageN = 10000
)

// MaxIndex is the maximum possible WAL index.
// If this index is reached then a new generation will be started.
const MaxIndex = 0x7FFFFFFF

// BusyTimeout is the timeout to wait for EBUSY from SQLite.
const BusyTimeout = 1 * time.Second

// DB represents a managed instance of a SQLite database in the file system.
type DB struct {
	mu       sync.RWMutex
	path     string        // part to database
	db       *sql.DB       // target database
	f        *os.File      // long-running db file descriptor
	rtx      *sql.Tx       // long running read transaction
	pos      Pos           // cached position
	pageSize int           // page size, in bytes
	notify   chan struct{} // closes on WAL change

	// Cached salt & checksum from current shadow header.
	hdr              []byte
	frame            []byte
	salt0, salt1     uint32
	chksum0, chksum1 uint32
	byteOrder        binary.ByteOrder

	fileInfo os.FileInfo // db info cached during init
	dirInfo  os.FileInfo // parent dir info cached during init

	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	// Metrics
	dbSizeGauge                 prometheus.Gauge
	walSizeGauge                prometheus.Gauge
	totalWALBytesCounter        prometheus.Counter
	shadowWALIndexGauge         prometheus.Gauge
	shadowWALSizeGauge          prometheus.Gauge
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

	// Time between automatic checkpoints in the WAL. This is done to allow
	// more fine-grained WAL files so that restores can be performed with
	// better precision.
	CheckpointInterval time.Duration

	// Frequency at which to perform db sync.
	MonitorInterval time.Duration

	// List of replicas for the database.
	// Must be set before calling Open().
	Replicas []*Replica

	Logger *log.Logger
}

// NewDB returns a new instance of DB for a given path.
func NewDB(path string) *DB {
	db := &DB{
		path:   path,
		notify: make(chan struct{}),

		MinCheckpointPageN: DefaultMinCheckpointPageN,
		MaxCheckpointPageN: DefaultMaxCheckpointPageN,
		CheckpointInterval: DefaultCheckpointInterval,
		MonitorInterval:    DefaultMonitorInterval,

		Logger: log.New(LogWriter, fmt.Sprintf("%s: ", path), LogFlags),
	}

	db.dbSizeGauge = dbSizeGaugeVec.WithLabelValues(db.path)
	db.walSizeGauge = walSizeGaugeVec.WithLabelValues(db.path)
	db.totalWALBytesCounter = totalWALBytesCounterVec.WithLabelValues(db.path)
	db.shadowWALIndexGauge = shadowWALIndexGaugeVec.WithLabelValues(db.path)
	db.shadowWALSizeGauge = shadowWALSizeGaugeVec.WithLabelValues(db.path)
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
	dir, file := filepath.Split(db.path)
	return filepath.Join(dir, file+MetaDirSuffix)
}

// GenerationNamePath returns the path of the name of the current generation.
func (db *DB) GenerationNamePath() string {
	return filepath.Join(db.MetaPath(), "generation")
}

// GenerationPath returns the path of a single generation.
// Panics if generation is blank.
func (db *DB) GenerationPath(generation string) string {
	assert(generation != "", "generation name required")
	return filepath.Join(db.MetaPath(), "generations", generation)
}

// ShadowWALDir returns the path of the shadow wal directory.
// Panics if generation is blank.
func (db *DB) ShadowWALDir(generation string) string {
	return filepath.Join(db.GenerationPath(generation), "wal")
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

// Pos returns the cached position of the database.
// Returns a zero position if no position has been calculated or if there is no generation.
func (db *DB) Pos() Pos {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.pos
}

// reset clears all cached data.
func (db *DB) reset() {
	db.pos = Pos{}
	db.hdr, db.frame = nil, nil
	db.salt0, db.salt1 = 0, 0
	db.chksum0, db.chksum1 = 0, 0
	db.byteOrder = nil
}

// invalidate refreshes cached position, salt, & checksum from on-disk data.
func (db *DB) invalidate(ctx context.Context) (err error) {
	// Clear cached data before starting.
	db.reset()

	// If any error occurs, ensure all cached data is cleared.
	defer func() {
		if err != nil {
			db.reset()
		}
	}()

	// Determine the last position of the current generation.
	if err := db.invalidatePos(ctx); err != nil {
		return fmt.Errorf("cannot determine pos: %w", err)
	} else if db.pos.IsZero() {
		db.Logger.Printf("init: no wal files available, clearing generation")
		if err := db.clearGeneration(ctx); err != nil {
			return fmt.Errorf("clear generation: %w", err)
		}
		return nil // no position, exit
	}

	// Determine salt & last checksum.
	if err := db.invalidateChecksum(ctx); err != nil {
		return fmt.Errorf("cannot determine last salt/checksum: %w", err)
	}
	return nil
}

func (db *DB) invalidatePos(ctx context.Context) error {
	// Determine generation based off "generation" file in meta directory.
	generation, err := db.CurrentGeneration()
	if err != nil {
		return err
	} else if generation == "" {
		return nil
	}

	// Iterate over all segments to find the last one.
	itr, err := db.WALSegments(context.Background(), generation)
	if err != nil {
		return err
	}
	defer itr.Close()

	var pos Pos
	for itr.Next() {
		info := itr.WALSegment()
		pos = info.Pos()
	}
	if err := itr.Close(); err != nil {
		return err
	}

	// Exit if no WAL segments exist.
	if pos.IsZero() {
		return nil
	}

	// Read size of last segment to determine ending position.
	rd, err := db.WALSegmentReader(ctx, pos)
	if err != nil {
		return fmt.Errorf("cannot read last wal segment: %w", err)
	}
	defer rd.Close()

	n, err := io.Copy(ioutil.Discard, lz4.NewReader(rd))
	if err != nil {
		return err
	}
	pos.Offset += n

	// Save position to cache.
	db.pos = pos

	return nil
}

func (db *DB) invalidateChecksum(ctx context.Context) error {
	assert(!db.pos.IsZero(), "position required to invalidate checksum")

	// Read entire WAL from combined segments.
	walReader, err := db.WALReader(ctx, db.pos.Generation, db.pos.Index)
	if err != nil {
		return fmt.Errorf("cannot read last wal: %w", err)
	}
	defer walReader.Close()

	// Ensure we don't read past our position.
	r := &io.LimitedReader{R: walReader, N: db.pos.Offset}

	// Read header.
	hdr := make([]byte, WALHeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return fmt.Errorf("read shadow wal header: %w", err)
	}

	// Read byte order.
	byteOrder, err := headerByteOrder(hdr)
	if err != nil {
		return err
	}

	// Save salt & checksum to cache, although checksum may be overridden later.
	db.salt0 = binary.BigEndian.Uint32(hdr[16:])
	db.salt1 = binary.BigEndian.Uint32(hdr[20:])
	db.chksum0 = binary.BigEndian.Uint32(hdr[24:])
	db.chksum1 = binary.BigEndian.Uint32(hdr[28:])
	db.byteOrder = byteOrder

	// Iterate over each page in the WAL and save the checksum.
	frame := make([]byte, db.pageSize+WALFrameHeaderSize)
	var hasFrame bool
	for {
		// Read next page from WAL file.
		if _, err := io.ReadFull(r, frame); err == io.EOF {
			break // end of WAL file
		} else if err != nil {
			return fmt.Errorf("read wal: %w", err)
		}

		// Save frame checksum to cache.
		hasFrame = true
		db.chksum0 = binary.BigEndian.Uint32(frame[16:])
		db.chksum1 = binary.BigEndian.Uint32(frame[20:])
	}

	// Save last frame to cache.
	if hasFrame {
		db.frame = frame
	} else {
		db.frame = nil
	}

	return nil
}

// WALReader returns the entire uncompressed WAL file for a given index.
func (db *DB) WALReader(ctx context.Context, generation string, index int) (_ io.ReadCloser, err error) {
	// If any error occurs, we need to clean up all open handles.
	var rcs []io.ReadCloser
	defer func() {
		if err != nil {
			for _, rc := range rcs {
				rc.Close()
			}
		}
	}()

	offsets, err := db.walSegmentOffsetsByIndex(generation, index)
	if err != nil {
		return nil, fmt.Errorf("wal segment offsets: %w", err)
	}

	for _, offset := range offsets {
		f, err := os.Open(filepath.Join(db.ShadowWALDir(generation), FormatIndex(index), FormatOffset(offset)+".wal.lz4"))
		if err != nil {
			return nil, err
		}
		rcs = append(rcs, internal.NewReadCloser(lz4.NewReader(f), f))
	}

	return internal.NewMultiReadCloser(rcs), nil
}

func (db *DB) walSegmentOffsetsByIndex(generation string, index int) ([]int64, error) {
	// Read files from index directory.
	ents, err := os.ReadDir(filepath.Join(db.ShadowWALDir(generation), FormatIndex(index)))
	if err != nil {
		return nil, err
	}

	var offsets []int64
	for _, ent := range ents {
		if !strings.HasSuffix(ent.Name(), ".wal.lz4") {
			continue
		}
		offset, err := ParseOffset(strings.TrimSuffix(filepath.Base(ent.Name()), ".wal.lz4"))
		if err != nil {
			continue
		}
		offsets = append(offsets, offset)
	}

	// Sort before returning.
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

	return offsets, nil
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
	if err := removeTmpFiles(db.MetaPath()); err != nil {
		return fmt.Errorf("cannot remove tmp files: %w", err)
	}

	// Start monitoring SQLite database in a separate goroutine.
	if db.MonitorInterval > 0 {
		db.wg.Add(1)
		go func() { defer db.wg.Done(); db.monitor() }()
	}

	return nil
}

// Close releases the read lock & closes the database. This method should only
// be called by tests as it causes the underlying database to be checkpointed.
func (db *DB) Close() (err error) {
	return db.close(false)
}

// SoftClose closes everything but the underlying db connection. This method
// is available because the binary needs to avoid closing the database on exit
// to prevent autocheckpointing.
func (db *DB) SoftClose() (err error) {
	return db.close(true)
}

func (db *DB) close(soft bool) (err error) {
	db.cancel()
	db.wg.Wait()

	// Start a new context for shutdown since we canceled the DB context.
	ctx := context.Background()

	// Perform a final db sync, if initialized.
	if db.db != nil {
		if e := db.Sync(ctx); e != nil && err == nil {
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
		r.Stop(!soft)
	}

	// Release the read lock to allow other applications to handle checkpointing.
	if db.rtx != nil {
		if e := db.releaseReadLock(); e != nil && err == nil {
			err = e
		}
	}

	// Only perform full close if this is not a soft close.
	// This closes the underlying database connection which can clean up the WAL.
	if !soft && db.db != nil {
		if e := db.db.Close(); e != nil && err == nil {
			err = e
		}
	}

	return err
}

// UpdatedAt returns the last modified time of the database or WAL file.
func (db *DB) UpdatedAt() (time.Time, error) {
	// Determine database modified time.
	fi, err := os.Stat(db.Path())
	if err != nil {
		return time.Time{}, err
	}
	t := fi.ModTime().UTC()

	// Use WAL modified time, if available & later.
	if fi, err := os.Stat(db.WALPath()); os.IsNotExist(err) {
		return t, nil
	} else if err != nil {
		return t, err
	} else if fi.ModTime().After(t) {
		t = fi.ModTime().UTC()
	}
	return t, nil
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
	dsn += fmt.Sprintf("?_busy_timeout=%d", BusyTimeout.Milliseconds())

	// Connect to SQLite database.
	if db.db, err = sql.Open("sqlite3", dsn); err != nil {
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
	if _, err := db.db.ExecContext(db.ctx, `CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);`); err != nil {
		return fmt.Errorf("create _litestream_seq table: %w", err)
	}

	// Create a lock table to force write locks during sync.
	// The sync write transaction always rolls back so no data should be in this table.
	if _, err := db.db.ExecContext(db.ctx, `CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);`); err != nil {
		return fmt.Errorf("create _litestream_lock table: %w", err)
	}

	// Start a long-running read transaction to prevent other transactions
	// from checkpointing.
	if err := db.acquireReadLock(); err != nil {
		return fmt.Errorf("acquire read lock: %w", err)
	}

	// Read page size.
	if err := db.db.QueryRowContext(db.ctx, `PRAGMA page_size;`).Scan(&db.pageSize); err != nil {
		return fmt.Errorf("read page size: %w", err)
	} else if db.pageSize <= 0 {
		return fmt.Errorf("invalid db page size: %d", db.pageSize)
	}

	// Ensure meta directory structure exists.
	if err := internal.MkdirAll(db.MetaPath(), db.dirInfo); err != nil {
		return err
	}

	// Determine current position, if available.
	if err := db.invalidate(db.ctx); err != nil {
		return fmt.Errorf("invalidate: %w", err)
	}

	// If we have an existing shadow WAL, ensure the headers match.
	if err := db.verifyHeadersMatch(); err != nil {
		db.Logger.Printf("init: cannot determine last wal position, clearing generation; %s", err)
		if err := db.clearGeneration(db.ctx); err != nil {
			return fmt.Errorf("clear generation: %w", err)
		}
	}

	// Clean up previous generations.
	if err := db.clean(db.ctx); err != nil {
		return fmt.Errorf("clean: %w", err)
	}

	// Start replication.
	for _, r := range db.Replicas {
		r.Start(db.ctx)
	}

	return nil
}

func (db *DB) clearGeneration(ctx context.Context) error {
	if err := os.Remove(db.GenerationNamePath()); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// verifyHeadersMatch returns true if the primary WAL and last shadow WAL header match.
func (db *DB) verifyHeadersMatch() error {
	// Skip verification if we have no current position.
	if db.pos.IsZero() {
		return nil
	}

	// Read header from the real WAL file.
	hdr, err := readWALHeader(db.WALPath())
	if os.IsNotExist(err) {
		return fmt.Errorf("no primary wal: %w", err)
	} else if err != nil {
		return fmt.Errorf("primary wal header: %w", err)
	}

	// Compare real WAL header with shadow WAL header.
	// If there is a mismatch then the real WAL has been restarted outside Litestream.
	if !bytes.Equal(hdr, db.hdr) {
		return fmt.Errorf("wal header mismatch at %s", db.pos.Truncate())
	}
	return nil
}

// clean removes old generations & WAL files.
func (db *DB) clean(ctx context.Context) error {
	if err := db.cleanGenerations(ctx); err != nil {
		return err
	}
	return db.cleanWAL(ctx)
}

// cleanGenerations removes old generations.
func (db *DB) cleanGenerations(ctx context.Context) error {
	generation, err := db.CurrentGeneration()
	if err != nil {
		return err
	}

	dir := filepath.Join(db.MetaPath(), "generations")
	fis, err := ioutil.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	for _, fi := range fis {
		// Skip the current generation.
		if filepath.Base(fi.Name()) == generation {
			continue
		}

		// Delete all other generations.
		if err := os.RemoveAll(filepath.Join(dir, fi.Name())); err != nil {
			return err
		}
	}
	return nil
}

// cleanWAL removes WAL files that have been replicated.
func (db *DB) cleanWAL(ctx context.Context) error {
	generation, err := db.CurrentGeneration()
	if err != nil {
		return fmt.Errorf("current generation: %w", err)
	}

	// Determine lowest index that's been replicated to all replicas.
	minIndex := -1
	for _, r := range db.Replicas {
		pos := r.Pos().Truncate()
		if pos.Generation != generation {
			continue // different generation, skip
		} else if minIndex == -1 || pos.Index < minIndex {
			minIndex = pos.Index
		}
	}

	// Skip if our lowest position is too small.
	if minIndex <= 0 {
		return nil
	}

	// Delete all WAL index directories below the minimum position.
	dir := db.ShadowWALDir(generation)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, ent := range ents {
		index, err := ParseIndex(ent.Name())
		if err != nil {
			continue
		} else if index >= minIndex {
			continue // not below min, skip
		}

		if err := os.RemoveAll(filepath.Join(dir, FormatIndex(index))); err != nil {
			return err
		}

		db.Logger.Printf("remove shadow index: %s/%08x", generation, index)
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
	if _, err := tx.Exec(`SELECT COUNT(1) FROM _litestream_seq;`); err != nil {
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

// CurrentGeneration returns the name of the generation saved to the "generation"
// file in the meta data directory. Returns empty string if none exists.
func (db *DB) CurrentGeneration() (string, error) {
	buf, err := os.ReadFile(db.GenerationNamePath())
	if os.IsNotExist(err) {
		return "", nil
	} else if err != nil {
		return "", err
	}

	generation := strings.TrimSpace(string(buf))
	if len(generation) != GenerationNameLen {
		return "", nil
	}
	return generation, nil
}

// createGeneration starts a new generation by creating the generation
// directory, snapshotting to each replica, and updating the current
// generation name.
func (db *DB) createGeneration(ctx context.Context) (string, error) {
	// Generate random generation hex name.
	buf := make([]byte, GenerationNameLen/2)
	_, _ = rand.New(rand.NewSource(time.Now().UnixNano())).Read(buf)
	generation := hex.EncodeToString(buf)

	// Generate new directory.
	dir := filepath.Join(db.MetaPath(), "generations", generation)
	if err := internal.MkdirAll(dir, db.dirInfo); err != nil {
		return "", err
	}

	// Initialize shadow WAL with copy of header.
	if err := db.initShadowWALIndex(ctx, Pos{Generation: generation}); err != nil {
		return "", fmt.Errorf("initialize shadow wal: %w", err)
	}

	// Atomically write generation name as current generation.
	generationNamePath := db.GenerationNamePath()
	mode := os.FileMode(0600)
	if db.fileInfo != nil {
		mode = db.fileInfo.Mode()
	}
	if err := os.WriteFile(generationNamePath+".tmp", []byte(generation+"\n"), mode); err != nil {
		return "", fmt.Errorf("write generation temp file: %w", err)
	}
	uid, gid := internal.Fileinfo(db.fileInfo)
	_ = os.Chown(generationNamePath+".tmp", uid, gid)
	if err := os.Rename(generationNamePath+".tmp", generationNamePath); err != nil {
		return "", fmt.Errorf("rename generation file: %w", err)
	}

	// Remove old generations.
	if err := db.clean(db.ctx); err != nil {
		return "", err
	}

	return generation, nil
}

// Sync copies pending data from the WAL to the shadow WAL.
func (db *DB) Sync(ctx context.Context) (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Initialize database, if necessary. Exit if no DB exists.
	if err := db.init(); err != nil {
		return err
	} else if db.db == nil {
		return nil
	}

	// Ensure the cached position exists.
	if db.pos.IsZero() {
		if err := db.invalidate(ctx); err != nil {
			return fmt.Errorf("invalidate: %w", err)
		}
	}
	origPos := db.pos

	// If sync fails, reset position & cache.
	defer func() {
		if err != nil {
			db.reset()
		}
	}()

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
	// This ensures that we have an existing generation & that the last sync
	// position of the real WAL hasn't been overwritten by another process.
	info, err := db.verify()
	if err != nil {
		return fmt.Errorf("cannot verify wal state: %w", err)
	}

	// If we are unable to verify the WAL state then we start a new generation.
	if info.reason != "" {
		// Start new generation & notify user via log message.
		if info.generation, err = db.createGeneration(ctx); err != nil {
			return fmt.Errorf("create generation: %w", err)
		}
		db.Logger.Printf("sync: new generation %q, %s", info.generation, info.reason)

		// Clear shadow wal info.
		info.restart = false
		info.reason = ""
	}

	// Synchronize real WAL with current shadow WAL.
	if err := db.copyToShadowWAL(ctx); err != nil {
		return fmt.Errorf("cannot copy to shadow wal: %w", err)
	}

	// If we are at the end of the WAL file, start a new index.
	if info.restart {
		// Move to beginning of next index.
		pos := db.pos.Truncate()
		pos.Index++

		// Attempt to restart WAL from beginning of new index.
		// Position is only committed to cache if successful.
		if err := db.initShadowWALIndex(ctx, pos); err != nil {
			return fmt.Errorf("cannot init shadow wal: pos=%s err=%w", pos, err)
		}
	}

	// If WAL size is great than max threshold, force checkpoint.
	// If WAL size is greater than min threshold, attempt checkpoint.
	var checkpoint bool
	checkpointMode := CheckpointModePassive
	if db.MaxCheckpointPageN > 0 && db.pos.Offset >= calcWALSize(db.pageSize, db.MaxCheckpointPageN) {
		checkpoint, checkpointMode = true, CheckpointModeRestart
	} else if db.pos.Offset >= calcWALSize(db.pageSize, db.MinCheckpointPageN) {
		checkpoint = true
	} else if db.CheckpointInterval > 0 && !info.dbModTime.IsZero() && time.Since(info.dbModTime) > db.CheckpointInterval && db.pos.Offset > calcWALSize(db.pageSize, 1) {
		checkpoint = true
	}

	// Issue the checkpoint.
	if checkpoint {
		if err := db.checkpoint(ctx, info.generation, checkpointMode); err != nil {
			return fmt.Errorf("checkpoint: mode=%v err=%w", checkpointMode, err)
		}
	}

	// Clean up any old files.
	if err := db.clean(ctx); err != nil {
		return fmt.Errorf("cannot clean: %w", err)
	}

	// Compute current index and total shadow WAL size.
	// This is only for metrics so we ignore any errors that occur.
	db.shadowWALIndexGauge.Set(float64(db.pos.Index))
	db.shadowWALSizeGauge.Set(float64(db.pos.Offset))

	// Notify replicas of WAL changes.
	if db.pos != origPos {
		close(db.notify)
		db.notify = make(chan struct{})
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

// verify ensures the current shadow WAL state matches where it left off from
// the real WAL. Returns generation & WAL sync information. If info.reason is
// not blank, verification failed and a new generation should be started.
func (db *DB) verify() (info syncInfo, err error) {
	// Look up existing generation.
	generation, err := db.CurrentGeneration()
	if err != nil {
		return info, fmt.Errorf("cannot find current generation: %w", err)
	} else if generation == "" {
		info.reason = "no generation exists"
		return info, nil
	}
	info.generation = generation

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
	walSize := fi.Size()
	info.walModTime = fi.ModTime()
	db.walSizeGauge.Set(float64(walSize))

	// Verify the index is not out of bounds.
	if db.pos.Index >= MaxIndex {
		info.reason = "max index exceeded"
		return info, nil
	}

	// If shadow WAL position is larger than real WAL then the WAL has been
	// truncated so we cannot determine our last state.
	if db.pos.Offset > walSize {
		info.reason = "wal truncated by another process"
		return info, nil
	}

	// Compare WAL headers. Start a new shadow WAL if they are mismatched.
	if hdr, err := readWALHeader(db.WALPath()); err != nil {
		return info, fmt.Errorf("cannot read wal header: %w", err)
	} else if !bytes.Equal(hdr, db.hdr) {
		info.restart = true
	}

	// Verify last frame synced still matches.
	if db.pos.Offset > WALHeaderSize {
		offset := db.pos.Offset - int64(db.pageSize+WALFrameHeaderSize)
		if frame, err := readWALFileAt(db.WALPath(), offset, int64(db.pageSize+WALFrameHeaderSize)); err != nil {
			return info, fmt.Errorf("cannot read last synced wal page: %w", err)
		} else if !bytes.Equal(frame, db.frame) {
			info.reason = "wal overwritten by another process"
			return info, nil
		}
	}

	return info, nil
}

type syncInfo struct {
	generation string    // generation name
	dbModTime  time.Time // last modified date of real DB file
	walModTime time.Time // last modified date of real WAL file
	restart    bool      // if true, real WAL header does not match shadow WAL
	reason     string    // if non-blank, reason for sync failure
}

func (db *DB) initShadowWALIndex(ctx context.Context, pos Pos) error {
	assert(pos.Offset == 0, "must init shadow wal index with zero offset")

	hdr, err := readWALHeader(db.WALPath())
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	// Determine byte order for checksumming from header magic.
	byteOrder, err := headerByteOrder(hdr)
	if err != nil {
		return err
	}

	// Verify checksum.
	chksum0 := binary.BigEndian.Uint32(hdr[24:])
	chksum1 := binary.BigEndian.Uint32(hdr[28:])
	if v0, v1 := Checksum(byteOrder, 0, 0, hdr[:24]); v0 != chksum0 || v1 != chksum1 {
		return fmt.Errorf("invalid header checksum: (%x,%x) != (%x,%x)", v0, v1, chksum0, chksum1)
	}

	// Compress header to LZ4.
	var buf bytes.Buffer
	zw := lz4.NewWriter(&buf)
	if _, err := zw.Write(hdr); err != nil {
		return err
	} else if err := zw.Close(); err != nil {
		return err
	}

	// Write header segment to shadow WAL & update position.
	if err := db.writeWALSegment(ctx, pos, &buf); err != nil {
		return fmt.Errorf("write shadow wal header: %w", err)
	}
	pos.Offset += int64(len(hdr))
	db.pos = pos

	// Save header, salt & checksum to cache.
	db.hdr = hdr
	db.salt0 = binary.BigEndian.Uint32(hdr[16:])
	db.salt1 = binary.BigEndian.Uint32(hdr[20:])
	db.chksum0, db.chksum1 = chksum0, chksum1
	db.byteOrder = byteOrder

	// Copy as much shadow WAL as available.
	if err := db.copyToShadowWAL(ctx); err != nil {
		return fmt.Errorf("cannot copy to new shadow wal: %w", err)
	}
	return nil
}

func (db *DB) copyToShadowWAL(ctx context.Context) error {
	pos := db.pos
	assert(!pos.IsZero(), "zero pos for wal copy")

	r, err := os.Open(db.WALPath())
	if err != nil {
		return err
	}
	defer r.Close()

	// Write to a temporary WAL segment file.
	tempFilename := filepath.Join(db.ShadowWALDir(pos.Generation), FormatIndex(pos.Index), FormatOffset(pos.Offset)+".wal.tmp")
	defer os.Remove(tempFilename)

	f, err := internal.CreateFile(tempFilename, db.fileInfo)
	if err != nil {
		return err
	}
	defer f.Close()

	// Seek to correct position on real wal.
	if _, err := r.Seek(pos.Offset, io.SeekStart); err != nil {
		return fmt.Errorf("real wal seek: %w", err)
	}

	// The high water mark (HWM) tracks the position & checksum of the position
	// of the last committed transaction frame.
	hwm := struct {
		pos     Pos
		chksum0 uint32
		chksum1 uint32
		frame   []byte
	}{db.pos, db.chksum0, db.chksum1, make([]byte, db.pageSize+WALFrameHeaderSize)}

	// Copy from last position in real WAL to the last committed transaction.
	frame := make([]byte, db.pageSize+WALFrameHeaderSize)
	chksum0, chksum1 := db.chksum0, db.chksum1
	for {
		// Read next page from WAL file.
		if _, err := io.ReadFull(r, frame); err == io.EOF || err == io.ErrUnexpectedEOF {
			break // end of file or partial page
		} else if err != nil {
			return fmt.Errorf("read wal: %w", err)
		}

		// Read frame salt & compare to header salt. Stop reading on mismatch.
		salt0 := binary.BigEndian.Uint32(frame[8:])
		salt1 := binary.BigEndian.Uint32(frame[12:])
		if salt0 != db.salt0 || salt1 != db.salt1 {
			break
		}

		// Verify checksum of page is valid.
		fchksum0 := binary.BigEndian.Uint32(frame[16:])
		fchksum1 := binary.BigEndian.Uint32(frame[20:])
		chksum0, chksum1 = Checksum(db.byteOrder, chksum0, chksum1, frame[:8])  // frame header
		chksum0, chksum1 = Checksum(db.byteOrder, chksum0, chksum1, frame[24:]) // frame data
		if chksum0 != fchksum0 || chksum1 != fchksum1 {
			break
		}

		// Add page to the new size of the shadow WAL.
		if _, err := f.Write(frame); err != nil {
			return fmt.Errorf("write temp shadow wal segment: %w", err)
		}

		pos.Offset += int64(len(frame))

		// Flush to shadow WAL if commit record.
		newDBSize := binary.BigEndian.Uint32(frame[4:])
		if newDBSize != 0 {
			hwm.pos = pos
			hwm.chksum0, hwm.chksum1 = chksum0, chksum1
			copy(hwm.frame, frame)
		}
	}

	// If no WAL writes found, exit.
	if db.pos == hwm.pos {
		return nil
	}

	walByteN := hwm.pos.Offset - db.pos.Offset

	// Move to beginning of temporary file.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("temp file seek: %w", err)
	}

	// Copy temporary file to a pipe while compressing the data.
	// Only read up to the number of bytes from the original position to the HWM.
	pr, pw := io.Pipe()
	go func() {
		zw := lz4.NewWriter(pw)
		if _, err := io.Copy(zw, &io.LimitedReader{R: f, N: walByteN}); err != nil {
			pw.CloseWithError(err)
		} else if err := zw.Close(); err != nil {
			pw.CloseWithError(err)
		}
		pw.Close()
	}()

	// Write a new, compressed segment via pipe.
	if err := db.writeWALSegment(ctx, db.pos, pr); err != nil {
		return fmt.Errorf("write wal segment: pos=%s err=%w", db.pos, err)
	}

	// Update the position & checksum on success.
	db.pos = hwm.pos
	db.chksum0, db.chksum1 = hwm.chksum0, hwm.chksum1
	db.frame = hwm.frame

	// Close & remove temporary file.
	if err := f.Close(); err != nil {
		return err
	} else if err := os.Remove(tempFilename); err != nil {
		return err
	}

	// Track total number of bytes written to WAL.
	db.totalWALBytesCounter.Add(float64(walByteN))

	return nil
}

// WALSegmentReader returns a reader for a section of WAL data at the given position.
// Returns os.ErrNotExist if no matching index/offset is found.
func (db *DB) WALSegmentReader(ctx context.Context, pos Pos) (io.ReadCloser, error) {
	if pos.Generation == "" {
		return nil, fmt.Errorf("generation required")
	}
	return os.Open(filepath.Join(db.ShadowWALDir(pos.Generation), FormatIndex(pos.Index), FormatOffset(pos.Offset)+".wal.lz4"))
}

// writeWALSegment writes LZ4 compressed data from rd into a file on disk.
func (db *DB) writeWALSegment(ctx context.Context, pos Pos, rd io.Reader) error {
	if pos.Generation == "" {
		return fmt.Errorf("generation required")
	}
	filename := filepath.Join(db.ShadowWALDir(pos.Generation), FormatIndex(pos.Index), FormatOffset(pos.Offset)+".wal.lz4")

	// Ensure parent directory exists.
	if err := internal.MkdirAll(filepath.Dir(filename), db.dirInfo); err != nil {
		return err
	}

	// Write WAL segment to temporary file next to destination path.
	f, err := internal.CreateFile(filename+".tmp", db.fileInfo)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rd); err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	// Move WAL segment to final path when it has been written & synced to disk.
	if err := os.Rename(filename+".tmp", filename); err != nil {
		return err
	}

	return nil
}

// WALSegments returns an iterator over all available WAL files for a generation.
func (db *DB) WALSegments(ctx context.Context, generation string) (WALSegmentIterator, error) {
	ents, err := os.ReadDir(db.ShadowWALDir(generation))
	if os.IsNotExist(err) {
		return NewWALSegmentInfoSliceIterator(nil), nil
	} else if err != nil {
		return nil, err
	}

	// Iterate over every file and convert to metadata.
	indexes := make([]int, 0, len(ents))
	for _, ent := range ents {
		index, err := ParseIndex(ent.Name())
		if err != nil {
			continue
		}
		indexes = append(indexes, index)
	}

	sort.Ints(indexes)

	return newShadowWALSegmentIterator(db, generation, indexes), nil
}

type shadowWALSegmentIterator struct {
	db         *DB
	generation string
	indexes    []int

	infos []WALSegmentInfo
	err   error
}

func newShadowWALSegmentIterator(db *DB, generation string, indexes []int) *shadowWALSegmentIterator {
	return &shadowWALSegmentIterator{
		db:         db,
		generation: generation,
		indexes:    indexes,
	}
}

func (itr *shadowWALSegmentIterator) Close() (err error) {
	return itr.err
}

func (itr *shadowWALSegmentIterator) Next() bool {
	// Exit if an error has already occurred.
	if itr.err != nil {
		return false
	}

	for {
		// Move to the next segment in cache, if available.
		if len(itr.infos) > 1 {
			itr.infos = itr.infos[1:]
			return true
		}
		itr.infos = itr.infos[:0] // otherwise clear infos

		// If no indexes remain, stop iteration.
		if len(itr.indexes) == 0 {
			return false
		}

		// Read segments into a cache for the current index.
		index := itr.indexes[0]
		itr.indexes = itr.indexes[1:]
		f, err := os.Open(filepath.Join(itr.db.ShadowWALDir(itr.generation), FormatIndex(index)))
		if err != nil {
			itr.err = err
			return false
		}
		defer func() { _ = f.Close() }()

		fis, err := f.Readdir(-1)
		if err != nil {
			itr.err = err
			return false
		} else if err := f.Close(); err != nil {
			itr.err = err
			return false
		}
		for _, fi := range fis {
			filename := filepath.Base(fi.Name())
			if fi.IsDir() {
				continue
			}

			offset, err := ParseOffset(strings.TrimSuffix(filename, ".wal.lz4"))
			if err != nil {
				continue
			}

			itr.infos = append(itr.infos, WALSegmentInfo{
				Generation: itr.generation,
				Index:      index,
				Offset:     offset,
				Size:       fi.Size(),
				CreatedAt:  fi.ModTime().UTC(),
			})
		}

		// Ensure segments are sorted within index.
		sort.Sort(WALSegmentInfoSlice(itr.infos))

		if len(itr.infos) > 0 {
			return true
		}
	}
}

func (itr *shadowWALSegmentIterator) Err() error { return itr.err }

func (itr *shadowWALSegmentIterator) WALSegment() WALSegmentInfo {
	if len(itr.infos) == 0 {
		return WALSegmentInfo{}
	}
	return itr.infos[0]
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

// SQLite WAL constants
const (
	WALHeaderChecksumOffset      = 24
	WALFrameHeaderChecksumOffset = 16
)

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

// Checkpoint performs a checkpoint on the WAL file.
func (db *DB) Checkpoint(ctx context.Context, mode string) (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	generation, err := db.CurrentGeneration()
	if err != nil {
		return fmt.Errorf("cannot determine generation: %w", err)
	}
	return db.checkpoint(ctx, generation, mode)
}

// checkpointAndInit performs a checkpoint on the WAL file and initializes a
// new shadow WAL file.
func (db *DB) checkpoint(ctx context.Context, generation, mode string) error {
	// Read WAL header before checkpoint to check if it has been restarted.
	hdr, err := readWALHeader(db.WALPath())
	if err != nil {
		return err
	}

	// Copy shadow WAL before checkpoint to copy as much as possible.
	if err := db.copyToShadowWAL(ctx); err != nil {
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
	if err := db.copyToShadowWAL(ctx); err != nil {
		return fmt.Errorf("cannot copy to end of shadow wal: %w", err)
	}

	// Start a new shadow WAL file with next index.
	pos := Pos{Generation: db.pos.Generation, Index: db.pos.Index + 1}
	if err := db.initShadowWALIndex(ctx, pos); err != nil {
		return fmt.Errorf("cannot init shadow wal file: pos=%s err=%w", pos, err)
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
	db.Logger.Printf("checkpoint(%s): [%d,%d,%d]", mode, row[0], row[1], row[2])

	// Reacquire the read lock immediately after the checkpoint.
	if err := db.acquireReadLock(); err != nil {
		return fmt.Errorf("reacquire read lock: %w", err)
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
			db.Logger.Printf("sync error: %s", err)
		}
	}
}

// CalcRestoreTarget returns a replica & generation to restore from based on opt criteria.
func (db *DB) CalcRestoreTarget(ctx context.Context, opt RestoreOptions) (*Replica, string, error) {
	var target struct {
		replica    *Replica
		generation string
		updatedAt  time.Time
	}

	for _, r := range db.Replicas {
		// Skip replica if it does not match filter.
		if opt.ReplicaName != "" && r.Name() != opt.ReplicaName {
			continue
		}

		generation, updatedAt, err := r.CalcRestoreTarget(ctx, opt)
		if err != nil {
			return nil, "", err
		}

		// Use the latest replica if we have multiple candidates.
		if !updatedAt.After(target.updatedAt) {
			continue
		}

		target.replica, target.generation, target.updatedAt = r, generation, updatedAt
	}
	return target.replica, target.generation, nil
}

// applyWAL performs a truncating checkpoint on the given database.
func applyWAL(ctx context.Context, index int, dbPath string) error {
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

	generation, err := db.CurrentGeneration()
	if err != nil {
		return 0, Pos{}, fmt.Errorf("cannot find current generation: %w", err)
	} else if generation == "" {
		return 0, Pos{}, fmt.Errorf("no current generation")
	}

	// Force a RESTART checkpoint to ensure the database is at the start of the WAL.
	if err := db.checkpoint(ctx, generation, CheckpointModeRestart); err != nil {
		return 0, Pos{}, err
	}

	// Obtain current position. Clear the offset since we are only reading the
	// DB and not applying the current WAL.
	pos := db.pos
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

// parseWALPath returns the index for the WAL file.
// Returns an error if the path is not a valid WAL path.
func parseWALPath(s string) (index int, err error) {
	s = filepath.Base(s)

	a := walPathRegex.FindStringSubmatch(s)
	if a == nil {
		return 0, fmt.Errorf("invalid wal path: %s", s)
	}

	i64, _ := strconv.ParseUint(a[1], 16, 64)
	return int(i64), nil
}

// formatWALPath formats a WAL filename with a given index.
func formatWALPath(index int) string {
	assert(index >= 0, "wal index must be non-negative")
	return FormatIndex(index) + ".wal"
}

var walPathRegex = regexp.MustCompile(`^([0-9a-f]{8})\.wal$`)

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

	// Specific generation to restore from.
	// If blank, all generations considered.
	Generation string

	// Specific index to restore from.
	// Set to math.MaxInt32 to ignore index.
	Index int

	// Point-in-time to restore database.
	// If zero, database restore to most recent state available.
	Timestamp time.Time

	// Specifies how many WAL files are downloaded in parallel during restore.
	Parallelism int

	// Logging settings.
	Logger  *log.Logger
	Verbose bool
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
