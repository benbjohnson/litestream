package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Default DB settings.
const (
	DefaultMonitorInterval    = 1 * time.Second
	DefaultMinCheckpointPageN = 1000
)

// DB represents a managed instance of a SQLite database in the file system.
type DB struct {
	mu       sync.RWMutex
	path     string  // part to database
	db       *sql.DB // target database
	rtx      *sql.Tx // long running read transaction
	pageSize int     // page size, in bytes

	notify chan struct{} // closes on WAL change

	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

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

	// List of replicators for the database.
	// Must be set before calling Open().
	Replicators []Replicator

	// Frequency at which to perform db sync.
	MonitorInterval time.Duration
}

// NewDB returns a new instance of DB for a given path.
func NewDB(path string) *DB {
	db := &DB{
		path:   path,
		notify: make(chan struct{}),

		MinCheckpointPageN: DefaultMinCheckpointPageN,
		MonitorInterval:    DefaultMonitorInterval,
	}
	db.ctx, db.cancel = context.WithCancel(context.Background())
	return db
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
	return filepath.Join(dir, "."+file+MetaDirSuffix)
}

// GenerationNamePath returns the path of the name of the current generation.
func (db *DB) GenerationNamePath() string {
	return filepath.Join(db.MetaPath(), "generation")
}

// GenerationPath returns the path of a single generation.
func (db *DB) GenerationPath(generation string) string {
	return filepath.Join(db.MetaPath(), "generations", generation)
}

// ShadowWALPath returns the path of a single shadow WAL file.
func (db *DB) ShadowWALPath(generation string, index int) string {
	assert(index >= 0, "shadow wal index cannot be negative")
	return filepath.Join(db.GenerationPath(generation), "wal", fmt.Sprintf("%016x", index)+WALExt)
}

// CurrentShadowWALPath returns the path to the last shadow WAL in a generation.
func (db *DB) CurrentShadowWALPath(generation string) (string, error) {
	index, err := db.CurrentShadowWALIndex(generation)
	if err != nil {
		return "", err
	}
	return db.ShadowWALPath(generation, index), nil
}

// CurrentShadowWALIndex returns the current WAL index for a given generation.
func (db *DB) CurrentShadowWALIndex(generation string) (int, error) {
	fis, err := ioutil.ReadDir(filepath.Join(db.GenerationPath(generation), "wal"))
	if os.IsNotExist(err) {
		return 0, nil // no wal files written for generation
	} else if err != nil {
		return 0, err
	}

	// Find highest wal index.
	var index int
	for _, fi := range fis {
		if !strings.HasSuffix(fi.Name(), WALExt) {
			continue
		}
		if v, err := ParseWALFilename(filepath.Base(fi.Name())); err != nil {
			continue // invalid wal filename
		} else if v > index {
			index = v
		}
	}
	return index, nil
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

func (db *DB) Open() (err error) {
	// Start monitoring SQLite database in a separate goroutine.
	db.wg.Add(1)
	go func() { defer db.wg.Done(); db.monitor() }()

	return nil
}

// Close releases the read lock & closes the database. This method should only
// be called by tests as it causes the underlying database to be checkpointed.
func (db *DB) Close() (err error) {
	if e := db.SoftClose(); e != nil && err == nil {
		err = e
	}

	if db.db != nil {
		if e := db.db.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// Init initializes the connection to the database.
// Skipped if already initialized or if the database file does not exist.
func (db *DB) Init() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Exit if already initialized.
	if db.db != nil {
		return nil
	}

	// Exit if no database file exists.
	if _, err := os.Stat(db.path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	// Connect to SQLite database & enable WAL.
	if db.db, err = sql.Open("sqlite3", db.path); err != nil {
		return err
	} else if _, err := db.db.Exec(`PRAGMA journal_mode = wal;`); err != nil {
		return fmt.Errorf("enable wal: %w", err)
	}

	// Disable autocheckpoint.
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
	if err := os.MkdirAll(db.MetaPath(), 0700); err != nil {
		return err
	}

	// Clean up previous generations.
	if err := db.clean(); err != nil {
		return fmt.Errorf("clean: %w", err)
	}

	// Start replication.
	for _, r := range db.Replicators {
		r.Start(db.ctx)
	}

	return nil
}

// clean removes old generations.
func (db *DB) clean() error {
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

// SoftClose closes everything but the underlying db connection. This method
// is available because the binary needs to avoid closing the database on exit
// to prevent autocheckpointing.
func (db *DB) SoftClose() (err error) {
	db.cancel()
	db.wg.Wait()

	// Ensure replicators all stop replicating.
	for _, r := range db.Replicators {
		r.Stop()
	}

	if db.rtx != nil {
		if e := db.releaseReadLock(); e != nil && err == nil {
			err = e
		}
	}
	return err
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
		tx.Rollback()
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
	buf, err := ioutil.ReadFile(db.GenerationNamePath())
	if os.IsNotExist(err) {
		return "", nil
	} else if err != nil {
		return "", err
	}

	// TODO: Verify if generation directory exists. If not, delete.

	generation := strings.TrimSpace(string(buf))
	if len(generation) != GenerationNameLen {
		return "", nil
	}
	return generation, nil
}

// createGeneration starts a new generation by creating the generation
// directory, snapshotting to each replicator, and updating the current
// generation name.
func (db *DB) createGeneration() (string, error) {
	// Generate random generation hex name.
	buf := make([]byte, GenerationNameLen/2)
	_, _ = rand.New(rand.NewSource(time.Now().UnixNano())).Read(buf)
	generation := hex.EncodeToString(buf)

	// Generate new directory.
	dir := filepath.Join(db.MetaPath(), "generations", generation)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}

	// Initialize shadow WAL with copy of header.
	if err := db.initShadowWALFile(db.ShadowWALPath(generation, 0)); err != nil {
		return "", fmt.Errorf("initialize shadow wal: %w", err)
	}

	// Atomically write generation name as current generation.
	generationNamePath := db.GenerationNamePath()
	if err := ioutil.WriteFile(generationNamePath+".tmp", []byte(generation+"\n"), 0600); err != nil {
		return "", fmt.Errorf("write generation temp file: %w", err)
	} else if err := os.Rename(generationNamePath+".tmp", generationNamePath); err != nil {
		return "", fmt.Errorf("rename generation file: %w", err)
	}

	// Remove old generations.
	if err := db.clean(); err != nil {
		return "", err
	}

	return generation, nil
}

// Sync copies pending data from the WAL to the shadow WAL.
func (db *DB) Sync() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// No database exists, exit.
	if db.db == nil {
		return nil
	}

	// TODO: Force "-wal" file if it doesn't exist.

	// Ensure WAL has at least one frame in it.
	if err := db.ensureWALExists(); err != nil {
		return fmt.Errorf("ensure wal exists: %w", err)
	}

	// Start a transaction. This will be promoted immediately after.
	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}

	// Ensure write transaction rolls back before returning.
	defer func() {
		if e := rollback(tx); e != nil && err == nil {
			err = e
		}
	}()

	// Insert into the lock table to promote to a write tx. The lock table
	// insert will never actually occur because our tx will be rolled back,
	// however, it will ensure our tx grabs the write lock. Unfortunately,
	// we can't call "BEGIN IMMEDIATE" as we are already in a transaction.
	if _, err := tx.ExecContext(db.ctx, `INSERT INTO _litestream_lock (id) VALUES (1);`); err != nil {
		return fmt.Errorf("_litestream_lock: %w", err)
	}

	// Verify our last sync matches the current state of the WAL.
	// This ensures that we have an existing generation & that the last sync
	// position of the real WAL hasn't been overwritten by another process.
	info, err := db.verify()
	if err != nil {
		return fmt.Errorf("cannot verify wal state: %w", err)
	}

	// Track if anything in the shadow WAL changes and then notify at the end.
	changed := info.walSize != info.shadowWALSize || info.restart || info.reason != ""

	// If we are unable to verify the WAL state then we start a new generation.
	if info.reason != "" {
		// Start new generation & notify user via log message.
		if info.generation, err = db.createGeneration(); err != nil {
			return fmt.Errorf("create generation: %w", err)
		}
		log.Printf("%s: new generation %q, %s", db.path, info.generation, info.reason)

		// Clear shadow wal info.
		info.shadowWALPath = db.ShadowWALPath(info.generation, 0)
		info.shadowWALSize = WALHeaderSize
		info.restart = false
		info.reason = ""

	}

	// Synchronize real WAL with current shadow WAL.
	newWALSize, err := db.syncWAL(info)
	if err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}

	// If WAL size is great than max threshold, force checkpoint.
	// If WAL size is greater than min threshold, attempt checkpoint.
	var checkpoint, forceCheckpoint bool
	if newWALSize >= calcWALSize(db.pageSize, db.MinCheckpointPageN) {
		checkpoint, forceCheckpoint = true, false
	} else if db.MaxCheckpointPageN > 0 && newWALSize >= calcWALSize(db.pageSize, db.MaxCheckpointPageN) {
		checkpoint, forceCheckpoint = true, true
	}

	// Release write lock before checkpointing & exiting.
	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("rollback write tx: %w", err)
	}

	// Issue the checkpoint.
	if checkpoint {
		changed = true

		if err := db.checkpoint(info, forceCheckpoint); err != nil {
			return fmt.Errorf("checkpoint: force=%v err=%w", err)
		}
	}

	// Notify replicators of WAL changes.
	if changed {
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

	// Determine total bytes of real WAL.
	fi, err := os.Stat(db.WALPath())
	if err != nil {
		return info, err
	}
	info.walSize = fi.Size()

	// Open shadow WAL to copy append to.
	info.shadowWALPath, err = db.CurrentShadowWALPath(info.generation)
	if info.shadowWALPath == "" {
		info.reason = "no shadow wal"
		return info, nil
	} else if err != nil {
		return info, fmt.Errorf("cannot determine shadow WAL: %w", err)
	}

	// Determine shadow WAL current size.
	fi, err = os.Stat(info.shadowWALPath)
	if err != nil {
		return info, err
	}
	info.shadowWALSize = frameAlign(fi.Size(), db.pageSize)

	// Truncate shadow WAL if there is a partial page.
	// Exit if shadow WAL does not contain a full header.
	if info.shadowWALSize < WALHeaderSize {
		return info, fmt.Errorf("short shadow wal: %s", info.shadowWALPath)
	}

	// If shadow WAL is larger than real WAL then the WAL has been truncated
	// so we cannot determine our last state.
	if info.shadowWALSize > info.walSize {
		info.reason = "wal truncated by another process"
		return info, nil
	}

	// Compare WAL headers. Start a new shadow WAL if they are mismatched.
	if hdr0, err := readWALHeader(db.WALPath()); err != nil {
		return info, fmt.Errorf("cannot read wal header: %w", err)
	} else if hdr1, err := readWALHeader(info.shadowWALPath); err != nil {
		return info, fmt.Errorf("cannot read shadow wal header: %w", err)
	} else if !bytes.Equal(hdr0, hdr1) {
		info.restart = !bytes.Equal(hdr0, hdr1)
	}

	// If we only have a header then ensure header matches.
	// Otherwise we need to start a new generation.
	if info.shadowWALSize == WALHeaderSize && info.restart {
		info.reason = "wal header only, mismatched"
		return info, nil
	}

	// Verify last page synced still matches.
	if info.shadowWALSize > WALHeaderSize {
		offset := info.shadowWALSize - int64(db.pageSize+WALFrameHeaderSize)
		if buf0, err := readFileAt(db.WALPath(), offset, int64(db.pageSize+WALFrameHeaderSize)); err != nil {
			return info, fmt.Errorf("cannot read last synced wal page: %w", err)
		} else if buf1, err := readFileAt(info.shadowWALPath, offset, int64(db.pageSize+WALFrameHeaderSize)); err != nil {
			return info, fmt.Errorf("cannot read last synced shadow wal page: %w", err)
		} else if !bytes.Equal(buf0, buf1) {
			info.reason = "wal overwritten by another process"
			return info, nil
		}
	}

	return info, nil
}

type syncInfo struct {
	generation    string // generation name
	walSize       int64  // size of real WAL file
	shadowWALPath string // name of last shadow WAL file
	shadowWALSize int64  // size of last shadow WAL file
	restart       bool   // if true, real WAL header does not match shadow WAL
	reason        string // if non-blank, reason for sync failure
}

// syncWAL copies pending bytes from the real WAL to the shadow WAL.
func (db *DB) syncWAL(info syncInfo) (newSize int64, err error) {
	// Copy WAL starting from end of shadow WAL. Exit if no new shadow WAL needed.
	newSize, err = db.copyToShadowWAL(info.shadowWALPath)
	if err != nil {
		return newSize, fmt.Errorf("cannot copy to shadow wal: %w", err)
	} else if !info.restart {
		return newSize, nil // If no restart required, exit.
	}

	// Parse index of current shadow WAL file.
	dir, base := filepath.Split(info.shadowWALPath)
	index, err := ParseWALFilename(base)
	if err != nil {
		return 0, fmt.Errorf("cannot parse shadow wal filename: %s", base)
	}

	// Start a new shadow WAL file with next index.
	newShadowWALPath := filepath.Join(dir, FormatWALFilename(index+1))
	if err := db.initShadowWALFile(newShadowWALPath); err != nil {
		return 0, fmt.Errorf("cannot init shadow wal file: name=%s err=%w", newShadowWALPath, err)
	}

	// Copy rest of valid WAL to new shadow WAL.
	newSize, err = db.copyToShadowWAL(newShadowWALPath)
	if err != nil {
		return 0, fmt.Errorf("cannot copy to new shadow wal: %w", err)
	}
	return newSize, nil
}

func (db *DB) initShadowWALFile(filename string) error {
	hdr, err := readWALHeader(db.WALPath())
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	// Determine byte order for checksumming from header magic.
	bo, err := headerByteOrder(hdr)
	if err != nil {
		return err
	}

	// Verify checksum.
	s0 := binary.BigEndian.Uint32(hdr[24:])
	s1 := binary.BigEndian.Uint32(hdr[28:])
	if v0, v1 := Checksum(bo, 0, 0, hdr[:24]); v0 != s0 || v1 != s1 {
		return fmt.Errorf("invalid header checksum: (%x,%x) != (%x,%x)", v0, v1, s0, s1)
	}

	// Write header to new WAL shadow file.
	if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		return err
	}
	return ioutil.WriteFile(filename, hdr, 0600)
}

func (db *DB) copyToShadowWAL(filename string) (newSize int64, err error) {
	r, err := os.Open(db.WALPath())
	if err != nil {
		return 0, err
	}
	defer r.Close()

	w, err := os.OpenFile(filename, os.O_RDWR, 0600)
	if err != nil {
		return 0, err
	}
	defer w.Close()

	fi, err := w.Stat()
	if err != nil {
		return 0, err
	}

	// Read shadow WAL header to determine byte order for checksum & salt.
	hdr := make([]byte, WALHeaderSize)
	if _, err := io.ReadFull(w, hdr); err != nil {
		return 0, fmt.Errorf("read header: %w", err)
	}
	hsalt0 := binary.BigEndian.Uint32(hdr[16:])
	hsalt1 := binary.BigEndian.Uint32(hdr[20:])

	bo, err := headerByteOrder(hdr)
	if err != nil {
		return 0, err
	}

	// Read previous checksum.
	chksum0, chksum1, err := readLastChecksumFrom(w, db.pageSize)
	if err != nil {
		return 0, fmt.Errorf("last checksum: %w", err)
	}

	// Seek to correct position on both files.
	if _, err := r.Seek(fi.Size(), io.SeekStart); err != nil {
		return 0, fmt.Errorf("wal seek: %w", err)
	} else if _, err := w.Seek(fi.Size(), io.SeekStart); err != nil {
		return 0, fmt.Errorf("shadow wal seek: %w", err)
	}

	// TODO: Optimize to use bufio on reader & writer to minimize syscalls.

	// Loop over each page, verify checksum, & copy to writer.
	newSize = fi.Size()
	buf := make([]byte, db.pageSize+WALFrameHeaderSize)
	for {
		// Read next page from WAL file.
		if _, err := io.ReadFull(r, buf); err == io.EOF || err == io.ErrUnexpectedEOF {
			break // end of file or partial page
		} else if err != nil {
			return newSize, fmt.Errorf("read wal: %w", err)
		}

		// Read frame salt & compare to header salt. Stop reading on mismatch.
		salt0 := binary.BigEndian.Uint32(buf[8:])
		salt1 := binary.BigEndian.Uint32(buf[12:])
		if salt0 != hsalt0 || salt1 != hsalt1 {
			break
		}

		// Verify checksum of page is valid.
		fchksum0 := binary.BigEndian.Uint32(buf[16:])
		fchksum1 := binary.BigEndian.Uint32(buf[20:])
		chksum0, chksum1 = Checksum(bo, chksum0, chksum1, buf[:8])  // frame header
		chksum0, chksum1 = Checksum(bo, chksum0, chksum1, buf[24:]) // frame data
		if chksum0 != fchksum0 || chksum1 != fchksum1 {
			return newSize, fmt.Errorf("checksum mismatch: offset=%d (%x,%x) != (%x,%x)", newSize, chksum0, chksum1, fchksum0, fchksum1)
		}

		// Write frame to shadow WAL.
		if _, err := w.Write(buf); err != nil {
			return newSize, err
		}

		// Add page to the new size of the shadow WAL.
		newSize += int64(len(buf))
	}

	// Sync & close writer.
	if err := w.Sync(); err != nil {
		return newSize, err
	} else if err := w.Close(); err != nil {
		return newSize, err
	}

	return newSize, nil
}

// WALReader opens a reader for a shadow WAL file at a given position.
// If the reader is at the end of the file, it attempts to return the next file.
//
// The caller should check Pos() & Size() on the returned reader to check offset.
func (db *DB) WALReader(pos Pos) (r *WALReader, err error) {
	// Fetch reader for the requested position. Return if it has data.
	r, err = db.walReader(pos)
	if err != nil {
		return nil, err
	} else if r.N() > 0 {
		return r, nil
	}

	// Otherwise attempt to read the start of the next WAL file.
	pos.Index, pos.Offset = pos.Index+1, 0

	r, err = db.walReader(pos)
	if os.IsNotExist(err) {
		return nil, io.EOF
	}
	return r, err
}

// walReader opens a file reader for a shadow WAL file at a given position.
func (db *DB) walReader(pos Pos) (r *WALReader, err error) {
	filename := db.ShadowWALPath(pos.Generation, pos.Index)

	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	// Ensure file is closed if any error occurs.
	defer func() {
		if err != nil {
			r.Close()
		}
	}()

	// Fetch frame-aligned file size and ensure requested offset is not past EOF.
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := frameAlign(fi.Size(), db.pageSize)
	if pos.Offset > fileSize {
		return nil, fmt.Errorf("wal reader offset too high: %d > %d", pos.Offset, fi.Size())
	}

	// Move file handle to offset position.
	if _, err := f.Seek(pos.Offset, io.SeekStart); err != nil {
		return nil, err
	}

	return &WALReader{
		f:   f,
		n:   fileSize - pos.Offset,
		pos: pos,
	}, nil
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

// WALReader represents a reader for a WAL file that tracks WAL position.
type WALReader struct {
	f   *os.File
	n   int64
	pos Pos
}

// Close closes the underlying WAL file handle.
func (r *WALReader) Close() error { return r.f.Close() }

// N returns the remaining bytes in the reader.
func (r *WALReader) N() int64 { return r.n }

// Pos returns the current WAL position.
func (r *WALReader) Pos() Pos { return r.pos }

// Read reads bytes into p, updates the position, and returns the bytes read.
// Returns io.EOF at the end of the available section of the WAL.
func (r *WALReader) Read(p []byte) (n int, err error) {
	if r.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.n {
		p = p[0:r.n]
	}
	n, err = r.f.Read(p)
	r.n -= int64(n)
	r.pos.Offset += int64(n)
	return n, err
}

const WALHeaderChecksumOffset = 24
const WALFrameHeaderChecksumOffset = 16

func readLastChecksumFrom(f *os.File, pageSize int) (uint32, uint32, error) {
	// Determine the byte offset of the checksum for the header (if no pages
	// exist) or for the last page (if at least one page exists).
	offset := int64(WALHeaderChecksumOffset)
	if fi, err := f.Stat(); err != nil {
		return 0, 0, err
	} else if fi.Size() > WALHeaderSize {
		offset = fi.Size() - int64(pageSize) - WALFrameHeaderSize + WALFrameHeaderChecksumOffset
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

// checkpoint performs a checkpoint on the WAL file.
func (db *DB) checkpoint(info syncInfo, force bool) error {
	// Read WAL header before checkpoint to check if it has been restarted.
	hdr, err := readWALHeader(db.WALPath())
	if err != nil {
		return err
	}

	// Ensure the read lock has been removed before issuing a checkpoint.
	// We defer the re-acquire to ensure it occurs even on an early return.
	if err := db.releaseReadLock(); err != nil {
		return fmt.Errorf("release read lock: %w", err)
	}
	defer db.acquireReadLock()

	// A non-forced checkpoint is issued as "PASSIVE". This will only checkpoint
	// if there are not pending transactions. A forced checkpoint ("RESTART")
	// will wait for pending transactions to end & block new transactions before
	// forcing the checkpoint and restarting the WAL.
	//
	// See: https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
	rawsql := `PRAGMA wal_checkpoint;`
	if force {
		rawsql = `PRAGMA wal_checkpoint(RESTART);`
	}

	var row [3]int
	if err := db.db.QueryRow(rawsql).Scan(&row[0], &row[1], &row[2]); err != nil {
		return err
	}
	log.Printf("%s: checkpoint: force=%v (%d,%d,%d)", db.path, force, row[0], row[1], row[2])

	// Reacquire the read lock immediately after the checkpoint.
	if err := db.acquireReadLock(); err != nil {
		return fmt.Errorf("release read lock: %w", err)
	}

	if _, err = db.db.Exec(`INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1`); err != nil {
		return err
	}

	// If WAL hasn't been restarted, exit.
	if other, err := readWALHeader(db.WALPath()); err != nil {
		return err
	} else if bytes.Equal(hdr, other) {
		return nil
	}

	// Copy the end of the previous WAL before starting a new shadow WAL.
	if _, err := db.copyToShadowWAL(info.shadowWALPath); err != nil {
		return fmt.Errorf("cannot copy to end of shadow wal: %w", err)
	}

	// Parse index of current shadow WAL file.
	dir, base := filepath.Split(info.shadowWALPath)
	index, err := ParseWALFilename(base)
	if err != nil {
		return fmt.Errorf("cannot parse shadow wal filename: %s", base)
	}

	// Start a new shadow WAL file with next index.
	newShadowWALPath := filepath.Join(dir, FormatWALFilename(index+1))
	if err := db.initShadowWALFile(newShadowWALPath); err != nil {
		return fmt.Errorf("cannot init shadow wal file: name=%s err=%w", newShadowWALPath, err)
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

		// Ensure the database is initialized.
		if err := db.Init(); err != nil {
			log.Printf("%s: init error: %s", db.path, err)
			continue
		}

		// Sync the database to the shadow WAL.
		if err := db.Sync(); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("%s: sync error: %s", db.path, err)
		}
	}
}

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
