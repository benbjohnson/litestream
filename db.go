package litestream

import (
	"context"
	"database/sql"
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

var ErrNoGeneration = errors.New("litestream: no generation")

const (
	MetaDirSuffix = "-litestream"

	WALDirName = "wal"
	WALExt     = ".wal"

	GenerationNameLen = 16
)

const (
	DefaultMonitorInterval = 1 * time.Second
)

// DB represents a managed instance of a SQLite database in the file system.
type DB struct {
	mu   sync.Mutex
	path string  // part to database
	db   *sql.DB // target database
	rtx  *sql.Tx // long running read transaction

	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	// List of replicators for the database.
	// Must be set before calling Open().
	Replicators []Replicator

	// Frequency at which to perform db sync.
	MonitorInterval time.Duration
}

// NewDB returns a new instance of DB for a given path.
func NewDB(path string) *DB {
	db := &DB{
		path:            path,
		MonitorInterval: DefaultMonitorInterval,
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
	return filepath.Join(db.GenerationPath(generation), "wal", fmt.Sprintf("%016x", index)+".wal")
}

// CurrentShadowWALPath returns the path to the last shadow WAL in a generation.
func (db *DB) CurrentShadowWALPath(generation string) (string, error) {
	// TODO: Cache current shadow WAL path.
	dir := filepath.Join(db.GenerationPath(generation), "wal")
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", err
	}

	// Find highest wal file.
	var max string
	for _, fi := range fis {
		if !strings.HasSuffix(fi.Name(), WALExt) {
			continue
		}
		if max == "" || fi.Name() > max {
			max = fi.Name()
		}
	}

	// Return error if we found no WAL files.
	if max == "" {
		return "", fmt.Errorf("no wal files found in %q", dir)
	}
	return filepath.Join(dir, max), nil
}

func (db *DB) Open() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Connect to SQLite database & enable WAL.
	if db.db, err = sql.Open("sqlite3", db.path); err != nil {
		return err
	} else if _, err := db.db.Exec(`PRAGMA journal_mode = wal;`); err != nil {
		return fmt.Errorf("enable wal: %w", err)
	}

	// Create a lock table to force write locks during sync.
	if _, err := db.db.Exec(`CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);`); err != nil {
		return fmt.Errorf("enable wal: %w", err)
	}

	// Start a long-running read transaction to prevent other transactions
	// from checkpointing.
	if err := db.acquireReadLock(); err != nil {
		return fmt.Errorf("acquire read lock: %w", err)
	}

	// Ensure meta directory structure exists.
	if err := os.MkdirAll(db.MetaPath(), 0700); err != nil {
		return err
	}

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

// SoftClose closes everything but the underlying db connection. This method
// is available because the binary needs to avoid closing the database on exit
// to prevent autocheckpointing.
func (db *DB) SoftClose() (err error) {
	db.cancel()
	db.wg.Wait()

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

	// Disable autocheckpointing on this connection.
	if _, err := tx.ExecContext(db.ctx, `PRAGMA wal_autocheckpoint = 0;`); err != nil {
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
// file in the meta data directory. Returns ErrNoGeneration if none exists.
func (db *DB) CurrentGeneration() (string, error) {
	buf, err := ioutil.ReadFile(db.GenerationNamePath())
	if os.IsNotExist(err) {
		return "", ErrNoGeneration
	} else if err != nil {
		return "", err
	}

	// TODO: Verify if generation directory exists. If not, delete.

	generation := strings.TrimSpace(string(buf))
	if len(generation) != GenerationNameLen {
		return "", ErrNoGeneration
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

	// Copy to shadow WAL.
	if err := db.copyInitialWAL(generation); err != nil {
		return "", fmt.Errorf("copy initial wal: %w", err)
	}

	// Atomically write generation name as current generation.
	generationNamePath := db.GenerationNamePath()
	if err := ioutil.WriteFile(generationNamePath+".tmp", []byte(generation+"\n"), 0600); err != nil {
		return "", fmt.Errorf("write generation temp file: %w", err)
	} else if err := os.Rename(generationNamePath+".tmp", generationNamePath); err != nil {
		return "", fmt.Errorf("rename generation file: %w", err)
	}

	// Issue snapshot by each replicator.
	for _, r := range db.Replicators {
		if err := r.BeginSnapshot(db.ctx); err != nil {
			return "", fmt.Errorf("cannot snapshot %q replicator: %s", r.Name(), err)
		}
	}

	return generation, nil
}

// copyInitialWAL copies the full WAL file to the initial shadow WAL path.
func (db *DB) copyInitialWAL(generation string) error {
	shadowWALPath := db.ShadowWALPath(generation, 0)
	if err := os.MkdirAll(filepath.Dir(shadowWALPath), 0700); err != nil {
		return err
	}

	// Open the initial shadow WAL file for writing.
	w, err := os.OpenFile(shadowWALPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer w.Close()

	// Open the database's WAL file for reading.
	r, err := os.Open(db.WALPath())
	if err != nil {
		return err
	}
	defer r.Close()

	// Copy & sync.
	if _, err := io.Copy(w, r); err != nil {
		return err
	} else if err := w.Sync(); err != nil {
		return err
	} else if err := w.Close(); err != nil {
		return err
	}
	return nil
}

// Sync copies pending data from the WAL to the shadow WAL.
func (db *DB) Sync() (err error) {
	// TODO: Lock DB while syncing?

	// Start a transaction. This will be promoted immediately after.
	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}

	// Ensure write transaction rolls back before returning.
	defer func() {
		if e := tx.Rollback(); e != nil && err == nil {
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

	// Disable the autocheckpoint.
	if _, err := tx.ExecContext(db.ctx, `PRAGMA wal_autocheckpoint = 0;`); err != nil {
		return fmt.Errorf("disable autocheckpoint: %w", err)
	}

	// Look up existing generation or start a new one.
	generation, err := db.CurrentGeneration()
	if err == ErrNoGeneration {
		if generation, err = db.createGeneration(); err != nil {
			return fmt.Errorf("create generation: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("cannot find current generation: %w", err)
	}

	// Synchronize real WAL with current shadow WAL.
	if err := db.syncWAL(generation); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}

	// TODO: If WAL size is greater than min threshold, attempt checkpoint: https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
	// TODO: If WAL size is great than max threshold, force checkpoint.
	// TODO: Release write lock on database.

	// TODO: On checkpoint, write new page and start new shadow WAL.

	return nil
}

// syncWAL copies pending bytes from the real WAL to the shadow WAL.
func (db *DB) syncWAL(generation string) error {
	// Determine total bytes of real WAL.
	fi, err := os.Stat(db.WALPath())
	if err != nil {
		return err
	}
	walSize := fi.Size()

	// Open shadow WAL to copy append to.
	shadowWALPath, err := db.CurrentShadowWALPath(generation)
	if err != nil {
		return fmt.Errorf("cannot determine shadow WAL: %w", err)
	}

	// TODO: Compare WAL headers.

	// Determine shadow WAL current size.
	fi, err = os.Stat(shadowWALPath)
	if err != nil {
		return err
	}
	shadowWALSize := fi.Size()

	// Ensure we have pending bytes to write.
	// TODO: Verify pending bytes is divisble by (pageSize+headerSize)?
	pendingN := walSize - shadowWALSize
	if pendingN < 0 {
		panic("shadow wal larger than real wal") // TODO: Handle gracefully
	} else if pendingN == 0 {
		return nil // wals match, exit
	}

	// TODO: Verify last page copied matches.

	// Open handles for the shadow WAL & real WAL.
	w, err := os.OpenFile(shadowWALPath, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer w.Close()

	r, err := os.Open(db.WALPath())
	if err != nil {
		return err
	}
	defer r.Close()

	// Seek to the correct position for each file.
	if _, err := r.Seek(shadowWALSize, io.SeekStart); err != nil {
		return fmt.Errorf("wal seek: %w", err)
	} else if _, err := w.Seek(shadowWALSize, io.SeekStart); err != nil {
		return fmt.Errorf("shadow wal seek: %w", err)
	}

	// Copy and sync.
	if _, err := io.CopyN(w, r, pendingN); err != nil {
		return fmt.Errorf("copy shadow wal error: %w", err)
	} else if err := w.Sync(); err != nil {
		return fmt.Errorf("shadow wal sync: %w", err)
	} else if err := w.Close(); err != nil {
		return fmt.Errorf("shadow wal close: %w", err)
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
		case <-ticker.C:
		}

		// Sync the database to the shadow WAL.
		if err := db.Sync(); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("sync error: path=%s err=%s", db.path, err)
		}

		// If context closed, exit after final sync.
		if db.ctx.Err() != nil {
			return
		}
	}
}
