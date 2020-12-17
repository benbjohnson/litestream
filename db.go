package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	MetaDirSuffix = "-litestream"

	WALDirName = "wal"
	WALExt     = ".wal"
)

const (
	DefaultMonitorInterval = 1 * time.Second
)

// DB represents a managed instance of a SQLite database in the file system.
type DB struct {
	mu   sync.Mutex
	path string
	db   *sql.DB

	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

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

// MetaPath returns the path to the database metadata.
func (db *DB) MetaPath() string {
	dir, file := filepath.Split(db.path)
	return filepath.Join(dir, "."+file+MetaDirSuffix)
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

	// Ensure meta directory structure exists.
	if err := os.MkdirAll(db.MetaPath(), 0700); err != nil {
		return err
	}

	db.wg.Add(1)
	go func() { defer db.wg.Done(); db.monitor() }()

	return nil
}

// Close disconnects from the database.
func (db *DB) Close() (err error) {
	db.cancel()
	db.wg.Wait()

	if db.db != nil {
		err = db.db.Close()
	}
	return err
}

// Sync copies pending data from the WAL to the shadow WAL.
func (db *DB) Sync() error {
	// TODO: Obtain write lock on database.
	// TODO: Start new generation if no generations exist.
	// TODO: Fetch latest generation.
	// TODO: Compare header on shadow WAL with real WAL. On mismatch, start new generation.
	// TODO: Copy pending data from real WAL to shadow WAL.

	// TODO: If WAL size is greater than min threshold, attempt checkpoint: https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
	// TODO: If WAL size is great than max threshold, force checkpoint.
	// TODO: Release write lock on database.

	// TODO: On checkpoint, write new page and start new shadow WAL.
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
		if err := db.Sync(); err != nil {
			log.Printf("sync error: path=%s err=%s", db.path, err)
		}

		// If context closed, exit after final sync.
		if db.ctx.Err() != nil {
			return
		}
	}
}
