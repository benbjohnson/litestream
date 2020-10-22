package litestream

import (
	"context"
	"path/filepath"
	"sync"
)

const (
	MetaDirSuffix = "-litestream"

	WALDirName  = "wal"
	LogFilename = "log"
)

// Mode represents the journaling mode of a DB.
type Mode int

const (
	ModeEmpty = Mode(iota + 1)
	ModeJournal
	ModeWAL
)

// DB represents an instance of a managed SQLite database in the file system.
type DB struct {
	path string
	mode Mode // method of writing to DB
	inTx bool // currently in transaction

	walFile *WALFile // active wal segment

	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup
}

// NewDB returns a new instance of DB for a given path.
func NewDB(path string) *DB {
	db := &DB{path: path}
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

// WALPath returns the path to the internal WAL directory.
func (db *DB) WALPath() string {
	return filepath.Join(db.MetaPath(), WALDirName)
}

// LogPath returns the path to the internal log directory.
func (db *DB) LogPath() string {
	return filepath.Join(db.MetaPath(), LogFilename)
}

// Open loads the configuration file
func (db *DB) Open() error {
	// TODO: Ensure sidecar directory structure exists.
	// TODO: Read WAL segments.
	return nil
}

// Close stops management of the database.
func (db *DB) Close() error {
	db.cancel()
	db.wg.Wait()
	// TODO: Close WAL segments.
	return nil
}

// ActiveWALFile returns the active WAL file.
func (db *DB) ActiveWALFile() *WALFile {
	return db.walFile
}
