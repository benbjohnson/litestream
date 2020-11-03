package litestream

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	MetaDirSuffix = "-litestream"
	ConfigSuffix  = ".litestream"

	WALDirName  = "wal"
	LogFilename = "log"
)

// DB represents an instance of a managed SQLite database in the file system.
type DB struct {
	path string
	inTx bool // currently in transaction

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
	// Ensure meta directory exists.
	if err := os.MkdirAll(db.MetaPath(), 0600); err != nil {
		return err
	}

	return nil
}

// Close stops management of the database.
func (db *DB) Close() error {
	db.cancel()
	db.wg.Wait()
	return nil
}

// IsMetaDir returns true if base in path is hidden and ends in "-litestream".
func IsMetaDir(path string) bool {
	base := filepath.Base(path)
	return strings.HasPrefix(base, ".") && strings.HasSuffix(base, MetaDirSuffix)
}

func IsConfigPath(path string) bool {
	return strings.HasSuffix(path, ConfigSuffix)
}

// ConfigPathToDBPath returns the path to the database based on a config path.
func ConfigPathToDBPath(path string) string {
	return strings.TrimSuffix(path, ConfigSuffix)
}
