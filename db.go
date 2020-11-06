package litestream

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/benbjohnson/litestream/sqlite"
)

const (
	MetaDirSuffix = "-litestream"
	ConfigSuffix  = ".litestream"

	WALDirName  = "wal"
	LogFilename = "log"
)

// DB represents an instance of a managed SQLite database in the file system.
type DB struct {
	mu   sync.Mutex
	path string

	isHeaderValid bool // true if meta page contains SQLITE3 header
	isWALEnabled  bool // true if file format version specifies WAL

	// Tracks offset of WAL data.
	processedWALByteN int64 // bytes copied to shadow WAL
	pendingWALByteN   int64 // bytes pending copy to shadow WAL

	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	// Database-specific logger
	logFile *os.File
	logger  *log.Logger
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

// InternalMetaPath returns the path to the database metadata.
func (db *DB) InternalMetaPath() string {
	dir, file := filepath.Split(db.path)
	return filepath.Join(db.node.fs.TargetPath, dir, "."+file+MetaDirSuffix)
}

// InternalWALPath returns the path to the internal WAL directory.
func (db *DB) InternalWALPath() string {
	return filepath.Join(db.MetaPath(), WALDirName)
}

// InternalLogPath returns the path to the internal log directory.
func (db *DB) InternalLogPath() string {
	return filepath.Join(db.MetaPath(), LogFilename)
}

// Open loads the configuration file
func (db *DB) Open() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Ensure meta directory structure exists.
	if err := os.MkdirAll(db.MetaPath(), 0600); err != nil {
		return err
	} else if err := os.MkdirAll(db.WALPath(), 0600); err != nil {
		return err
	}

	// Initialize per-db logger.
	if db.logFile, err = os.OpenFile(db.LogPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600); err != nil {
		return err
	}
	db.logger = log.New(db.logFile, "", log.LstdFlags)

	// If database file exists, read & set the header.
	if err := db.readHeader(); err != nil {
		db.setHeader(nil) // invalidate header
		db.logger.Printf("cannot read db header: %s", err)
	}

	return nil
}

// Close stops management of the database.
func (db *DB) Close() (err error) {
	db.cancel()
	db.wg.Wait()

	// Close per-db log file.
	if e := db.logFile.Close(); e != nil && err == nil {
		err = e
	}
	return err
}

// readHeader reads the SQLite header and sets the initial DB flags.
func (db *DB) readHeader() error {
	f, err := os.Open(db.path)
	if err != nil {
		return err
	}
	defer f.Close()

	hdr := make([]byte, sqlite.HeaderSize)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return err
	}

	db.setHeader(hdr)
	return nil
}

// Valid returns true if there is a valid, WAL-enabled SQLite database on-disk.
func (db *DB) Valid() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.valid()
}

func (db *DB) valid() bool {
	return db.isHeaderValid && db.isWALEnabled
}

// SetHeader checks if the page has a valid header & uses a WAL.
func (db *DB) SetHeader(page []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.setHeader(page)
}

func (db *DB) setHeader(page []byte) {
	db.isHeaderValid = sqlite.IsValidHeader(page)
	db.isWALEnabled = sqlite.IsWALEnabled(page)
}

func (db *DB) AddPendingWALByteN(n int64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.pendingWALByteN += n
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
