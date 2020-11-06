package litestream

import (
	"context"
	"fmt"
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

	WALDirName    = "wal"
	WALExt        = ".wal"
	ActiveWALName = "active.wal"
	LogFilename   = "log"
)

// DB represents an instance of a managed SQLite database in the file system.
type DB struct {
	mu   sync.Mutex
	fs   *FileSystem
	path string

	isHeaderValid bool // true if meta page contains SQLITE3 header
	isWALEnabled  bool // true if file format version specifies WAL

	// Tracks WAL state.
	walHeader         *sqlite.WALHeader
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
func NewDB(fs *FileSystem, path string) *DB {
	db := &DB{fs: fs, path: path}
	db.ctx, db.cancel = context.WithCancel(context.Background())
	return db
}

// Path returns the path to the database.
func (db *DB) Path() string {
	return db.path
}

// WALPath returns the full path to the real WAL.
func (db *DB) WALPath() string {
	return filepath.Join(db.fs.TargetPath, db.path+"-wal")
}

// MetaPath returns the path to the database metadata.
func (db *DB) MetaPath() string {
	dir, file := filepath.Split(db.path)
	return filepath.Join(db.fs.TargetPath, dir, "."+file+MetaDirSuffix)
}

// ShadowWALDir returns the path to the internal WAL directory.
func (db *DB) ShadowWALDir() string {
	return filepath.Join(db.MetaPath(), WALDirName)
}

// ActiveShadowWALPath returns the path to the internal active WAL file.
func (db *DB) ActiveShadowWALPath() string {
	return filepath.Join(db.ShadowWALDir(), ActiveWALName)
}

// LogPath returns the path to the internal log directory.
func (db *DB) LogPath() string {
	return filepath.Join(db.MetaPath(), LogFilename)
}

// Open loads the configuration file
func (db *DB) Open() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Ensure meta directory structure exists.
	if err := os.MkdirAll(db.MetaPath(), 0700); err != nil {
		return err
	} else if err := os.MkdirAll(db.ShadowWALDir(), 0700); err != nil {
		return err
	}

	// Initialize per-db logger.
	if db.logFile, err = os.OpenFile(db.LogPath(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600); err != nil {
		return err
	}
	db.logger = log.New(db.logFile, "", log.LstdFlags)

	db.logger.Printf("open: %s", db.path)

	// If database file exists, read & set the header.
	if err := db.readHeader(); os.IsNotExist(err) {
		db.setHeader(nil) // invalidate header for missing file
	} else if err != nil {
		db.setHeader(nil) // invalidate header
		db.logger.Printf("cannot read db header: %s", err)
	}

	// If WAL is enabled & WAL file exists, attempt to recover.
	if db.isWALEnabled {
		if err := db.recoverWAL(); err != nil {
			return fmt.Errorf("recover wal: %w", err)
		}
	}

	return nil
}

func (db *DB) recoverWAL() error {
	// Check for the existence of the real & shadow WAL.
	// We need to sync up between the two.
	hasShadowWAL, err := db.shadowWALExists()
	if err != nil {
		return fmt.Errorf("check shadow wal: %w", err)
	}
	hasRealWAL, err := db.walExists()
	if err != nil {
		return fmt.Errorf("check real wal: %w", err)
	}

	// Neither the real WAL or shadow WAL exist so no pages have been written
	// since the DB's journal mode has been set to "wal". In this case, do
	// nothing and wait for the first WAL write to occur.
	if !hasShadowWAL && !hasRealWAL {
		return nil
	}

	if hasRealWAL {
		if hasShadowWAL {
			return db.recoverRealAndShadowWALs()
		}
		return db.recoverRealWALOnly()
	}

	if hasShadowWAL {
		return db.recoverShadowWALOnly()
	}
	return nil // no-op, wait for first WAL write
}

// recoverRealWALOnly copies the real WAL to the active shadow WAL.
func (db *DB) recoverRealWALOnly() error {
	// Open real WAL to read from.
	r, err := os.Open(db.WALPath())
	if err != nil {
		return fmt.Errorf("cannot open wal: %w", err)
	}
	defer r.Close()

	// Read header from real WAL.
	var hdr sqlite.WALHeader
	if _, err := hdr.ReadFrom(r); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	// Create a new shadow WAL file.
	w, err := db.createActiveShadowWAL(hdr)
	if err != nil {
		return fmt.Errorf("cannot create active shadow wal: %w", err)
	}
	defer w.Close()

	// Read from real WAL and copy to shadow WAL.
	buf := make([]byte, hdr.PageSize)
	for {
		// Read frame header & data from real WAL.
		var fhdr sqlite.WALFrameHeader
		if _, err := fhdr.ReadFrom(r); err != nil {
			return fmt.Errorf("cannot read frame header: %w", err)
		} else if _, err := io.ReadFull(r, buf); err != nil {
			return fmt.Errorf("cannot read frame: %w", err)
		}

		// Copy to the shadow WAL.
		if _, err := fhdr.WriteTo(w); err != nil {
			return fmt.Errorf("cannot write frame to shadow: %w", err)
		} else if _, err := w.Write(buf); err != nil {
			return fmt.Errorf("cannot write frame to shadow: %w", err)
		}
	}

	return nil
}

// walExists returns true if the real WAL exists.
func (db *DB) walExists() (bool, error) {
	if _, err := os.Stat(db.WALPath()); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

// shadowWALExists returns true if the shadow WAL exists.
func (db *DB) shadowWALExists() (bool, error) {
	f, err := os.Open(db.ShadowWALDir())
	if err != nil {
		return false, err
	}

	// Read directory entries until we find a WAL file.
	for {
		fis, err := f.Readdir(1)
		if err == io.EOF {
			return false, nil
		} else if err != nil {
			return false, err
		} else if strings.HasSuffix(fis[0].Name(), WALExt) {
			return true, nil
		}
	}
}

// createActiveShadowWAL creates a new shadow WAL file with the given header.
func (db *DB) createActiveShadowWAL(hdr sqlite.WALHeader) (f *os.File, err error) {
	if f, err = os.OpenFile(db.ActiveShadowWALPath(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600); err != nil {
		return nil, err
	}

	// Attempt to clean up if shadow WAL creation fails.
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(f.Name())
		}
	}()

	// Clear some fields from header that we won't use for the shadow WAL.
	hdr = ClearShadowWALHeader(hdr)

	// Write header & save it to the DB to ensure new WAL header writes match.
	if _, err := hdr.WriteTo(f); err != nil {
		return nil, err
	}
	db.walHeader = &hdr

	return f, nil
}

// recoverShadowWALOnly verifies the last page in the shadow WAL matches the
// contents of the database page.
func (db *DB) recoverShadowWALOnly() error {
	panic("TODO")
}

// recoverRealAndShadowWALs verifies the last page of the real & shadow WALs match.
func (db *DB) recoverRealAndShadowWALs() error {
	panic("TODO")
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

	// Clear WAL data if WAL is disabled.
	if !db.isWALEnabled {
		db.walHeader = nil
		db.processedWALByteN = 0
		db.pendingWALByteN = 0
	}
	db.logger.Printf("header: valid=%v wal=%v", db.isHeaderValid, db.isWALEnabled)
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

// ClearShadowWALHeader clears the checkpoint, salt, & checksum in the header.
// These fields are unused by the shadow WAL because we don't overwrite the WAL.
func ClearShadowWALHeader(hdr sqlite.WALHeader) sqlite.WALHeader {
	hdr.CheckpointSeqNo = 0
	hdr.Salt = 0
	hdr.Checksum = 0
	return hdr
}
