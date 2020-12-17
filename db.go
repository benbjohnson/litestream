package litestream

import (
	"bytes"
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
	path string

	isHeaderValid bool // true if meta page contains SQLITE3 header
	isWALEnabled  bool // true if file format version specifies WAL

	// Tracks WAL state.
	walHeader         *sqlite.WALHeader
	processedWALByteN int64  // bytes copied to shadow WAL
	pendingWALByteN   int64  // bytes pending copy to shadow WAL
	checksum          uint64 // running checksum on real WAL

	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	// Database-specific logger
	logFile *os.File
	logger  *log.Logger
}

// NewDB returns a new instance of DB for a given path.
func NewDB() *DB {
	db := &DB{}
	db.ctx, db.cancel = context.WithCancel(context.Background())
	return db
}

// Path returns the path to the database.
func (db *DB) Path() string {
	return db.path
}

// AbsPath returns the full path to the database.
func (db *DB) AbsPath() string {
	return filepath.Join(db.fs.TargetPath, db.path)
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

// LastShadowWALPath returns the active shadow WAL or the last snapshotted shadow WAL path.
// Returns an empty string if no shadow WAL files exist.
func (db *DB) LastShadowWALPath() (string, error) {
	// Return active shadow WAL if it exists.
	if _, err := os.Stat(db.ActiveShadowWALPath()); err == nil {
		return db.ActiveShadowWALPath(), nil
	} else if !os.IsNotExist(err) {
		return "", nil
	}

	// Otherwise search for the largest shadow WAL file.
	f, err := os.Open(db.ShadowWALDir())
	if os.IsNotExist(err) {
		return "", nil
	} else if err != nil {
		return "", err
	}
	defer f.Close()

	var filename string
	for {
		fis, err := f.Readdir(512)
		if err != nil {
			return "", err
		}

		for _, fi := range fis {
			if !strings.HasSuffix(fi.Name(), WALExt) {
				continue
			} else if filename == "" || fi.Name() > filename {
				filename = fi.Name()
			}
		}
	}

	// Return an error if there is no shadow WAL files.
	if filename == "" {
		return "", os.ErrNotExist
	}
	return filepath.Join(db.ShadowWALDir(), filename), nil
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
	db.logger.Printf("opening: path=%s", db.path)

	// If database file exists, read & set the header.
	if err := db.readHeader(); os.IsNotExist(err) {
		db.logger.Printf("opening: db not found")
		db.setHeader(nil) // invalidate header for missing file
	} else if err != nil {
		db.logger.Printf("opening: cannot read db header: %s", err)
		db.setHeader(nil) // invalidate header
	}

	// If WAL is enabled & WAL file exists, attempt to recover.
	if err := db.recover(); err != nil {
		return fmt.Errorf("recover wal: %w", err)
	}

	db.logger.Printf("open: %s", db.path)

	return nil
}

func (db *DB) recover() error {
	// Ignore if the DB is invalid.
	if !db.valid() {
		db.logger.Printf("recovering: invalid db, skipping; valid-header:%v wal-enabled=%v", db.isHeaderValid, db.isWALEnabled)
		return nil
	}

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

	if hasRealWAL {
		if hasShadowWAL {
			return db.recoverRealAndShadowWALs()
		}
		return db.recoverRealWALOnly()
	}

	if hasShadowWAL {
		return db.recoverShadowWALOnly()
	}

	db.logger.Printf("recovering: no WALs available, skipping")
	return nil // no-op, wait for first WAL write
}

// recoverRealWALOnly copies the real WAL to the active shadow WAL.
func (db *DB) recoverRealWALOnly() error {
	db.logger.Printf("recovering: real WAL only")
	return db.sync()
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

// recoverShadowWALOnly verifies the last page in the shadow WAL matches the
// contents of the database page.
func (db *DB) recoverShadowWALOnly() error {
	db.logger.Printf("recovering: shadow WAL only")

	// TODO: Verify last page in shadow WAL matches data in DB.

	db.processedWALByteN = 0
	db.pendingWALByteN = 0

	return nil
}

// recoverRealAndShadowWALs verifies the last page of the real & shadow WALs match.
func (db *DB) recoverRealAndShadowWALs() error {
	db.logger.Printf("recovering: real & shadow WAL")

	// Read WAL header from shadow WAL.
	lastShadowWALPath, err := db.LastShadowWALPath()
	if err != nil {
		return fmt.Errorf("cannot find last shadow wal path: %w", err)
	}
	hdr, err := readFileWALHeader(lastShadowWALPath)
	if err != nil {
		return fmt.Errorf("cannot read last shadow wal header: %w", err)
	}
	db.walHeader = &hdr

	// Read last pages from shadow WAL & real WAL and ensure they match.
	if fhdr0, data0, err := readLastWALPage(lastShadowWALPath); err != nil {
		return fmt.Errorf("cannot read last shadow wal page: %w", err)
	} else if fhdr1, data1, err := readLastWALPage(db.WALPath()); err != nil {
		return fmt.Errorf("cannot read last shadow wal page: %w", err)
	} else if fhdr0 != fhdr1 {
		return fmt.Errorf("last frame header mismatch: %#v != %#v", fhdr0, fhdr1)
	} else if !bytes.Equal(data0, data1) {
		return fmt.Errorf("last frame data mismatch")
	} else {
		db.checksum = fhdr1.Checksum
	}

	// Update position within real WAL.
	fi, err := os.Stat(db.WALPath())
	if err != nil {
		return fmt.Errorf("cannot stat wal: %w", err)
	}
	db.processedWALByteN = fi.Size()
	db.pendingWALByteN = 0

	return nil
}

// Sync synchronizes the real WAL to the shadow WAL.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.sync()
}

func (db *DB) sync() (err error) {
	db.logger.Printf("sync: begin")
	defer func() {
		db.logger.Printf("sync: end: err=%v", err)
	}()

	// Open real WAL to read from.
	r, err := os.Open(db.WALPath())
	if err != nil {
		return fmt.Errorf("cannot open wal: %w", err)
	} else if _, err := r.Seek(db.processedWALByteN, io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek wal: %w", err)
	}
	defer r.Close()

	// TODO: Verify all frames are valid & committed before copy.

	// Read header if we are at the beginning of the WAL.
	if db.processedWALByteN == 0 {
		var hdr sqlite.WALHeader
		if _, err := hdr.ReadFrom(r); err != nil {
			return fmt.Errorf("cannot read wal header: %w", err)
		}

		// Save checksum to verify later pages in WAL.
		db.checksum = hdr.Checksum

		// Clear out salt & checksum from header for shadow WAL.
		hdr = ClearShadowWALHeader(hdr)
		db.walHeader = &hdr
	}

	// Open shadow WAL to copy to.
	w, err := os.OpenFile(db.ActiveShadowWALPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("cannot open active shadow wal: %w", err)
	}
	defer w.Close()

	// If we are at the start of a new shadow WAL, write a header.
	if n, err := w.Seek(0, io.SeekCurrent); err != nil {
		return fmt.Errorf("cannot seek shadow wal: %w", err)
	} else if n == 0 {
		db.logger.Printf("sync: new shadow wal, writing header: magic=%x page-size=%d", db.walHeader.Magic, db.walHeader.PageSize)
		if _, err := db.walHeader.WriteTo(w); err != nil {
			return fmt.Errorf("cannot write shadow wal header: %w", err)
		}
	}

	// Read from real WAL and copy to shadow WAL.
	buf := make([]byte, db.walHeader.PageSize)
	for db.pendingWALByteN != 0 {
		// Read frame header & data from real WAL.
		var fhdr sqlite.WALFrameHeader
		if _, err := fhdr.ReadFrom(r); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("cannot read frame header: %w", err)
		} else if _, err := io.ReadFull(r, buf); err != nil {
			return fmt.Errorf("cannot read frame: %w", err)
		}

		db.logger.Printf("sync: copy frame: pgno=%d pageN=%d salt=%x checksum=%x",
			fhdr.Pgno,
			fhdr.PageN,
			fhdr.Salt,
			fhdr.Checksum,
		)

		// Copy to the shadow WAL.
		if _, err := fhdr.WriteTo(w); err != nil {
			return fmt.Errorf("cannot write frame to shadow: %w", err)
		} else if _, err := w.Write(buf); err != nil {
			return fmt.Errorf("cannot write frame to shadow: %w", err)
		}

		byteN := int64(sqlite.WALFrameHeaderSize + len(buf))
		db.processedWALByteN += byteN
		db.pendingWALByteN -= byteN
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
	f, err := os.Open(db.AbsPath())
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
	db.logger.Printf("write: n=%d pending=%d", n, db.pendingWALByteN)
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

// readLastWALPage reads the last frame header & data from a WAL file.
func readLastWALPage(path string) (fhdr sqlite.WALFrameHeader, data []byte, err error) {
	f, err := os.Open(path)
	if err != nil {
		return fhdr, data, err
	}
	defer f.Close()

	// Determine WAL file size.
	fi, err := f.Stat()
	if err != nil {
		return fhdr, data, err
	}

	// Read WAL header to determine page size.
	var hdr sqlite.WALHeader
	if _, err := hdr.ReadFrom(f); err != nil {
		return fhdr, data, fmt.Errorf("cannot read wal header: %w", err)
	}

	// WAL file size must be divisible by frame size (minus the header).
	if (fi.Size()-sqlite.WALHeaderSize)%(sqlite.WALFrameHeaderSize+int64(hdr.PageSize)) != 0 {
		return fhdr, data, fmt.Errorf("partial wal record: path=%s sz=%d", path, fi.Size())
	}

	// Seek to last frame and read header & data.
	data = make([]byte, hdr.PageSize)
	if _, err := f.Seek(sqlite.WALFrameHeaderSize+int64(hdr.PageSize), io.SeekStart); err != nil {
		return fhdr, data, fmt.Errorf("cannot seek: %w", err)
	} else if _, err := fhdr.ReadFrom(f); err != nil {
		return fhdr, data, fmt.Errorf("cannot read frame header: %w", err)
	} else if _, err := io.ReadFull(f, data); err != nil {
		return fhdr, data, fmt.Errorf("cannot read frame data: %w", err)
	}
	return fhdr, data, nil
}

// readFileWALHeader reads the WAL header from file.
func readFileWALHeader(path string) (hdr sqlite.WALHeader, err error) {
	f, err := os.Open(path)
	if err != nil {
		return hdr, err
	}
	defer f.Close()

	_, err = hdr.ReadFrom(f)
	return hdr, err
}
