package litestream

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	"bazil.org/fuse/fs"
	// "github.com/pelletier/go-toml"
)

var _ fs.FS = (*FileSystem)(nil)

// FileSystem represents the file system that is mounted.
// It returns a root node that represents the root directory.
type FileSystem struct {
	mu  sync.RWMutex
	dbs map[string]*DB // databases by path

	// Filepath to the root of the source directory.
	TargetPath string
}

func NewFileSystem(target string) *FileSystem {
	return &FileSystem{
		dbs:        make(map[string]*DB),
		TargetPath: target,
	}
}

// Open initializes the file system and finds all managed database files.
func (f *FileSystem) Open() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return filepath.Walk(f.TargetPath, func(path string, info os.FileInfo, err error) error {
		// Log errors while traversing file system.
		if err != nil {
			log.Printf("walk error: %s", err)
			return nil
		}

		// Ignore .<db>-litestream metadata directories.
		if IsMetaDir(path) {
			return filepath.SkipDir
		} else if !IsConfigPath(path) {
			return nil
		}

		// Determine the DB path relative to the target path.
		rel, err := filepath.Rel(f.TargetPath, ConfigPathToDBPath(path))
		if err != nil {
			return err
		}

		// Initialize a DB object based on the config path.
		// The database doesn't need to exist. It will be tracked when created.
		db := NewDB(f, rel)
		if err := db.Open(); err != nil {
			log.Printf("cannot open db %q: %s", rel, err)
			return nil
		}
		f.dbs[db.Path()] = db

		log.Printf("[DB]: %s", rel)

		return nil
	})
}

// DB returns the DB object associated with path.
func (f *FileSystem) DB(path string) *DB {
	f.mu.RLock()
	defer f.mu.RUnlock()
	db := f.dbs[path]
	return db
}

// OpenDB initializes a DB for a given path.
func (f *FileSystem) OpenDB(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.openDB(path)
}

func (f *FileSystem) openDB(path string) error {
	db := NewDB(f, path)
	if err := db.Open(); err != nil {
		return err
	}
	f.dbs[db.Path()] = db
	return nil
}

// Close closes the file system and flushes all managed database files.
func (f *FileSystem) Close() (err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeDBs()
}

func (f *FileSystem) closeDBs() (err error) {
	for key, db := range f.dbs {
		if e := db.Close(); e != nil && err == nil {
			err = e
		}
		delete(f.dbs, key)
	}
	return err
}

// Root returns the file system root node.
func (f *FileSystem) Root() (fs.Node, error) {
	return &Node{fs: f}, nil
}

// Config represents the configuration file for the file system.
type Config struct {
	Pattern     string `toml:"pattern"`      // glob pattern
	ReadOnly    bool   `toml:"read-only"`    // if true, expose only read access via FUSE
	RecoverFrom string `toml:"recover-from"` // http URL, S3, etc.

	HTTP HTTPConfig `toml:"http"`
	S3   S3Config   `toml:"s3"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{}
}

// S3Config represents the configuration for replicating to/from an S3-compatible store.
type S3Config struct {
	AccessKeyID     string `toml:"access-key-id"`     // AWS access key
	SecretAccessKey string `toml:"secret-access-key"` // AWS secret access key
}

// HTTPConfig represents the configuration for exposing data via HTTP.
type HTTPConfig struct {
	Addr     string `toml:"addr"`      // bind address
	CertFile string `toml:"cert-file"` // TLS certificate path
	KeyFile  string `toml:"key-file"`  // TLS key path
}
