package litestream

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync"

	"bazil.org/fuse/fs"
	"github.com/pelletier/go-toml"
)

const (
	ConfigName = "litestream.config"
)

var _ fs.FS = (*FileSystem)(nil)

// FileSystem represents the file system that is mounted.
// It returns a root node that represents the root directory.
type FileSystem struct {
	mu     sync.RWMutex
	dbs    map[string]*DB // databases by path
	config Config         // configuration file

	// Filepath to the root of the source directory.
	TargetPath string
}

func NewFileSystem() *FileSystem {
	return &FileSystem{
		config: DefaultConfig(),
	}
}

// ConfigPath returns the path to the file system config file.
func (f *FileSystem) ConfigPath() string {
	return filepath.Join(f.TargetPath, ConfigName)
}

// Open initializes the file system and finds all managed database files.
func (f *FileSystem) Open() error {
	f.mu.Lock()
	defer f.mu.Lock()
	return f.load()
}

// load loads the configuration file.
func (f *FileSystem) load() error {
	// Read configuration file.
	config := DefaultConfig()
	if buf, err := ioutil.ReadFile(f.ConfigPath()); err != nil {
		return err
	} else if err := toml.Unmarshal(buf, &config); err != nil {
		return fmt.Errorf("unmarshal(): cannot read config file: %w", err)
	}
	f.config = config

	// Close dbs opened under previous configuration.
	if err := f.closeDBs(); err != nil {
		return fmt.Errorf("load(): cannot close db: %w", err)
	}

	// Search for matching DB files.
	filenames, err := filepath.Glob(config.Pattern)
	if err != nil {
		return fmt.Errorf("load(): cannot glob: %w", err)
	}

	// Loop over matching files and create a DB for them.
	for _, filename := range filenames {
		db := NewDB(filename)
		if err := db.Open(); err != nil {
			return err
		}
		f.dbs[db.Path()] = db
	}

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
