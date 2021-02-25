package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/signal"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/s3"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v2"
)

// Build information.
var (
	Version = "(development build)"
)

// errStop is a terminal error for indicating program should quit.
var errStop = errors.New("stop")

func main() {
	log.SetFlags(0)

	m := NewMain()
	if err := m.Run(context.Background(), os.Args[1:]); err == flag.ErrHelp || err == errStop {
		os.Exit(1)
	} else if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct{}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{}
}

// Run executes the program.
func (m *Main) Run(ctx context.Context, args []string) (err error) {
	// Execute replication command if running as a Windows service.
	if isService, err := isWindowsService(); err != nil {
		return err
	} else if isService {
		return runWindowsService(ctx)
	}

	// Extract command name.
	var cmd string
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "databases":
		return (&DatabasesCommand{}).Run(ctx, args)
	case "generations":
		return (&GenerationsCommand{}).Run(ctx, args)
	case "replicate":
		c := NewReplicateCommand()
		if err := c.ParseFlags(ctx, args); err != nil {
			return err
		}

		// Setup signal handler.
		ctx, cancel := context.WithCancel(ctx)
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt)
		go func() { <-ch; cancel() }()

		if err := c.Run(ctx); err != nil {
			return err
		}

		// Wait for signal to stop program.
		<-ctx.Done()
		signal.Reset()

		// Gracefully close.
		return c.Close()

	case "restore":
		return (&RestoreCommand{}).Run(ctx, args)
	case "snapshots":
		return (&SnapshotsCommand{}).Run(ctx, args)
	case "version":
		return (&VersionCommand{}).Run(ctx, args)
	case "wal":
		return (&WALCommand{}).Run(ctx, args)
	default:
		if cmd == "" || cmd == "help" || strings.HasPrefix(cmd, "-") {
			m.Usage()
			return flag.ErrHelp
		}
		return fmt.Errorf("litestream %s: unknown command", cmd)
	}
}

// Usage prints the help screen to STDOUT.
func (m *Main) Usage() {
	fmt.Println(`
litestream is a tool for replicating SQLite databases.

Usage:

	litestream <command> [arguments]

The commands are:

	databases    list databases specified in config file
	generations  list available generations for a database
	replicate    runs a server to replicate databases
	restore      recovers database backup from a replica
	snapshots    list available snapshots for a database
	version      prints the binary version
	wal          list available WAL files for a database
`[1:])
}

// Config represents a configuration file for the litestream daemon.
type Config struct {
	// Bind address for serving metrics.
	Addr string `yaml:"addr"`

	// List of databases to manage.
	DBs []*DBConfig `yaml:"dbs"`

	// Global S3 settings
	AccessKeyID     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
}

// propagateGlobalSettings copies global S3 settings to replica configs.
func (c *Config) propagateGlobalSettings() {
	for _, dbc := range c.DBs {
		for _, rc := range dbc.Replicas {
			if rc.AccessKeyID == "" {
				rc.AccessKeyID = c.AccessKeyID
			}
			if rc.SecretAccessKey == "" {
				rc.SecretAccessKey = c.SecretAccessKey
			}
		}
	}
}

// DefaultConfig returns a new instance of Config with defaults set.
func DefaultConfig() Config {
	return Config{}
}

// DBConfig returns database configuration by path.
func (c *Config) DBConfig(path string) *DBConfig {
	for _, dbConfig := range c.DBs {
		if dbConfig.Path == path {
			return dbConfig
		}
	}
	return nil
}

// ReadConfigFile unmarshals config from filename. Expands path if needed.
func ReadConfigFile(filename string) (_ Config, err error) {
	config := DefaultConfig()

	// Expand filename, if necessary.
	filename, err = expand(filename)
	if err != nil {
		return config, err
	}

	// Read & deserialize configuration.
	if buf, err := ioutil.ReadFile(filename); os.IsNotExist(err) {
		return config, fmt.Errorf("config file not found: %s", filename)
	} else if err != nil {
		return config, err
	} else if err := yaml.Unmarshal(buf, &config); err != nil {
		return config, err
	}

	// Normalize paths.
	for _, dbConfig := range config.DBs {
		if dbConfig.Path, err = expand(dbConfig.Path); err != nil {
			return config, err
		}
	}

	// Propage settings from global config to replica configs.
	config.propagateGlobalSettings()

	return config, nil
}

// DBConfig represents the configuration for a single database.
type DBConfig struct {
	Path     string           `yaml:"path"`
	Replicas []*ReplicaConfig `yaml:"replicas"`
}

// NewDBFromConfig instantiates a DB based on a configuration.
func NewDBFromConfig(dbc *DBConfig) (*litestream.DB, error) {
	path, err := expand(dbc.Path)
	if err != nil {
		return nil, err
	}

	// Initialize database with given path.
	db := litestream.NewDB(path)

	// Instantiate and attach replicas.
	for _, rc := range dbc.Replicas {
		r, err := NewReplicaFromConfig(rc, db)
		if err != nil {
			return nil, err
		}
		db.Replicas = append(db.Replicas, r)
	}

	return db, nil
}

// ReplicaConfig represents the configuration for a single replica in a database.
type ReplicaConfig struct {
	Type                   string        `yaml:"type"` // "file", "s3"
	Name                   string        `yaml:"name"` // name of replica, optional.
	Path                   string        `yaml:"path"`
	URL                    string        `yaml:"url"`
	Retention              time.Duration `yaml:"retention"`
	RetentionCheckInterval time.Duration `yaml:"retention-check-interval"`
	SyncInterval           time.Duration `yaml:"sync-interval"` // s3 only
	SnapshotInterval       time.Duration `yaml:"snapshot-interval"`
	ValidationInterval     time.Duration `yaml:"validation-interval"`

	// S3 settings
	AccessKeyID     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
	Endpoint        string `yaml:"endpoint"`
	ForcePathStyle  bool   `yaml:"force-path-style"`
}

// NewReplicaFromConfig instantiates a replica for a DB based on a config.
func NewReplicaFromConfig(c *ReplicaConfig, db *litestream.DB) (litestream.Replica, error) {
	// Ensure user did not specify URL in path.
	if isURL(c.Path) {
		return nil, fmt.Errorf("replica path cannot be a url, please use the 'url' field instead: %s", c.Path)
	}

	switch c.ReplicaType() {
	case "file":
		return newFileReplicaFromConfig(c, db)
	case "s3":
		return newS3ReplicaFromConfig(c, db)
	default:
		return nil, fmt.Errorf("unknown replica type in config: %q", c.Type)
	}
}

// newFileReplicaFromConfig returns a new instance of FileReplica build from config.
func newFileReplicaFromConfig(c *ReplicaConfig, db *litestream.DB) (_ *litestream.FileReplica, err error) {
	// Ensure URL & path are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for file replica")
	}

	// Parse path from URL, if specified.
	path := c.Path
	if c.URL != "" {
		if _, _, path, err = ParseReplicaURL(c.URL); err != nil {
			return nil, err
		}
	}

	// Ensure path is set explicitly or derived from URL field.
	if path == "" {
		return nil, fmt.Errorf("file replica path required")
	}

	// Expand home prefix and return absolute path.
	if path, err = expand(path); err != nil {
		return nil, err
	}

	// Instantiate replica and apply time fields, if set.
	r := litestream.NewFileReplica(db, c.Name, path)
	if v := c.Retention; v > 0 {
		r.Retention = v
	}
	if v := c.RetentionCheckInterval; v > 0 {
		r.RetentionCheckInterval = v
	}
	if v := c.SnapshotInterval; v > 0 {
		r.SnapshotInterval = v
	}
	if v := c.ValidationInterval; v > 0 {
		r.ValidationInterval = v
	}
	return r, nil
}

// newS3ReplicaFromConfig returns a new instance of S3Replica build from config.
func newS3ReplicaFromConfig(c *ReplicaConfig, db *litestream.DB) (_ *s3.Replica, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for s3 replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for s3 replica")
	}

	bucket, path := c.Bucket, c.Path
	region, endpoint, forcePathStyle := c.Region, c.Endpoint, c.ForcePathStyle

	// Apply settings from URL, if specified.
	if c.URL != "" {
		_, host, upath, err := ParseReplicaURL(c.URL)
		if err != nil {
			return nil, err
		}
		ubucket, uregion, uendpoint, uforcePathStyle := s3.ParseHost(host)

		// Only apply URL parts to field that have not been overridden.
		if path == "" {
			path = upath
		}
		if bucket == "" {
			bucket = ubucket
		}
		if region == "" {
			region = uregion
		}
		if endpoint == "" {
			endpoint = uendpoint
		}
		if !forcePathStyle {
			forcePathStyle = uforcePathStyle
		}
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for s3 replica")
	}

	// Build replica.
	r := s3.NewReplica(db, c.Name)
	r.AccessKeyID = c.AccessKeyID
	r.SecretAccessKey = c.SecretAccessKey
	r.Bucket = bucket
	r.Path = path
	r.Region = region
	r.Endpoint = endpoint
	r.ForcePathStyle = forcePathStyle

	if v := c.Retention; v > 0 {
		r.Retention = v
	}
	if v := c.RetentionCheckInterval; v > 0 {
		r.RetentionCheckInterval = v
	}
	if v := c.SyncInterval; v > 0 {
		r.SyncInterval = v
	}
	if v := c.SnapshotInterval; v > 0 {
		r.SnapshotInterval = v
	}
	if v := c.ValidationInterval; v > 0 {
		r.ValidationInterval = v
	}
	return r, nil
}

// ParseReplicaURL parses a replica URL.
func ParseReplicaURL(s string) (scheme, host, urlpath string, err error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", "", "", err
	}

	switch u.Scheme {
	case "file":
		scheme, u.Scheme = u.Scheme, ""
		return scheme, "", path.Clean(u.String()), nil

	case "":
		return u.Scheme, u.Host, u.Path, fmt.Errorf("replica url scheme required: %s", s)

	default:
		return u.Scheme, u.Host, strings.TrimPrefix(path.Clean(u.Path), "/"), nil
	}
}

// isURL returns true if s can be parsed and has a scheme.
func isURL(s string) bool {
	return regexp.MustCompile(`^\w+:\/\/`).MatchString(s)
}

// ReplicaType returns the type based on the type field or extracted from the URL.
func (c *ReplicaConfig) ReplicaType() string {
	scheme, _, _, _ := ParseReplicaURL(c.URL)
	if scheme != "" {
		return scheme
	} else if c.Type != "" {
		return c.Type
	}
	return "file"
}

// DefaultConfigPath returns the default config path.
func DefaultConfigPath() string {
	if v := os.Getenv("LITESTREAM_CONFIG"); v != "" {
		return v
	}
	return defaultConfigPath
}

func registerConfigFlag(fs *flag.FlagSet, p *string) {
	fs.StringVar(p, "config", DefaultConfigPath(), "config path")
}

// expand returns an absolute path for s.
func expand(s string) (string, error) {
	// Just expand to absolute path if there is no home directory prefix.
	prefix := "~" + string(os.PathSeparator)
	if s != "~" && !strings.HasPrefix(s, prefix) {
		return filepath.Abs(s)
	}

	// Look up home directory.
	u, err := user.Current()
	if err != nil {
		return "", err
	} else if u.HomeDir == "" {
		return "", fmt.Errorf("cannot expand path %s, no home directory available", s)
	}

	// Return path with tilde replaced by the home directory.
	if s == "~" {
		return u.HomeDir, nil
	}
	return filepath.Join(u.HomeDir, strings.TrimPrefix(s, prefix)), nil
}
