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
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
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

	return config, nil
}

// DBConfig represents the configuration for a single database.
type DBConfig struct {
	Path     string           `yaml:"path"`
	Replicas []*ReplicaConfig `yaml:"replicas"`
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
	ValidationInterval     time.Duration `yaml:"validation-interval"`

	// S3 settings
	AccessKeyID     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
}

// NewReplicaFromURL returns a new Replica instance configured from a URL.
// The replica's database is not set.
func NewReplicaFromURL(s string) (litestream.Replica, error) {
	scheme, host, path, err := ParseReplicaURL(s)
	if err != nil {
		return nil, err
	}

	switch scheme {
	case "file":
		return litestream.NewFileReplica(nil, "", path), nil
	case "s3":
		r := s3.NewReplica(nil, "")
		r.Bucket, r.Path = host, path
		return r, nil
	default:
		return nil, fmt.Errorf("invalid replica url type: %s", s)
	}
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
	typ, _, _, _ := ParseReplicaURL(c.URL)
	if typ != "" {
		return typ
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

// newDBFromConfig instantiates a DB based on a configuration.
func newDBFromConfig(c *Config, dbc *DBConfig) (*litestream.DB, error) {
	path, err := expand(dbc.Path)
	if err != nil {
		return nil, err
	}

	// Initialize database with given path.
	db := litestream.NewDB(path)

	// Instantiate and attach replicas.
	for _, rc := range dbc.Replicas {
		r, err := newReplicaFromConfig(db, c, dbc, rc)
		if err != nil {
			return nil, err
		}
		db.Replicas = append(db.Replicas, r)
	}

	return db, nil
}

// newReplicaFromConfig instantiates a replica for a DB based on a config.
func newReplicaFromConfig(db *litestream.DB, c *Config, dbc *DBConfig, rc *ReplicaConfig) (litestream.Replica, error) {
	// Ensure user did not specify URL in path.
	if isURL(rc.Path) {
		return nil, fmt.Errorf("replica path cannot be a url, please use the 'url' field instead: %s", rc.Path)
	}

	switch rc.ReplicaType() {
	case "file":
		return newFileReplicaFromConfig(db, c, dbc, rc)
	case "s3":
		return newS3ReplicaFromConfig(db, c, dbc, rc)
	default:
		return nil, fmt.Errorf("unknown replica type in config: %q", rc.Type)
	}
}

// newFileReplicaFromConfig returns a new instance of FileReplica build from config.
func newFileReplicaFromConfig(db *litestream.DB, c *Config, dbc *DBConfig, rc *ReplicaConfig) (_ *litestream.FileReplica, err error) {
	path := rc.Path
	if rc.URL != "" {
		_, _, path, err = ParseReplicaURL(rc.URL)
		if err != nil {
			return nil, err
		}
	}

	if path == "" {
		return nil, fmt.Errorf("%s: file replica path required", db.Path())
	}

	if path, err = expand(path); err != nil {
		return nil, err
	}

	r := litestream.NewFileReplica(db, rc.Name, path)
	if v := rc.Retention; v > 0 {
		r.Retention = v
	}
	if v := rc.RetentionCheckInterval; v > 0 {
		r.RetentionCheckInterval = v
	}
	if v := rc.ValidationInterval; v > 0 {
		r.ValidationInterval = v
	}
	return r, nil
}

// newS3ReplicaFromConfig returns a new instance of S3Replica build from config.
func newS3ReplicaFromConfig(db *litestream.DB, c *Config, dbc *DBConfig, rc *ReplicaConfig) (_ *s3.Replica, err error) {
	bucket := c.Bucket
	if v := rc.Bucket; v != "" {
		bucket = v
	}

	path := rc.Path
	if rc.URL != "" {
		_, bucket, path, err = ParseReplicaURL(rc.URL)
		if err != nil {
			return nil, err
		}
	}

	// Use global or replica-specific S3 settings.
	accessKeyID := c.AccessKeyID
	if v := rc.AccessKeyID; v != "" {
		accessKeyID = v
	}
	secretAccessKey := c.SecretAccessKey
	if v := rc.SecretAccessKey; v != "" {
		secretAccessKey = v
	}
	region := c.Region
	if v := rc.Region; v != "" {
		region = v
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("%s: s3 bucket required", db.Path())
	}

	// Build replica.
	r := s3.NewReplica(db, rc.Name)
	r.AccessKeyID = accessKeyID
	r.SecretAccessKey = secretAccessKey
	r.Region = region
	r.Bucket = bucket
	r.Path = path

	if v := rc.Retention; v > 0 {
		r.Retention = v
	}
	if v := rc.RetentionCheckInterval; v > 0 {
		r.RetentionCheckInterval = v
	}
	if v := rc.SyncInterval; v > 0 {
		r.SyncInterval = v
	}
	if v := rc.ValidationInterval; v > 0 {
		r.ValidationInterval = v
	}
	return r, nil
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
