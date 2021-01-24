package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/user"
	"path"
	"path/filepath"
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

func main() {
	log.SetFlags(0)

	m := NewMain()
	if err := m.Run(context.Background(), os.Args[1:]); err == flag.ErrHelp {
		os.Exit(1)
	} else if err != nil {
		fmt.Fprintln(os.Stderr, err)
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
		return (&ReplicateCommand{}).Run(ctx, args)
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

	generations  list available generations for a database
	replicate    runs a server to replicate databases
	restore      recovers database backup from a replica
	snapshots    list available snapshots for a database
	validate     checks replica to ensure a consistent state with primary
	version      prints the version
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

// Normalize expands paths and parses URL-specified replicas.
func (c *Config) Normalize() error {
	for i := range c.DBs {
		if err := c.DBs[i].Normalize(); err != nil {
			return err
		}
	}
	return nil
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

	if err := config.Normalize(); err != nil {
		return config, err
	}
	return config, nil
}

// DBConfig represents the configuration for a single database.
type DBConfig struct {
	Path     string           `yaml:"path"`
	Replicas []*ReplicaConfig `yaml:"replicas"`
}

// Normalize expands paths and parses URL-specified replicas.
func (c *DBConfig) Normalize() (err error) {
	c.Path, err = expand(c.Path)
	if err != nil {
		return err
	}

	for i := range c.Replicas {
		if err := c.Replicas[i].Normalize(); err != nil {
			return err
		}
	}
	return nil
}

// ReplicaConfig represents the configuration for a single replica in a database.
type ReplicaConfig struct {
	Type                   string        `yaml:"type"` // "file", "s3"
	Name                   string        `yaml:"name"` // name of replica, optional.
	Path                   string        `yaml:"path"`
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

// Normalize expands paths and parses URL-specified replicas.
func (c *ReplicaConfig) Normalize() error {
	// Attempt to parse as URL. Ignore if it is not a URL or if there is no scheme.
	u, err := url.Parse(c.Path)
	if err != nil || u.Scheme == "" {
		if c.Type == "" || c.Type == "file" {
			c.Path, err = expand(c.Path)
			return err
		}
		return nil
	}

	switch u.Scheme {
	case "file":
		c.Type, u.Scheme = u.Scheme, ""
		c.Path = path.Clean(u.String())
		return nil

	case "s3":
		c.Type = u.Scheme
		c.Path = strings.TrimPrefix(path.Clean(u.Path), "/")
		c.Bucket = u.Host
		return nil

	default:
		return fmt.Errorf("unrecognized replica type in path scheme: %s", c.Path)
	}
}

// DefaultConfigPath returns the default config path.
func DefaultConfigPath() string {
	if v := os.Getenv("LITESTREAM_CONFIG"); v != "" {
		return v
	}
	return "/etc/litestream.yml"
}

func registerConfigFlag(fs *flag.FlagSet, p *string) {
	fs.StringVar(p, "config", DefaultConfigPath(), "config path")
}

// newDBFromConfig instantiates a DB based on a configuration.
func newDBFromConfig(c *Config, dbc *DBConfig) (*litestream.DB, error) {
	// Initialize database with given path.
	db := litestream.NewDB(dbc.Path)

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
	switch rc.Type {
	case "", "file":
		return newFileReplicaFromConfig(db, c, dbc, rc)
	case "s3":
		return newS3ReplicaFromConfig(db, c, dbc, rc)
	default:
		return nil, fmt.Errorf("unknown replica type in config: %q", rc.Type)
	}
}

// newFileReplicaFromConfig returns a new instance of FileReplica build from config.
func newFileReplicaFromConfig(db *litestream.DB, c *Config, dbc *DBConfig, rc *ReplicaConfig) (*litestream.FileReplica, error) {
	if rc.Path == "" {
		return nil, fmt.Errorf("%s: file replica path required", db.Path())
	}

	r := litestream.NewFileReplica(db, rc.Name, rc.Path)
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
func newS3ReplicaFromConfig(db *litestream.DB, c *Config, dbc *DBConfig, rc *ReplicaConfig) (*s3.Replica, error) {
	// Use global or replica-specific S3 settings.
	accessKeyID := c.AccessKeyID
	if v := rc.AccessKeyID; v != "" {
		accessKeyID = v
	}
	secretAccessKey := c.SecretAccessKey
	if v := rc.SecretAccessKey; v != "" {
		secretAccessKey = v
	}
	bucket := c.Bucket
	if v := rc.Bucket; v != "" {
		bucket = v
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
	r.Path = rc.Path

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
