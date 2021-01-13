package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/s3"
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

type Main struct{}

func NewMain() *Main {
	return &Main{}
}

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
	case "validate":
		return (&ValidateCommand{}).Run(ctx, args)
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

// Default configuration settings.
const (
	DefaultAddr = ":9090"
)

// Config represents a configuration file for the litestream daemon.
type Config struct {
	// Bind address for serving metrics.
	Addr string `yaml:"addr"`

	// List of databases to manage.
	DBs []*DBConfig `yaml:"dbs"`
}

// DefaultConfig returns a new instance of Config with defaults set.
func DefaultConfig() Config {
	return Config{
		Addr: DefaultAddr,
	}
}

func (c *Config) DBConfig(path string) *DBConfig {
	for _, dbConfig := range c.DBs {
		if dbConfig.Path == path {
			return dbConfig
		}
	}
	return nil
}

// ReadConfigFile unmarshals config from filename. Expands path if needed.
func ReadConfigFile(filename string) (Config, error) {
	config := DefaultConfig()

	// Expand filename, if necessary.
	if prefix := "~" + string(os.PathSeparator); strings.HasPrefix(filename, prefix) {
		u, err := user.Current()
		if err != nil {
			return config, err
		} else if u.HomeDir == "" {
			return config, fmt.Errorf("home directory unset")
		}
		filename = filepath.Join(u.HomeDir, strings.TrimPrefix(filename, prefix))
	}

	// Read & deserialize configuration.
	if buf, err := ioutil.ReadFile(filename); os.IsNotExist(err) {
		return config, fmt.Errorf("config file not found: %s", filename)
	} else if err != nil {
		return config, err
	} else if err := yaml.Unmarshal(buf, &config); err != nil {
		return config, err
	}
	return config, nil
}

type DBConfig struct {
	Path     string           `yaml:"path"`
	Replicas []*ReplicaConfig `yaml:"replicas"`
}

type ReplicaConfig struct {
	Type      string        `yaml:"type"` // "file", "s3"
	Name      string        `yaml:"name"` // name of replica, optional.
	Path      string        `yaml:"path"`
	Retention time.Duration `yaml:"retention"`

	// S3 settings
	AccessKeyID     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
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
func newDBFromConfig(config *DBConfig) (*litestream.DB, error) {
	// Initialize database with given path.
	db := litestream.NewDB(config.Path)

	// Instantiate and attach replicas.
	for _, rconfig := range config.Replicas {
		r, err := newReplicaFromConfig(db, rconfig)
		if err != nil {
			return nil, err
		}
		db.Replicas = append(db.Replicas, r)
	}

	return db, nil
}

// newReplicaFromConfig instantiates a replica for a DB based on a config.
func newReplicaFromConfig(db *litestream.DB, config *ReplicaConfig) (litestream.Replica, error) {
	switch config.Type {
	case "", "file":
		return newFileReplicaFromConfig(db, config)
	case "s3":
		return newS3ReplicaFromConfig(db, config)
	default:
		return nil, fmt.Errorf("unknown replica type in config: %q", config.Type)
	}
}

// newFileReplicaFromConfig returns a new instance of FileReplica build from config.
func newFileReplicaFromConfig(db *litestream.DB, config *ReplicaConfig) (*litestream.FileReplica, error) {
	if config.Path == "" {
		return nil, fmt.Errorf("%s: file replica path required", db.Path())
	}

	r := litestream.NewFileReplica(db, config.Name, config.Path)
	if v := config.Retention; v > 0 {
		r.RetentionInterval = v
	}
	return r, nil
}

// newS3ReplicaFromConfig returns a new instance of S3Replica build from config.
func newS3ReplicaFromConfig(db *litestream.DB, config *ReplicaConfig) (*s3.Replica, error) {
	if config.AccessKeyID == "" {
		return nil, fmt.Errorf("%s: s3 access key id required", db.Path())
	} else if config.SecretAccessKey == "" {
		return nil, fmt.Errorf("%s: s3 secret access key required", db.Path())
	} else if config.Region == "" {
		return nil, fmt.Errorf("%s: s3 region required", db.Path())
	} else if config.Bucket == "" {
		return nil, fmt.Errorf("%s: s3 bucket required", db.Path())
	}

	r := s3.NewReplica(db, config.Name)
	r.AccessKeyID = config.AccessKeyID
	r.SecretAccessKey = config.SecretAccessKey
	r.Region = config.Region
	r.Bucket = config.Bucket
	r.Path = config.Path

	if v := config.Retention; v > 0 {
		r.RetentionInterval = v
	}
	return r, nil
}
