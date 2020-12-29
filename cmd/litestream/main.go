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

	"github.com/benbjohnson/litestream"
	"gopkg.in/yaml.v2"
)

// Build information.
var (
	Version = "(development build)"
)

// DefaultConfigPath is the default configuration path.
const DefaultConfigPath = "/etc/litestream.yml"

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
	case "generations":
		return (&GenerationsCommand{}).Run(ctx, args)
	case "replicate":
		return (&ReplicateCommand{}).Run(ctx, args)
	case "restore":
		return (&RestoreCommand{}).Run(ctx, args)
	case "version":
		return (&VersionCommand{}).Run(ctx, args)
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

	generations  list available generations across all dbs & replicas
	replicate    runs a server to replicate databases
	restore      recovers database backup from a replica
	version      prints the version
`[1:])
}

// Config represents a configuration file for the litestream daemon.
type Config struct {
	DBs []*DBConfig `yaml:"databases"`
}

// DefaultConfig returns a new instance of Config with defaults set.
func DefaultConfig() Config {
	return Config{}
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
	Type string `yaml:"type"` // "file", "s3"
	Name string `yaml:"name"` // name of replica, optional.
	Path string `yaml:"path"` // used for file replicas
}

func registerConfigFlag(fs *flag.FlagSet, p *string) {
	fs.StringVar(p, "config", DefaultConfigPath, "config path")
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
	default:
		return nil, fmt.Errorf("unknown replica type in config: %q", config.Type)
	}
}

// newFileReplicaFromConfig returns a new instance of FileReplica build from config.
func newFileReplicaFromConfig(db *litestream.DB, config *ReplicaConfig) (*litestream.FileReplica, error) {
	if config.Path == "" {
		return nil, fmt.Errorf("file replica path require for db %q", db.Path())
	}
	return litestream.NewFileReplica(db, config.Name, config.Path), nil
}
