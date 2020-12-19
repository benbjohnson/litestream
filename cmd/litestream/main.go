package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/benbjohnson/litestream"
)

// Build information.
var (
	Version = "(development build)"
)

// Default settings.
const (
	DefaultConfigPath = "~/litestream.yml"
)

func main() {
	// Setup signal handler.
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() { <-c; cancel() }()

	// Initialize program and read flags/config.
	m := NewMain()
	if err := m.ParseFlags(ctx, os.Args[1:]); err == flag.ErrHelp {
		os.Exit(1)
	} else if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Display version information.
	fmt.Printf("Litestream %s\n", Version)

	// Start monitoring databases.
	if err := m.Run(ctx); err != nil {
		m.Close()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Notify user that initialization is done.
	fmt.Printf("Initialized with %d databases; replication initialized.\n", len(m.DBs))

	// Wait for signal to stop program.
	<-ctx.Done()
	signal.Reset()

	// Gracefully close
	if err := m.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type Main struct {
	ConfigPath string
	Config     Config

	// List of managed databases specified in the config.
	DBs []*litestream.DB
}

func NewMain() *Main {
	return &Main{}
}

// ParseFlags parses the flag set from args & loads the configuration.
func (m *Main) ParseFlags(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream", flag.ContinueOnError)
	fs.StringVar(&m.ConfigPath, "config", DefaultConfigPath, "configuration path")
	fs.Usage = m.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Initialize log.
	log.SetFlags(0)

	// Load configuration.
	if m.ConfigPath == "" {
		return errors.New("-config required")
	} else if m.Config, err = ReadConfigFile(m.ConfigPath); err != nil {
		return err
	}

	return nil
}

// Run loads all databases specified in the configuration.
func (m *Main) Run(ctx context.Context) (err error) {
	if len(m.Config.DBs) == 0 {
		return errors.New("configuration must specify at least one database")
	}

	for _, dbc := range m.Config.DBs {
		if err := m.openDB(dbc); err != nil {
			return err
		}
	}

	return nil
}

// openDB instantiates and initializes a DB based on a configuration.
func (m *Main) openDB(config *DBConfig) error {
	// Initialize database with given path.
	db := litestream.NewDB(config.Path)

	// Instantiate and attach replicators.
	for _, rconfig := range config.Replicators {
		r, err := m.createReplicator(db, rconfig)
		if err != nil {
			return err
		}
		db.Replicators = append(db.Replicators, r)
	}

	// Open database & attach to program.
	if err := db.Open(); err != nil {
		return err
	}
	m.DBs = append(m.DBs, db)

	return nil
}

// createReplicator instantiates a replicator for a DB based on a config.
func (m *Main) createReplicator(db *litestream.DB, config *ReplicatorConfig) (litestream.Replicator, error) {
	switch config.Type {
	case "", "file":
		return m.createFileReplicator(db, config)
	default:
		return nil, fmt.Errorf("unknown replicator type in config: %q", config.Type)
	}
}

// createFileReplicator returns a new instance of FileReplicator build from config.
func (m *Main) createFileReplicator(db *litestream.DB, config *ReplicatorConfig) (*litestream.FileReplicator, error) {
	if config.Path == "" {
		return nil, fmt.Errorf("file replicator path require for db %q", db.Path())
	}
	return litestream.NewFileReplicator(db, config.Name, config.Path), nil
}

// Close closes all open databases.
func (m *Main) Close() (err error) {
	for _, db := range m.DBs {
		if e := db.SoftClose(); e != nil {
			fmt.Printf("error closing db: path=%s err=%s\n", db.Path(), e)
			if err == nil {
				err = e
			}
		}
	}
	return err
}

func (m *Main) Usage() {
	fmt.Println(`
Litestream is a daemon for replicating SQLite databases.

Usage:

	litestream [arguments]

Arguments:

	-config PATH
	    Specifies the configuration file. Required.

`[1:])
}
