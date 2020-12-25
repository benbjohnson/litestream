package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/benbjohnson/litestream"
)

type ReplicateCommand struct {
	ConfigPath string
	Config     Config

	// List of managed databases specified in the config.
	DBs []*litestream.DB
}

func NewReplicateCommand() *ReplicateCommand {
	return &ReplicateCommand{}
}

// Run loads all databases specified in the configuration.
func (c *ReplicateCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-replicate", flag.ContinueOnError)
	registerConfigFlag(fs, &c.ConfigPath)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Load configuration.
	if c.ConfigPath == "" {
		return errors.New("-config required")
	}
	config, err := ReadConfigFile(c.ConfigPath)
	if err != nil {
		return err
	}

	// Setup signal handler.
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() { <-ch; cancel() }()

	// Display version information.
	fmt.Printf("litestream %s\n", Version)

	if len(config.DBs) == 0 {
		return errors.New("configuration must specify at least one database")
	}

	for _, dbc := range config.DBs {
		if err := c.openDB(dbc); err != nil {
			return err
		}
	}

	// Notify user that initialization is done.
	fmt.Printf("Initialized with %d databases.\n", len(c.DBs))

	// Wait for signal to stop program.
	<-ctx.Done()
	signal.Reset()

	// Gracefully close
	if err := c.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return nil
}

// openDB instantiates and initializes a DB based on a configuration.
func (c *ReplicateCommand) openDB(config *DBConfig) error {
	// Initialize database with given path.
	db := litestream.NewDB(config.Path)

	// Instantiate and attach replicators.
	for _, rconfig := range config.Replicas {
		r, err := c.createReplicator(db, rconfig)
		if err != nil {
			return err
		}
		db.Replicators = append(db.Replicators, r)
	}

	// Open database & attach to program.
	if err := db.Open(); err != nil {
		return err
	}
	c.DBs = append(c.DBs, db)

	return nil
}

// createReplicator instantiates a replicator for a DB based on a config.
func (c *ReplicateCommand) createReplicator(db *litestream.DB, config *ReplicaConfig) (litestream.Replicator, error) {
	switch config.Type {
	case "", "file":
		return c.createFileReplicator(db, config)
	default:
		return nil, fmt.Errorf("unknown replicator type in config: %q", config.Type)
	}
}

// createFileReplicator returns a new instance of FileReplicator build from config.
func (c *ReplicateCommand) createFileReplicator(db *litestream.DB, config *ReplicaConfig) (*litestream.FileReplicator, error) {
	if config.Path == "" {
		return nil, fmt.Errorf("file replicator path require for db %q", db.Path())
	}
	return litestream.NewFileReplicator(db, config.Name, config.Path), nil
}

// Close closes all open databases.
func (c *ReplicateCommand) Close() (err error) {
	for _, db := range c.DBs {
		if e := db.SoftClose(); e != nil {
			fmt.Printf("error closing db: path=%s err=%s\n", db.Path(), e)
			if err == nil {
				err = e
			}
		}
	}
	return err
}

func (c *ReplicateCommand) Usage() {
	fmt.Println(`
The replicate command starts a server to monitor & replicate databases 
specified in your configuration file.

Usage:

	litestream replicate [arguments]

Arguments:

	-config PATH
	    Specifies the configuration file. Defaults to ~/litestream.yml

`[1:])
}
