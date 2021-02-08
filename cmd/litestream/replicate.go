package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/s3"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReplicateCommand represents a command that continuously replicates SQLite databases.
type ReplicateCommand struct {
	ConfigPath string
	Config     Config

	// List of managed databases specified in the config.
	DBs []*litestream.DB
}

// Run loads all databases specified in the configuration.
func (c *ReplicateCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-replicate", flag.ContinueOnError)
	tracePath := fs.String("trace", "", "trace path")
	registerConfigFlag(fs, &c.ConfigPath)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Load configuration or use CLI args to build db/replica.
	var config Config
	if fs.NArg() == 1 {
		return fmt.Errorf("must specify at least one replica URL for %s", fs.Arg(0))
	} else if fs.NArg() > 1 {
		dbConfig := &DBConfig{Path: fs.Arg(0)}
		for _, u := range fs.Args()[1:] {
			dbConfig.Replicas = append(dbConfig.Replicas, &ReplicaConfig{
				URL:          u,
				SyncInterval: 1 * time.Second,
			})
		}
		config.DBs = []*DBConfig{dbConfig}
	} else if c.ConfigPath != "" {
		config, err = ReadConfigFile(c.ConfigPath)
		if err != nil {
			return err
		}
	} else {
		return errors.New("-config flag or database/replica arguments required")
	}

	// Enable trace logging.
	if *tracePath != "" {
		f, err := os.Create(*tracePath)
		if err != nil {
			return err
		}
		defer f.Close()
		litestream.Tracef = log.New(f, "", log.LstdFlags|log.LUTC|log.Lshortfile).Printf
	}

	// Setup signal handler.
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() { <-ch; cancel() }()

	// Display version information.
	fmt.Printf("litestream %s\n", Version)

	if len(config.DBs) == 0 {
		fmt.Println("no databases specified in configuration")
	}

	for _, dbConfig := range config.DBs {
		db, err := newDBFromConfig(&config, dbConfig)
		if err != nil {
			return err
		}

		// Open database & attach to program.
		if err := db.Open(); err != nil {
			return err
		}
		c.DBs = append(c.DBs, db)
	}

	// Notify user that initialization is done.
	for _, db := range c.DBs {
		fmt.Printf("initialized db: %s\n", db.Path())
		for _, r := range db.Replicas {
			switch r := r.(type) {
			case *litestream.FileReplica:
				fmt.Printf("replicating to: name=%q type=%q path=%q\n", r.Name(), r.Type(), r.Path())
			case *s3.Replica:
				fmt.Printf("replicating to: name=%q type=%q bucket=%q path=%q region=%q\n", r.Name(), r.Type(), r.Bucket, r.Path, r.Region)
			default:
				fmt.Printf("replicating to: name=%q type=%q\n", r.Name(), r.Type())
			}
		}
	}

	// Serve metrics over HTTP if enabled.
	if config.Addr != "" {
		_, port, _ := net.SplitHostPort(config.Addr)
		fmt.Printf("serving metrics on http://localhost:%s/metrics\n", port)
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(config.Addr, nil); err != nil {
				log.Printf("cannot start metrics server: %s", err)
			}
		}()
	}

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

// Usage prints the help screen to STDOUT.
func (c *ReplicateCommand) Usage() {
	fmt.Printf(`
The replicate command starts a server to monitor & replicate databases. 
You can specify your database & replicas in a configuration file or you can
replicate a single database file by specifying its path and its replicas in the
command line arguments.

Usage:

	litestream replicate [arguments]

	litestream replicate [arguments] DB_PATH REPLICA_URL [REPLICA_URL...]

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-trace PATH
	    Write verbose trace logging to PATH.

`[1:], DefaultConfigPath())
}
