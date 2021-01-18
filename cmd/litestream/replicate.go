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
	verbose := fs.Bool("v", false, "verbose logging")
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

	// Enable trace logging.
	if *verbose {
		litestream.Tracef = log.Printf
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
The replicate command starts a server to monitor & replicate databases 
specified in your configuration file.

Usage:

	litestream replicate [arguments]

Arguments:

	-config PATH
	    Specifies the configuration file. Defaults to %s

	-v
	    Enable verbose logging output.

`[1:], DefaultConfigPath())
}
