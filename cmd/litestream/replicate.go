package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/benbjohnson/litestream"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ReplicateCommand struct {
	ConfigPath string
	Config     Config

	// List of managed databases specified in the config.
	DBs []*litestream.DB
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
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() { <-ch; cancel() }()

	// Display version information.
	fmt.Printf("litestream %s\n", Version)

	if len(config.DBs) == 0 {
		return errors.New("configuration must specify at least one database")
	}

	for _, dbConfig := range config.DBs {
		db, err := newDBFromConfig(dbConfig)
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
	fmt.Printf("Initialized with %d databases.\n", len(c.DBs))

	// Serve metrics over HTTP if enabled.
	if config.Addr != "" {
		_, port, _ := net.SplitHostPort(config.Addr)
		fmt.Printf("Serving metrics on http://localhost:%s/metrics\n", port)
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(config.Addr, nil)
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

func (c *ReplicateCommand) Usage() {
	fmt.Printf(`
The replicate command starts a server to monitor & replicate databases 
specified in your configuration file.

Usage:

	litestream replicate [arguments]

Arguments:

	-config PATH
	    Specifies the configuration file. Defaults to %s

`[1:], DefaultConfigPath)
}
