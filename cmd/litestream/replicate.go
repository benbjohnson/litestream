package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/s3"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReplicateCommand represents a command that continuously replicates SQLite databases.
type ReplicateCommand struct {
	Config Config

	// List of managed databases specified in the config.
	DBs []*litestream.DB
}

func NewReplicateCommand() *ReplicateCommand {
	return &ReplicateCommand{}
}

// ParseFlags parses the CLI flags and loads the configuration file.
func (c *ReplicateCommand) ParseFlags(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-replicate", flag.ContinueOnError)
	tracePath := fs.String("trace", "", "trace path")
	configPath, noExpandEnv := registerConfigFlag(fs)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Load configuration or use CLI args to build db/replica.
	if fs.NArg() == 1 {
		return fmt.Errorf("must specify at least one replica URL for %s", fs.Arg(0))
	} else if fs.NArg() > 1 {
		if *configPath != "" {
			return fmt.Errorf("cannot specify a replica URL and the -config flag")
		}

		dbConfig := &DBConfig{Path: fs.Arg(0)}
		for _, u := range fs.Args()[1:] {
			syncInterval := litestream.DefaultSyncInterval
			dbConfig.Replicas = append(dbConfig.Replicas, &ReplicaConfig{
				URL:          u,
				SyncInterval: &syncInterval,
			})
		}
		c.Config.DBs = []*DBConfig{dbConfig}
	} else {
		if *configPath == "" {
			*configPath = DefaultConfigPath()
		}
		if c.Config, err = ReadConfigFile(*configPath, !*noExpandEnv); err != nil {
			return err
		}
	}

	// Enable trace logging.
	if *tracePath != "" {
		f, err := os.Create(*tracePath)
		if err != nil {
			return err
		}
		defer f.Close()
		litestream.Tracef = log.New(f, "", log.LstdFlags|log.Lmicroseconds|log.LUTC|log.Lshortfile).Printf
	}

	return nil
}

// Run loads all databases specified in the configuration.
func (c *ReplicateCommand) Run(ctx context.Context) (err error) {
	// Display version information.
	log.Printf("litestream %s", Version)

	if len(c.Config.DBs) == 0 {
		log.Println("no databases specified in configuration")
	}

	for _, dbConfig := range c.Config.DBs {
		db, err := NewDBFromConfig(dbConfig)
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
		log.Printf("initialized db: %s", db.Path())
		for _, r := range db.Replicas {
			switch client := r.Client.(type) {
			case *file.ReplicaClient:
				log.Printf("replicating to: name=%q type=%q path=%q", r.Name(), client.Type(), client.Path())
			case *s3.ReplicaClient:
				log.Printf("replicating to: name=%q type=%q bucket=%q path=%q region=%q endpoint=%q sync-interval=%s", r.Name(), client.Type(), client.Bucket, client.Path, client.Region, client.Endpoint, r.SyncInterval)
			default:
				log.Printf("replicating to: name=%q type=%q", r.Name(), client.Type())
			}
		}
	}

	// Serve metrics over HTTP if enabled.
	if c.Config.Addr != "" {
		hostport := c.Config.Addr
		if host, port, _ := net.SplitHostPort(c.Config.Addr); port == "" {
			return fmt.Errorf("must specify port for bind address: %q", c.Config.Addr)
		} else if host == "" {
			hostport = net.JoinHostPort("localhost", port)
		}

		log.Printf("serving metrics on http://%s/metrics", hostport)
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(c.Config.Addr, nil); err != nil {
				log.Printf("cannot start metrics server: %s", err)
			}
		}()
	}

	return nil
}

// Close closes all open databases.
func (c *ReplicateCommand) Close() (err error) {
	for _, db := range c.DBs {
		if e := db.SoftClose(); e != nil {
			log.Printf("error closing db: path=%s err=%s", db.Path(), e)
			if err == nil {
				err = e
			}
		}
	}
	// TODO(windows): Clear DBs
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

	-no-expand-env
	    Disables environment variable expansion in configuration file.

	-trace PATH
	    Write verbose trace logging to PATH.

`[1:], DefaultConfigPath())
}
