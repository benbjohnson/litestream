package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/benbjohnson/litestream"
)

// RestoreCommand represents a command to restore a database from a backup.
type RestoreCommand struct{}

// Run executes the command.
func (c *RestoreCommand) Run(ctx context.Context, args []string) (err error) {
	var configPath string
	opt := litestream.NewRestoreOptions()
	opt.Verbose = true

	fs := flag.NewFlagSet("litestream-restore", flag.ContinueOnError)
	registerConfigFlag(fs, &configPath)
	fs.StringVar(&opt.OutputPath, "o", "", "output path")
	fs.StringVar(&opt.ReplicaName, "replica", "", "replica name")
	fs.StringVar(&opt.Generation, "generation", "", "generation name")
	fs.IntVar(&opt.Index, "index", opt.Index, "wal index")
	fs.BoolVar(&opt.DryRun, "dry-run", false, "dry run")
	timestampStr := fs.String("timestamp", "", "timestamp")
	verbose := fs.Bool("v", false, "verbose output")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 || fs.Arg(0) == "" {
		return fmt.Errorf("database path or replica URL required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	// Parse timestamp, if specified.
	if *timestampStr != "" {
		if opt.Timestamp, err = time.Parse(time.RFC3339, *timestampStr); err != nil {
			return errors.New("invalid -timestamp, must specify in ISO 8601 format (e.g. 2000-01-01T00:00:00Z)")
		}
	}

	// Verbose output is automatically enabled if dry run is specified.
	if opt.DryRun {
		*verbose = true
	}

	// Instantiate logger if verbose output is enabled.
	if *verbose {
		opt.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	// Determine replica & generation to restore from.
	var r litestream.Replica
	if isURL(fs.Arg(0)) {
		if r, err = c.loadFromURL(ctx, fs.Arg(0), &opt); err != nil {
			return err
		}
	} else if configPath != "" {
		if r, err = c.loadFromConfig(ctx, fs.Arg(0), configPath, &opt); err != nil {
			return err
		}
	} else {
		return errors.New("config path or replica URL required")
	}

	// Return an error if no matching targets found.
	if opt.Generation == "" {
		return fmt.Errorf("no matching backups found")
	}

	return litestream.RestoreReplica(ctx, r, opt)
}

// loadFromURL creates a replica & updates the restore options from a replica URL.
func (c *RestoreCommand) loadFromURL(ctx context.Context, replicaURL string, opt *litestream.RestoreOptions) (litestream.Replica, error) {
	r, err := NewReplicaFromURL(replicaURL)
	if err != nil {
		return nil, err
	}
	opt.Generation, _, err = litestream.CalcReplicaRestoreTarget(ctx, r, *opt)
	return r, err
}

// loadFromConfig returns a replica & updates the restore options from a DB reference.
func (c *RestoreCommand) loadFromConfig(ctx context.Context, dbPath, configPath string, opt *litestream.RestoreOptions) (litestream.Replica, error) {
	// Load configuration.
	config, err := ReadConfigFile(configPath)
	if err != nil {
		return nil, err
	}

	// Lookup database from configuration file by path.
	if dbPath, err = expand(dbPath); err != nil {
		return nil, err
	}
	dbConfig := config.DBConfig(dbPath)
	if dbConfig == nil {
		return nil, fmt.Errorf("database not found in config: %s", dbPath)
	}
	db, err := newDBFromConfig(&config, dbConfig)
	if err != nil {
		return nil, err
	}

	// Restore into original database path if not specified.
	if opt.OutputPath == "" {
		opt.OutputPath = dbPath
	}

	// Determine the appropriate replica & generation to restore from,
	r, generation, err := db.CalcRestoreTarget(ctx, *opt)
	if err != nil {
		return nil, err
	}
	opt.Generation = generation

	return r, nil
}

// Usage prints the help screen to STDOUT.
func (c *RestoreCommand) Usage() {
	fmt.Printf(`
The restore command recovers a database from a previous snapshot and WAL.

Usage:

	litestream restore [arguments] DB_PATH

	litestream restore [arguments] REPLICA_URL

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-replica NAME
	    Restore from a specific replica.
	    Defaults to replica with latest data.

	-generation NAME
	    Restore from a specific generation.
	    Defaults to generation with latest data.

	-index NUM
	    Restore up to a specific WAL index (inclusive).
	    Defaults to use the highest available index.

	-timestamp TIMESTAMP
	    Restore to a specific point-in-time.
	    Defaults to use the latest available backup.

	-o PATH
	    Output path of the restored database.
	    Defaults to original DB path.

	-dry-run
	    Prints all log output as if it were running but does
	    not perform actual restore.

	-v
	    Verbose output.


Examples:

	# Restore latest replica for database to original location.
	$ litestream restore /path/to/db

	# Restore replica for database to a given point in time.
	$ litestream restore -timestamp 2020-01-01T00:00:00Z /path/to/db

	# Restore latest replica for database to new /tmp directory
	$ litestream restore -o /tmp/db /path/to/db

	# Restore database from latest generation on S3.
	$ litestream restore -replica s3 /path/to/db

	# Restore database from specific generation on S3.
	$ litestream restore -replica s3 -generation xxxxxxxx /path/to/db

`[1:],
		DefaultConfigPath(),
	)
}
