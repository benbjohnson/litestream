package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/benbjohnson/litestream"
)

type RestoreCommand struct {
	DBPath string
}

func (c *RestoreCommand) Run(ctx context.Context, args []string) (err error) {
	var configPath string
	var opt litestream.RestoreOptions
	fs := flag.NewFlagSet("litestream-restore", flag.ContinueOnError)
	registerConfigFlag(fs, &configPath)
	fs.StringVar(&opt.OutputPath, "o", "", "output path")
	fs.StringVar(&opt.ReplicaName, "replica", "", "replica name")
	fs.StringVar(&opt.Generation, "generation", "", "generation name")
	fs.BoolVar(&opt.DryRun, "dry-run", false, "dry run")
	timestampStr := fs.String("timestamp", "", "timestamp")
	verbose := fs.Bool("v", false, "verbose output")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 || fs.Arg(0) == "" {
		return fmt.Errorf("database path required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	// Load configuration.
	if configPath == "" {
		return errors.New("-config required")
	}
	config, err := ReadConfigFile(configPath)
	if err != nil {
		return err
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

	// Determine absolute path for database, if specified.
	if c.DBPath, err = filepath.Abs(fs.Arg(0)); err != nil {
		return err
	}

	// Instantiate DB.
	dbConfig := config.DBConfig(c.DBPath)
	if dbConfig == nil {
		return fmt.Errorf("database not found in config: %s", c.DBPath)
	}
	db, err := newDBFromConfig(dbConfig)
	if err != nil {
		return err
	}

	return db.Restore(ctx, opt)
}

func (c *RestoreCommand) Usage() {
	fmt.Printf(`
The restore command recovers a database from a previous snapshot and WAL.

Usage:

	litestream restore [arguments] DB

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
		DefaultConfigPath,
	)
}
