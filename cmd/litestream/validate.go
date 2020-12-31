package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/benbjohnson/litestream"
)

type ValidateCommand struct{}

func (c *ValidateCommand) Run(ctx context.Context, args []string) (err error) {
	var configPath string
	opt := litestream.NewRestoreOptions()
	fs := flag.NewFlagSet("litestream-validate", flag.ContinueOnError)
	registerConfigFlag(fs, &configPath)
	fs.StringVar(&opt.ReplicaName, "replica", "", "replica name")
	fs.BoolVar(&opt.DryRun, "dry-run", false, "dry run")
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

	// Verbose output is automatically enabled if dry run is specified.
	if opt.DryRun {
		*verbose = true
	}

	// Instantiate logger if verbose output is enabled.
	if *verbose {
		opt.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	// Determine absolute path for database.
	dbPath, err := filepath.Abs(fs.Arg(0))
	if err != nil {
		return err
	}

	// Instantiate DB.
	dbConfig := config.DBConfig(dbPath)
	if dbConfig == nil {
		return fmt.Errorf("database not found in config: %s", dbPath)
	}
	db, err := newDBFromConfig(dbConfig)
	if err != nil {
		return err
	}

	// Ensure replica exists, if specified.
	if opt.ReplicaName != "" && db.Replica(opt.ReplicaName) == nil {
		return fmt.Errorf("replica not found: %s", opt.ReplicaName)
	}

	// Validate all matching replicas.
	var hasInvalidReplica bool
	for _, r := range db.Replicas {
		if opt.ReplicaName != "" && opt.ReplicaName != r.Name() {
			continue
		}

		if err := db.Validate(ctx, r.Name(), opt); err != nil {
			fmt.Printf("%s: replica invalid: %s\n", r.Name(), err)
		}
	}

	if hasInvalidReplica {
		return fmt.Errorf("one or more invalid replicas found")
	}

	fmt.Println("ok")
	return nil
}

func (c *ValidateCommand) Usage() {
	fmt.Printf(`
The validate command compares a checksum of the primary database with a
checksum of the replica at the same point in time. Returns an error if the
databases are not equal.

The restored database must be written to a temporary file so you must ensure
you have enough disk space before performing this operation.

Usage:

	litestream validate [arguments] DB

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-replica NAME
	    Validate a specific replica.
	    Defaults to validating all replicas.

	-dry-run
	    Prints all log output as if it were running but does
	    not perform actual validation.

	-v
	    Verbose output.


Examples:

	# Validate all replicas for the given database.
	$ litestream validate /path/to/db

	# Validate only the S3 replica.
	$ litestream restore -replica s3 /path/to/db

`[1:],
		DefaultConfigPath,
	)
}
