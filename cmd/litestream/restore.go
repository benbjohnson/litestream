package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/srilekha98/litestream"
)

// RestoreCommand represents a command to restore a database from a backup.
type RestoreCommand struct{}

// Run executes the command.
func (c *RestoreCommand) Run(ctx context.Context, args []string) (err error) {
	opt := litestream.NewRestoreOptions()

	fs := flag.NewFlagSet("litestream-restore", flag.ContinueOnError)
	configPath, noExpandEnv := registerConfigFlag(fs)
	fs.StringVar(&opt.OutputPath, "o", "", "output path")
	fs.StringVar(&opt.ReplicaName, "replica", "", "replica name")
	fs.StringVar(&opt.Generation, "generation", "", "generation name")
	fs.Var((*indexVar)(&opt.Index), "index", "wal index")
	fs.IntVar(&opt.Parallelism, "parallelism", opt.Parallelism, "parallelism")
	ifDBNotExists := fs.Bool("if-db-not-exists", false, "")
	ifReplicaExists := fs.Bool("if-replica-exists", false, "")
	timestampStr := fs.String("timestamp", "", "timestamp")
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

	// Determine replica & generation to restore from.
	var r *litestream.Replica
	if isURL(fs.Arg(0)) {
		if *configPath != "" {
			return fmt.Errorf("cannot specify a replica URL and the -config flag")
		}
		if r, err = c.loadFromURL(ctx, fs.Arg(0), *ifDBNotExists, &opt); err == errSkipDBExists {
			slog.Info("database already exists, skipping")
			return nil
		} else if err != nil {
			return err
		}
	} else {
		if *configPath == "" {
			*configPath = DefaultConfigPath()
		}
		if r, err = c.loadFromConfig(ctx, fs.Arg(0), *configPath, !*noExpandEnv, *ifDBNotExists, &opt); err == errSkipDBExists {
			slog.Info("database already exists, skipping")
			return nil
		} else if err != nil {
			return err
		}
	}

	// Return an error if no matching targets found.
	// If optional flag set, return success. Useful for automated recovery.
	if opt.Generation == "" {
		if *ifReplicaExists {
			slog.Info("no matching backups found")
			return nil
		}
		return fmt.Errorf("no matching backups found")
	}

	return r.Restore(ctx, opt)
}

// loadFromURL creates a replica & updates the restore options from a replica URL.
func (c *RestoreCommand) loadFromURL(ctx context.Context, replicaURL string, ifDBNotExists bool, opt *litestream.RestoreOptions) (*litestream.Replica, error) {
	if opt.OutputPath == "" {
		return nil, fmt.Errorf("output path required")
	}

	// Exit successfully if the output file already exists.
	if _, err := os.Stat(opt.OutputPath); !os.IsNotExist(err) && ifDBNotExists {
		return nil, errSkipDBExists
	}

	syncInterval := litestream.DefaultSyncInterval
	r, err := NewReplicaFromConfig(&ReplicaConfig{
		URL:          replicaURL,
		SyncInterval: &syncInterval,
	}, nil)
	if err != nil {
		return nil, err
	}
	opt.Generation, _, err = r.CalcRestoreTarget(ctx, *opt)
	return r, err
}

// loadFromConfig returns a replica & updates the restore options from a DB reference.
func (c *RestoreCommand) loadFromConfig(ctx context.Context, dbPath, configPath string, expandEnv, ifDBNotExists bool, opt *litestream.RestoreOptions) (*litestream.Replica, error) {
	// Load configuration.
	config, err := ReadConfigFile(configPath, expandEnv)
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
	db, err := NewDBFromConfig(dbConfig)
	if err != nil {
		return nil, err
	}

	// Restore into original database path if not specified.
	if opt.OutputPath == "" {
		opt.OutputPath = dbPath
	}

	// Exit successfully if the output file already exists.
	if _, err := os.Stat(opt.OutputPath); !os.IsNotExist(err) && ifDBNotExists {
		return nil, errSkipDBExists
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

	-no-expand-env
	    Disables environment variable expansion in configuration file.

	-replica NAME
	    Restore from a specific replica.
	    Defaults to replica with latest data.

	-generation NAME
	    Restore from a specific generation.
	    Defaults to generation with latest data.

	-index NUM
	    Restore up to a specific hex-encoded WAL index (inclusive).
	    Defaults to use the highest available index.

	-timestamp TIMESTAMP
	    Restore to a specific point-in-time.
	    Defaults to use the latest available backup.

	-o PATH
	    Output path of the restored database.
	    Defaults to original DB path.

	-if-db-not-exists
	    Returns exit code of 0 if the database already exists.

	-if-replica-exists
	    Returns exit code of 0 if no backups found.

	-parallelism NUM
	    Determines the number of WAL files downloaded in parallel.
	    Defaults to `+strconv.Itoa(litestream.DefaultRestoreParallelism)+`.


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

var errSkipDBExists = errors.New("database already exists, skipping")
