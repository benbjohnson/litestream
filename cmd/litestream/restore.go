package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/benbjohnson/litestream"
)

// RestoreCommand represents a command to restore a database from a backup.
type RestoreCommand struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer

	snapshotIndex int // index of snapshot to start from

	// CLI options
	configPath      string // path to config file
	noExpandEnv     bool   // if true, do not expand env variables in config
	outputPath      string // path to restore database to
	replicaName     string // optional, name of replica to restore from
	generation      string // optional, generation to restore
	targetIndex     int    // optional, last WAL index to replay
	ifDBNotExists   bool   // if true, skips restore if output path already exists
	ifReplicaExists bool   // if true, skips if no backups exist
	opt             litestream.RestoreOptions
}

// NewRestoreCommand returns a new instance of RestoreCommand.
func NewRestoreCommand(stdin io.Reader, stdout, stderr io.Writer) *RestoreCommand {
	return &RestoreCommand{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,

		targetIndex: -1,
		opt:         litestream.NewRestoreOptions(),
	}
}

// Run executes the command.
func (c *RestoreCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-restore", flag.ContinueOnError)
	registerConfigFlag(fs, &c.configPath, &c.noExpandEnv)
	fs.StringVar(&c.outputPath, "o", "", "output path")
	fs.StringVar(&c.replicaName, "replica", "", "replica name")
	fs.StringVar(&c.generation, "generation", "", "generation name")
	fs.Var((*indexVar)(&c.targetIndex), "index", "wal index")
	fs.IntVar(&c.opt.Parallelism, "parallelism", c.opt.Parallelism, "parallelism")
	fs.BoolVar(&c.ifDBNotExists, "if-db-not-exists", false, "")
	fs.BoolVar(&c.ifReplicaExists, "if-replica-exists", false, "")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 || fs.Arg(0) == "" {
		return fmt.Errorf("database path or replica URL required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}
	pathOrURL := fs.Arg(0)

	// Ensure a generation is specified if target index is specified.
	if c.targetIndex != -1 && c.generation == "" {
		return fmt.Errorf("must specify -generation flag when using -index flag")
	}

	// Default to original database path if output path not specified.
	if !isURL(pathOrURL) && c.outputPath == "" {
		c.outputPath = pathOrURL
	}

	// Exit successfully if the output file already exists and flag is set.
	if _, err := os.Stat(c.outputPath); os.IsNotExist(err) {
		// file doesn't exist, continue
	} else if err != nil {
		return err
	} else if err == nil {
		if c.ifDBNotExists {
			fmt.Fprintln(c.stdout, "database already exists, skipping")
			return nil
		}
		return fmt.Errorf("output file already exists: %s", c.outputPath)
	}

	// Load configuration.
	config, err := ReadConfigFile(c.configPath, !c.noExpandEnv)
	if err != nil {
		return err
	}

	// Build replica from either a URL or config.
	r, err := c.loadReplica(ctx, config, pathOrURL)
	if err != nil {
		return err
	}

	// Determine latest generation if one is not specified.
	if c.generation == "" {
		if c.generation, err = litestream.FindLatestGeneration(ctx, r.Client()); err == litestream.ErrNoGeneration {
			// Return an error if no matching targets found.
			// If optional flag set, return success. Useful for automated recovery.
			if c.ifReplicaExists {
				fmt.Fprintln(c.stdout, "no matching backups found, skipping")
				return nil
			}
			return fmt.Errorf("no matching backups found")
		} else if err != nil {
			return fmt.Errorf("cannot determine latest generation: %w", err)
		}
	}

	// Determine the maximum available index for the generation if one is not specified.
	if c.targetIndex == -1 {
		if c.targetIndex, err = litestream.FindMaxIndexByGeneration(ctx, r.Client(), c.generation); err != nil {
			return fmt.Errorf("cannot determine latest index in generation %q: %w", c.generation, err)
		}
	}

	// Find lastest snapshot that occurs before the index.
	// TODO: Optionally allow -snapshot-index
	if c.snapshotIndex, err = litestream.FindSnapshotForIndex(ctx, r.Client(), c.generation, c.targetIndex); err != nil {
		return fmt.Errorf("cannot find snapshot index: %w", err)
	}

	// Create parent directory if it doesn't already exist.
	if err := os.MkdirAll(filepath.Dir(c.outputPath), 0700); err != nil {
		return fmt.Errorf("cannot create parent directory: %w", err)
	}

	c.opt.Logger = log.New(c.stdout, "", log.LstdFlags|log.Lmicroseconds)

	return litestream.Restore(ctx, r.Client(), c.outputPath, c.generation, c.snapshotIndex, c.targetIndex, c.opt)
}

func (c *RestoreCommand) loadReplica(ctx context.Context, config Config, arg string) (*litestream.Replica, error) {
	if isURL(arg) {
		return c.loadReplicaFromURL(ctx, config, arg)
	}
	return c.loadReplicaFromConfig(ctx, config, arg)
}

// loadReplicaFromURL creates a replica & updates the restore options from a replica URL.
func (c *RestoreCommand) loadReplicaFromURL(ctx context.Context, config Config, replicaURL string) (*litestream.Replica, error) {
	if c.replicaName != "" {
		return nil, fmt.Errorf("cannot specify both the replica URL and the -replica flag")
	} else if c.outputPath == "" {
		return nil, fmt.Errorf("output path required when using a replica URL")
	}

	syncInterval := litestream.DefaultSyncInterval
	return NewReplicaFromConfig(&ReplicaConfig{
		URL:             replicaURL,
		AccessKeyID:     config.AccessKeyID,
		SecretAccessKey: config.SecretAccessKey,
		SyncInterval:    &syncInterval,
	}, nil)
}

// loadReplicaFromConfig returns replicas based on the specific config path.
func (c *RestoreCommand) loadReplicaFromConfig(ctx context.Context, config Config, dbPath string) (_ *litestream.Replica, err error) {
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
	} else if len(db.Replicas) == 0 {
		return nil, fmt.Errorf("database has no replicas: %s", dbPath)
	}

	// Filter by replica name if specified.
	if c.replicaName != "" {
		r := db.Replica(c.replicaName)
		if r == nil {
			return nil, fmt.Errorf("replica %q not found", c.replicaName)
		}
		return r, nil
	}

	// Choose only replica if only one available and no name is specified.
	if len(db.Replicas) == 1 {
		return db.Replicas[0], nil
	}

	// A replica must be specified when restoring a specific generation with multiple replicas.
	if c.generation != "" {
		return nil, fmt.Errorf("must specify -replica flag when restoring from a specific generation")
	}

	// Determine latest replica to restore from.
	r, err := litestream.LatestReplica(ctx, db.Replicas)
	if err != nil {
		return nil, fmt.Errorf("cannot determine latest replica: %w", err)
	}
	return r, nil
}

// Usage prints the help screen to STDOUT.
func (c *RestoreCommand) Usage() {
	fmt.Fprintf(c.stdout, `
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

	-v
	    Verbose output.


Examples:

	# Restore latest replica for database to original location.
	$ litestream restore /path/to/db

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
