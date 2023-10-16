package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"text/tabwriter"
	"time"

	"github.com/benbjohnson/litestream"
)

// SnapshotsCommand represents a command to list snapshots for a command.
type SnapshotsCommand struct{}

// Run executes the command.
func (c *SnapshotsCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-snapshots", flag.ContinueOnError)
	configPath, noExpandEnv := registerConfigFlag(fs)
	replicaName := fs.String("replica", "", "replica name")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 || fs.Arg(0) == "" {
		return fmt.Errorf("database path required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	var db *litestream.DB
	var r *litestream.Replica
	if isURL(fs.Arg(0)) {
		if *configPath != "" {
			return fmt.Errorf("cannot specify a replica URL and the -config flag")
		}
		if r, err = NewReplicaFromConfig(&ReplicaConfig{URL: fs.Arg(0)}, nil); err != nil {
			return err
		}
	} else {
		if *configPath == "" {
			*configPath = DefaultConfigPath()
		}

		// Load configuration.
		config, err := ReadConfigFile(*configPath, !*noExpandEnv)
		if err != nil {
			return err
		}

		// Lookup database from configuration file by path.
		if path, err := expand(fs.Arg(0)); err != nil {
			return err
		} else if dbc := config.DBConfig(path); dbc == nil {
			return fmt.Errorf("database not found in config: %s", path)
		} else if db, err = NewDBFromConfig(dbc); err != nil {
			return err
		}

		// Filter by replica, if specified.
		if *replicaName != "" {
			if r = db.Replica(*replicaName); r == nil {
				return fmt.Errorf("replica %q not found for database %q", *replicaName, db.Path())
			}
		}
	}

	// Find snapshots by db or replica.
	var replicas []*litestream.Replica
	if r != nil {
		replicas = []*litestream.Replica{r}
	} else {
		replicas = db.Replicas
	}

	// List all snapshots.
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "replica\tgeneration\tindex\tsize\tcreated")
	for _, r := range replicas {
		infos, err := r.Snapshots(ctx)
		if err != nil {
			slog.Error("cannot determine snapshots", "error", err)
			continue
		}
		for _, info := range infos {
			fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\n",
				r.Name(),
				info.Generation,
				info.Index,
				info.Size,
				info.CreatedAt.Format(time.RFC3339),
			)
		}
	}

	return nil
}

// Usage prints the help screen to STDOUT.
func (c *SnapshotsCommand) Usage() {
	fmt.Printf(`
The snapshots command lists all snapshots available for a database or replica.

Usage:

	litestream snapshots [arguments] DB_PATH

	litestream snapshots [arguments] REPLICA_URL

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-no-expand-env
	    Disables environment variable expansion in configuration file.

	-replica NAME
	    Optional, filter by a specific replica.

Examples:

	# List all snapshots for a database.
	$ litestream snapshots /path/to/db

	# List all snapshots on S3.
	$ litestream snapshots -replica s3 /path/to/db

	# List all snapshots by replica URL.
	$ litestream snapshots s3://mybkt/db

`[1:],
		DefaultConfigPath(),
	)
}
