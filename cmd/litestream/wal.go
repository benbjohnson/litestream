package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/benbjohnson/litestream"
)

// WALCommand represents a command to list WAL files for a database.
type WALCommand struct{}

// Run executes the command.
func (c *WALCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-wal", flag.ContinueOnError)
	configPath, noExpandEnv := registerConfigFlag(fs)
	replicaName := fs.String("replica", "", "replica name")
	generation := fs.String("generation", "", "generation name")
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

	// Find WAL files by db or replica.
	var replicas []*litestream.Replica
	if r != nil {
		replicas = []*litestream.Replica{r}
	} else {
		replicas = db.Replicas
	}

	// List all WAL files.
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "replica\tgeneration\tindex\toffset\tsize\tcreated")
	for _, r := range replicas {
		var generations []string
		if *generation != "" {
			generations = []string{*generation}
		} else {
			if generations, err = r.Client.Generations(ctx); err != nil {
				r.Logger().Error("cannot determine generations", "error", err)
				continue
			}
		}

		for _, generation := range generations {
			if err := func() error {
				itr, err := r.Client.WALSegments(ctx, generation)
				if err != nil {
					return err
				}
				defer itr.Close()

				for itr.Next() {
					info := itr.WALSegment()

					fmt.Fprintf(w, "%s\t%s\t%x\t%d\t%d\t%s\n",
						r.Name(),
						info.Generation,
						info.Index,
						info.Offset,
						info.Size,
						info.CreatedAt.Format(time.RFC3339),
					)
				}
				return itr.Close()
			}(); err != nil {
				r.Logger().Error("cannot fetch wal segments", "error", err)
				continue
			}
		}
	}

	return nil
}

// Usage prints the help screen to STDOUT.
func (c *WALCommand) Usage() {
	fmt.Printf(`
The wal command lists all wal segments available for a database.

Usage:

	litestream wal [arguments] DB_PATH

	litestream wal [arguments] REPLICA_URL

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-no-expand-env
	    Disables environment variable expansion in configuration file.

	-replica NAME
	    Optional, filter by a specific replica.

	-generation NAME
	    Optional, filter by a specific generation.

Examples:

	# List all WAL segments for a database.
	$ litestream wal /path/to/db

	# List all WAL segments on S3 for a specific generation.
	$ litestream wal -replica s3 -generation xxxxxxxx /path/to/db

	# List all WAL segments for replica URL.
	$ litestream wal s3://mybkt/db

`[1:],
		DefaultConfigPath(),
	)
}
