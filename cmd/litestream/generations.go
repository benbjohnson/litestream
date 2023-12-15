package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/benbjohnson/litestream"
)

// GenerationsCommand represents a command to list all generations for a database.
type GenerationsCommand struct{}

// Run executes the command.
func (c *GenerationsCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-generations", flag.ContinueOnError)
	configPath, noExpandEnv := registerConfigFlag(fs)
	replicaName := fs.String("replica", "", "replica name")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 || fs.Arg(0) == "" {
		return fmt.Errorf("database path or replica URL required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	var db *litestream.DB
	var r *litestream.Replica
	dbUpdatedAt := time.Now()
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

		// Determine last time database or WAL was updated.
		if dbUpdatedAt, err = db.UpdatedAt(); err != nil {
			return err
		}
	}

	var replicas []*litestream.Replica
	if r != nil {
		replicas = []*litestream.Replica{r}
	} else {
		replicas = db.Replicas
	}

	// List each generation.
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "name\tgeneration\tlag\tstart\tend")
	for _, r := range replicas {
		generations, err := r.Client.Generations(ctx)
		if err != nil {
			r.Logger().Error("cannot list generations", "error", err)
			continue
		}

		sort.Strings(generations)

		// Iterate over each generation for the replica.
		for _, generation := range generations {
			createdAt, updatedAt, err := r.GenerationTimeBounds(ctx, generation)
			if err != nil {
				r.Logger().Error("cannot determine generation time bounds", "error", err)
				continue
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				r.Name(),
				generation,
				truncateDuration(dbUpdatedAt.Sub(updatedAt)).String(),
				createdAt.Format(time.RFC3339),
				updatedAt.Format(time.RFC3339),
			)
		}
	}

	return nil
}

// Usage prints the help message to STDOUT.
func (c *GenerationsCommand) Usage() {
	fmt.Printf(`
The generations command lists all generations for a database or replica. It also
lists stats about their lag behind the primary database and the time range they
cover.

Usage:

	litestream generations [arguments] DB_PATH

	litestream generations [arguments] REPLICA_URL

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-no-expand-env
	    Disables environment variable expansion in configuration file.

	-replica NAME
	    Optional, filters by replica.

`[1:],
		DefaultConfigPath(),
	)
}

func truncateDuration(d time.Duration) time.Duration {
	if d < 0 {
		if d < -10*time.Second {
			return d.Truncate(time.Second)
		} else if d < -time.Second {
			return d.Truncate(time.Second / 10)
		} else if d < -time.Millisecond {
			return d.Truncate(time.Millisecond)
		} else if d < -time.Microsecond {
			return d.Truncate(time.Microsecond)
		}
		return d
	}

	if d > 10*time.Second {
		return d.Truncate(time.Second)
	} else if d > time.Second {
		return d.Truncate(time.Second / 10)
	} else if d > time.Millisecond {
		return d.Truncate(time.Millisecond)
	} else if d > time.Microsecond {
		return d.Truncate(time.Microsecond)
	}
	return d
}
