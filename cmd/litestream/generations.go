package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"github.com/benbjohnson/litestream"
)

// GenerationsCommand represents a command to list all generations for a database.
type GenerationsCommand struct{}

// Run executes the command.
func (c *GenerationsCommand) Run(ctx context.Context, args []string) (err error) {
	var configPath string
	fs := flag.NewFlagSet("litestream-generations", flag.ContinueOnError)
	registerConfigFlag(fs, &configPath)
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
	var r litestream.Replica
	updatedAt := time.Now()
	if isURL(fs.Arg(0)) {
		if r, err = NewReplicaFromURL(fs.Arg(0)); err != nil {
			return err
		}
	} else if configPath != "" {
		// Load configuration.
		config, err := ReadConfigFile(configPath)
		if err != nil {
			return err
		}

		// Lookup database from configuration file by path.
		if path, err := expand(fs.Arg(0)); err != nil {
			return err
		} else if dbc := config.DBConfig(path); dbc == nil {
			return fmt.Errorf("database not found in config: %s", path)
		} else if db, err = newDBFromConfig(&config, dbc); err != nil {
			return err
		}

		// Filter by replica, if specified.
		if *replicaName != "" {
			if r = db.Replica(*replicaName); r == nil {
				return fmt.Errorf("replica %q not found for database %q", *replicaName, db.Path())
			}
		}

		// Determine last time database or WAL was updated.
		if updatedAt, err = db.UpdatedAt(); err != nil {
			return err
		}
	} else {
		return errors.New("config path or replica URL required")
	}

	var replicas []litestream.Replica
	if r != nil {
		replicas = []litestream.Replica{r}
	} else {
		replicas = db.Replicas
	}

	// List each generation.
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "name\tgeneration\tlag\tstart\tend")
	for _, r := range replicas {
		generations, err := r.Generations(ctx)
		if err != nil {
			log.Printf("%s: cannot list generations: %s", r.Name(), err)
			continue
		}

		// Iterate over each generation for the replica.
		for _, generation := range generations {
			stats, err := r.GenerationStats(ctx, generation)
			if err != nil {
				log.Printf("%s: cannot find generation stats: %s", r.Name(), err)
				continue
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				r.Name(),
				generation,
				truncateDuration(updatedAt.Sub(stats.UpdatedAt)).String(),
				stats.CreatedAt.Format(time.RFC3339),
				stats.UpdatedAt.Format(time.RFC3339),
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
