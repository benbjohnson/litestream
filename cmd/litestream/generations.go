package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"
)

type GenerationsCommand struct{}

func (c *GenerationsCommand) Run(ctx context.Context, args []string) (err error) {
	var configPath string
	fs := flag.NewFlagSet("litestream-generations", flag.ContinueOnError)
	registerConfigFlag(fs, &configPath)
	replicaName := fs.String("replica", "", "replica name")
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

	// Determine absolute path for database.
	dbPath, err := filepath.Abs(fs.Arg(0))
	if err != nil {
		return err
	}

	// Instantiate DB from from configuration.
	dbConfig := config.DBConfig(dbPath)
	if dbConfig == nil {
		return fmt.Errorf("database not found in config: %s", dbPath)
	}
	db, err := newDBFromConfig(&config, dbConfig)
	if err != nil {
		return err
	}

	// Determine last time database or WAL was updated.
	updatedAt, err := db.UpdatedAt()
	if err != nil {
		return err
	}

	// List each generation.
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', 0)
	fmt.Fprintln(w, "name\tgeneration\tlag\tstart\tend")
	for _, r := range db.Replicas {
		if *replicaName != "" && r.Name() != *replicaName {
			continue
		}

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
			w.Flush()
		}
	}
	w.Flush()

	return nil
}

func (c *GenerationsCommand) Usage() {
	fmt.Printf(`
The generations command lists all generations for a database. It also lists
stats about their lag behind the primary database and the time range they cover.

Usage:

	litestream generations [arguments] DB

Arguments:

	-config PATH
	    Specifies the configuration file. Defaults to %s

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
