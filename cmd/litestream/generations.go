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

type GenerationsCommand struct {
	ConfigPath string
	Config     Config

	DBPath string
}

func NewGenerationsCommand() *GenerationsCommand {
	return &GenerationsCommand{}
}

func (c *GenerationsCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-generations", flag.ContinueOnError)
	registerConfigFlag(fs, &c.ConfigPath)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	// Load configuration.
	if c.ConfigPath == "" {
		return errors.New("-config required")
	}
	config, err := ReadConfigFile(c.ConfigPath)
	if err != nil {
		return err
	}

	// Determine absolute path for database, if specified.
	if c.DBPath = fs.Arg(0); c.DBPath != "" {
		if c.DBPath, err = filepath.Abs(c.DBPath); err != nil {
			return err
		}
	}

	// List each generation.
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', 0)
	fmt.Fprintln(w, "db\tname\tgeneration\tlag\tstart\tend")
	for _, dbConfig := range config.DBs {
		// Filter database, if specified in the arguments.
		if c.DBPath != "" && dbConfig.Path != c.DBPath {
			continue
		}

		// Instantiate DB from from configuration.
		db, err := newDBFromConfig(dbConfig)
		if err != nil {
			return err
		}

		// Determine last time database or WAL was updated.
		updatedAt, err := db.UpdatedAt()
		if err != nil {
			return err
		}

		// Iterate over each replicator in the database.
		for _, r := range db.Replicators {
			generations, err := r.Generations(ctx)
			if err != nil {
				log.Printf("%s: cannot list generations", r.Name(), err)
				continue
			}

			// Iterate over each generation for the replicator.
			for _, generation := range generations {
				stats, err := r.GenerationStats(ctx, generation)
				if err != nil {
					log.Printf("%s: cannot find generation stats: %s", r.Name(), err)
					continue
				}

				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
					db.Path(),
					r.Name(),
					generation,
					truncateDuration(stats.UpdatedAt.Sub(updatedAt)).String(),
					stats.CreatedAt.Format(time.RFC3339),
					stats.UpdatedAt.Format(time.RFC3339),
				)
			}
		}
	}
	w.Flush()

	return nil
}

func (c *GenerationsCommand) Usage() {
	fmt.Printf(`
The generations command lists all generations across all replicas along with
stats about their lag behind the primary database and the time range they cover.

Usage:

	litestream generations [arguments] DB

Arguments:

	-config PATH
	    Specifies the configuration file. Defaults to %s

`[1:],
		DefaultConfigPath,
	)
}

func truncateDuration(d time.Duration) time.Duration {
	if d > time.Hour {
		return d.Truncate(time.Hour)
	} else if d > time.Minute {
		return d.Truncate(time.Minute)
	} else if d > time.Second {
		return d.Truncate(time.Second)
	} else if d > time.Millisecond {
		return d.Truncate(time.Millisecond)
	} else if d > time.Microsecond {
		return d.Truncate(time.Microsecond)
	}
	return d
}
