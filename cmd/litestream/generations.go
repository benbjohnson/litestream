package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

// GenerationsCommand represents a command to list all generations for a database.
type GenerationsCommand struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer

	configPath  string
	noExpandEnv bool

	replicaName string
}

// NewGenerationsCommand returns a new instance of GenerationsCommand.
func NewGenerationsCommand(stdin io.Reader, stdout, stderr io.Writer) *GenerationsCommand {
	return &GenerationsCommand{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}
}

// Run executes the command.
func (c *GenerationsCommand) Run(ctx context.Context, args []string) (ret error) {
	fs := flag.NewFlagSet("litestream-generations", flag.ContinueOnError)
	registerConfigFlag(fs, &c.configPath, &c.noExpandEnv)
	fs.StringVar(&c.replicaName, "replica", "", "replica name")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.Arg(0) == "" {
		return fmt.Errorf("database path or replica URL required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	// Load configuration.
	config, err := ReadConfigFile(c.configPath, !c.noExpandEnv)
	if err != nil {
		return err
	}

	replicas, db, err := loadReplicas(ctx, config, fs.Arg(0), c.replicaName)
	if err != nil {
		return err
	}

	// Determine last time database or WAL was updated.
	var dbUpdatedAt time.Time
	if db != nil {
		if dbUpdatedAt, err = db.UpdatedAt(); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	// List each generation.
	w := tabwriter.NewWriter(c.stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "name\tgeneration\tlag\tstart\tend")

	for _, r := range replicas {
		generations, err := r.Client().Generations(ctx)
		if err != nil {
			fmt.Fprintf(c.stderr, "%s: cannot list generations: %s", r.Name(), err)
			ret = errExit // signal error return without printing message
			continue
		}

		// Iterate over each generation for the replica.
		for _, generation := range generations {
			createdAt, updatedAt, err := litestream.GenerationTimeBounds(ctx, r.Client(), generation)
			if err != nil {
				fmt.Fprintf(c.stderr, "%s: cannot determine generation time bounds: %s", r.Name(), err)
				ret = errExit // signal error return without printing message
				continue
			}

			// Calculate lag from database mod time to the replica mod time.
			// This is ignored if the database mod time is unavailable such as
			// when specifying the replica URL or if the database file is missing.
			lag := "-"
			if !dbUpdatedAt.IsZero() {
				lag = internal.TruncateDuration(dbUpdatedAt.Sub(updatedAt)).String()
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				r.Name(),
				generation,
				lag,
				createdAt.Format(time.RFC3339),
				updatedAt.Format(time.RFC3339),
			)
		}
	}

	return ret
}

// Usage prints the help message to STDOUT.
func (c *GenerationsCommand) Usage() {
	fmt.Fprintf(c.stdout, `
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
