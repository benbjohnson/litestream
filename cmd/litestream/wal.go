package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/benbjohnson/litestream"
)

// WALCommand represents a command to list WAL files for a database.
type WALCommand struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer

	configPath  string
	noExpandEnv bool

	replicaName string
	generation  string
}

// NewWALCommand returns a new instance of WALCommand.
func NewWALCommand(stdin io.Reader, stdout, stderr io.Writer) *WALCommand {
	return &WALCommand{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}
}

// Run executes the command.
func (c *WALCommand) Run(ctx context.Context, args []string) (ret error) {
	fs := flag.NewFlagSet("litestream-wal", flag.ContinueOnError)
	registerConfigFlag(fs, &c.configPath, &c.noExpandEnv)
	fs.StringVar(&c.replicaName, "replica", "", "replica name")
	fs.StringVar(&c.generation, "generation", "", "generation name")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 || fs.Arg(0) == "" {
		return fmt.Errorf("database path or replica URL required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	// Load configuration.
	config, err := ReadConfigFile(c.configPath, !c.noExpandEnv)
	if err != nil {
		return err
	}

	// Build list of replicas from CLI flags.
	replicas, _, err := loadReplicas(ctx, config, fs.Arg(0), c.replicaName)
	if err != nil {
		return err
	}

	// Build list of WAL metadata with associated replica.
	var infos []replicaWALSegmentInfo
	for _, r := range replicas {
		var generations []string
		if c.generation != "" {
			generations = []string{c.generation}
		} else {
			if generations, err = r.Client().Generations(ctx); err != nil {
				log.Printf("%s: cannot determine generations: %s", r.Name(), err)
				ret = errExit // signal error return without printing message
				continue
			}
		}

		for _, generation := range generations {
			if err := func() error {
				itr, err := r.Client().WALSegments(ctx, generation)
				if err != nil {
					return err
				}
				defer itr.Close()

				for itr.Next() {
					infos = append(infos, replicaWALSegmentInfo{
						WALSegmentInfo: itr.WALSegment(),
						replicaName:    r.Name(),
					})
				}
				return itr.Close()
			}(); err != nil {
				log.Printf("%s: cannot fetch wal segments: %s", r.Name(), err)
				ret = errExit // signal error return without printing message
				continue
			}
		}
	}

	// Sort WAL segments by creation time from newest to oldest.
	sort.Slice(infos, func(i, j int) bool { return infos[i].CreatedAt.After(infos[j].CreatedAt) })

	// List all WAL files.
	w := tabwriter.NewWriter(c.stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "replica\tgeneration\tindex\toffset\tsize\tcreated")
	for _, info := range infos {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\n",
			info.replicaName,
			info.Generation,
			litestream.FormatIndex(info.Index),
			litestream.FormatOffset(info.Offset),
			info.Size,
			info.CreatedAt.Format(time.RFC3339),
		)
	}

	return ret
}

// Usage prints the help screen to STDOUT.
func (c *WALCommand) Usage() {
	fmt.Fprintf(c.stdout, `
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

// replicaWALSegmentInfo represents WAL segment metadata with associated replica name.
type replicaWALSegmentInfo struct {
	litestream.WALSegmentInfo
	replicaName string
}
