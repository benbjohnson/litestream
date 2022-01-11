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

// SnapshotsCommand represents a command to list snapshots for a command.
type SnapshotsCommand struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer

	configPath  string
	noExpandEnv bool

	replicaName string
}

// NewSnapshotsCommand returns a new instance of SnapshotsCommand.
func NewSnapshotsCommand(stdin io.Reader, stdout, stderr io.Writer) *SnapshotsCommand {
	return &SnapshotsCommand{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}
}

// Run executes the command.
func (c *SnapshotsCommand) Run(ctx context.Context, args []string) (ret error) {
	fs := flag.NewFlagSet("litestream-snapshots", flag.ContinueOnError)
	registerConfigFlag(fs, &c.configPath, &c.noExpandEnv)
	fs.StringVar(&c.replicaName, "replica", "", "replica name")
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

	// Determine list of replicas to pull snapshots from.
	replicas, _, err := loadReplicas(ctx, config, fs.Arg(0), c.replicaName)
	if err != nil {
		return err
	}

	// Build list of snapshot metadata with associated replica.
	var infos []replicaSnapshotInfo
	for _, r := range replicas {
		a, err := r.Snapshots(ctx)
		if err != nil {
			log.Printf("cannot determine snapshots: %s", err)
			ret = errExit // signal error return without printing message
			continue
		}
		for i := range a {
			infos = append(infos, replicaSnapshotInfo{SnapshotInfo: a[i], replicaName: r.Name()})
		}
	}

	// Sort snapshots by creation time from newest to oldest.
	sort.Slice(infos, func(i, j int) bool { return infos[i].CreatedAt.After(infos[j].CreatedAt) })

	// List all snapshots.
	w := tabwriter.NewWriter(c.stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "replica\tgeneration\tindex\tsize\tcreated")
	for _, info := range infos {
		fmt.Fprintf(w, "%s\t%s\t%08x\t%d\t%s\n",
			info.replicaName,
			info.Generation,
			info.Index,
			info.Size,
			info.CreatedAt.Format(time.RFC3339),
		)
	}

	return ret
}

// Usage prints the help screen to STDOUT.
func (c *SnapshotsCommand) Usage() {
	fmt.Fprintf(c.stdout, `
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

// replicaSnapshotInfo represents snapshot metadata with associated replica name.
type replicaSnapshotInfo struct {
	litestream.SnapshotInfo
	replicaName string
}
