package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/benbjohnson/litestream"
)

// SnapshotsCommand represents a command to list snapshots for a command.
type SnapshotsCommand struct{}

// Run executes the command.
func (c *SnapshotsCommand) Run(ctx context.Context, args []string) (err error) {
	var configPath string
	fs := flag.NewFlagSet("litestream-snapshots", flag.ContinueOnError)
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

	// Instantiate DB.
	dbConfig := config.DBConfig(dbPath)
	if dbConfig == nil {
		return fmt.Errorf("database not found in config: %s", dbPath)
	}
	db, err := newDBFromConfig(&config, dbConfig)
	if err != nil {
		return err
	}

	// Find snapshots by db or replica.
	var infos []*litestream.SnapshotInfo
	if *replicaName != "" {
		if r := db.Replica(*replicaName); r == nil {
			return fmt.Errorf("replica %q not found for database %q", *replicaName, dbPath)
		} else if infos, err = r.Snapshots(ctx); err != nil {
			return err
		}
	} else {
		if infos, err = db.Snapshots(ctx); err != nil {
			return err
		}
	}

	// List all snapshots.
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', 0)
	fmt.Fprintln(w, "replica\tgeneration\tindex\tsize\tcreated")
	for _, info := range infos {
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\n",
			info.Replica,
			info.Generation,
			info.Index,
			info.Size,
			info.CreatedAt.Format(time.RFC3339),
		)
	}
	w.Flush()

	return nil
}

// Usage prints the help screen to STDOUT.
func (c *SnapshotsCommand) Usage() {
	fmt.Printf(`
The snapshots command lists all snapshots available for a database.

Usage:

	litestream snapshots [arguments] DB

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-replica NAME
	    Optional, filter by a specific replica.


Examples:

	# List all snapshots for a database.
	$ litestream snapshots /path/to/db

	# List all snapshots on S3.
	$ litestream snapshots -replica s3 /path/to/db

`[1:],
		DefaultConfigPath(),
	)
}
