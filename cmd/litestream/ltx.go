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

// LTXCommand represents a command to list LTX files for a database.
type LTXCommand struct{}

// Run executes the command.
func (c *LTXCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-ltx", flag.ContinueOnError)
	configPath, noExpandEnv := registerConfigFlag(fs)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 || fs.Arg(0) == "" {
		return fmt.Errorf("database path required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	var r *litestream.Replica
	if litestream.IsURL(fs.Arg(0)) {
		if *configPath != "" {
			return fmt.Errorf("cannot specify a replica URL and the -config flag")
		}
		if r, err = NewReplicaFromConfig(&ReplicaConfig{URL: fs.Arg(0)}, nil); err != nil {
			return err
		}
		initLog(os.Stdout, "INFO", "text")
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
		path, err := expand(fs.Arg(0))
		if err != nil {
			return err
		}
		dbc := config.DBConfig(path)
		if dbc == nil {
			return fmt.Errorf("database not found in config: %s", path)
		}

		db, err := NewDBFromConfig(dbc)
		if err != nil {
			return err
		} else if db.Replica == nil {
			return fmt.Errorf("database has no replica")
		}
		r = db.Replica
	}

	// List all WAL files.
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "min_txid\tmax_txid\tsize\tcreated")
	// Normal operation - use fast timestamps
	itr, err := r.Client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		return err
	}
	defer itr.Close()

	for itr.Next() {
		info := itr.Item()

		fmt.Fprintf(w, "%s\t%s\t%d\t%s\n",
			info.MinTXID,
			info.MaxTXID,
			info.Size,
			info.CreatedAt.Format(time.RFC3339),
		)
	}
	return itr.Close()
}

// Usage prints the help screen to STDOUT.
func (c *LTXCommand) Usage() {
	fmt.Printf(`
The ltx command lists all LTX files available for a database.

Usage:

	litestream ltx [arguments] DB_PATH

	litestream ltx [arguments] REPLICA_URL

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-no-expand-env
	    Disables environment variable expansion in configuration file.

	-replica NAME
	    Optional, filter by a specific replica.

Examples:

	# List all LTX files for a database.
	$ litestream ltx /path/to/db

	# List all LTX files on S3
	$ litestream ltx -replica s3 /path/to/db

	# List all LTX files for replica URL.
	$ litestream ltx s3://mybkt/db

`[1:],
		DefaultConfigPath(),
	)
}
