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

type WALCommand struct{}

func (c *WALCommand) Run(ctx context.Context, args []string) (err error) {
	var configPath string
	fs := flag.NewFlagSet("litestream-wal", flag.ContinueOnError)
	registerConfigFlag(fs, &configPath)
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
	db, err := newDBFromConfig(dbConfig)
	if err != nil {
		return err
	}

	// Find snapshots by db or replica.
	var infos []*litestream.WALInfo
	if *replicaName != "" {
		if r := db.Replica(*replicaName); r == nil {
			return fmt.Errorf("replica %q not found for database %q", *replicaName, dbPath)
		} else if infos, err = r.WALs(ctx); err != nil {
			return err
		}
	} else {
		if infos, err = db.WALs(ctx); err != nil {
			return err
		}
	}

	// List all WAL files.
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', 0)
	fmt.Fprintln(w, "replica\tgeneration\tindex\toffset\tsize\tcreated")
	for _, info := range infos {
		if *generation != "" && info.Generation != *generation {
			continue
		}

		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%s\n",
			info.Replica,
			info.Generation,
			info.Index,
			info.Offset,
			info.Size,
			info.CreatedAt.Format(time.RFC3339),
		)
	}
	w.Flush()

	return nil
}

func (c *WALCommand) Usage() {
	fmt.Printf(`
The wal command lists all wal files available for a database.

Usage:

	litestream wal [arguments] DB

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-replica NAME
	    Optional, filter by a specific replica.

	-generation NAME
	    Optional, filter by a specific generation.


Examples:

	# List all WAL files for a database.
	$ litestream wal /path/to/db

	# List all WAL files on S3 for a specific generation.
	$ litestream snapshots -replica s3 -generation xxxxxxxx /path/to/db

`[1:],
		DefaultConfigPath(),
	)
}
