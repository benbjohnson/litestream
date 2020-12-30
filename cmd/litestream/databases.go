package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
)

type DatabasesCommand struct{}

func (c *DatabasesCommand) Run(ctx context.Context, args []string) (err error) {
	var configPath string
	fs := flag.NewFlagSet("litestream-databases", flag.ContinueOnError)
	registerConfigFlag(fs, &configPath)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() != 0 {
		return fmt.Errorf("too many argument")
	}

	// Load configuration.
	if configPath == "" {
		return errors.New("-config required")
	}
	config, err := ReadConfigFile(configPath)
	if err != nil {
		return err
	}

	// List all databases.
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 1, '\t', 0)
	fmt.Fprintln(w, "path\treplicas")
	for _, dbConfig := range config.DBs {
		db, err := newDBFromConfig(dbConfig)
		if err != nil {
			return err
		}

		var replicaNames []string
		for _, r := range db.Replicas {
			replicaNames = append(replicaNames, r.Name())
		}

		fmt.Fprintf(w, "%s\t%s\n",
			db.Path(),
			strings.Join(replicaNames, ","),
		)
	}
	w.Flush()

	return nil
}

func (c *DatabasesCommand) Usage() {
	fmt.Printf(`
The databases command lists all databases in the configuration file.

Usage:

	litestream databases [arguments]

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

`[1:],
		DefaultConfigPath,
	)
}
