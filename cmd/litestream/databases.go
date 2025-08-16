package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
)

// DatabasesCommand is a command for listing managed databases.
type DatabasesCommand struct{}

// Run executes the command.
func (c *DatabasesCommand) Run(_ context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-databases", flag.ContinueOnError)
	configPath, noExpandEnv := registerConfigFlag(fs)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() != 0 {
		return fmt.Errorf("too many arguments")
	}

	// Load configuration.
	if *configPath == "" {
		*configPath = DefaultConfigPath()
	}
	config, err := ReadConfigFile(*configPath, !*noExpandEnv)
	if err != nil {
		return err
	}

	// List all databases.
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "path\treplica")
	for _, dbConfig := range config.DBs {
		db, err := NewDBFromConfig(dbConfig)
		if err != nil {
			return err
		}

		fmt.Fprintf(w, "%s\t%s\n",
			db.Path(),
			db.Replica.Client.Type())
	}

	return nil
}

// Usage prints the help screen to STDOUT.
func (c *DatabasesCommand) Usage() {
	fmt.Printf(`
The databases command lists all databases in the configuration file.

Usage:

	litestream databases [arguments]

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-no-expand-env
	    Disables environment variable expansion in configuration file.

`[1:],
		DefaultConfigPath(),
	)
}
