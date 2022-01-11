package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// DatabasesCommand is a command for listing managed databases.
type DatabasesCommand struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer

	configPath  string
	noExpandEnv bool
}

// NewDatabasesCommand returns a new instance of DatabasesCommand.
func NewDatabasesCommand(stdin io.Reader, stdout, stderr io.Writer) *DatabasesCommand {
	return &DatabasesCommand{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}
}

// Run executes the command.
func (c *DatabasesCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-databases", flag.ContinueOnError)
	registerConfigFlag(fs, &c.configPath, &c.noExpandEnv)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() != 0 {
		return fmt.Errorf("too many arguments")
	}

	// Load configuration.
	config, err := ReadConfigFile(c.configPath, !c.noExpandEnv)
	if err != nil {
		return err
	} else if len(config.DBs) == 0 {
		fmt.Fprintln(c.stdout, "No databases found in config file.")
		return nil
	}

	// List all databases.
	w := tabwriter.NewWriter(c.stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "path\treplicas")
	for _, dbConfig := range config.DBs {
		db, err := NewDBFromConfig(dbConfig)
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

	return nil
}

// Usage prints the help screen to STDOUT.
func (c *DatabasesCommand) Usage() {
	fmt.Fprintf(c.stdout, `
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
