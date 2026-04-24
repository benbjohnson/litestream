package main

import (
	"context"
	"encoding/json"
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
	jsonOutput := fs.Bool("json", false, "output raw JSON")
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

	var databases []DatabaseInfo
	for _, dbConfig := range config.DBs {
		db, err := NewDBFromConfig(dbConfig)
		if err != nil {
			return err
		}

		databases = append(databases, DatabaseInfo{
			Path:    db.Path(),
			Replica: db.Replica.Client.Type(),
		})
	}

	if *jsonOutput {
		output, err := json.MarshalIndent(databases, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to format response: %w", err)
		}
		fmt.Println(string(output))
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "path\treplica")
	for _, db := range databases {
		fmt.Fprintf(w, "%s\t%s\n", db.Path, db.Replica)
	}

	return nil
}

type DatabaseInfo struct {
	Path    string `json:"path"`
	Replica string `json:"replica"`
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

	-json
	    Output raw JSON instead of human-readable text.

	-no-expand-env
	    Disables environment variable expansion in configuration file.

Examples:

	$ litestream databases
	$ litestream databases -config /path/to/litestream.yml
	$ litestream databases -no-expand-env -config /path/to/litestream.yml

`[1:],
		DefaultConfigPath(),
	)
}
