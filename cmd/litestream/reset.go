package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/benbjohnson/litestream"
)

// ResetCommand is a command for resetting local Litestream state for a database.
type ResetCommand struct{}

// Run executes the command.
func (c *ResetCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-reset", flag.ContinueOnError)
	configPath, noExpandEnv := registerConfigFlag(fs)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Validate arguments - need exactly one database path
	if fs.NArg() == 0 {
		return fmt.Errorf("database path required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	dbPath := fs.Arg(0)

	// Make absolute if needed
	if !filepath.IsAbs(dbPath) {
		if dbPath, err = filepath.Abs(dbPath); err != nil {
			return err
		}
	}

	// Load configuration to find the database (if config exists)
	var dbConfig *DBConfig
	if *configPath != "" {
		config, configErr := ReadConfigFile(*configPath, !*noExpandEnv)
		if configErr != nil {
			return fmt.Errorf("cannot read config: %w", configErr)
		}

		// Find database config
		for _, dbc := range config.DBs {
			expandedPath := dbc.Path
			if !filepath.IsAbs(expandedPath) {
				expandedPath, _ = filepath.Abs(expandedPath)
			}
			if expandedPath == dbPath {
				dbConfig = dbc
				break
			}
		}
	}

	// If no config found, check if database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database does not exist: %s", dbPath)
	} else if err != nil {
		return fmt.Errorf("cannot access database: %w", err)
	}

	// Create DB instance
	var db *litestream.DB
	if dbConfig != nil {
		db, err = NewDBFromConfig(dbConfig)
		if err != nil {
			return fmt.Errorf("cannot create database from config: %w", err)
		}
	} else {
		db = litestream.NewDB(dbPath)
	}

	// Check if meta path exists
	metaPath := db.MetaPath()
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		fmt.Printf("No local state to reset for %s\n", dbPath)
		fmt.Printf("Meta directory does not exist: %s\n", metaPath)
		return nil
	}

	// Perform the reset
	fmt.Printf("Resetting local Litestream state for: %s\n", dbPath)
	fmt.Printf("Removing: %s\n", db.LTXDir())

	if err := db.ResetLocalState(ctx); err != nil {
		return fmt.Errorf("reset failed: %w", err)
	}

	fmt.Println("Reset complete. Next replication sync will create a fresh snapshot.")
	return nil
}

// Usage prints the help screen to STDOUT.
func (c *ResetCommand) Usage() {
	fmt.Printf(`
The reset command clears local Litestream state for a database.

This is useful for recovering from corrupted or missing LTX files. The reset
removes local LTX files from the metadata directory, forcing Litestream to
create a fresh snapshot on the next sync. The database file itself is not
modified.

Usage:

	litestream reset [arguments] <path>

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-no-expand-env
	    Disables environment variable expansion in configuration file.

Examples:

	# Reset local state for a specific database
	litestream reset /path/to/database.db

	# Reset using a specific configuration file
	litestream reset -config /etc/litestream.yml /path/to/database.db

`[1:],
		DefaultConfigPath(),
	)
}
