package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/dustin/go-humanize"

	"github.com/benbjohnson/litestream"
)

// StatusCommand is a command for displaying replication status.
type StatusCommand struct{}

// Run executes the command.
func (c *StatusCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-status", flag.ContinueOnError)
	configPath, noExpandEnv := registerConfigFlag(fs)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Load configuration.
	if *configPath == "" {
		*configPath = DefaultConfigPath()
	}
	config, err := ReadConfigFile(*configPath, !*noExpandEnv)
	if err != nil {
		return err
	}

	// If a specific database path is provided, filter to just that one.
	var filterPath string
	if fs.NArg() > 0 {
		filterPath = fs.Arg(0)
	}

	// Print status for each database.
	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "database\tstatus\tlocal txid\twal size")

	for _, dbConfig := range config.DBs {
		db, err := NewDBFromConfig(dbConfig)
		if err != nil {
			return err
		}

		// Filter if path specified.
		if filterPath != "" && db.Path() != filterPath {
			continue
		}

		status := c.getDBStatus(db)
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
			db.Path(),
			status.Status,
			status.LocalTXID,
			status.WALSize,
		)
	}

	return nil
}

// DBStatus holds the status information for a single database.
type DBStatus struct {
	Status    string
	LocalTXID string
	WALSize   string
}

// getDBStatus gathers status information for a database.
func (c *StatusCommand) getDBStatus(db *litestream.DB) DBStatus {
	status := DBStatus{
		Status:    "unknown",
		LocalTXID: "-",
		WALSize:   "-",
	}

	// Check if database file exists.
	dbPath := db.Path()
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		status.Status = "no database"
		return status
	}

	// Get WAL file size.
	walPath := db.WALPath()
	if walInfo, err := os.Stat(walPath); err == nil {
		status.WALSize = humanize.Bytes(uint64(walInfo.Size()))
	} else if os.IsNotExist(err) {
		status.WALSize = "0 B"
	}

	// Get local TXID from L0 directory.
	_, maxTXID, err := db.MaxLTX()
	if err == nil && maxTXID > 0 {
		status.LocalTXID = maxTXID.String()
		status.Status = "ok"
	} else if err == nil {
		status.Status = "not initialized"
	} else {
		status.Status = "error"
	}

	return status
}

// Usage prints the help screen to STDOUT.
func (c *StatusCommand) Usage() {
	fmt.Printf(`
The status command displays the replication status of databases.

Usage:

	litestream status [arguments] [database path]

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-no-expand-env
	    Disables environment variable expansion in configuration file.

If a database path is provided, only that database's status is shown.
Otherwise, all configured databases are displayed.

Output columns:
  database      Path to the SQLite database
  status        Current status (ok, not initialized, no database, error)
  local txid    Latest local transaction ID
  wal size      Current WAL file size

Note: To see replica TXID and sync status, use the MCP tools or check logs
while the replication daemon is running.

`[1:],
		DefaultConfigPath(),
	)
}
