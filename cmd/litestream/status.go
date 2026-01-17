package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"text/tabwriter"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/benbjohnson/litestream"
)

// StatusCommand is a command for displaying replication status.
type StatusCommand struct{}

// Run executes the command.
func (c *StatusCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-status", flag.ContinueOnError)
	configPath, noExpandEnv := registerConfigFlag(fs)
	socketPath := fs.String("socket", "", "control socket path for querying running daemon")
	timeout := fs.Int("timeout", 10, "timeout in seconds for socket connection")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// If socket path is provided, query the running daemon.
	if *socketPath != "" {
		return c.runWithSocket(ctx, *socketPath, *timeout, fs.Args())
	}

	// Otherwise, use config file to get static status.
	return c.runWithConfig(ctx, *configPath, *noExpandEnv, fs.Args())
}

// runWithSocket queries the running daemon via Unix socket.
func (c *StatusCommand) runWithSocket(_ context.Context, socketPath string, timeout int, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("database path required when using -socket")
	}
	if len(args) > 1 {
		return fmt.Errorf("too many arguments")
	}

	dbPath := args[0]

	clientTimeout := time.Duration(timeout) * time.Second
	client := &http.Client{
		Timeout: clientTimeout,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.DialTimeout("unix", socketPath, clientTimeout)
			},
		},
	}

	reqURL := fmt.Sprintf("http://localhost/status?path=%s", url.QueryEscape(dbPath))
	resp, err := client.Get(reqURL)
	if err != nil {
		return fmt.Errorf("failed to connect to control socket: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp litestream.ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error != "" {
			return fmt.Errorf("status failed: %s", errResp.Error)
		}
		return fmt.Errorf("status failed: %s", string(body))
	}

	var result litestream.StatusResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to format response: %w", err)
	}
	fmt.Println(string(output))

	return nil
}

// runWithConfig reads the config file and displays static status.
func (c *StatusCommand) runWithConfig(_ context.Context, configPath string, noExpandEnv bool, args []string) error {
	// Load configuration.
	if configPath == "" {
		configPath = DefaultConfigPath()
	}
	config, err := ReadConfigFile(configPath, !noExpandEnv)
	if err != nil {
		return err
	}

	// If a specific database path is provided, filter to just that one.
	var filterPath string
	if len(args) > 0 {
		filterPath = args[0]
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

	litestream status -socket PATH DB_PATH

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-socket PATH
	    Path to control socket for querying a running daemon.
	    When specified, queries the daemon for live status.

	-timeout SECONDS
	    Timeout for socket connection (default: 10).

	-no-expand-env
	    Disables environment variable expansion in configuration file.

Without -socket flag:
  Reads the config file and displays static status from local files.
  If a database path is provided, only that database's status is shown.
  Otherwise, all configured databases are displayed.

With -socket flag:
  Queries the running daemon for live replication status.
  A database path is required when using -socket.
  Returns JSON output with current status, position, and replica info.

`[1:],
		DefaultConfigPath(),
	)
}
