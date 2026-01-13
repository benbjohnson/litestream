package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
)

// SyncCommand represents the command to force a sync for a database.
type SyncCommand struct{}

// Run executes the sync command.
func (c *SyncCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-sync", flag.ContinueOnError)
	wait := fs.Bool("wait", false, "wait for sync to complete")
	timeout := fs.Int("timeout", 30, "timeout in seconds")
	socketPath := fs.String("socket", "/var/run/litestream.sock", "control socket path")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() == 0 {
		return fmt.Errorf("database path required")
	}
	if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	dbPath := fs.Arg(0)

	// Connect to control socket
	conn, err := net.Dial("unix", *socketPath)
	if err != nil {
		return fmt.Errorf("failed to connect to control socket: %w", err)
	}
	defer conn.Close()

	// Build request
	params := SyncParams{
		Path:    dbPath,
		Wait:    *wait,
		Timeout: *timeout,
	}
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("failed to marshal params: %w", err)
	}

	req := RPCRequest{
		JSONRPC: "2.0",
		Method:  "sync",
		Params:  paramsJSON,
		ID:      1,
	}

	// Send request
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	// Read response
	var resp RPCResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	// Check for error
	if resp.Error != nil {
		return fmt.Errorf("sync failed: %s", resp.Error.Message)
	}

	// Print result
	resultJSON, err := json.MarshalIndent(resp.Result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}
	fmt.Println(string(resultJSON))

	return nil
}

// Usage prints the help text for the sync command.
func (c *SyncCommand) Usage() {
	fmt.Println(`
usage: litestream sync [OPTIONS] DB_PATH

Force an immediate sync for a database.

Options:
  -wait
      Block until sync completes.

  -timeout SECONDS
      Maximum time to wait in seconds (default: 30).

  -socket PATH
      Path to control socket (default: /var/run/litestream.sock).
`[1:])
}
