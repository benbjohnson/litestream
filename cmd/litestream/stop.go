package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
)

// StopCommand represents the command to stop replication for a database.
type StopCommand struct{}

// Run executes the stop command.
func (c *StopCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-stop", flag.ContinueOnError)
	timeout := fs.Int("timeout", 30, "timeout in seconds")
	socketPath := fs.String("socket", "", "control socket path (required)")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *socketPath == "" {
		return fmt.Errorf("socket path required; use -socket flag")
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
	params := StopParams{
		Path:    dbPath,
		Timeout: *timeout,
	}
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("failed to marshal params: %w", err)
	}

	req := RPCRequest{
		JSONRPC: "2.0",
		Method:  "stop",
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
		return fmt.Errorf("stop failed: %s", resp.Error.Message)
	}

	// Print result
	resultJSON, err := json.MarshalIndent(resp.Result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}
	fmt.Println(string(resultJSON))

	return nil
}

// Usage prints the help text for the stop command.
func (c *StopCommand) Usage() {
	fmt.Println(`
usage: litestream stop [OPTIONS] DB_PATH

Stop replication for a database.
Stop always waits for shutdown and final sync.

Options:
  -timeout SECONDS
      Maximum time to wait in seconds (default: 30).

  -socket PATH
      Path to control socket (required).
`[1:])
}
