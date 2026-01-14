package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
)

// ListCommand represents the command to list managed databases.
type ListCommand struct{}

// Run executes the list command.
func (c *ListCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-list", flag.ContinueOnError)
	socketPath := fs.String("socket", "/var/run/litestream.sock", "control socket path")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("too many arguments")
	}

	// Connect to control socket
	conn, err := net.Dial("unix", *socketPath)
	if err != nil {
		return fmt.Errorf("failed to connect to control socket: %w", err)
	}
	defer conn.Close()

	paramsJSON, err := json.Marshal(map[string]interface{}{})
	if err != nil {
		return fmt.Errorf("failed to marshal params: %w", err)
	}

	req := RPCRequest{
		JSONRPC: "2.0",
		Method:  "list",
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

	if resp.Error != nil {
		return fmt.Errorf("list failed: %s", resp.Error.Message)
	}

	resultJSON, err := json.MarshalIndent(resp.Result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}
	fmt.Println(string(resultJSON))
	return nil
}

// Usage prints the help text for the list command.
func (c *ListCommand) Usage() {
	fmt.Println(`
usage: litestream list [OPTIONS]

List databases managed by the daemon.

Options:
  -socket PATH
      Path to control socket (default: /var/run/litestream.sock).
`[1:])
}
