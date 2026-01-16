package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
)

// StartCommand represents the command to start replication for a database.
type StartCommand struct{}

// Run executes the start command.
func (c *StartCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-start", flag.ContinueOnError)
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

	// Create HTTP client that connects via Unix socket
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", *socketPath)
			},
		},
	}

	req := StartRequest{
		Path:    dbPath,
		Timeout: *timeout,
	}
	reqBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := client.Post("http://localhost/start", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("failed to connect to control socket: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error != "" {
			return fmt.Errorf("start failed: %s", errResp.Error)
		}
		return fmt.Errorf("start failed: %s", string(body))
	}

	var result StartResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	output, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(output))

	return nil
}

// Usage prints the help text for the start command.
func (c *StartCommand) Usage() {
	fmt.Println(`
usage: litestream start [OPTIONS] DB_PATH

Start replication for a database.

Options:
  -timeout SECONDS
      Maximum time to wait in seconds (default: 30).

  -socket PATH
      Path to control socket (required).
`[1:])
}
