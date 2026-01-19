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
	"time"

	"github.com/benbjohnson/litestream"
)

// AddCommand represents the command to add a database for replication.
type AddCommand struct{}

// Run executes the add command.
func (c *AddCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-add", flag.ContinueOnError)
	timeout := fs.Int("timeout", 30, "timeout in seconds")
	socketPath := fs.String("socket", "/var/run/litestream.sock", "control socket path")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() < 2 {
		return fmt.Errorf("database path and replica URL required")
	}
	if fs.NArg() > 2 {
		return fmt.Errorf("too many arguments")
	}

	dbPath := fs.Arg(0)
	replicaURL := fs.Arg(1)

	// Create HTTP client that connects via Unix socket with timeout.
	clientTimeout := time.Duration(*timeout) * time.Second
	client := &http.Client{
		Timeout: clientTimeout,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.DialTimeout("unix", *socketPath, clientTimeout)
			},
		},
	}

	req := litestream.AddDatabaseRequest{
		Path:       dbPath,
		ReplicaURL: replicaURL,
	}
	reqBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := client.Post("http://localhost/add", "application/json", bytes.NewReader(reqBody))
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
			return fmt.Errorf("add failed: %s", errResp.Error)
		}
		return fmt.Errorf("add failed: %s", string(body))
	}

	var result litestream.AddDatabaseResponse
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

// Usage prints the help text for the add command.
func (c *AddCommand) Usage() {
	fmt.Println(`
usage: litestream add [OPTIONS] DB_PATH REPLICA_URL

Add a database for replication.

Arguments:
  DB_PATH      Path to the SQLite database file.
  REPLICA_URL  URL of the replica destination (e.g., s3://bucket/prefix, file:///backup/path).

Options:
  -timeout SECONDS
      Maximum time to wait in seconds (default: 30).

  -socket PATH
      Path to control socket (default: /var/run/litestream.sock).
`[1:])
}
