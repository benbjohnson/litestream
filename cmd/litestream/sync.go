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

// SyncCommand represents the command to force an immediate sync for a database.
type SyncCommand struct{}

// Run executes the sync command.
func (c *SyncCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-sync", flag.ContinueOnError)
	timeout := fs.Int("timeout", 30, "timeout in seconds")
	socketPath := fs.String("socket", "/var/run/litestream.sock", "control socket path")
	wait := fs.Bool("wait", false, "block until sync completes")
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
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be greater than 0")
	}

	dbPath := fs.Arg(0)

	clientTimeout := time.Duration(*timeout) * time.Second
	client := &http.Client{
		Timeout: clientTimeout,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.DialTimeout("unix", *socketPath, clientTimeout)
			},
		},
	}

	req := litestream.SyncRequest{
		Path:    dbPath,
		Wait:    *wait,
		Timeout: *timeout,
	}
	reqBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := client.Post("http://localhost/sync", "application/json", bytes.NewReader(reqBody))
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
			return fmt.Errorf("sync failed: %s", errResp.Error)
		}
		return fmt.Errorf("sync failed: %s", string(body))
	}

	var result litestream.SyncResponse
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

// Usage prints the help text for the sync command.
func (c *SyncCommand) Usage() {
	fmt.Println(`
usage: litestream sync [OPTIONS] DB_PATH

Force an immediate sync for a database.

Options:
  -wait
      Block until sync completes including remote replication (default: false).

  -timeout SECONDS
      Maximum time to wait in seconds, best-effort (default: 30).

  -socket PATH
      Path to control socket (default: /var/run/litestream.sock).
`[1:])
}
