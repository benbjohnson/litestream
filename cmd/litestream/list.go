package main

import (
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

// ListCommand represents the command to list all managed databases.
type ListCommand struct{}

// Run executes the list command.
func (c *ListCommand) Run(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-list", flag.ContinueOnError)
	socketPath := fs.String("socket", "/var/run/litestream.sock", "control socket path")
	timeout := fs.Int("timeout", 10, "timeout in seconds")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() > 0 {
		return fmt.Errorf("too many arguments")
	}

	clientTimeout := time.Duration(*timeout) * time.Second
	client := &http.Client{
		Timeout: clientTimeout,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.DialTimeout("unix", *socketPath, clientTimeout)
			},
		},
	}

	resp, err := client.Get("http://localhost/list")
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
			return fmt.Errorf("list failed: %s", errResp.Error)
		}
		return fmt.Errorf("list failed: %s", string(body))
	}

	var result litestream.ListResponse
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

// Usage prints the help text for the list command.
func (c *ListCommand) Usage() {
	fmt.Println(`
usage: litestream list [OPTIONS]

List all managed databases from a running daemon.

Options:
  -socket PATH
      Path to control socket (default: /var/run/litestream.sock).

  -timeout SECONDS
      Maximum time to wait in seconds (default: 10).
`[1:])
}
