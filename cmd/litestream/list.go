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
	jsonOutput := fs.Bool("json", false, "output raw JSON")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() > 0 {
		return fmt.Errorf("too many arguments")
	}

	if *timeout <= 0 {
		return fmt.Errorf("timeout must be greater than 0")
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

	if *jsonOutput {
		output, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to format response: %w", err)
		}
		fmt.Println(string(output))
	} else {
		if len(result.Databases) == 0 {
			fmt.Println("No databases configured")
		} else {
			for _, db := range result.Databases {
				syncInfo := "never"
				if db.LastSyncAt != nil {
					syncInfo = db.LastSyncAt.Format(time.RFC3339)
				}
				fmt.Printf("%s [%s] (last sync: %s)\n", db.Path, db.Status, syncInfo)
			}
		}
	}

	return nil
}

// Usage prints the help text for the list command.
func (c *ListCommand) Usage() {
	fmt.Println(`
usage: litestream list [OPTIONS]

List all managed databases from a running daemon.

Options:
  -json
      Output raw JSON instead of human-readable text.

  -socket PATH
      Path to control socket (default: /var/run/litestream.sock).

  -timeout SECONDS
      Maximum time to wait in seconds (default: 10).
`[1:])
}
