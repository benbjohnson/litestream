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

// InfoCommand represents the command to show daemon information.
type InfoCommand struct{}

// Run executes the info command.
func (c *InfoCommand) Run(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-info", flag.ContinueOnError)
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

	resp, err := client.Get("http://localhost/info")
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
			return fmt.Errorf("info failed: %s", errResp.Error)
		}
		return fmt.Errorf("info failed: %s", string(body))
	}

	var result litestream.InfoResponse
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

// Usage prints the help text for the info command.
func (c *InfoCommand) Usage() {
	fmt.Println(`
usage: litestream info [OPTIONS]

Show daemon information from a running Litestream instance.

Options:
  -socket PATH
      Path to control socket (default: /var/run/litestream.sock).

  -timeout SECONDS
      Maximum time to wait in seconds (default: 10).
`[1:])
}
