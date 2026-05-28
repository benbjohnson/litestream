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
	"os"
	"time"

	"github.com/benbjohnson/litestream"
)

// StartCommand represents the command to start replication for a database.
type StartCommand struct{}

// Run executes the start command.
func (c *StartCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-start", flag.ContinueOnError)
	timeout := fs.Int("timeout", 30, "timeout in seconds")
	socketPath := fs.String("socket", "/var/run/litestream.sock", "control socket path")
	jsonOutput := fs.Bool("json", false, "output raw JSON")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() == 0 {
		os.Stderr.WriteString(`
Note: 'litestream start' enables replication for a single database on a running daemon.
To start the replication daemon, use 'litestream replicate' instead.
Run 'litestream start -h' for usage details.
`)
		return fmt.Errorf("database path required")
	}
	if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	dbPath := fs.Arg(0)

	// Create HTTP client that connects via Unix socket with timeout
	clientTimeout := time.Duration(*timeout) * time.Second
	client := &http.Client{
		Timeout: clientTimeout,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.DialTimeout("unix", *socketPath, clientTimeout)
			},
		},
	}

	req := litestream.StartRequest{
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
		var errResp litestream.ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error != "" {
			return fmt.Errorf("start failed: %s", errResp.Error)
		}
		return fmt.Errorf("start failed: %s", string(body))
	}

	var result litestream.StartResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	confirmation := StartStopResult{
		Status: result.Status,
		DBPath: result.Path,
		State:  "running",
		TXID:   result.TXID,
		Socket: *socketPath,
	}
	if err := printStartStopResult(confirmation, *jsonOutput); err != nil {
		return err
	}

	return nil
}

type StartStopResult struct {
	Status string `json:"status"`
	DBPath string `json:"db_path"`
	State  string `json:"state"`
	TXID   uint64 `json:"txid"`
	Socket string `json:"socket"`
}

func printStartStopResult(result StartStopResult, jsonOutput bool) error {
	if jsonOutput {
		output, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to format response: %w", err)
		}
		fmt.Println(string(output))
		return nil
	}

	fmt.Printf("status: %s\n", result.Status)
	fmt.Printf("db_path: %s\n", result.DBPath)
	fmt.Printf("state: %s\n", result.State)
	fmt.Printf("txid: %d\n", result.TXID)
	fmt.Printf("socket: %s\n", result.Socket)

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
      Path to control socket (default: /var/run/litestream.sock).

  -json
      Output raw JSON instead of human-readable text.

Examples:
  # Start replication for a database on the running daemon.
  $ litestream start /path/to/db

  # Start replication and emit a JSON confirmation.
  $ litestream start -json /path/to/db

  # Start replication using a non-default control socket.
  $ litestream start -socket /tmp/litestream.sock /path/to/db

  # Start replication and wait up to 10 seconds for the daemon response.
  $ litestream start -timeout 10 /path/to/db
`[1:])
}
