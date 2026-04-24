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

type UnregisterCommand struct{}

func (c *UnregisterCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-unregister", flag.ContinueOnError)
	timeout := fs.Int("timeout", 30, "timeout in seconds")
	socketPath := fs.String("socket", "/var/run/litestream.sock", "control socket path")
	dryRun := fs.Bool("dry-run", false, "print what would be unregistered without changing the daemon")
	jsonOutput := fs.Bool("json", false, "output raw JSON")
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

	if *dryRun {
		fmt.Println("Dry run: unregister request preview")
		fmt.Printf("  database: %s\n", dbPath)
		fmt.Printf("  socket: %s\n", *socketPath)
		fmt.Printf("  replicas: daemon-managed replica for this database\n")
		fmt.Printf("  final sync: daemon close will sync the database and replica before the command completes\n")
		fmt.Printf("  timeout: %ds\n", *timeout)
		fmt.Println("No unregister request was sent.")
		return nil
	}

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

	req := litestream.UnregisterDatabaseRequest{
		Path:    dbPath,
		Timeout: *timeout,
	}
	reqBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := client.Post("http://localhost/unregister", "application/json", bytes.NewReader(reqBody))
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
			return fmt.Errorf("unregister failed: %s", errResp.Error)
		}
		return fmt.Errorf("unregister failed: %s", string(body))
	}

	var result litestream.UnregisterDatabaseResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	confirmation := UnregisterResult{
		Status:    result.Status,
		DBPath:    result.Path,
		FinalTXID: result.TXID,
		Socket:    *socketPath,
	}
	if err := printUnregisterResult(confirmation, *jsonOutput); err != nil {
		return err
	}

	return nil
}

type UnregisterResult struct {
	Status    string `json:"status"`
	DBPath    string `json:"db_path"`
	FinalTXID uint64 `json:"final_txid"`
	Socket    string `json:"socket"`
}

func printUnregisterResult(result UnregisterResult, jsonOutput bool) error {
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
	fmt.Printf("final_txid: %d\n", result.FinalTXID)
	fmt.Printf("socket: %s\n", result.Socket)

	return nil
}

func (c *UnregisterCommand) Usage() {
	fmt.Println(`
usage: litestream unregister [OPTIONS] DB_PATH

Unregister a database from replication.

Arguments:
  DB_PATH      Path to the SQLite database file.

Options:
  -timeout SECONDS
      Maximum time to wait in seconds (default: 30).

  -socket PATH
      Path to control socket (default: /var/run/litestream.sock).

  -dry-run
      Preview what would be unregistered without changing the daemon.

  -json
      Output raw JSON instead of human-readable text.

Examples:
  # Unregister a database from the running daemon.
  $ litestream unregister /path/to/db

  # Unregister a database and emit a JSON confirmation.
  $ litestream unregister -json /path/to/db

  # Preview an unregister request without changing the daemon.
  $ litestream unregister -dry-run /path/to/db

  # Unregister a database using a non-default control socket.
  $ litestream unregister -socket /tmp/litestream.sock /path/to/db

  # Unregister a database and wait up to 10 seconds for shutdown.
  $ litestream unregister -timeout 10 /path/to/db
`[1:])
}
