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

	start := time.Now()
	resp, err := client.Post("http://localhost/sync", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("failed to connect to control socket: %w", err)
	}
	defer resp.Body.Close()
	durationMS := time.Since(start).Milliseconds()

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

	confirmation := SyncResult{
		DBPath:     result.Path,
		TXID:       result.TXID,
		DurationMS: durationMS,
	}
	if *wait {
		confirmation.ReplicaTXID = &result.ReplicatedTXID
	}
	if err := printSyncResult(confirmation, *jsonOutput); err != nil {
		return err
	}

	return nil
}

type SyncResult struct {
	DBPath      string  `json:"db_path"`
	TXID        uint64  `json:"txid"`
	ReplicaTXID *uint64 `json:"replica_txid,omitempty"`
	DurationMS  int64   `json:"duration_ms"`
}

func printSyncResult(result SyncResult, jsonOutput bool) error {
	if jsonOutput {
		output, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to format response: %w", err)
		}
		fmt.Println(string(output))
		return nil
	}

	fmt.Printf("db_path: %s\n", result.DBPath)
	fmt.Printf("txid: %d\n", result.TXID)
	if result.ReplicaTXID != nil {
		fmt.Printf("replica_txid: %d\n", *result.ReplicaTXID)
	}
	fmt.Printf("duration_ms: %d\n", result.DurationMS)

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

  -json
      Output raw JSON instead of human-readable text.

Examples:
  $ litestream sync /path/to/db
  $ litestream sync -json /path/to/db
  $ litestream sync -wait /path/to/db
  $ litestream sync -wait -timeout 120 /path/to/db
`[1:])
}
