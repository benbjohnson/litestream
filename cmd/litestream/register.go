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

type RegisterCommand struct{}

func (c *RegisterCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-register", flag.ContinueOnError)
	timeout := fs.Int("timeout", 30, "timeout in seconds")
	socketPath := fs.String("socket", "/var/run/litestream.sock", "control socket path")
	replicaFlag := fs.String("replica", "", "replica URL (e.g., s3://bucket/prefix, file:///backup/path)")
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
	if *replicaFlag == "" {
		return fmt.Errorf("-replica is required. Try: litestream register -replica s3://bucket/prefix /path/to/db")
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be greater than 0")
	}

	dbPath := fs.Arg(0)
	replicaURL := *replicaFlag

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

	req := litestream.RegisterDatabaseRequest{
		Path:       dbPath,
		ReplicaURL: replicaURL,
	}
	reqBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := client.Post("http://localhost/register", "application/json", bytes.NewReader(reqBody))
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
			return fmt.Errorf("register failed: %s", errResp.Error)
		}
		return fmt.Errorf("register failed: %s", string(body))
	}

	var result litestream.RegisterDatabaseResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	confirmation := RegisterResult{
		Status:  result.Status,
		DBPath:  result.Path,
		Replica: replicaURL,
		Socket:  *socketPath,
	}
	if *jsonOutput {
		output, err := json.MarshalIndent(confirmation, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to format response: %w", err)
		}
		fmt.Println(string(output))
		return nil
	}

	fmt.Printf("status: %s\n", confirmation.Status)
	fmt.Printf("db_path: %s\n", confirmation.DBPath)
	fmt.Printf("replica: %s\n", confirmation.Replica)
	fmt.Printf("socket: %s\n", confirmation.Socket)

	return nil
}

type RegisterResult struct {
	Status  string `json:"status"`
	DBPath  string `json:"db_path"`
	Replica string `json:"replica"`
	Socket  string `json:"socket"`
}

func (c *RegisterCommand) Usage() {
	fmt.Println(`
usage: litestream register [OPTIONS] DB_PATH

Register a database for replication.

Arguments:
  DB_PATH      Path to the SQLite database file.

Options:
  -replica URL
      Replica destination URL (e.g., s3://bucket/prefix, file:///backup/path).
      Required.

  -timeout SECONDS
      Maximum time to wait in seconds (default: 30).

  -socket PATH
      Path to control socket (default: /var/run/litestream.sock).

  -json
      Output raw JSON instead of human-readable text.

Examples:
  # Register a database with an S3 replica.
  $ litestream register -replica s3://mybucket/db /path/to/db

  # Register a database with a file replica.
  $ litestream register -replica file:///backup/path /path/to/db

  # Register using a non-default control socket.
  $ litestream register -socket /tmp/litestream.sock -replica s3://mybucket/db /path/to/db

  # Register and emit a JSON confirmation.
  $ litestream register -json -replica s3://mybucket/db /path/to/db
`[1:])
}
