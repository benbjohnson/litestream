package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/benbjohnson/litestream"
)

// MonitorCommand represents the command to monitor replication status.
type MonitorCommand struct{}

// Run executes the monitor command.
func (c *MonitorCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-monitor", flag.ContinueOnError)
	socketPath := fs.String("socket", "/var/run/litestream.sock", "control socket path")
	dbFilter := fs.String("db", "", "filter to specific database path")
	rawOutput := fs.Bool("raw", false, "output raw NDJSON without formatting")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Create HTTP client that connects via Unix socket
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", *socketPath)
			},
		},
	}

	req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost/monitor", nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to control socket: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("monitor failed: %s", resp.Status)
	}

	// Stream NDJSON events
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		if *rawOutput {
			fmt.Println(line)
			continue
		}

		var event litestream.StatusEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to parse event: %v\n", err)
			continue
		}

		c.printEvent(&event, *dbFilter)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading stream: %w", err)
	}

	return nil
}

// printEvent formats and prints a status event.
func (c *MonitorCommand) printEvent(event *litestream.StatusEvent, dbFilter string) {
	switch event.Type {
	case "full":
		fmt.Printf("[%s] Connected - monitoring %d database(s)\n",
			event.Timestamp.Format(time.RFC3339),
			len(event.Databases))
		for _, db := range event.Databases {
			if dbFilter != "" && db.Path != dbFilter {
				continue
			}
			c.printDatabaseStatus(&db)
		}

	case "sync":
		if event.Database == nil {
			return
		}
		if dbFilter != "" && event.Database.Path != dbFilter {
			return
		}
		fmt.Printf("[%s] SYNC %s -> txid:%s\n",
			event.Timestamp.Format(time.RFC3339),
			event.Database.Path,
			event.Database.ReplicaTXID)
	}
}

// printDatabaseStatus prints the status of a single database.
func (c *MonitorCommand) printDatabaseStatus(db *litestream.DatabaseStatus) {
	status := db.Status
	if !db.Enabled {
		status = "disabled"
	}

	txidInfo := ""
	if db.LocalTXID != "" {
		txidInfo = fmt.Sprintf(" local:%s", db.LocalTXID)
	}
	if db.ReplicaTXID != "" {
		txidInfo += fmt.Sprintf(" replica:%s", db.ReplicaTXID)
	}
	if db.SyncLag > 0 {
		txidInfo += fmt.Sprintf(" lag:%d", db.SyncLag)
	}

	fmt.Printf("  %s [%s]%s\n", db.Path, status, txidInfo)
}

// Usage prints the help text for the monitor command.
func (c *MonitorCommand) Usage() {
	fmt.Println(`
usage: litestream monitor [OPTIONS]

Monitor replication status in real-time.

Connects to the litestream control socket and streams replication events
as they occur. Events are emitted when LTX files are uploaded to replicas.

Options:
  -socket PATH
      Path to control socket (default: /var/run/litestream.sock).

  -db PATH
      Filter events to a specific database path.

  -raw
      Output raw NDJSON without formatting.

Examples:
  # Monitor all databases
  litestream monitor

  # Monitor specific database
  litestream monitor -db /data/app.db

  # Output raw NDJSON (for piping to other tools)
  litestream monitor -raw | jq .
`[1:])
}
