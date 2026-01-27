package litestream_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// shortSocketPath returns a short socket path that won't exceed Unix socket limits.
// Unix sockets have a max path length of ~104 chars on macOS.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ls")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func TestServer_HandleMonitor(t *testing.T) {
	t.Run("StreamsInitialFullStatus", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore([]*litestream.DB{db}, levels)
		store.CompactionMonitorEnabled = false

		if err := store.Open(t.Context()); err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer store.Close(t.Context())

		// Create and sync data
		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := db.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Create server with socket (use short path to avoid Unix socket length limits)
		socketPath := shortSocketPath(t)
		server := litestream.NewServer(store)
		server.SocketPath = socketPath

		if err := server.Start(); err != nil {
			t.Fatalf("start server: %v", err)
		}
		defer server.Close()

		// Connect via Unix socket
		client := &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", socketPath)
				},
			},
		}

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost/monitor", nil)
		if err != nil {
			t.Fatalf("create request: %v", err)
		}

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", resp.StatusCode)
		}

		contentType := resp.Header.Get("Content-Type")
		if contentType != "application/x-ndjson" {
			t.Errorf("expected Content-Type 'application/x-ndjson', got %q", contentType)
		}

		// Read initial full status event
		scanner := bufio.NewScanner(resp.Body)
		if !scanner.Scan() {
			t.Fatal("expected to read first event")
		}

		var event litestream.StatusEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}

		if event.Type != "full" {
			t.Errorf("expected event type 'full', got %q", event.Type)
		}
		if len(event.Databases) != 1 {
			t.Errorf("expected 1 database in full status, got %d", len(event.Databases))
		}
		if event.Databases[0].Path != db.Path() {
			t.Errorf("expected path %q, got %q", db.Path(), event.Databases[0].Path)
		}
	})

	t.Run("StreamsSyncEvents", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore([]*litestream.DB{db}, levels)
		store.CompactionMonitorEnabled = false

		if err := store.Open(t.Context()); err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer store.Close(t.Context())

		// Create server with socket (use short path to avoid Unix socket length limits)
		socketPath := shortSocketPath(t)
		server := litestream.NewServer(store)
		server.SocketPath = socketPath

		if err := server.Start(); err != nil {
			t.Fatalf("start server: %v", err)
		}
		defer server.Close()

		// Wire up OnSync callback
		db.Replica.OnSync = func(pos ltx.Pos) {
			server.StatusMonitor.NotifySync(db, pos)
		}

		// Connect via Unix socket
		client := &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", socketPath)
				},
			},
		}

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost/monitor", nil)
		if err != nil {
			t.Fatalf("create request: %v", err)
		}

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		scanner := bufio.NewScanner(resp.Body)

		// Skip initial full status
		if !scanner.Scan() {
			t.Fatal("expected to read first event")
		}

		// Now trigger a sync
		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := db.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Read sync event
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				t.Fatalf("scanner error: %v", err)
			}
			t.Fatal("expected to read sync event")
		}

		var event litestream.StatusEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}

		if event.Type != "sync" {
			t.Errorf("expected event type 'sync', got %q", event.Type)
		}
		if event.Database == nil {
			t.Fatal("expected non-nil database in sync event")
		}
		if event.Database.ReplicaTXID == "" {
			t.Error("expected non-empty replica_txid in sync event")
		}
	})

	t.Run("ClientDisconnect", func(t *testing.T) {
		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore(nil, levels)

		socketPath := shortSocketPath(t)
		server := litestream.NewServer(store)
		server.SocketPath = socketPath

		if err := server.Start(); err != nil {
			t.Fatalf("start server: %v", err)
		}
		defer server.Close()

		// Connect and immediately disconnect
		conn, err := net.Dial("unix", socketPath)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}

		// Send HTTP request
		_, err = conn.Write([]byte("GET /monitor HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		if err != nil {
			t.Fatalf("write: %v", err)
		}

		// Close connection
		conn.Close()

		// Server should handle this gracefully (test passes if no panic)
	})
}

func TestServer_SocketPermissions(t *testing.T) {
	levels := litestream.CompactionLevels{{Level: 0}}
	store := litestream.NewStore(nil, levels)

	socketPath := shortSocketPath(t)
	server := litestream.NewServer(store)
	server.SocketPath = socketPath
	server.SocketPerms = 0600

	if err := server.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer server.Close()

	info, err := os.Stat(socketPath)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Errorf("expected socket permissions 0600, got %o", perm)
	}
}
