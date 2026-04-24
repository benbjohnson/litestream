package main_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestUnregisterCommand_Run(t *testing.T) {
	t.Run("MissingPath", func(t *testing.T) {
		cmd := &main.UnregisterCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock"})
		if err == nil {
			t.Error("expected error for missing path")
		}
		if err.Error() != "database path required" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("TooManyArguments", func(t *testing.T) {
		cmd := &main.UnregisterCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock", "/tmp/test.db", "extra"})
		if err == nil {
			t.Error("expected error for too many arguments")
		}
		if err.Error() != "too many arguments" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("InvalidTimeoutZero", func(t *testing.T) {
		cmd := &main.UnregisterCommand{}
		err := cmd.Run(context.Background(), []string{"-timeout", "0", "/tmp/test.db"})
		if err == nil {
			t.Error("expected error for zero timeout")
		}
		if err.Error() != "timeout must be greater than 0" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("InvalidTimeoutNegative", func(t *testing.T) {
		cmd := &main.UnregisterCommand{}
		err := cmd.Run(context.Background(), []string{"-timeout", "-1", "/tmp/test.db"})
		if err == nil {
			t.Error("expected error for negative timeout")
		}
		if err.Error() != "timeout must be greater than 0" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("SocketConnectionError", func(t *testing.T) {
		cmd := &main.UnregisterCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/nonexistent/socket.sock", "/tmp/test.db"})
		if err == nil {
			t.Error("expected error for socket connection failure")
		}
	})

	t.Run("DryRunDoesNotConnect", func(t *testing.T) {
		output := captureStdout(t, func() {
			cmd := &main.UnregisterCommand{}
			err := cmd.Run(context.Background(), []string{"-dry-run", "-socket", "/nonexistent/socket.sock", "/tmp/test.db"})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})

		for _, substr := range []string{
			"Dry run: unregister request preview",
			"database: /tmp/test.db",
			"socket: /nonexistent/socket.sock",
			"replicas: daemon-managed replica for this database",
			"final sync: daemon close will sync the database and replica before the command completes",
			"No unregister request was sent.",
		} {
			if !strings.Contains(output, substr) {
				t.Fatalf("output missing %q:\n%s", substr, output)
			}
		}
	})

	t.Run("NotFoundIsIdempotent", func(t *testing.T) {
		store := litestream.NewStore(nil, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		if err := store.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer store.Close(context.Background())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		defer server.Close()

		output := captureStdout(t, func() {
			cmd := &main.UnregisterCommand{}
			// Should not error even though database doesn't exist.
			err := cmd.Run(context.Background(), []string{"-socket", server.SocketPath, "/nonexistent/db"})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "status: not_registered") {
			t.Fatalf("expected not_registered status, got:\n%s", output)
		}
		if !strings.Contains(output, "final_txid: 0") {
			t.Fatalf("expected zero final txid, got:\n%s", output)
		}
	})

	t.Run("Success", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)
		dbPath := db.Path()

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		if err := store.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer store.Close(context.Background())

		// Verify database is initially in store.
		if len(store.DBs()) != 1 {
			t.Fatalf("expected 1 database in store, got %d", len(store.DBs()))
		}

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		defer server.Close()

		cmd := &main.UnregisterCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", server.SocketPath, dbPath})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify database was unregistered from store.
		if len(store.DBs()) != 0 {
			t.Errorf("expected 0 databases in store, got %d", len(store.DBs()))
		}
	})

	t.Run("JSONOutput", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)
		dbPath := db.Path()

		_, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		if err := store.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer store.Close(context.Background())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		defer server.Close()

		output := captureStdout(t, func() {
			cmd := &main.UnregisterCommand{}
			err := cmd.Run(context.Background(), []string{"-json", "-socket", server.SocketPath, dbPath})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		var got main.UnregisterResult
		if err := json.Unmarshal([]byte(output), &got); err != nil {
			t.Fatalf("failed to parse output: %v\n%s", err, output)
		}
		if got.Status != "unregistered" {
			t.Fatalf("unexpected status: %s", got.Status)
		}
		if got.DBPath != dbPath {
			t.Fatalf("unexpected db path: %s", got.DBPath)
		}
		if got.FinalTXID == 0 {
			t.Fatal("expected non-zero final txid")
		}
		if got.Socket != server.SocketPath {
			t.Fatalf("unexpected socket: %s", got.Socket)
		}
		if len(store.DBs()) != 0 {
			t.Errorf("expected 0 databases in store, got %d", len(store.DBs()))
		}
	})
}

func TestUnregisterCommand_Usage(t *testing.T) {
	output := captureStdout(t, func() {
		(&main.UnregisterCommand{}).Usage()
	})

	for _, example := range []string{
		"Examples:",
		"$ litestream unregister /path/to/db",
		"$ litestream unregister -json /path/to/db",
		"$ litestream unregister -dry-run /path/to/db",
		"$ litestream unregister -socket /tmp/litestream.sock /path/to/db",
		"$ litestream unregister -timeout 10 /path/to/db",
	} {
		if !strings.Contains(output, example) {
			t.Fatalf("usage output missing %q:\n%s", example, output)
		}
	}
}
