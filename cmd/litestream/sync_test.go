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

func TestSyncCommand_Run(t *testing.T) {
	t.Run("MissingDBPath", func(t *testing.T) {
		cmd := &main.SyncCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock"})
		if err == nil {
			t.Error("expected error for missing database path")
		}
		if err.Error() != "database path required" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("TooManyArguments", func(t *testing.T) {
		cmd := &main.SyncCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock", "/tmp/a.db", "extra"})
		if err == nil {
			t.Error("expected error for too many arguments")
		}
		if err.Error() != "too many arguments" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("InvalidTimeoutZero", func(t *testing.T) {
		cmd := &main.SyncCommand{}
		err := cmd.Run(context.Background(), []string{"-timeout", "0", "/tmp/test.db"})
		if err == nil {
			t.Error("expected error for zero timeout")
		}
		if err.Error() != "timeout must be greater than 0" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("SocketConnectionError", func(t *testing.T) {
		cmd := &main.SyncCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/nonexistent/socket.sock", "/tmp/test.db"})
		if err == nil {
			t.Error("expected error for socket connection failure")
		}
	})

	t.Run("Success", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		_, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`)
		if err != nil {
			t.Fatal(err)
		}

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		if err := store.Open(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		defer server.Close()

		cmd := &main.SyncCommand{}
		err = cmd.Run(t.Context(), []string{"-socket", server.SocketPath, db.Path()})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("SuccessWithWait", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		_, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`)
		if err != nil {
			t.Fatal(err)
		}

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		if err := store.Open(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		defer server.Close()

		cmd := &main.SyncCommand{}
		err = cmd.Run(t.Context(), []string{"-socket", server.SocketPath, "-wait", db.Path()})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("JSONOutput", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		_, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`)
		if err != nil {
			t.Fatal(err)
		}

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		if err := store.Open(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		defer server.Close()

		output := captureStdout(t, func() {
			cmd := &main.SyncCommand{}
			err = cmd.Run(t.Context(), []string{"-json", "-socket", server.SocketPath, db.Path()})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		var got main.SyncResult
		if err := json.Unmarshal([]byte(output), &got); err != nil {
			t.Fatalf("failed to parse output: %v\n%s", err, output)
		}
		if got.DBPath != db.Path() {
			t.Fatalf("unexpected db path: %s", got.DBPath)
		}
		if got.TXID == 0 {
			t.Fatal("expected non-zero txid")
		}
		if got.ReplicaTXID != nil {
			t.Fatalf("expected omitted replica txid without -wait, got %d", *got.ReplicaTXID)
		}
		if got.DurationMS < 0 {
			t.Fatalf("unexpected duration: %d", got.DurationMS)
		}
	})

	t.Run("JSONOutputWithWait", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		_, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`)
		if err != nil {
			t.Fatal(err)
		}

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		if err := store.Open(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		defer server.Close()

		output := captureStdout(t, func() {
			cmd := &main.SyncCommand{}
			err = cmd.Run(t.Context(), []string{"-json", "-wait", "-socket", server.SocketPath, db.Path()})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		var got main.SyncResult
		if err := json.Unmarshal([]byte(output), &got); err != nil {
			t.Fatalf("failed to parse output: %v\n%s", err, output)
		}
		if got.DBPath != db.Path() {
			t.Fatalf("unexpected db path: %s", got.DBPath)
		}
		if got.TXID == 0 {
			t.Fatal("expected non-zero txid")
		}
		if got.ReplicaTXID == nil {
			t.Fatal("expected replica txid with -wait")
		}
		if *got.ReplicaTXID == 0 {
			t.Fatal("expected non-zero replica txid")
		}
		if got.DurationMS < 0 {
			t.Fatalf("unexpected duration: %d", got.DurationMS)
		}
	})
}

func TestSyncCommand_Usage(t *testing.T) {
	output := captureStdout(t, func() {
		(&main.SyncCommand{}).Usage()
	})

	for _, example := range []string{
		"Examples:",
		"$ litestream sync /path/to/db",
		"$ litestream sync -json /path/to/db",
		"$ litestream sync -wait /path/to/db",
		"$ litestream sync -wait -timeout 120 /path/to/db",
	} {
		if !strings.Contains(output, example) {
			t.Fatalf("usage output missing %q:\n%s", example, output)
		}
	}
}
