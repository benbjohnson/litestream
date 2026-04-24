package main_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestRegisterCommand_Run(t *testing.T) {
	t.Run("MissingDBPath", func(t *testing.T) {
		cmd := &main.RegisterCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock", "-replica", "file:///tmp/backup"})
		if err == nil {
			t.Error("expected error for missing database path")
		}
		if err.Error() != "database path required" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("MissingReplicaFlag", func(t *testing.T) {
		cmd := &main.RegisterCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock", "/tmp/test.db"})
		if err == nil {
			t.Error("expected error for missing replica flag")
		}
		if err.Error() != "-replica is required. Try: litestream register -replica s3://bucket/prefix /path/to/db" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("TooManyArguments", func(t *testing.T) {
		cmd := &main.RegisterCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock", "-replica", "file:///tmp/backup", "/tmp/test.db", "extra"})
		if err == nil {
			t.Error("expected error for too many arguments")
		}
		if err.Error() != "too many arguments" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("InvalidTimeoutZero", func(t *testing.T) {
		cmd := &main.RegisterCommand{}
		err := cmd.Run(context.Background(), []string{"-timeout", "0", "-replica", "file:///tmp/backup", "/tmp/test.db"})
		if err == nil {
			t.Error("expected error for zero timeout")
		}
		if err.Error() != "timeout must be greater than 0" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("SocketConnectionError", func(t *testing.T) {
		cmd := &main.RegisterCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/nonexistent/socket.sock", "-replica", "file:///tmp/backup", "/tmp/test.db"})
		if err == nil {
			t.Error("expected error for socket connection failure")
		}
	})

	t.Run("Success", func(t *testing.T) {
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

		// Create a temporary database file.
		db, sqldb := testingutil.MustOpenDBs(t)
		testingutil.MustCloseDBs(t, db, sqldb)
		dbPath := db.Path()

		// Create a temp directory for backup.
		backupDir := filepath.Join(t.TempDir(), "backup")

		cmd := &main.RegisterCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", server.SocketPath, "-replica", "file://" + backupDir, dbPath})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify database was registered with store.
		if len(store.DBs()) != 1 {
			t.Errorf("expected 1 database in store, got %d", len(store.DBs()))
		}
	})

	t.Run("AlreadyExists", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

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

		// Create a temp directory for backup.
		backupDir := filepath.Join(t.TempDir(), "backup")

		cmd := &main.RegisterCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", server.SocketPath, "-replica", "file://" + backupDir, db.Path()})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Still only 1 database - didn't register a duplicate.
		if len(store.DBs()) != 1 {
			t.Errorf("expected 1 database in store, got %d", len(store.DBs()))
		}
	})

	t.Run("JSONOutput", func(t *testing.T) {
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

		db, sqldb := testingutil.MustOpenDBs(t)
		testingutil.MustCloseDBs(t, db, sqldb)
		backupDir := filepath.Join(t.TempDir(), "backup")
		replicaURL := "file://" + backupDir

		output := captureStdout(t, func() {
			cmd := &main.RegisterCommand{}
			err := cmd.Run(context.Background(), []string{"-json", "-socket", server.SocketPath, "-replica", replicaURL, db.Path()})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		var got main.RegisterResult
		if err := json.Unmarshal([]byte(output), &got); err != nil {
			t.Fatalf("failed to parse output: %v\n%s", err, output)
		}
		if got.Status != "registered" {
			t.Fatalf("unexpected status: %s", got.Status)
		}
		if got.DBPath != db.Path() {
			t.Fatalf("unexpected db path: %s", got.DBPath)
		}
		if got.Replica != replicaURL {
			t.Fatalf("unexpected replica: %s", got.Replica)
		}
		if got.Socket != server.SocketPath {
			t.Fatalf("unexpected socket: %s", got.Socket)
		}
	})

	t.Run("SuggestedReplicaHintExample", func(t *testing.T) {
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

		db, sqldb := testingutil.MustOpenDBs(t)
		testingutil.MustCloseDBs(t, db, sqldb)

		cmd := &main.RegisterCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", server.SocketPath, "-replica", "s3://bucket/prefix", db.Path()})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(store.DBs()) != 1 {
			t.Fatalf("expected 1 database in store, got %d", len(store.DBs()))
		}
	})
}

func TestRegisterCommand_Usage(t *testing.T) {
	output := captureStdout(t, func() {
		(&main.RegisterCommand{}).Usage()
	})

	for _, example := range []string{
		"Examples:",
		"$ litestream register -replica s3://mybucket/db /path/to/db",
		"$ litestream register -replica file:///backup/path /path/to/db",
		"$ litestream register -socket /tmp/litestream.sock -replica s3://mybucket/db /path/to/db",
		"$ litestream register -json -replica s3://mybucket/db /path/to/db",
	} {
		if !strings.Contains(output, example) {
			t.Fatalf("usage output missing %q:\n%s", example, output)
		}
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	t.Cleanup(func() {
		os.Stdout = orig
	})

	fn()

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return string(output)
}
