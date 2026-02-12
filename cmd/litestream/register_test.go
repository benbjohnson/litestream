package main_test

import (
	"context"
	"path/filepath"
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
		if err.Error() != "replica URL required (use -replica flag)" {
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
}
