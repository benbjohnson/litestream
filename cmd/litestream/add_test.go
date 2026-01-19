package main_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/benbjohnson/litestream"
	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestAddCommand_Run(t *testing.T) {
	t.Run("MissingArguments", func(t *testing.T) {
		cmd := &main.AddCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock"})
		if err == nil {
			t.Error("expected error for missing arguments")
		}
		if err.Error() != "database path and replica URL required" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("MissingReplicaURL", func(t *testing.T) {
		cmd := &main.AddCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock", "/tmp/test.db"})
		if err == nil {
			t.Error("expected error for missing replica URL")
		}
		if err.Error() != "database path and replica URL required" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("TooManyArguments", func(t *testing.T) {
		cmd := &main.AddCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock", "/tmp/test.db", "file:///tmp/backup", "extra"})
		if err == nil {
			t.Error("expected error for too many arguments")
		}
		if err.Error() != "too many arguments" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("SocketConnectionError", func(t *testing.T) {
		cmd := &main.AddCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/nonexistent/socket.sock", "/tmp/test.db", "file:///tmp/backup"})
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

		cmd := &main.AddCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", server.SocketPath, dbPath, "file://" + backupDir})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify database was added to store.
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

		cmd := &main.AddCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", server.SocketPath, db.Path(), "file://" + backupDir})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Still only 1 database - didn't add a duplicate.
		if len(store.DBs()) != 1 {
			t.Errorf("expected 1 database in store, got %d", len(store.DBs()))
		}
	})
}
