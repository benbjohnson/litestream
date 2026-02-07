package main_test

import (
	"context"
	"testing"

	"github.com/benbjohnson/litestream"
	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestRemoveCommand_Run(t *testing.T) {
	t.Run("MissingPath", func(t *testing.T) {
		cmd := &main.RemoveCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock"})
		if err == nil {
			t.Error("expected error for missing path")
		}
		if err.Error() != "database path required" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("TooManyArguments", func(t *testing.T) {
		cmd := &main.RemoveCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock", "/tmp/test.db", "extra"})
		if err == nil {
			t.Error("expected error for too many arguments")
		}
		if err.Error() != "too many arguments" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("InvalidTimeoutZero", func(t *testing.T) {
		cmd := &main.RemoveCommand{}
		err := cmd.Run(context.Background(), []string{"-timeout", "0", "/tmp/test.db"})
		if err == nil {
			t.Error("expected error for zero timeout")
		}
		if err.Error() != "timeout must be greater than 0" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("InvalidTimeoutNegative", func(t *testing.T) {
		cmd := &main.RemoveCommand{}
		err := cmd.Run(context.Background(), []string{"-timeout", "-1", "/tmp/test.db"})
		if err == nil {
			t.Error("expected error for negative timeout")
		}
		if err.Error() != "timeout must be greater than 0" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("SocketConnectionError", func(t *testing.T) {
		cmd := &main.RemoveCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/nonexistent/socket.sock", "/tmp/test.db"})
		if err == nil {
			t.Error("expected error for socket connection failure")
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

		cmd := &main.RemoveCommand{}
		// Should not error even though database doesn't exist.
		err := cmd.Run(context.Background(), []string{"-socket", server.SocketPath, "/nonexistent/db"})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
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

		cmd := &main.RemoveCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", server.SocketPath, dbPath})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify database was removed from store.
		if len(store.DBs()) != 0 {
			t.Errorf("expected 0 databases in store, got %d", len(store.DBs()))
		}
	})
}
