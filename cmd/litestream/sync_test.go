package main_test

import (
	"context"
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
}
