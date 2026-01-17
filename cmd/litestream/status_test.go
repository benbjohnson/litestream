package main_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/benbjohnson/litestream"
	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestStatusCommand_Run(t *testing.T) {
	t.Run("NoConfig", func(t *testing.T) {
		cmd := &main.StatusCommand{}
		err := cmd.Run(context.Background(), []string{"-config", "/nonexistent/config.yml"})
		if err == nil {
			t.Error("expected error for missing config")
		}
	})

	t.Run("SocketModeRequiresPath", func(t *testing.T) {
		cmd := &main.StatusCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock"})
		if err == nil {
			t.Error("expected error for missing database path")
		}
		if err.Error() != "database path required when using -socket" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("SocketModeTooManyArgs", func(t *testing.T) {
		cmd := &main.StatusCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/tmp/test.sock", "/db1", "/db2"})
		if err == nil {
			t.Error("expected error for too many arguments")
		}
		if err.Error() != "too many arguments" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("SocketModeConnectionError", func(t *testing.T) {
		cmd := &main.StatusCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/nonexistent/socket.sock", "/path/to/db"})
		if err == nil {
			t.Error("expected error for socket connection failure")
		}
	})

	t.Run("WithConfig", func(t *testing.T) {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "test.db")
		configPath := filepath.Join(dir, "litestream.yml")

		// Create a SQLite database.
		if err := os.WriteFile(dbPath, []byte{}, 0644); err != nil {
			t.Fatal(err)
		}

		// Create config file.
		config := `dbs:
  - path: ` + dbPath + `
    replicas:
      - url: file://` + filepath.Join(dir, "replica") + `
`
		if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
			t.Fatal(err)
		}

		cmd := &main.StatusCommand{}
		err := cmd.Run(context.Background(), []string{"-config", configPath})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("FilterByPath", func(t *testing.T) {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "test.db")
		configPath := filepath.Join(dir, "litestream.yml")

		// Create a SQLite database.
		if err := os.WriteFile(dbPath, []byte{}, 0644); err != nil {
			t.Fatal(err)
		}

		// Create config file.
		config := `dbs:
  - path: ` + dbPath + `
    replicas:
      - url: file://` + filepath.Join(dir, "replica") + `
`
		if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
			t.Fatal(err)
		}

		cmd := &main.StatusCommand{}
		err := cmd.Run(context.Background(), []string{"-config", configPath, dbPath})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("SocketModeSuccess", func(t *testing.T) {
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

		cmd := &main.StatusCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", server.SocketPath, db.Path()})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
