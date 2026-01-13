package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/benbjohnson/litestream"
)

func TestControlServer_loadAndRegisterDB(t *testing.T) {
	t.Run("LoadValidConfig", func(t *testing.T) {
		// Create temp directory for test
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		configPath := filepath.Join(tmpDir, "config.yml")

		// Write a minimal config file
		configContent := `dbs:
  - path: ` + dbPath + `
    replicas:
      - url: file://` + filepath.Join(tmpDir, "replica") + `
`
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			t.Fatalf("failed to write config: %v", err)
		}

		// Create control server with store
		store := litestream.NewStore(nil, nil)
		controlServer := &ControlServer{
			store: store,
			ctx:   context.Background(),
		}

		// Load and register DB
		err := controlServer.loadAndRegisterDB(dbPath, configPath)
		if err != nil {
			t.Fatalf("loadAndRegisterDB failed: %v", err)
		}

		// Verify DB was registered
		db := store.FindDB(dbPath)
		if db == nil {
			t.Fatal("expected database to be registered")
		}

		if db.Path() != dbPath {
			t.Fatalf("expected path %s, got %s", dbPath, db.Path())
		}

		if db.Replica == nil {
			t.Fatal("expected replica to be configured")
		}
	})

	t.Run("ErrorOnNonexistentConfig", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		configPath := filepath.Join(tmpDir, "nonexistent.yml")

		store := litestream.NewStore(nil, nil)
		controlServer := &ControlServer{
			store: store,
			ctx:   context.Background(),
		}

		err := controlServer.loadAndRegisterDB(dbPath, configPath)
		if err == nil {
			t.Fatal("expected error for nonexistent config file")
		}
	})

	t.Run("ErrorOnDatabaseNotInConfig", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		otherPath := filepath.Join(tmpDir, "other.db")
		configPath := filepath.Join(tmpDir, "config.yml")

		// Config file has a different database path
		configContent := `dbs:
  - path: ` + otherPath + `
    replicas:
      - url: file://` + filepath.Join(tmpDir, "replica") + `
`
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			t.Fatalf("failed to write config: %v", err)
		}

		store := litestream.NewStore(nil, nil)
		controlServer := &ControlServer{
			store: store,
			ctx:   context.Background(),
		}

		err := controlServer.loadAndRegisterDB(dbPath, configPath)
		if err == nil {
			t.Fatal("expected error when database not found in config")
		}
	})
}
