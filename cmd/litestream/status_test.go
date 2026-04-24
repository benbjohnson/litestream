package main_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	main "github.com/benbjohnson/litestream/cmd/litestream"
)

func TestStatusCommand_Run(t *testing.T) {
	t.Run("NoConfig", func(t *testing.T) {
		cmd := &main.StatusCommand{}
		err := cmd.Run(context.Background(), []string{"-config", "/nonexistent/config.yml"})
		if err == nil {
			t.Error("expected error for missing config")
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

	t.Run("JSONOutput", func(t *testing.T) {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "missing.db")
		configPath := filepath.Join(dir, "litestream.yml")

		config := `dbs:
  - path: ` + dbPath + `
    replicas:
      - url: file://` + filepath.Join(dir, "replica") + `
`
		if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
			t.Fatal(err)
		}

		output := captureStdout(t, func() {
			cmd := &main.StatusCommand{}
			if err := cmd.Run(context.Background(), []string{"-config", configPath, "-json"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		var got []struct {
			Database  string `json:"database"`
			Status    string `json:"status"`
			LocalTXID string `json:"local_txid"`
			WALSize   string `json:"wal_size"`
		}
		if err := json.Unmarshal([]byte(output), &got); err != nil {
			t.Fatalf("failed to parse output: %v\n%s", err, output)
		}
		if len(got) != 1 {
			t.Fatalf("expected 1 status row, got %d", len(got))
		}
		if got[0].Database != dbPath {
			t.Fatalf("unexpected database path: %s", got[0].Database)
		}
		if got[0].Status != "no database" {
			t.Fatalf("unexpected status: %s", got[0].Status)
		}
		if got[0].LocalTXID != "-" {
			t.Fatalf("unexpected local txid: %s", got[0].LocalTXID)
		}
		if got[0].WALSize != "-" {
			t.Fatalf("unexpected wal size: %s", got[0].WALSize)
		}
	})
}
