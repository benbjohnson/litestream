package main_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	main "github.com/benbjohnson/litestream/cmd/litestream"
)

func TestDatabasesCommand_Run(t *testing.T) {
	t.Run("TooManyArguments", func(t *testing.T) {
		cmd := &main.DatabasesCommand{}
		err := cmd.Run(context.Background(), []string{"extra-arg"})
		if err == nil {
			t.Fatal("expected error for too many arguments")
		}
		if err.Error() != "too many arguments" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("JSONOutput", func(t *testing.T) {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "test.db")
		configPath := filepath.Join(dir, "litestream.yml")

		if err := os.WriteFile(dbPath, []byte{}, 0644); err != nil {
			t.Fatal(err)
		}

		config := `dbs:
  - path: ` + dbPath + `
    replicas:
      - url: file://` + filepath.Join(dir, "replica") + `
`
		if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
			t.Fatal(err)
		}

		output := captureStdout(t, func() {
			cmd := &main.DatabasesCommand{}
			if err := cmd.Run(context.Background(), []string{"-config", configPath, "-json"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		var got []struct {
			Path    string `json:"path"`
			Replica string `json:"replica"`
		}
		if err := json.Unmarshal([]byte(output), &got); err != nil {
			t.Fatalf("failed to parse output: %v\n%s", err, output)
		}
		if len(got) != 1 {
			t.Fatalf("expected 1 database row, got %d", len(got))
		}
		if got[0].Path != dbPath {
			t.Fatalf("unexpected database path: %s", got[0].Path)
		}
		if got[0].Replica != "file" {
			t.Fatalf("unexpected replica type: %s", got[0].Replica)
		}
	})

	t.Run("StdinJSONOutput", func(t *testing.T) {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "test.db")

		if err := os.WriteFile(dbPath, []byte{}, 0644); err != nil {
			t.Fatal(err)
		}

		withStdin(t, `dbs:
  - path: `+dbPath+`
    replicas:
      - url: file://`+filepath.Join(dir, "replica")+`
`)

		output := captureStdout(t, func() {
			cmd := &main.DatabasesCommand{}
			if err := cmd.Run(context.Background(), []string{"-stdin", "-json"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		var got []struct {
			Path    string `json:"path"`
			Replica string `json:"replica"`
		}
		if err := json.Unmarshal([]byte(output), &got); err != nil {
			t.Fatalf("failed to parse output: %v\n%s", err, output)
		}
		if len(got) != 1 {
			t.Fatalf("expected 1 database row, got %d", len(got))
		}
		if got[0].Path != dbPath {
			t.Fatalf("unexpected database path: %s", got[0].Path)
		}
		if got[0].Replica != "file" {
			t.Fatalf("unexpected replica type: %s", got[0].Replica)
		}
	})

	t.Run("EmptyJSONOutput", func(t *testing.T) {
		dir := t.TempDir()
		configPath := filepath.Join(dir, "litestream.yml")
		if err := os.WriteFile(configPath, []byte("dbs: []\n"), 0644); err != nil {
			t.Fatal(err)
		}

		output := captureStdout(t, func() {
			cmd := &main.DatabasesCommand{}
			if err := cmd.Run(context.Background(), []string{"-config", configPath, "-json"}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})

		if output != "[]\n" {
			t.Fatalf("unexpected output: %q", output)
		}
	})
}
