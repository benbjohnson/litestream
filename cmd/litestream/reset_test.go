package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
)

func TestResetCommand_RunDryRun(t *testing.T) {
	dbPath, ltxPath := createResetCommandTestData(t)

	output := captureLTXCommandStdout(t, func() {
		cmd := &ResetCommand{}
		if err := cmd.Run(context.Background(), []string{"-dry-run", dbPath}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	for _, substr := range []string{
		"Dry run: local Litestream state would be reset for:",
		"Files that would be removed:",
		ltxPath,
		"No files were removed.",
	} {
		if !strings.Contains(output, substr) {
			t.Fatalf("expected output to contain %q:\n%s", substr, output)
		}
	}
	if _, err := os.Stat(ltxPath); err != nil {
		t.Fatalf("expected dry run to keep LTX file: %v", err)
	}
}

func TestResetCommand_Run(t *testing.T) {
	dbPath, ltxPath := createResetCommandTestData(t)

	output := captureLTXCommandStdout(t, func() {
		cmd := &ResetCommand{}
		if err := cmd.Run(context.Background(), []string{dbPath}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(output, "Reset complete.") {
		t.Fatalf("expected reset completion output:\n%s", output)
	}
	if _, err := os.Stat(ltxPath); !os.IsNotExist(err) {
		t.Fatalf("expected LTX file to be removed, stat err=%v", err)
	}
}

func TestResetCommand_RunWithConfigEnv(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db.sqlite")
	if err := os.WriteFile(dbPath, []byte(""), 0600); err != nil {
		t.Fatal(err)
	}

	// Put the Litestream state at a non-default meta path that is only
	// discoverable through the config, so the test fails if reset ignores
	// LITESTREAM_CONFIG and falls back to the default path.
	metaPath := filepath.Join(t.TempDir(), "meta")
	db := litestream.NewDB(dbPath)
	db.SetMetaPath(metaPath)
	ltxDir := filepath.Join(db.LTXDir(), "0")
	if err := os.MkdirAll(ltxDir, 0700); err != nil {
		t.Fatal(err)
	}
	ltxPath := filepath.Join(ltxDir, "0000000000000001-0000000000000001.ltx")
	if err := os.WriteFile(ltxPath, []byte("ltx"), 0600); err != nil {
		t.Fatal(err)
	}

	replicaPath := filepath.Join(t.TempDir(), "replica")
	configPath := filepath.Join(t.TempDir(), "litestream.yml")
	config := "dbs:\n" +
		"  - path: " + dbPath + "\n" +
		"    meta-path: " + metaPath + "\n" +
		"    replicas:\n" +
		"      - url: file://" + replicaPath + "\n"
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LITESTREAM_CONFIG", configPath)

	output := captureLTXCommandStdout(t, func() {
		cmd := &ResetCommand{}
		if err := cmd.Run(context.Background(), []string{dbPath}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(output, "Reset complete.") {
		t.Fatalf("expected reset completion output:\n%s", output)
	}
	if _, err := os.Stat(ltxPath); !os.IsNotExist(err) {
		t.Fatalf("expected LTX file at the config meta path to be removed, stat err=%v", err)
	}
}

func TestResetCommand_RunWithMissingConfigFlag(t *testing.T) {
	dbPath, ltxPath := createResetCommandTestData(t)
	t.Setenv("LITESTREAM_CONFIG", "")
	configPath := filepath.Join(t.TempDir(), "missing.yml")

	cmd := &ResetCommand{}
	if err := cmd.Run(context.Background(), []string{"-config", configPath, dbPath}); !errors.Is(err, ErrConfigFileNotFound) {
		t.Fatalf("expected ErrConfigFileNotFound, got %v", err)
	}
	if _, err := os.Stat(ltxPath); err != nil {
		t.Fatalf("expected LTX file to be kept: %v", err)
	}
}

func TestResetCommand_RunWithMissingConfigEnv(t *testing.T) {
	dbPath, ltxPath := createResetCommandTestData(t)
	t.Setenv("LITESTREAM_CONFIG", filepath.Join(t.TempDir(), "missing.yml"))

	cmd := &ResetCommand{}
	if err := cmd.Run(context.Background(), []string{dbPath}); !errors.Is(err, ErrConfigFileNotFound) {
		t.Fatalf("expected ErrConfigFileNotFound, got %v", err)
	}
	if _, err := os.Stat(ltxPath); err != nil {
		t.Fatalf("expected LTX file to be kept: %v", err)
	}
}

func TestResetCommand_RunWithMissingDefaultConfig(t *testing.T) {
	dbPath, ltxPath := createResetCommandTestData(t)
	t.Setenv("LITESTREAM_CONFIG", "")

	output := captureLTXCommandStdout(t, func() {
		cmd := &ResetCommand{}
		if err := cmd.Run(context.Background(), []string{dbPath}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	if !strings.Contains(output, "Reset complete.") {
		t.Fatalf("expected reset completion output:\n%s", output)
	}
	if _, err := os.Stat(ltxPath); !os.IsNotExist(err) {
		t.Fatalf("expected LTX file to be removed, stat err=%v", err)
	}
}

func createResetCommandTestData(t *testing.T) (string, string) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "db.sqlite")
	if err := os.WriteFile(dbPath, []byte(""), 0600); err != nil {
		t.Fatal(err)
	}

	db := litestream.NewDB(dbPath)
	ltxDir := filepath.Join(db.LTXDir(), "0")
	if err := os.MkdirAll(ltxDir, 0700); err != nil {
		t.Fatal(err)
	}
	ltxPath := filepath.Join(ltxDir, "0000000000000001-0000000000000001.ltx")
	if err := os.WriteFile(ltxPath, []byte("ltx"), 0600); err != nil {
		t.Fatal(err)
	}

	return dbPath, ltxPath
}
