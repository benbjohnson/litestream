package main

import (
	"context"
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
