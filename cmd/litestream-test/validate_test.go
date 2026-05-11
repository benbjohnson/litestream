package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestValidatePerformRestorePassesTXID(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script fake binary requires Unix")
	}

	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args")
	writeFakeLitestream(t, dir, `
if [ "$1" = "restore" ]; then
  shift
fi
printf '%s\n' "$@" > "$LITESTREAM_ARGS"
out=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    shift
    out="$1"
  fi
  shift
done
: > "$out"
`)
	t.Setenv("LITESTREAM_ARGS", argsPath)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	cmd := &ValidateCommand{
		SourceDB:   filepath.Join(dir, "test.db"),
		ConfigPath: filepath.Join(dir, "litestream.yml"),
		RestoredDB: filepath.Join(dir, "test.db.restored"),
		TXID:       "00000000000224b6",
	}
	result := cmd.performRestore(context.Background())
	if !result.Passed {
		t.Fatalf("restore failed: %s", result.ErrorMessage)
	}

	args := readLines(t, argsPath)
	assertContains(t, args, "-config")
	assertContains(t, args, cmd.ConfigPath)
	assertContains(t, args, "-txid")
	assertContains(t, args, cmd.TXID)
	assertContains(t, args, "-o")
	assertContains(t, args, cmd.RestoredDB)
	assertContains(t, args, cmd.SourceDB)
}

func writeFakeLitestream(t *testing.T, dir, body string) {
	t.Helper()

	path := filepath.Join(dir, "litestream")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
}

func readLines(t *testing.T, path string) []string {
	t.Helper()

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.TrimSpace(string(body))
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

func assertContains(t *testing.T, values []string, want string) {
	t.Helper()

	for _, value := range values {
		if value == want {
			return
		}
	}
	t.Fatalf("values=%v want %q", values, want)
}
