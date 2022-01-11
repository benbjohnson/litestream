package main_test

import (
	"context"
	"flag"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestDatabasesCommand(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		testDir := filepath.Join("testdata", "databases", "ok")
		m, _, stdout, _ := newMain()
		if err := m.Run(context.Background(), []string{"databases", "-config", filepath.Join(testDir, "litestream.yml")}); err != nil {
			t.Fatal(err)
		} else if got, want := stdout.String(), string(testingutil.MustReadFile(t, filepath.Join(testDir, "stdout"))); got != want {
			t.Fatalf("stdout=%q, want %q", got, want)
		}
	})

	t.Run("NoDatabases", func(t *testing.T) {
		testDir := filepath.Join("testdata", "databases", "no-databases")
		m, _, stdout, _ := newMain()
		if err := m.Run(context.Background(), []string{"databases", "-config", filepath.Join(testDir, "litestream.yml")}); err != nil {
			t.Fatal(err)
		} else if got, want := stdout.String(), string(testingutil.MustReadFile(t, filepath.Join(testDir, "stdout"))); got != want {
			t.Fatalf("stdout=%q, want %q", got, want)
		}
	})

	t.Run("ErrConfigNotFound", func(t *testing.T) {
		testDir := filepath.Join("testdata", "databases", "no-config")
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"databases", "-config", filepath.Join(testDir, "litestream.yml")})
		if err == nil || !strings.Contains(err.Error(), `config file not found:`) {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrInvalidConfig", func(t *testing.T) {
		testDir := filepath.Join("testdata", "databases", "invalid-config")
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"databases", "-config", filepath.Join(testDir, "litestream.yml")})
		if err == nil || !strings.Contains(err.Error(), `replica path cannot be a url`) {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrTooManyArguments", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"databases", "xyz"})
		if err == nil || err.Error() != `too many arguments` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("Usage", func(t *testing.T) {
		m, _, _, _ := newMain()
		if err := m.Run(context.Background(), []string{"databases", "-h"}); err != flag.ErrHelp {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}
