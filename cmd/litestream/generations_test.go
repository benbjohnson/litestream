package main_test

import (
	"context"
	"flag"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestGenerationsCommand(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		testDir := filepath.Join("testdata", "generations", "ok")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()

		m, _, stdout, _ := newMain()
		if err := m.Run(context.Background(), []string{"generations", "-config", filepath.Join(testDir, "litestream.yml"), filepath.Join(testDir, "db")}); err != nil {
			t.Fatal(err)
		} else if got, want := stdout.String(), string(testingutil.ReadFile(t, filepath.Join(testDir, "stdout"))); got != want {
			t.Fatalf("stdout=%q, want %q", got, want)
		}
	})

	t.Run("ReplicaName", func(t *testing.T) {
		testDir := filepath.Join("testdata", "generations", "replica-name")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()

		m, _, stdout, _ := newMain()
		if err := m.Run(context.Background(), []string{"generations", "-config", filepath.Join(testDir, "litestream.yml"), "-replica", "replica1", filepath.Join(testDir, "db")}); err != nil {
			t.Fatal(err)
		} else if got, want := stdout.String(), string(testingutil.ReadFile(t, filepath.Join(testDir, "stdout"))); got != want {
			t.Fatalf("stdout=%q, want %q", got, want)
		}
	})

	t.Run("ReplicaURL", func(t *testing.T) {
		testDir := filepath.Join(testingutil.Getwd(t), "testdata", "generations", "replica-url")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()
		replicaURL := "file://" + filepath.ToSlash(testDir) + "/replica"

		m, _, stdout, _ := newMain()
		if err := m.Run(context.Background(), []string{"generations", replicaURL}); err != nil {
			t.Fatal(err)
		} else if got, want := stdout.String(), string(testingutil.ReadFile(t, filepath.Join(testDir, "stdout"))); got != want {
			t.Fatalf("stdout=%q, want %q", got, want)
		}
	})

	t.Run("NoDatabase", func(t *testing.T) {
		testDir := filepath.Join("testdata", "generations", "no-database")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()

		m, _, stdout, _ := newMain()
		if err := m.Run(context.Background(), []string{"generations", "-config", filepath.Join(testDir, "litestream.yml"), filepath.Join(testDir, "db")}); err != nil {
			t.Fatal(err)
		} else if got, want := stdout.String(), string(testingutil.ReadFile(t, filepath.Join(testDir, "stdout"))); got != want {
			t.Fatalf("stdout=%q, want %q", got, want)
		}
	})

	t.Run("ErrDatabaseOrReplicaRequired", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"generations"})
		if err == nil || err.Error() != `database path or replica URL required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrTooManyArguments", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"generations", "abc", "123"})
		if err == nil || err.Error() != `too many arguments` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrInvalidFlags", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"generations", "-no-such-flag"})
		if err == nil || err.Error() != `flag provided but not defined: -no-such-flag` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrConfigFileNotFound", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"generations", "-config", "/no/such/file", "/var/lib/db"})
		if err == nil || err.Error() != `config file not found: /no/such/file` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrInvalidConfig", func(t *testing.T) {
		testDir := filepath.Join("testdata", "generations", "invalid-config")
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"generations", "-config", filepath.Join(testDir, "litestream.yml"), "/var/lib/db"})
		if err == nil || !strings.Contains(err.Error(), `replica path cannot be a url`) {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrDatabaseNotFound", func(t *testing.T) {
		testDir := filepath.Join("testdata", "generations", "database-not-found")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()

		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"generations", "-config", filepath.Join(testDir, "litestream.yml"), "/no/such/db"})
		if err == nil || err.Error() != `database not found in config: /no/such/db` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrReplicaNotFound", func(t *testing.T) {
		testDir := filepath.Join(testingutil.Getwd(t), "testdata", "generations", "replica-not-found")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()

		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"generations", "-config", filepath.Join(testDir, "litestream.yml"), "-replica", "no_such_replica", filepath.Join(testDir, "db")})
		if err == nil || err.Error() != `replica "no_such_replica" not found for database "`+filepath.Join(testDir, "db")+`"` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrInvalidReplicaURL", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"generations", "xyz://xyz"})
		if err == nil || !strings.Contains(err.Error(), `unknown replica type in config: "xyz"`) {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("Usage", func(t *testing.T) {
		m, _, _, _ := newMain()
		if err := m.Run(context.Background(), []string{"generations", "-h"}); err != flag.ErrHelp {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}
