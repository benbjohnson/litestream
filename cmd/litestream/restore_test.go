package main_test

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestRestoreCommand(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "ok")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()
		tempDir := t.TempDir()

		m, _, stdout, stderr := newMain()
		if err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), "-o", filepath.Join(tempDir, "db"), filepath.Join(testDir, "db")}); err != nil {
			t.Fatal(err)
		} else if got, want := stderr.String(), ""; got != want {
			t.Fatalf("stderr=%q, want %q", got, want)
		}

		// STDOUT has timing info so we need to grep per line.
		lines := strings.Split(stdout.String(), "\n")
		for i, substr := range []string{
			`restoring snapshot 0000000000000000/00000000 to ` + filepath.Join(tempDir, "db.tmp"),
			`applied wal 0000000000000000/00000000 elapsed=`,
			`applied wal 0000000000000000/00000001 elapsed=`,
			`applied wal 0000000000000000/00000002 elapsed=`,
			`renaming database from temporary location`,
		} {
			if !strings.Contains(lines[i], substr) {
				t.Fatalf("stdout: unexpected line %d:\n%s", i+1, stdout)
			}
		}
	})

	t.Run("ReplicaName", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "replica-name")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()
		tempDir := t.TempDir()

		m, _, stdout, stderr := newMain()
		if err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), "-o", filepath.Join(tempDir, "db"), "-replica", "replica1", filepath.Join(testDir, "db")}); err != nil {
			t.Fatal(err)
		} else if got, want := stderr.String(), ""; got != want {
			t.Fatalf("stderr=%q, want %q", got, want)
		}

		// STDOUT has timing info so we need to grep per line.
		lines := strings.Split(stdout.String(), "\n")
		for i, substr := range []string{
			`restoring snapshot 0000000000000001/00000001 to ` + filepath.Join(tempDir, "db.tmp"),
			`no wal files found, snapshot only`,
			`renaming database from temporary location`,
		} {
			if !strings.Contains(lines[i], substr) {
				t.Fatalf("stdout: unexpected line %d:\n%s", i+1, stdout)
			}
		}
	})

	t.Run("ReplicaURL", func(t *testing.T) {
		testDir := filepath.Join(testingutil.Getwd(t), "testdata", "restore", "replica-url")
		tempDir := t.TempDir()
		replicaURL := "file://" + filepath.ToSlash(testDir) + "/replica"

		m, _, stdout, stderr := newMain()
		if err := m.Run(context.Background(), []string{"restore", "-o", filepath.Join(tempDir, "db"), replicaURL}); err != nil {
			t.Fatal(err)
		} else if got, want := stderr.String(), ""; got != want {
			t.Fatalf("stderr=%q, want %q", got, want)
		}

		lines := strings.Split(stdout.String(), "\n")
		for i, substr := range []string{
			`restoring snapshot 0000000000000000/00000000 to ` + filepath.Join(tempDir, "db.tmp"),
			`no wal files found, snapshot only`,
			`renaming database from temporary location`,
		} {
			if !strings.Contains(lines[i], substr) {
				t.Fatalf("stdout: unexpected line %d:\n%s", i+1, stdout)
			}
		}
	})

	t.Run("LatestReplica", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "latest-replica")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()
		tempDir := t.TempDir()

		m, _, stdout, stderr := newMain()
		if err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), "-o", filepath.Join(tempDir, "db"), filepath.Join(testDir, "db")}); err != nil {
			t.Fatal(err)
		} else if got, want := stderr.String(), ""; got != want {
			t.Fatalf("stderr=%q, want %q", got, want)
		}

		lines := strings.Split(stdout.String(), "\n")
		for i, substr := range []string{
			`restoring snapshot 0000000000000001/00000000 to ` + filepath.Join(tempDir, "db.tmp"),
			`no wal files found, snapshot only`,
			`renaming database from temporary location`,
		} {
			if !strings.Contains(lines[i], substr) {
				t.Fatalf("stdout: unexpected line %d:\n%s", i+1, stdout)
			}
		}
	})

	t.Run("IfDBNotExistsFlag", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "if-db-not-exists-flag")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()

		m, _, stdout, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), "-if-db-not-exists", filepath.Join(testDir, "db")})
		if err != nil {
			t.Fatal(err)
		} else if got, want := stdout.String(), string(testingutil.ReadFile(t, filepath.Join(testDir, "stdout"))); got != want {
			t.Fatalf("stdout=%q, want %q", got, want)
		}
	})

	t.Run("IfReplicaExists", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "if-replica-exists-flag")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()

		m, _, stdout, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), "-if-replica-exists", filepath.Join(testDir, "db")})
		if err != nil {
			t.Fatal(err)
		} else if got, want := stdout.String(), string(testingutil.ReadFile(t, filepath.Join(testDir, "stdout"))); got != want {
			t.Fatalf("stdout=%q, want %q", got, want)
		}
	})

	t.Run("ErrNoBackups", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "no-backups")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()
		tempDir := t.TempDir()

		m, _, stdout, stderr := newMain()
		err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), "-o", filepath.Join(tempDir, "db"), filepath.Join(testDir, "db")})
		if err == nil || err.Error() != `no matching backups found` {
			t.Fatalf("unexpected error: %s", err)
		} else if got, want := stdout.String(), string(testingutil.ReadFile(t, filepath.Join(testDir, "stdout"))); got != want {
			t.Fatalf("stdout=%q, want %q", got, want)
		} else if got, want := stderr.String(), string(testingutil.ReadFile(t, filepath.Join(testDir, "stderr"))); got != want {
			t.Fatalf("stderr=%q, want %q", got, want)
		}
	})

	t.Run("ErrNoGeneration", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "no-generation")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()

		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), filepath.Join(testDir, "db")})
		if err == nil || err.Error() != `no matching backups found` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrOutputPathExists", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "output-path-exists")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()

		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), filepath.Join(testDir, "db")})
		if err == nil || err.Error() != `output file already exists: `+filepath.Join(testDir, "db") {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrDatabaseOrReplicaRequired", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore"})
		if err == nil || err.Error() != `database path or replica URL required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrTooManyArguments", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "abc", "123"})
		if err == nil || err.Error() != `too many arguments` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrInvalidFlags", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-no-such-flag"})
		if err == nil || err.Error() != `flag provided but not defined: -no-such-flag` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrIndexFlagOnly", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-index", "0", "/var/lib/db"})
		if err == nil || err.Error() != `must specify -generation flag when using -index flag` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrConfigFileNotFound", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-config", "/no/such/file", "/var/lib/db"})
		if err == nil || err.Error() != `config file not found: /no/such/file` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrInvalidConfig", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "invalid-config")
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), "/var/lib/db"})
		if err == nil || !strings.Contains(err.Error(), `replica path cannot be a url`) {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrMkdir", func(t *testing.T) {
		tempDir := t.TempDir()
		if err := os.Mkdir(filepath.Join(tempDir, "noperm"), 0000); err != nil {
			t.Fatal(err)
		}

		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-o", filepath.Join(tempDir, "noperm", "subdir", "db"), "/var/lib/db"})
		if err == nil || !strings.Contains(err.Error(), `permission denied`) {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrNoOutputPathWithReplicaURL", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "file://path/to/replica"})
		if err == nil || err.Error() != `output path required when using a replica URL` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrReplicaNameWithReplicaURL", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-replica", "replica0", "file://path/to/replica"})
		if err == nil || err.Error() != `cannot specify both the replica URL and the -replica flag` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrInvalidReplicaURL", func(t *testing.T) {
		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-o", "/tmp/db", "xyz://xyz"})
		if err == nil || err.Error() != `unknown replica type in config: "xyz"` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrDatabaseNotFound", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "database-not-found")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()

		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), "/no/such/db"})
		if err == nil || err.Error() != `database not found in config: /no/such/db` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrNoReplicas", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "no-replicas")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()
		tempDir := t.TempDir()

		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), "-o", filepath.Join(tempDir, "db"), filepath.Join(testDir, "db")})
		if err == nil || err.Error() != `database has no replicas: `+filepath.Join(testingutil.Getwd(t), testDir, "db") {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrReplicaNotFound", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "replica-not-found")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()
		tempDir := t.TempDir()

		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), "-o", filepath.Join(tempDir, "db"), "-replica", "no_such_replica", filepath.Join(testDir, "db")})
		if err == nil || err.Error() != `replica "no_such_replica" not found` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrGenerationWithNoReplicaName", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "generation-with-no-replica")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()
		tempDir := t.TempDir()

		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), "-o", filepath.Join(tempDir, "db"), "-generation", "0000000000000000", filepath.Join(testDir, "db")})
		if err == nil || err.Error() != `must specify -replica flag when restoring from a specific generation` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrNoSnapshotsAvailable", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "no-snapshots")
		defer testingutil.Setenv(t, "LITESTREAM_TESTDIR", testDir)()
		tempDir := t.TempDir()

		m, _, _, _ := newMain()
		err := m.Run(context.Background(), []string{"restore", "-config", filepath.Join(testDir, "litestream.yml"), "-o", filepath.Join(tempDir, "db"), "-generation", "0000000000000000", filepath.Join(testDir, "db")})
		if err == nil || err.Error() != `cannot determine latest index in generation "0000000000000000": no snapshots available` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("Usage", func(t *testing.T) {
		m, _, _, _ := newMain()
		if err := m.Run(context.Background(), []string{"restore", "-h"}); err != flag.ErrHelp {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}
