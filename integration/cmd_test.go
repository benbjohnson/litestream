package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/litestream/internal"
	"github.com/benbjohnson/litestream/internal/testingutil"
	_ "github.com/mattn/go-sqlite3"
)

var longRunningDuration = flag.Duration("long-running-duration", 0, "")

func init() {
	fmt.Fprintln(os.Stderr, "# ")
	fmt.Fprintln(os.Stderr, "# NOTE: Build litestream to your PATH before running integration tests")
	fmt.Fprintln(os.Stderr, "#")
	fmt.Fprintln(os.Stderr, "")
}

// Ensure the default configuration works with light database load.
func TestCmd_Replicate_OK(t *testing.T) {
	ctx := context.Background()
	testDir, tempDir := filepath.Join("testdata", "replicate", "ok"), t.TempDir()
	env := []string{"LITESTREAM_TEMPDIR=" + tempDir}

	cmd, stdout, _ := commandContext(ctx, env, "replicate", "-config", filepath.Join(testDir, "litestream.yml"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite3", filepath.Join(tempDir, "db"))
	if err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `PRAGMA journal_mode = wal`); err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Execute writes periodically.
	for i := 0; i < 100; i++ {
		t.Logf("[exec] INSERT INTO t (id) VALUES (%d)", i)
		if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (?)`, i); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Stop & wait for Litestream command.
	killLitestreamCmd(t, cmd, stdout)

	// Ensure signal and shutdown are logged.
	if s := stdout.String(); !strings.Contains(s, `signal received, litestream shutting down`) {
		t.Fatal("missing log output for signal received")
	} else if s := stdout.String(); !strings.Contains(s, `litestream shut down`) {
		t.Fatal("missing log output for shut down")
	}

	// Checkpoint & verify original SQLite database.
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	restoreAndVerify(t, ctx, env, filepath.Join(testDir, "litestream.yml"), filepath.Join(tempDir, "db"))
}

// Ensure that stopping and restarting Litestream before an application-induced
// checkpoint will cause Litestream to continue replicating using the same generation.
func TestCmd_Replicate_ResumeWithCurrentGeneration(t *testing.T) {
	ctx := context.Background()
	testDir, tempDir := filepath.Join("testdata", "replicate", "resume-with-current-generation"), t.TempDir()
	env := []string{"LITESTREAM_TEMPDIR=" + tempDir}

	cmd, stdout, _ := commandContext(ctx, env, "replicate", "-config", filepath.Join(testDir, "litestream.yml"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	t.Log("writing to database during replication")

	db, err := sql.Open("sqlite3", filepath.Join(tempDir, "db"))
	if err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Execute a few writes to populate the WAL.
	if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (1)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (2)`); err != nil {
		t.Fatal(err)
	}

	// Wait for replication to occur & shutdown.
	waitForLogMessage(t, stdout, `wal segment written`)
	killLitestreamCmd(t, cmd, stdout)
	t.Log("replication shutdown, continuing database writes")

	// Execute a few more writes while replication is stopped.
	if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (3)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (4)`); err != nil {
		t.Fatal(err)
	}

	t.Log("restarting replication")

	cmd, stdout, _ = commandContext(ctx, env, "replicate", "-config", filepath.Join(testDir, "litestream.yml"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	waitForLogMessage(t, stdout, `wal segment written`)
	killLitestreamCmd(t, cmd, stdout)

	t.Log("replication shutdown again")

	// Litestream should resume replication from the previous generation.
	if s := stdout.String(); strings.Contains(s, "no generation exists") {
		t.Fatal("expected existing generation to resume; started new generation instead")
	}

	// Checkpoint & verify original SQLite database.
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	restoreAndVerify(t, ctx, env, filepath.Join(testDir, "litestream.yml"), filepath.Join(tempDir, "db"))
}

// Ensure that restarting Litestream after a full checkpoint has occurred will
// cause it to begin a new generation.
func TestCmd_Replicate_ResumeWithNewGeneration(t *testing.T) {
	ctx := context.Background()
	testDir, tempDir := filepath.Join("testdata", "replicate", "resume-with-new-generation"), t.TempDir()
	env := []string{"LITESTREAM_TEMPDIR=" + tempDir}

	cmd, stdout, _ := commandContext(ctx, env, "replicate", "-config", filepath.Join(testDir, "litestream.yml"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	t.Log("writing to database during replication")

	db, err := sql.Open("sqlite3", filepath.Join(tempDir, "db"))
	if err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Execute a few writes to populate the WAL.
	if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (1)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (2)`); err != nil {
		t.Fatal(err)
	}

	// Wait for replication to occur & shutdown.
	waitForLogMessage(t, stdout, `wal segment written`)
	killLitestreamCmd(t, cmd, stdout)
	t.Log("replication shutdown, continuing database writes")

	// Execute a few more writes while replication is stopped.
	if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (3)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (4)`); err != nil {
		t.Fatal(err)
	}

	t.Log("issuing checkpoint")

	// Issue a checkpoint to restart WAL.
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(RESTART)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (5)`); err != nil {
		t.Fatal(err)
	}

	t.Log("restarting replication")

	cmd, stdout, _ = commandContext(ctx, env, "replicate", "-config", filepath.Join(testDir, "litestream.yml"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	waitForLogMessage(t, stdout, `wal segment written`)
	killLitestreamCmd(t, cmd, stdout)

	t.Log("replication shutdown again")

	// Litestream should resume replication from the previous generation.
	if s := stdout.String(); !strings.Contains(s, "no generation exists") {
		t.Fatal("expected new generation to start; continued existing generation instead")
	}

	// Checkpoint & verify original SQLite database.
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	restoreAndVerify(t, ctx, env, filepath.Join(testDir, "litestream.yml"), filepath.Join(tempDir, "db"))
}

// Ensure the monitor interval can be turned off.
func TestCmd_Replicate_NoMonitorDelayInterval(t *testing.T) {
	ctx := context.Background()
	testDir, tempDir := filepath.Join("testdata", "replicate", "no-monitor-delay-interval"), t.TempDir()
	env := []string{"LITESTREAM_TEMPDIR=" + tempDir}

	cmd, stdout, _ := commandContext(ctx, env, "replicate", "-config", filepath.Join(testDir, "litestream.yml"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite3", filepath.Join(tempDir, "db"))
	if err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `PRAGMA journal_mode = wal`); err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	time.Sleep(1 * time.Second)

	// Execute writes periodically.
	for i := 0; i < 10; i++ {
		t.Logf("[exec] INSERT INTO t (id) VALUES (%d)", i)
		if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (?)`, i); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Stop & wait for Litestream command.
	killLitestreamCmd(t, cmd, stdout)

	// Ensure signal and shutdown are logged.
	if s := stdout.String(); !strings.Contains(s, `signal received, litestream shutting down`) {
		t.Fatal("missing log output for signal received")
	} else if s := stdout.String(); !strings.Contains(s, `litestream shut down`) {
		t.Fatal("missing log output for shut down")
	}

	// Checkpoint & verify original SQLite database.
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	restoreAndVerify(t, ctx, env, filepath.Join(testDir, "litestream.yml"), filepath.Join(tempDir, "db"))
}

// Ensure the default configuration works with heavy write load.
func TestCmd_Replicate_HighLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode enabled, skipping")
	} else if os.Getenv("CI") != "" {
		t.Skip("ci, skipping")
	}

	const writeDuration = 30 * time.Second

	ctx := context.Background()
	testDir, tempDir := filepath.Join("testdata", "replicate", "high-load"), t.TempDir()
	env := []string{"LITESTREAM_TEMPDIR=" + tempDir}

	cmd, stdout, _ := commandContext(ctx, env, "replicate", "-config", filepath.Join(testDir, "litestream.yml"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite3", filepath.Join(tempDir, "db"))
	if err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `PRAGMA journal_mode = WAL`); err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `PRAGMA synchronous = NORMAL`); err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `PRAGMA wal_autocheckpoint = 0`); err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Execute writes as fast as possible for a period of time.
	timer := time.NewTimer(writeDuration)
	defer timer.Stop()

	t.Logf("executing writes for %s", writeDuration)

LOOP:
	for i := 0; ; i++ {
		select {
		case <-timer.C:
			break LOOP
		default:
			if i%1000 == 0 {
				t.Logf("[exec] INSERT INTO t (id) VALUES (%d)", i)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (?)`, i); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Logf("writes complete, shutting down")

	// Stop & wait for Litestream command.
	time.Sleep(5 * time.Second)
	killLitestreamCmd(t, cmd, stdout)

	// Checkpoint & verify original SQLite database.
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	restoreAndVerify(t, ctx, env, filepath.Join(testDir, "litestream.yml"), filepath.Join(tempDir, "db"))
}

// Ensure replication works for an extended period.
func TestCmd_Replicate_LongRunning(t *testing.T) {
	if *longRunningDuration == 0 {
		t.Skip("long running test duration not specified, skipping")
	}

	ctx := context.Background()
	testDir, tempDir := filepath.Join("testdata", "replicate", "long-running"), t.TempDir()
	env := []string{"LITESTREAM_TEMPDIR=" + tempDir}

	cmd, stdout, _ := commandContext(ctx, env, "replicate", "-config", filepath.Join(testDir, "litestream.yml"))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite3", filepath.Join(tempDir, "db"))
	if err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `PRAGMA journal_mode = WAL`); err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `PRAGMA synchronous = NORMAL`); err != nil {
		t.Fatal(err)
	} else if _, err := db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Execute writes as fast as possible for a period of time.
	timer := time.NewTimer(*longRunningDuration)
	defer timer.Stop()

	t.Logf("executing writes for %s", longRunningDuration)

LOOP:
	for i := 0; ; i++ {
		select {
		case <-timer.C:
			break LOOP
		default:
			t.Logf("[exec] INSERT INTO t (id) VALUES (%d)", i)
			if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (?)`, i); err != nil {
				t.Fatal(err)
			}

			time.Sleep(time.Duration(rand.Intn(int(time.Second))))
		}
	}

	t.Logf("writes complete, shutting down")

	// Stop & wait for Litestream command.
	killLitestreamCmd(t, cmd, stdout)

	// Checkpoint & verify original SQLite database.
	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	restoreAndVerify(t, ctx, env, filepath.Join(testDir, "litestream.yml"), filepath.Join(tempDir, "db"))
}

// commandContext returns a "litestream" command with stdout/stderr buffers.
func commandContext(ctx context.Context, env []string, arg ...string) (cmd *exec.Cmd, stdout, stderr *internal.LockingBuffer) {
	cmd = exec.CommandContext(ctx, "litestream", arg...)
	cmd.Env = env
	var outBuf, errBuf internal.LockingBuffer

	// Split stdout/stderr to terminal if verbose flag set.
	cmd.Stdout, cmd.Stderr = &outBuf, &errBuf
	if testing.Verbose() {
		cmd.Stdout = io.MultiWriter(&outBuf, os.Stdout)
		cmd.Stderr = io.MultiWriter(&errBuf, os.Stderr)
	}

	return cmd, &outBuf, &errBuf
}

// waitForLogMessage continuously checks b for a message and returns when it occurs.
func waitForLogMessage(tb testing.TB, b *internal.LockingBuffer, msg string) {
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			tb.Fatal("timed out waiting for cmd initialization")
		case <-ticker.C:
			if strings.Contains(b.String(), msg) {
				return
			}
		}
	}
}

// killLitestreamCmd interrupts the process and waits for a clean shutdown.
func killLitestreamCmd(tb testing.TB, cmd *exec.Cmd, stdout *internal.LockingBuffer) {
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		tb.Fatal("kill litestream: signal:", err)
	} else if err := cmd.Wait(); err != nil {
		tb.Fatal("kill litestream: cmd:", err)
	}
}

// restoreAndVerify executes a "restore" and compares byte with the original database.
func restoreAndVerify(tb testing.TB, ctx context.Context, env []string, configPath, dbPath string) {
	restorePath := filepath.Join(tb.TempDir(), "db")

	// Restore database.
	cmd, _, _ := commandContext(ctx, env, "restore", "-config", configPath, "-o", restorePath, dbPath)
	if err := cmd.Run(); err != nil {
		tb.Fatalf("error running 'restore' command: %s", err)
	}

	// Compare original database & restored database.
	buf0 := testingutil.ReadFile(tb, dbPath)
	buf1 := testingutil.ReadFile(tb, restorePath)
	if bytes.Equal(buf0, buf1) {
		return // ok, exit
	}

	// On mismatch, copy out original & restored DBs.
	dir, err := os.MkdirTemp("", "litestream-*")
	if err != nil {
		tb.Fatal(err)
	}
	testingutil.CopyFile(tb, dbPath, filepath.Join(dir, "original.db"))
	testingutil.CopyFile(tb, restorePath, filepath.Join(dir, "restored.db"))

	tb.Fatalf("database mismatch; databases copied to %s", dir)
}
