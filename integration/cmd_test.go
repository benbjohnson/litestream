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

// Ensure a database can be replicated over HTTP.
func TestCmd_Replicate_HTTP(t *testing.T) {
	ctx := context.Background()
	testDir, tempDir := filepath.Join("testdata", "replicate", "http"), t.TempDir()
	if err := os.Mkdir(filepath.Join(tempDir, "0"), 0777); err != nil {
		t.Fatal(err)
	} else if err := os.Mkdir(filepath.Join(tempDir, "1"), 0777); err != nil {
		t.Fatal(err)
	}

	env0 := []string{"LITESTREAM_TEMPDIR=" + tempDir}
	env1 := []string{"LITESTREAM_TEMPDIR=" + tempDir, "LITESTREAM_UPSTREAM_URL=http://localhost:10001"}

	cmd0, stdout0, _ := commandContext(ctx, env0, "replicate", "-config", filepath.Join(testDir, "litestream.0.yml"))
	if err := cmd0.Start(); err != nil {
		t.Fatal(err)
	}
	cmd1, stdout1, _ := commandContext(ctx, env1, "replicate", "-config", filepath.Join(testDir, "litestream.1.yml"))
	if err := cmd1.Start(); err != nil {
		t.Fatal(err)
	}

	db0, err := sql.Open("sqlite3", filepath.Join(tempDir, "0", "db"))
	if err != nil {
		t.Fatal(err)
	} else if _, err := db0.ExecContext(ctx, `PRAGMA journal_mode = wal`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	defer db0.Close()

	// Execute writes periodically.
	for i := 0; i < 100; i++ {
		t.Logf("[exec] INSERT INTO t (id) VALUES (%d)", i)
		if _, err := db0.ExecContext(ctx, `INSERT INTO t (id) VALUES (?)`, i); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for replica to catch up.
	time.Sleep(1 * time.Second)

	// Verify count in replica table.
	db1, err := sql.Open("sqlite3", filepath.Join(tempDir, "1", "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()

	var n int
	if err := db1.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	} else if got, want := n, 100; got != want {
		t.Fatalf("replica count=%d, want %d", got, want)
	}

	// Stop & wait for Litestream command.
	killLitestreamCmd(t, cmd1, stdout1) // kill
	killLitestreamCmd(t, cmd0, stdout0)
}

// Ensure a database can recover when disconnected from HTTP.
func TestCmd_Replicate_HTTP_PartialRecovery(t *testing.T) {
	ctx := context.Background()
	testDir, tempDir := filepath.Join("testdata", "replicate", "http-partial-recovery"), t.TempDir()
	if err := os.Mkdir(filepath.Join(tempDir, "0"), 0777); err != nil {
		t.Fatal(err)
	} else if err := os.Mkdir(filepath.Join(tempDir, "1"), 0777); err != nil {
		t.Fatal(err)
	}

	env0 := []string{"LITESTREAM_TEMPDIR=" + tempDir}
	env1 := []string{"LITESTREAM_TEMPDIR=" + tempDir, "LITESTREAM_UPSTREAM_URL=http://localhost:10002"}

	cmd0, stdout0, _ := commandContext(ctx, env0, "replicate", "-config", filepath.Join(testDir, "litestream.0.yml"))
	if err := cmd0.Start(); err != nil {
		t.Fatal(err)
	}
	cmd1, stdout1, _ := commandContext(ctx, env1, "replicate", "-config", filepath.Join(testDir, "litestream.1.yml"))
	if err := cmd1.Start(); err != nil {
		t.Fatal(err)
	}

	db0, err := sql.Open("sqlite3", filepath.Join(tempDir, "0", "db"))
	if err != nil {
		t.Fatal(err)
	} else if _, err := db0.ExecContext(ctx, `PRAGMA journal_mode = wal`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, DATA TEXT)`); err != nil {
		t.Fatal(err)
	}
	defer db0.Close()

	var index int
	insertAndWait := func() {
		index++
		t.Logf("[exec] INSERT INTO t (id, data) VALUES (%d, '...')", index)
		if _, err := db0.ExecContext(ctx, `INSERT INTO t (id, data) VALUES (?, ?)`, index, strings.Repeat("x", 512)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Execute writes periodically.
	for i := 0; i < 50; i++ {
		insertAndWait()
	}

	// Kill the replica.
	t.Logf("Killing replica...")
	killLitestreamCmd(t, cmd1, stdout1)
	t.Logf("Replica killed")

	// Keep writing.
	for i := 0; i < 25; i++ {
		insertAndWait()
	}

	// Restart replica.
	t.Logf("Restarting replica...")
	cmd1, stdout1, _ = commandContext(ctx, env1, "replicate", "-config", filepath.Join(testDir, "litestream.1.yml"))
	if err := cmd1.Start(); err != nil {
		t.Fatal(err)
	}
	t.Logf("Replica restarted")

	// Continue writing...
	for i := 0; i < 25; i++ {
		insertAndWait()
	}

	// Wait for replica to catch up.
	time.Sleep(1 * time.Second)

	// Verify count in replica table.
	db1, err := sql.Open("sqlite3", filepath.Join(tempDir, "1", "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()

	var n int
	if err := db1.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	} else if got, want := n, 100; got != want {
		t.Fatalf("replica count=%d, want %d", got, want)
	}

	// Stop & wait for Litestream command.
	killLitestreamCmd(t, cmd1, stdout1) // kill
	killLitestreamCmd(t, cmd0, stdout0)
}

// Ensure a database can recover when disconnected from HTTP but when last index
// is no longer available.
func TestCmd_Replicate_HTTP_FullRecovery(t *testing.T) {
	ctx := context.Background()
	testDir, tempDir := filepath.Join("testdata", "replicate", "http-full-recovery"), t.TempDir()
	if err := os.Mkdir(filepath.Join(tempDir, "0"), 0777); err != nil {
		t.Fatal(err)
	} else if err := os.Mkdir(filepath.Join(tempDir, "1"), 0777); err != nil {
		t.Fatal(err)
	}

	env0 := []string{"LITESTREAM_TEMPDIR=" + tempDir}
	env1 := []string{"LITESTREAM_TEMPDIR=" + tempDir, "LITESTREAM_UPSTREAM_URL=http://localhost:10002"}

	cmd0, stdout0, _ := commandContext(ctx, env0, "replicate", "-config", filepath.Join(testDir, "litestream.0.yml"))
	if err := cmd0.Start(); err != nil {
		t.Fatal(err)
	}
	cmd1, stdout1, _ := commandContext(ctx, env1, "replicate", "-config", filepath.Join(testDir, "litestream.1.yml"))
	if err := cmd1.Start(); err != nil {
		t.Fatal(err)
	}

	db0, err := sql.Open("sqlite3", filepath.Join(tempDir, "0", "db"))
	if err != nil {
		t.Fatal(err)
	} else if _, err := db0.ExecContext(ctx, `PRAGMA journal_mode = wal`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	defer db0.Close()

	var index int
	insertAndWait := func() {
		index++
		t.Logf("[exec] INSERT INTO t (id) VALUES (%d)", index)
		if _, err := db0.ExecContext(ctx, `INSERT INTO t (id) VALUES (?)`, index); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Execute writes periodically.
	for i := 0; i < 50; i++ {
		insertAndWait()
	}

	// Kill the replica.
	t.Logf("Killing replica...")
	killLitestreamCmd(t, cmd1, stdout1)
	t.Logf("Replica killed")

	// Keep writing.
	for i := 0; i < 25; i++ {
		insertAndWait()
	}

	// Restart replica.
	t.Logf("Restarting replica...")
	cmd1, stdout1, _ = commandContext(ctx, env1, "replicate", "-config", filepath.Join(testDir, "litestream.1.yml"))
	if err := cmd1.Start(); err != nil {
		t.Fatal(err)
	}
	t.Logf("Replica restarted")

	// Continue writing...
	for i := 0; i < 25; i++ {
		insertAndWait()
	}

	// Wait for replica to catch up.
	time.Sleep(1 * time.Second)

	// Verify count in replica table.
	db1, err := sql.Open("sqlite3", filepath.Join(tempDir, "1", "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()

	var n int
	if err := db1.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	} else if got, want := n, 100; got != want {
		t.Fatalf("replica count=%d, want %d", got, want)
	}

	// Stop & wait for Litestream command.
	killLitestreamCmd(t, cmd1, stdout1) // kill
	killLitestreamCmd(t, cmd0, stdout0)
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
	tb.Helper()
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
