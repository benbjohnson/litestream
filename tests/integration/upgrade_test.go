//go:build integration

package integration

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/benbjohnson/litestream"
)

func TestUpgrade_V3ToV5(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Skip if v0.3 binary not provided.
	v3Bin := os.Getenv("LITESTREAM_V3_BIN")
	if v3Bin == "" {
		t.Skip("LITESTREAM_V3_BIN not set, skipping upgrade test")
	}

	// Verify v0.3 binary exists and is executable.
	if info, err := os.Stat(v3Bin); err != nil {
		t.Fatalf("v0.3 binary not found at %s: %v", v3Bin, err)
	} else if info.Mode()&0111 == 0 {
		t.Fatalf("v0.3 binary at %s is not executable", v3Bin)
	}

	// Verify current binary exists and is executable.
	v5Bin := getBinaryPath("litestream")
	if info, err := os.Stat(v5Bin); err != nil {
		t.Fatalf("current binary not found at %s: %v", v5Bin, err)
	} else if info.Mode()&0111 == 0 {
		t.Fatalf("current binary at %s is not executable", v5Bin)
	}

	// Set up temp directory with database and replica paths.
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "upgrade.db")
	replicaPath := filepath.Join(tmpDir, "replica")
	replicaURL := fmt.Sprintf("file://%s", filepath.ToSlash(replicaPath))
	restoredPath := filepath.Join(tmpDir, "restored.db")

	// Create WAL-mode database with test table.
	t.Log("Creating WAL-mode database with upgrade_test table")
	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer sqlDB.Close()

	if _, err := sqlDB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("set WAL mode: %v", err)
	}
	if _, err := sqlDB.Exec("CREATE TABLE upgrade_test(id INTEGER PRIMARY KEY, phase TEXT, data BLOB)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// =========================================================================
	// Phase 1: v0.3.x replication
	// =========================================================================
	t.Log("Phase 1: v0.3.x replication")

	// Insert 10 rows with phase='v3-initial'.
	t.Log("  Inserting 10 v3-initial rows")
	for i := 0; i < 10; i++ {
		if _, err := sqlDB.Exec("INSERT INTO upgrade_test(phase, data) VALUES(?, randomblob(100))", "v3-initial"); err != nil {
			t.Fatalf("insert v3-initial row %d: %v", i, err)
		}
	}

	// Start v0.3 replicate subprocess.
	t.Logf("  Starting v0.3 binary: %s replicate %s %s", v3Bin, dbPath, replicaURL)
	v3Cmd := exec.Command(v3Bin, "replicate", dbPath, replicaURL)
	v3LogFile, err := os.Create(filepath.Join(tmpDir, "v3-replicate.log"))
	if err != nil {
		t.Fatalf("create v3 log file: %v", err)
	}
	defer v3LogFile.Close()
	v3Cmd.Stdout = v3LogFile
	v3Cmd.Stderr = v3LogFile
	if err := v3Cmd.Start(); err != nil {
		t.Fatalf("start v0.3 replicate: %v", err)
	}
	t.Cleanup(func() {
		if v3Cmd.ProcessState == nil {
			v3Cmd.Process.Signal(syscall.SIGINT)
			v3Cmd.Wait()
		}
	})

	// Wait for initial sync.
	t.Log("  Waiting 3s for initial sync")
	time.Sleep(3 * time.Second)

	// Insert 10 more rows with phase='v3-running' across 5 transactions (2 rows each).
	t.Log("  Inserting 10 v3-running rows across 5 transactions")
	for txn := 0; txn < 5; txn++ {
		tx, err := sqlDB.Begin()
		if err != nil {
			t.Fatalf("begin tx %d: %v", txn, err)
		}
		for j := 0; j < 2; j++ {
			if _, err := tx.Exec("INSERT INTO upgrade_test(phase, data) VALUES(?, randomblob(100))", "v3-running"); err != nil {
				tx.Rollback()
				t.Fatalf("insert v3-running tx %d row %d: %v", txn, j, err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit tx %d: %v", txn, err)
		}
	}

	// Force checkpoint.
	t.Log("  Running PRAGMA wal_checkpoint(TRUNCATE)")
	if _, err := sqlDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// Insert 5 more rows with phase='v3-post-checkpoint'.
	t.Log("  Inserting 5 v3-post-checkpoint rows")
	for i := 0; i < 5; i++ {
		if _, err := sqlDB.Exec("INSERT INTO upgrade_test(phase, data) VALUES(?, randomblob(100))", "v3-post-checkpoint"); err != nil {
			t.Fatalf("insert v3-post-checkpoint row %d: %v", i, err)
		}
	}

	// Wait for sync, then stop v0.3.
	t.Log("  Waiting 3s for sync")
	time.Sleep(3 * time.Second)

	t.Log("  Sending SIGINT to v0.3 process")
	if err := v3Cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatalf("signal v0.3 process: %v", err)
	}
	if err := v3Cmd.Wait(); err != nil {
		t.Logf("  v0.3 process exited with: %v (expected for SIGINT)", err)
	}

	// Verify generations/ directory exists (v0.3 layout).
	generationsDir := filepath.Join(replicaPath, "generations")
	if _, err := os.Stat(generationsDir); err != nil {
		t.Fatalf("v0.3 replica generations/ directory not found: %v", err)
	}
	t.Log("  Verified generations/ directory exists in replica")

	// =========================================================================
	// Phase 2: v0.5.x replication
	// =========================================================================
	t.Log("Phase 2: v0.5.x replication")

	// Start current binary replicate subprocess.
	t.Logf("  Starting v0.5 binary: %s replicate %s %s", v5Bin, dbPath, replicaURL)
	v5Cmd := exec.Command(v5Bin, "replicate", dbPath, replicaURL)
	v5LogFile, err := os.Create(filepath.Join(tmpDir, "v5-replicate.log"))
	if err != nil {
		t.Fatalf("create v5 log file: %v", err)
	}
	defer v5LogFile.Close()
	v5Cmd.Stdout = v5LogFile
	v5Cmd.Stderr = v5LogFile
	if err := v5Cmd.Start(); err != nil {
		t.Fatalf("start v0.5 replicate: %v", err)
	}
	t.Cleanup(func() {
		if v5Cmd.ProcessState == nil {
			v5Cmd.Process.Signal(syscall.SIGINT)
			v5Cmd.Wait()
		}
	})

	// Wait up to 30s for ltx/9/ directory to contain at least one .ltx file (snapshot).
	t.Log("  Waiting up to 30s for snapshot in ltx/9/")
	snapshotDir := filepath.Join(replicaPath, "ltx", fmt.Sprintf("%d", litestream.SnapshotLevel))
	deadline := time.Now().Add(30 * time.Second)
	snapshotFound := false
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(filepath.Join(snapshotDir, "*.ltx"))
		if len(matches) > 0 {
			snapshotFound = true
			t.Logf("  Found %d snapshot file(s) in ltx/9/", len(matches))
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !snapshotFound {
		t.Fatal("timeout waiting for v0.5.x snapshot in ltx/9/")
	}

	// Insert 10 rows with phase='v5-running' across 5 transactions.
	t.Log("  Inserting 10 v5-running rows across 5 transactions")
	for txn := 0; txn < 5; txn++ {
		tx, err := sqlDB.Begin()
		if err != nil {
			t.Fatalf("begin tx %d: %v", txn, err)
		}
		for j := 0; j < 2; j++ {
			if _, err := tx.Exec("INSERT INTO upgrade_test(phase, data) VALUES(?, randomblob(100))", "v5-running"); err != nil {
				tx.Rollback()
				t.Fatalf("insert v5-running tx %d row %d: %v", txn, j, err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit tx %d: %v", txn, err)
		}
	}

	// Force checkpoint.
	t.Log("  Running PRAGMA wal_checkpoint(TRUNCATE)")
	if _, err := sqlDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// Insert 5 more rows with phase='v5-post-checkpoint'.
	t.Log("  Inserting 5 v5-post-checkpoint rows")
	for i := 0; i < 5; i++ {
		if _, err := sqlDB.Exec("INSERT INTO upgrade_test(phase, data) VALUES(?, randomblob(100))", "v5-post-checkpoint"); err != nil {
			t.Fatalf("insert v5-post-checkpoint row %d: %v", i, err)
		}
	}

	// Wait for sync, then stop v0.5.
	t.Log("  Waiting 3s for sync")
	time.Sleep(3 * time.Second)

	t.Log("  Sending SIGINT to v0.5 process")
	if err := v5Cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatalf("signal v0.5 process: %v", err)
	}
	if err := v5Cmd.Wait(); err != nil {
		t.Logf("  v0.5 process exited with: %v (expected for SIGINT)", err)
	}

	// =========================================================================
	// Phase 3: Restore
	// =========================================================================
	t.Log("Phase 3: Restore using current binary")

	restoreCmd := exec.Command(v5Bin, "restore", "-o", restoredPath, replicaURL)
	restoreOutput, err := restoreCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("restore failed: %v\nOutput: %s", err, restoreOutput)
	}
	t.Log("  Restore completed successfully")

	// =========================================================================
	// Phase 4: Validate
	// =========================================================================
	t.Log("Phase 4: Validate restored database")

	restoredDB, err := sql.Open("sqlite3", restoredPath)
	if err != nil {
		t.Fatalf("open restored database: %v", err)
	}
	defer restoredDB.Close()

	// Integrity check.
	var integrity string
	if err := restoredDB.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil {
		t.Fatalf("integrity check query: %v", err)
	}
	if integrity != "ok" {
		t.Fatalf("integrity check failed: %s", integrity)
	}
	t.Log("  PRAGMA integrity_check: ok")

	// Count total rows.
	var totalCount int
	if err := restoredDB.QueryRow("SELECT COUNT(*) FROM upgrade_test").Scan(&totalCount); err != nil {
		t.Fatalf("count total rows: %v", err)
	}
	if totalCount != 40 {
		t.Fatalf("total row count: got %d, want 40", totalCount)
	}
	t.Logf("  Total rows: %d (expected 40)", totalCount)

	// Count rows per phase. The v3-initial rows were inserted before v0.3
	// started, so their presence in the restore proves the v0.5.x snapshot
	// captured the full database state (not just changes made after v0.5.x started).
	phases := []struct {
		name     string
		expected int
	}{
		{"v3-initial", 10},
		{"v3-running", 10},
		{"v3-post-checkpoint", 5},
		{"v5-running", 10},
		{"v5-post-checkpoint", 5},
	}
	for _, p := range phases {
		var count int
		if err := restoredDB.QueryRow("SELECT COUNT(*) FROM upgrade_test WHERE phase = ?", p.name).Scan(&count); err != nil {
			t.Fatalf("count phase %q: %v", p.name, err)
		}
		if count != p.expected {
			t.Errorf("phase %q: got %d rows, want %d", p.name, count, p.expected)
		}
		t.Logf("  Phase %q: %d rows", p.name, count)
	}
}
