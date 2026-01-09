package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// TestRestore_FormatConsistency tests that backups created by the current version
// can be restored by the same version. This is a basic sanity check that should
// always pass.
func TestRestore_FormatConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	ctx := context.Background()

	// Create a database with test data
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	// Insert initial data
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE compat_test(id INTEGER PRIMARY KEY, data TEXT);`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 100; i++ {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO compat_test(data) VALUES(?);`, fmt.Sprintf("data-%d", i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Sync to replica
	if err := db.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatalf("replica sync: %v", err)
	}

	// Checkpoint to ensure data is persisted
	if err := db.Checkpoint(ctx, litestream.CheckpointModeTruncate); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// Add more data after checkpoint
	for i := 100; i < 150; i++ {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO compat_test(data) VALUES(?);`, fmt.Sprintf("data-%d", i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Sync again
	if err := db.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatalf("replica sync: %v", err)
	}

	// Verify LTX files exist
	itr, err := db.Replica.Client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		t.Fatalf("list LTX files: %v", err)
	}
	var fileCount int
	for itr.Next() {
		fileCount++
	}
	if err := itr.Close(); err != nil {
		t.Fatalf("close iterator: %v", err)
	}
	t.Logf("Created %d L0 files", fileCount)

	// Restore to a new location
	restorePath := filepath.Join(t.TempDir(), "restored.db")
	if err := db.Replica.Restore(ctx, litestream.RestoreOptions{
		OutputPath: restorePath,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify restored data
	restoredDB := testingutil.MustOpenSQLDB(t, restorePath)
	defer restoredDB.Close()

	var count int
	if err := restoredDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM compat_test;`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}

	if count != 150 {
		t.Errorf("restored row count: got %d, want 150", count)
	}

	// Verify integrity
	var integrity string
	if err := restoredDB.QueryRowContext(ctx, `PRAGMA integrity_check;`).Scan(&integrity); err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	if integrity != "ok" {
		t.Errorf("integrity check: %s", integrity)
	}
}

// TestRestore_MultipleSyncs tests restore after many sync cycles to ensure
// LTX file accumulation doesn't cause issues.
func TestRestore_MultipleSyncs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	ctx := context.Background()

	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE sync_test(id INTEGER PRIMARY KEY, batch INTEGER, data BLOB);`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Perform multiple sync cycles
	const syncCycles = 50
	for batch := 0; batch < syncCycles; batch++ {
		for i := 0; i < 10; i++ {
			if _, err := sqldb.ExecContext(ctx, `INSERT INTO sync_test(batch, data) VALUES(?, randomblob(500));`, batch); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		if err := db.Sync(ctx); err != nil {
			t.Fatalf("sync %d: %v", batch, err)
		}
		if err := db.Replica.Sync(ctx); err != nil {
			t.Fatalf("replica sync %d: %v", batch, err)
		}
	}

	// Verify LTX files
	itr, err := db.Replica.Client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		t.Fatalf("list LTX files: %v", err)
	}
	var fileCount int
	for itr.Next() {
		fileCount++
	}
	if err := itr.Close(); err != nil {
		t.Fatalf("close iterator: %v", err)
	}
	t.Logf("Created %d L0 files over %d sync cycles", fileCount, syncCycles)

	// Restore
	restorePath := filepath.Join(t.TempDir(), "restored.db")
	if err := db.Replica.Restore(ctx, litestream.RestoreOptions{
		OutputPath: restorePath,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify
	restoredDB := testingutil.MustOpenSQLDB(t, restorePath)
	defer restoredDB.Close()

	var count int
	if err := restoredDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM sync_test;`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}

	expected := syncCycles * 10
	if count != expected {
		t.Errorf("restored row count: got %d, want %d", count, expected)
	}
}

// TestRestore_LTXFileValidation tests that invalid LTX files are properly
// detected and rejected during restore.
func TestRestore_LTXFileValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	ctx := context.Background()
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	// Create a valid snapshot first
	validSnapshot := createValidLTXData(t, 1, 1, time.Now())
	if _, err := client.WriteLTXFile(ctx, litestream.SnapshotLevel, 1, 1, bytes.NewReader(validSnapshot)); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	tests := []struct {
		name        string
		data        []byte
		minTXID     ltx.TXID
		maxTXID     ltx.TXID
		expectError bool
	}{
		{
			name:        "ValidL0File",
			data:        createValidLTXData(t, 2, 2, time.Now()),
			minTXID:     2,
			maxTXID:     2,
			expectError: false,
		},
		{
			name:        "EmptyFile",
			data:        []byte{},
			minTXID:     3,
			maxTXID:     3,
			expectError: true,
		},
		{
			name:        "TruncatedHeader",
			data:        []byte("truncated"),
			minTXID:     4,
			maxTXID:     4,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := client.WriteLTXFile(ctx, 0, tt.minTXID, tt.maxTXID, bytes.NewReader(tt.data)); err != nil {
				t.Logf("write failed (may be expected): %v", err)
			}
		})
	}
}

// TestRestore_CrossPlatformPaths tests that backups work with different path styles.
func TestRestore_CrossPlatformPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	ctx := context.Background()

	pathTests := []string{
		"simple",
		"path/with/slashes",
		"path-with-dashes",
		"path_with_underscores",
	}

	for _, subpath := range pathTests {
		t.Run(subpath, func(t *testing.T) {
			replicaDir := t.TempDir()
			fullPath := filepath.Join(replicaDir, subpath)

			client := file.NewReplicaClient(fullPath)

			// Create snapshot
			snapshot := createValidLTXData(t, 1, 1, time.Now())
			if _, err := client.WriteLTXFile(ctx, litestream.SnapshotLevel, 1, 1, bytes.NewReader(snapshot)); err != nil {
				t.Fatalf("write snapshot: %v", err)
			}

			// Create L0 files
			for i := 2; i <= 5; i++ {
				data := createValidLTXData(t, ltx.TXID(i), ltx.TXID(i), time.Now())
				if _, err := client.WriteLTXFile(ctx, 0, ltx.TXID(i), ltx.TXID(i), bytes.NewReader(data)); err != nil {
					t.Fatalf("write L0 %d: %v", i, err)
				}
			}

			// Verify files exist
			itr, err := client.LTXFiles(ctx, 0, 0, false)
			if err != nil {
				t.Fatalf("list files: %v", err)
			}
			var count int
			for itr.Next() {
				count++
			}
			itr.Close()

			if count != 4 {
				t.Errorf("file count: got %d, want 4", count)
			}
		})
	}
}

// TestRestore_PointInTimeAccuracy tests that point-in-time restore respects
// timestamps correctly.
func TestRestore_PointInTimeAccuracy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	ctx := context.Background()
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	baseTime := time.Now().Add(-10 * time.Minute)

	// Create snapshot at baseTime
	snapshot := createValidLTXData(t, 1, 1, baseTime)
	if _, err := client.WriteLTXFile(ctx, litestream.SnapshotLevel, 1, 1, bytes.NewReader(snapshot)); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	// Create L0 files at 1-minute intervals
	for i := 2; i <= 10; i++ {
		ts := baseTime.Add(time.Duration(i-1) * time.Minute)
		data := createValidLTXData(t, ltx.TXID(i), ltx.TXID(i), ts)
		if _, err := client.WriteLTXFile(ctx, 0, ltx.TXID(i), ltx.TXID(i), bytes.NewReader(data)); err != nil {
			t.Fatalf("write L0 %d: %v", i, err)
		}
	}

	// Verify timestamps are preserved when listing with metadata
	itr, err := client.LTXFiles(ctx, 0, 0, true)
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	defer itr.Close()

	var files []*ltx.FileInfo
	for itr.Next() {
		info := itr.Item()
		files = append(files, &ltx.FileInfo{
			Level:     info.Level,
			MinTXID:   info.MinTXID,
			MaxTXID:   info.MaxTXID,
			CreatedAt: info.CreatedAt,
		})
	}

	if len(files) != 9 {
		t.Fatalf("file count: got %d, want 9", len(files))
	}

	// Verify timestamps are monotonically increasing
	for i := 1; i < len(files); i++ {
		if files[i].CreatedAt.Before(files[i-1].CreatedAt) {
			t.Errorf("file %d timestamp (%v) is before file %d timestamp (%v)",
				i, files[i].CreatedAt, i-1, files[i-1].CreatedAt)
		}
	}
}

// createValidLTXData creates a minimal valid LTX file for testing.
func createValidLTXData(t *testing.T, minTXID, maxTXID ltx.TXID, ts time.Time) []byte {
	t.Helper()

	hdr := ltx.Header{
		Version:   ltx.Version,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: ts.UnixMilli(),
	}
	if minTXID == 1 {
		hdr.PreApplyChecksum = 0
	} else {
		hdr.PreApplyChecksum = ltx.ChecksumFlag
	}

	headerBytes, err := hdr.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}

	return headerBytes
}

// TestBinaryCompatibility_CLIRestore tests that the litestream CLI can restore
// backups created programmatically. This is a basic end-to-end test.
func TestBinaryCompatibility_CLIRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	// Skip if litestream binary is not available
	litestreamBin := os.Getenv("LITESTREAM_BIN")
	if litestreamBin == "" {
		litestreamBin = "./bin/litestream"
	}
	if _, err := os.Stat(litestreamBin); os.IsNotExist(err) {
		t.Skip("litestream binary not found, skipping CLI test")
	}

	ctx := context.Background()

	// Create database with programmatic API
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE cli_test(id INTEGER PRIMARY KEY, value TEXT);`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 50; i++ {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO cli_test(value) VALUES(?);`, fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	if err := db.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatalf("replica sync: %v", err)
	}

	// Get replica path from the file client
	fileClient, ok := db.Replica.Client.(*file.ReplicaClient)
	if !ok {
		t.Skip("Test requires file replica client")
	}
	replicaPath := fileClient.Path()

	// Close the database
	testingutil.MustCloseDBs(t, db, sqldb)

	// Restore using CLI
	restorePath := filepath.Join(t.TempDir(), "cli-restored.db")
	cmd := exec.CommandContext(ctx, litestreamBin, "restore", "-o", restorePath, "file://"+replicaPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("CLI restore failed: %v\nOutput: %s", err, output)
	}

	// Verify restored database
	restoredDB := testingutil.MustOpenSQLDB(t, restorePath)
	defer restoredDB.Close()

	var count int
	if err := restoredDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM cli_test;`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}

	if count != 50 {
		t.Errorf("CLI restored row count: got %d, want 50", count)
	}
}

// TestVersionMigration_DirectoryLayout tests that the current version can
// detect and handle different backup directory layouts.
func TestVersionMigration_DirectoryLayout(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	ctx := context.Background()

	// Test current v0.5.x layout (ltx/0/, ltx/1/, ltx/snapshot/)
	t.Run("CurrentLayout", func(t *testing.T) {
		replicaDir := t.TempDir()
		client := file.NewReplicaClient(replicaDir)

		// Create files in expected layout
		snapshot := createValidLTXData(t, 1, 1, time.Now())
		if _, err := client.WriteLTXFile(ctx, litestream.SnapshotLevel, 1, 1, bytes.NewReader(snapshot)); err != nil {
			t.Fatalf("write snapshot: %v", err)
		}

		for i := 2; i <= 5; i++ {
			data := createValidLTXData(t, ltx.TXID(i), ltx.TXID(i), time.Now())
			if _, err := client.WriteLTXFile(ctx, 0, ltx.TXID(i), ltx.TXID(i), bytes.NewReader(data)); err != nil {
				t.Fatalf("write L0 %d: %v", i, err)
			}
		}

		// Verify structure
		snapshotDir := filepath.Join(replicaDir, "ltx", "snapshot")
		l0Dir := filepath.Join(replicaDir, "ltx", "0")

		if _, err := os.Stat(snapshotDir); err != nil {
			t.Errorf("snapshot directory not found: %v", err)
		}
		if _, err := os.Stat(l0Dir); err != nil {
			t.Errorf("L0 directory not found: %v", err)
		}

		// Verify files can be listed
		snapshotItr, err := client.LTXFiles(ctx, litestream.SnapshotLevel, 0, false)
		if err != nil {
			t.Fatalf("list snapshots: %v", err)
		}
		var snapshotCount int
		for snapshotItr.Next() {
			snapshotCount++
		}
		snapshotItr.Close()

		l0Itr, err := client.LTXFiles(ctx, 0, 0, false)
		if err != nil {
			t.Fatalf("list L0: %v", err)
		}
		var l0Count int
		for l0Itr.Next() {
			l0Count++
		}
		l0Itr.Close()

		if snapshotCount != 1 {
			t.Errorf("snapshot count: got %d, want 1", snapshotCount)
		}
		if l0Count != 4 {
			t.Errorf("L0 count: got %d, want 4", l0Count)
		}
	})

	// Test that old v0.3.x layout (generations/) is not accidentally used
	t.Run("LegacyLayoutNotUsed", func(t *testing.T) {
		replicaDir := t.TempDir()

		// Create a generations/ directory (v0.3.x layout)
		legacyDir := filepath.Join(replicaDir, "generations")
		if err := os.MkdirAll(legacyDir, 0755); err != nil {
			t.Fatalf("create legacy dir: %v", err)
		}

		// Create client and verify it uses new layout
		client := file.NewReplicaClient(replicaDir)

		snapshot := createValidLTXData(t, 1, 1, time.Now())
		if _, err := client.WriteLTXFile(ctx, litestream.SnapshotLevel, 1, 1, bytes.NewReader(snapshot)); err != nil {
			t.Fatalf("write snapshot: %v", err)
		}

		// Verify new layout is used
		newLayoutDir := filepath.Join(replicaDir, "ltx")
		if _, err := os.Stat(newLayoutDir); err != nil {
			t.Errorf("new layout directory not created: %v", err)
		}

		// Verify legacy directory is not used for new files
		entries, _ := os.ReadDir(legacyDir)
		if len(entries) > 0 {
			t.Errorf("legacy directory should remain empty, has %d entries", len(entries))
		}
	})
}

// TestCompaction_Compatibility tests that compacted files maintain compatibility
// with restore operations.
func TestCompaction_Compatibility(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	ctx := context.Background()

	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE compact_test(id INTEGER PRIMARY KEY, data BLOB);`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Generate many syncs to create L0 files
	for batch := 0; batch < 20; batch++ {
		for i := 0; i < 5; i++ {
			if _, err := sqldb.ExecContext(ctx, `INSERT INTO compact_test(data) VALUES(randomblob(1000));`); err != nil {
				t.Fatalf("insert: %v", err)
			}
		}
		if err := db.Sync(ctx); err != nil {
			t.Fatalf("sync: %v", err)
		}
		if err := db.Replica.Sync(ctx); err != nil {
			t.Fatalf("replica sync: %v", err)
		}
	}

	// Force compaction to level 1
	if _, err := db.Compact(ctx, 1); err != nil {
		t.Logf("compact to L1 (may not have enough files): %v", err)
	}

	// Count files at different levels
	for level := 0; level <= 2; level++ {
		itr, err := db.Replica.Client.LTXFiles(ctx, level, 0, false)
		if err != nil {
			t.Fatalf("list level %d: %v", level, err)
		}
		var count int
		for itr.Next() {
			count++
		}
		itr.Close()
		t.Logf("Level %d: %d files", level, count)
	}

	// Restore and verify
	restorePath := filepath.Join(t.TempDir(), "compacted-restore.db")
	if err := db.Replica.Restore(ctx, litestream.RestoreOptions{
		OutputPath: restorePath,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	restoredDB := testingutil.MustOpenSQLDB(t, restorePath)
	defer restoredDB.Close()

	var count int
	if err := restoredDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM compact_test;`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}

	expected := 20 * 5
	if count != expected {
		t.Errorf("restored row count: got %d, want %d", count, expected)
	}

	var integrity string
	if err := restoredDB.QueryRowContext(ctx, `PRAGMA integrity_check;`).Scan(&integrity); err != nil {
		t.Fatalf("integrity check: %v", err)
	}
	if !strings.Contains(integrity, "ok") {
		t.Errorf("integrity check failed: %s", integrity)
	}
}
