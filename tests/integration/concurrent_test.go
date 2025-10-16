//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestRapidCheckpoints(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	RequireBinaries(t)

	t.Log("Testing: Litestream under rapid checkpoint pressure")

	db := SetupTestDB(t, "rapid-checkpoints")
	defer db.Cleanup()

	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	t.Log("[1] Starting Litestream...")
	if err := db.StartLitestream(); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}

	time.Sleep(3 * time.Second)

	t.Log("[2] Generating rapid writes with frequent checkpoints...")
	sqlDB, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer sqlDB.Close()

	if _, err := sqlDB.Exec(`
		CREATE TABLE checkpoint_test (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			data BLOB,
			timestamp INTEGER
		)
	`); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	data := make([]byte, 4096)
	checkpointCount := 0

	for i := 0; i < 1000; i++ {
		if _, err := sqlDB.Exec(
			"INSERT INTO checkpoint_test (data, timestamp) VALUES (?, ?)",
			data,
			time.Now().Unix(),
		); err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}

		if i%100 == 0 {
			if _, err := sqlDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
				t.Logf("Checkpoint %d failed: %v", checkpointCount, err)
			} else {
				checkpointCount++
				t.Logf("Checkpoint %d completed at row %d", checkpointCount, i)
			}
		}
	}

	t.Logf("✓ Generated 1000 writes with %d checkpoints", checkpointCount)

	time.Sleep(5 * time.Second)

	db.StopLitestream()
	time.Sleep(2 * time.Second)

	t.Log("[3] Checking for errors...")
	errors, err := db.CheckForErrors()
	if err != nil {
		t.Fatalf("Failed to check errors: %v", err)
	}

	if len(errors) > 5 {
		t.Fatalf("Too many errors (%d), showing first 5:\n%v", len(errors), errors[:5])
	} else if len(errors) > 0 {
		t.Logf("Found %d errors (acceptable for checkpoint stress)", len(errors))
	}

	t.Log("[4] Verifying replica...")
	fileCount, err := db.GetReplicaFileCount()
	if err != nil {
		t.Fatalf("Failed to check replica: %v", err)
	}

	if fileCount == 0 {
		t.Fatal("No replica files created!")
	}

	t.Logf("✓ Replica created with %d files", fileCount)

	t.Log("[5] Testing restore...")
	restoredPath := filepath.Join(db.TempDir, "checkpoint-restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	t.Log("✓ Restore successful")

	origCount, err := db.GetRowCount("checkpoint_test")
	if err != nil {
		t.Fatalf("Failed to get original row count: %v", err)
	}

	restoredDB := &TestDB{Path: restoredPath, t: t}
	restCount, err := restoredDB.GetRowCount("checkpoint_test")
	if err != nil {
		t.Fatalf("Failed to get restored row count: %v", err)
	}

	if origCount != restCount {
		t.Fatalf("Count mismatch: original=%d, restored=%d", origCount, restCount)
	}

	t.Logf("✓ Data integrity verified: %d rows", origCount)
	t.Log("TEST PASSED: Handled rapid checkpoints successfully")
}

func TestWALGrowth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	RequireBinaries(t)

	duration := GetTestDuration(t, 2*time.Minute)
	t.Logf("Testing: Large WAL file handling (duration: %v)", duration)

	db := SetupTestDB(t, "wal-growth")
	defer db.Cleanup()

	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	t.Log("[1] Creating test table...")
	sqlDB, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer sqlDB.Close()

	if _, err := sqlDB.Exec(`
		CREATE TABLE wal_test (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			data BLOB
		)
	`); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Log("✓ Table created")

	t.Log("[2] Starting Litestream...")
	if err := db.StartLitestream(); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}

	time.Sleep(3 * time.Second)

	t.Log("[3] Generating sustained write load...")
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	config := DefaultLoadConfig()
	config.WriteRate = 400
	config.Duration = duration
	config.Pattern = LoadPatternWave
	config.PayloadSize = 10 * 1024
	config.Workers = 4

	if err := db.GenerateLoad(ctx, config.WriteRate, config.Duration, string(config.Pattern)); err != nil && ctx.Err() == nil {
		t.Fatalf("Load generation failed: %v", err)
	}

	t.Log("✓ Load generation complete")

	time.Sleep(5 * time.Second)

	t.Log("[4] Checking WAL size...")
	walPath := db.Path + "-wal"
	walSize, err := getFileSize(walPath)
	if err != nil {
		t.Logf("WAL file not found (may have been checkpointed): %v", err)
	} else {
		t.Logf("WAL size: %.2f MB", float64(walSize)/(1024*1024))
	}

	dbSize, err := db.GetDatabaseSize()
	if err != nil {
		t.Fatalf("Failed to get database size: %v", err)
	}

	t.Logf("Total database size: %.2f MB", float64(dbSize)/(1024*1024))

	db.StopLitestream()
	time.Sleep(2 * time.Second)

	t.Log("[5] Checking for errors...")
	errors, err := db.CheckForErrors()
	if err != nil {
		t.Fatalf("Failed to check errors: %v", err)
	}

	if len(errors) > 10 {
		t.Fatalf("Too many errors (%d), showing first 5:\n%v", len(errors), errors[:5])
	}

	t.Logf("✓ Found %d errors (acceptable)", len(errors))

	t.Log("[6] Testing restore...")
	restoredPath := filepath.Join(db.TempDir, "wal-restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	t.Log("✓ Restore successful")

	origCount, err := db.GetRowCount("wal_test")
	if err != nil {
		t.Fatalf("Failed to get original row count: %v", err)
	}

	restoredDB := &TestDB{Path: restoredPath, t: t}
	restCount, err := restoredDB.GetRowCount("wal_test")
	if err != nil {
		t.Fatalf("Failed to get restored row count: %v", err)
	}

	if origCount != restCount {
		t.Fatalf("Count mismatch: original=%d, restored=%d", origCount, restCount)
	}

	t.Logf("✓ Data integrity verified: %d rows", origCount)
	t.Log("TEST PASSED: Handled large WAL successfully")
}

func TestConcurrentOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	RequireBinaries(t)

	duration := GetTestDuration(t, 3*time.Minute)
	t.Logf("Testing: Multiple databases replicating concurrently (duration: %v)", duration)

	dbCount := 3
	dbs := make([]*TestDB, dbCount)

	for i := 0; i < dbCount; i++ {
		dbs[i] = SetupTestDB(t, fmt.Sprintf("concurrent-%d", i))
		defer dbs[i].Cleanup()
	}

	t.Log("[1] Creating databases...")
	for i, db := range dbs {
		if err := db.Create(); err != nil {
			t.Fatalf("Failed to create database %d: %v", i, err)
		}

		if err := CreateTestTable(t, db.Path); err != nil {
			t.Fatalf("Failed to create table for database %d: %v", i, err)
		}
	}

	t.Logf("✓ Created %d databases", dbCount)

	t.Log("[2] Starting Litestream for all databases...")
	for i, db := range dbs {
		if err := db.StartLitestream(); err != nil {
			t.Fatalf("Failed to start Litestream for database %d: %v", i, err)
		}
		time.Sleep(1 * time.Second)
	}

	t.Logf("✓ All Litestream instances running")

	t.Log("[3] Generating concurrent load...")
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	done := make(chan error, dbCount)

	for i, db := range dbs {
		go func(idx int, database *TestDB) {
			config := DefaultLoadConfig()
			config.WriteRate = 50
			config.Duration = duration
			config.Pattern = LoadPatternConstant
			config.Workers = 2

			err := database.GenerateLoad(ctx, config.WriteRate, config.Duration, string(config.Pattern))
			done <- err
		}(i, db)
	}

	for i := 0; i < dbCount; i++ {
		if err := <-done; err != nil && ctx.Err() == nil {
			t.Logf("Load generation %d had error: %v", i, err)
		}
	}

	t.Log("✓ Concurrent load complete")

	time.Sleep(5 * time.Second)

	t.Log("[4] Stopping all Litestream instances...")
	for _, db := range dbs {
		db.StopLitestream()
	}

	time.Sleep(2 * time.Second)

	t.Log("[5] Verifying all replicas...")
	for i, db := range dbs {
		fileCount, err := db.GetReplicaFileCount()
		if err != nil {
			t.Fatalf("Failed to check replica %d: %v", i, err)
		}

		if fileCount == 0 {
			t.Fatalf("Database %d has no replica files!", i)
		}

		t.Logf("✓ Database %d: %d replica files", i, fileCount)
	}

	t.Log("[6] Testing restore for all databases...")
	for i, db := range dbs {
		restoredPath := filepath.Join(db.TempDir, fmt.Sprintf("concurrent-restored-%d.db", i))
		if err := db.Restore(restoredPath); err != nil {
			t.Fatalf("Restore failed for database %d: %v", i, err)
		}

		origCount, _ := db.GetRowCount("test_data")
		restoredDB := &TestDB{Path: restoredPath, t: t}
		restCount, _ := restoredDB.GetRowCount("test_data")

		if origCount != restCount {
			t.Fatalf("Database %d count mismatch: original=%d, restored=%d", i, origCount, restCount)
		}

		t.Logf("✓ Database %d verified: %d rows", i, origCount)
	}

	t.Log("TEST PASSED: Concurrent replication works correctly")
}

func TestBusyTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	RequireBinaries(t)

	t.Log("Testing: Database busy timeout handling")

	db := SetupTestDB(t, "busy-timeout")
	defer db.Cleanup()

	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	t.Log("[1] Creating test data...")
	if err := CreateTestTable(t, db.Path); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := InsertTestData(t, db.Path, 100); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	t.Log("✓ Created table with 100 rows")

	t.Log("[2] Starting Litestream...")
	if err := db.StartLitestream(); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}

	time.Sleep(3 * time.Second)

	t.Log("[3] Simulating concurrent access with long transactions...")
	sqlDB, err := sql.Open("sqlite3", db.Path+"?_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer sqlDB.Close()

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 0; i < 500; i++ {
		if _, err := tx.Exec(
			"INSERT INTO test_data (data, created_at) VALUES (?, ?)",
			fmt.Sprintf("busy test %d", i),
			time.Now().Unix(),
		); err != nil {
			t.Fatalf("Failed to insert in transaction: %v", err)
		}

		if i%100 == 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	t.Log("✓ Long transaction completed")

	time.Sleep(5 * time.Second)

	db.StopLitestream()
	time.Sleep(2 * time.Second)

	t.Log("[4] Checking for errors...")
	errors, err := db.CheckForErrors()
	if err != nil {
		t.Fatalf("Failed to check errors: %v", err)
	}

	if len(errors) > 0 {
		t.Logf("Found %d errors (may include busy timeout messages)", len(errors))
	}

	t.Log("[5] Testing restore...")
	restoredPath := filepath.Join(db.TempDir, "busy-restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	t.Log("✓ Restore successful")

	origCount, err := db.GetRowCount("test_data")
	if err != nil {
		t.Fatalf("Failed to get original row count: %v", err)
	}

	restoredDB := &TestDB{Path: restoredPath, t: t}
	restCount, err := restoredDB.GetRowCount("test_data")
	if err != nil {
		t.Fatalf("Failed to get restored row count: %v", err)
	}

	if origCount != restCount {
		t.Fatalf("Count mismatch: original=%d, restored=%d", origCount, restCount)
	}

	t.Logf("✓ Data integrity verified: %d rows", origCount)
	t.Log("TEST PASSED: Busy timeout handled correctly")
}

func getFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
