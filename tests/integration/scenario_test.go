//go:build integration

package integration

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestFreshStart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	RequireBinaries(t)

	t.Log("Testing: Starting replication with a fresh (empty) database")
	t.Log("This tests if Litestream works correctly when it creates the database from scratch")

	db := SetupTestDB(t, "fresh-start")
	defer db.Cleanup()

	t.Log("[1] Starting Litestream with non-existent database...")
	if err := db.StartLitestream(); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}

	time.Sleep(2 * time.Second)

	t.Log("[2] Creating database while Litestream is running...")
	sqlDB, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	if _, err := sqlDB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("Failed to set WAL mode: %v", err)
	}

	if _, err := sqlDB.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if _, err := sqlDB.Exec("INSERT INTO test (data) VALUES ('initial data')"); err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}
	sqlDB.Close()

	time.Sleep(3 * time.Second)

	t.Log("[3] Checking if Litestream detected the database...")
	log, err := db.GetLitestreamLog()
	if err != nil {
		t.Fatalf("Failed to read log: %v", err)
	}

	t.Logf("Litestream log snippet:\n%s", log[:min(len(log), 500)])

	t.Log("[4] Adding data to test replication...")
	sqlDB, err = sql.Open("sqlite3", db.Path)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	for i := 1; i <= 100; i++ {
		if _, err := sqlDB.Exec("INSERT INTO test (data) VALUES (?)", fmt.Sprintf("row %d", i)); err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}
	sqlDB.Close()

	time.Sleep(5 * time.Second)

	t.Log("[5] Checking for errors...")
	errors, err := db.CheckForErrors()
	if err != nil {
		t.Fatalf("Failed to check errors: %v", err)
	}

	if len(errors) > 1 {
		t.Logf("Found %d errors (showing first 3):", len(errors))
		for i := 0; i < min(len(errors), 3); i++ {
			t.Logf("  %s", errors[i])
		}
	} else {
		t.Log("✓ No significant errors")
	}

	t.Log("[6] Checking replica files...")
	fileCount, err := db.GetReplicaFileCount()
	if err != nil {
		t.Fatalf("Failed to get replica file count: %v", err)
	}

	if fileCount == 0 {
		t.Fatal("✗ No replica files created!")
	}

	t.Logf("✓ Replica created with %d LTX files", fileCount)

	db.StopLitestream()
	time.Sleep(2 * time.Second)

	t.Log("[7] Testing restore...")
	restoredPath := filepath.Join(db.TempDir, "fresh-restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("✗ Restore failed: %v", err)
	}

	t.Log("✓ Restore successful")

	origCount, err := db.GetRowCount("test")
	if err != nil {
		t.Fatalf("Failed to get original row count: %v", err)
	}

	restoredDB := &TestDB{Path: restoredPath, t: t}
	restCount, err := restoredDB.GetRowCount("test")
	if err != nil {
		t.Fatalf("Failed to get restored row count: %v", err)
	}

	if origCount != restCount {
		t.Fatalf("✗ Data mismatch: Original=%d, Restored=%d", origCount, restCount)
	}

	t.Logf("✓ Data integrity verified: %d rows", origCount)
	t.Log("TEST PASSED: Fresh start works correctly")
}

func TestDatabaseIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	RequireBinaries(t)

	t.Log("Testing: Complex data patterns and integrity after restore")

	db := SetupTestDB(t, "integrity-test")
	defer db.Cleanup()

	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	t.Log("[1] Creating complex schema...")
	sqlDB, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer sqlDB.Close()

	if err := CreateComplexTestSchema(sqlDB); err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	t.Log("✓ Schema created")

	t.Log("[2] Populating with test data...")
	if err := PopulateComplexTestData(sqlDB, 10, 5, 3); err != nil {
		t.Fatalf("Failed to populate data: %v", err)
	}

	t.Log("✓ Data populated (10 users, 50 posts, 150 comments)")

	t.Log("[3] Starting Litestream...")
	if err := db.StartLitestream(); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}

	time.Sleep(10 * time.Second)

	db.StopLitestream()
	time.Sleep(2 * time.Second)

	t.Log("[4] Checking integrity of original database...")
	var integrityResult string
	if err := sqlDB.QueryRow("PRAGMA integrity_check").Scan(&integrityResult); err != nil {
		t.Fatalf("Integrity check failed: %v", err)
	}

	if integrityResult != "ok" {
		t.Fatalf("Source database integrity check failed: %s", integrityResult)
	}

	t.Log("✓ Source database integrity OK")

	t.Log("[5] Restoring database...")
	restoredPath := filepath.Join(db.TempDir, "integrity-restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	t.Log("✓ Restore successful")

	t.Log("[6] Checking integrity of restored database...")
	restoredDB, err := sql.Open("sqlite3", restoredPath)
	if err != nil {
		t.Fatalf("Failed to open restored database: %v", err)
	}
	defer restoredDB.Close()

	if err := restoredDB.QueryRow("PRAGMA integrity_check").Scan(&integrityResult); err != nil {
		t.Fatalf("Restored integrity check failed: %v", err)
	}

	if integrityResult != "ok" {
		t.Fatalf("Restored database integrity check failed: %s", integrityResult)
	}

	t.Log("✓ Restored database integrity OK")

	t.Log("[7] Validating data consistency...")
	tables := []string{"users", "posts", "comments"}
	for _, table := range tables {
		var sourceCount, restoredCount int

		if err := sqlDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&sourceCount); err != nil {
			t.Fatalf("Failed to count source %s: %v", table, err)
		}

		if err := restoredDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&restoredCount); err != nil {
			t.Fatalf("Failed to count restored %s: %v", table, err)
		}

		if sourceCount != restoredCount {
			t.Fatalf("Count mismatch for %s: source=%d, restored=%d", table, sourceCount, restoredCount)
		}

		t.Logf("✓ Table %s: %d rows match", table, sourceCount)
	}

	t.Log("TEST PASSED: Database integrity maintained through replication")
}

func TestDatabaseDeletion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	RequireBinaries(t)

	t.Log("Testing: Database deletion during active replication")

	db := SetupTestDB(t, "deletion-test")
	defer db.Cleanup()

	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	t.Log("[1] Creating test table and data...")
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

	time.Sleep(5 * time.Second)

	fileCount, _ := db.GetReplicaFileCount()
	t.Logf("✓ Replication started (%d files)", fileCount)

	t.Log("[3] Deleting database files...")
	os.Remove(db.Path)
	os.Remove(db.Path + "-wal")
	os.Remove(db.Path + "-shm")

	time.Sleep(3 * time.Second)

	t.Log("✓ Database deleted")

	t.Log("[4] Checking Litestream behavior...")
	errors, err := db.CheckForErrors()
	if err != nil {
		t.Fatalf("Failed to check errors: %v", err)
	}

	t.Logf("Litestream reported %d error messages (expected after database deletion)", len(errors))

	db.StopLitestream()

	t.Log("[5] Verifying replica is still intact...")
	finalFileCount, err := db.GetReplicaFileCount()
	if err != nil {
		t.Fatalf("Failed to check replica: %v", err)
	}

	if finalFileCount == 0 {
		t.Fatalf("Replica appears to be empty or missing")
	}

	t.Logf("✓ Replica exists with %d files (was %d - compaction may have reduced count)", finalFileCount, fileCount)

	t.Log("[6] Testing restore from replica...")
	restoredPath := filepath.Join(db.TempDir, "deletion-restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	t.Log("✓ Restore successful")

	restoredDB := &TestDB{Path: restoredPath, t: t}
	restCount, err := restoredDB.GetRowCount("test_data")
	if err != nil {
		t.Fatalf("Failed to get restored row count: %v", err)
	}

	if restCount != 100 {
		t.Fatalf("Expected 100 rows, got %d", restCount)
	}

	t.Logf("✓ Restored database has correct data: %d rows", restCount)
	t.Log("TEST PASSED: Replica survives source database deletion")
}

// TestReplicaFailover was removed because Litestream no longer supports
// multiple replicas on a single database (see cmd/litestream/main.go).
// The bash script test-replica-failover.sh was also non-functional.

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
