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

	if finalFileCount < fileCount {
		t.Fatalf("Replica files were lost: had %d, now %d", fileCount, finalFileCount)
	}

	t.Logf("✓ Replica intact with %d files", finalFileCount)

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

func TestReplicaFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	RequireBinaries(t)

	t.Log("Testing: Multiple replica failover scenarios")

	db := SetupTestDB(t, "failover-test")
	defer db.Cleanup()

	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	t.Log("[1] Creating test data...")
	if err := CreateTestTable(t, db.Path); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if err := InsertTestData(t, db.Path, 50); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	t.Log("✓ Created table with 50 rows")

	replica2Path := filepath.Join(db.TempDir, "replica2")
	configPath := filepath.Join(db.TempDir, "config.yml")

	configContent := fmt.Sprintf(`dbs:
  - path: %s
    replicas:
      - url: file://%s
      - url: file://%s
`, db.Path, db.ReplicaPath, replica2Path)

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	t.Log("[2] Starting Litestream with multiple replicas...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}

	time.Sleep(10 * time.Second)

	db.StopLitestream()

	t.Log("[3] Verifying both replicas...")
	fileCount1, err := db.GetReplicaFileCount()
	if err != nil || fileCount1 == 0 {
		t.Fatalf("Primary replica missing or empty")
	}

	replica2DB := &TestDB{ReplicaPath: replica2Path, t: t}
	fileCount2, err := replica2DB.GetReplicaFileCount()
	if err != nil || fileCount2 == 0 {
		t.Fatalf("Secondary replica missing or empty")
	}

	t.Logf("✓ Replica 1: %d files", fileCount1)
	t.Logf("✓ Replica 2: %d files", fileCount2)

	t.Log("[4] Testing restore from primary replica...")
	restoredPath1 := filepath.Join(db.TempDir, "failover-restored1.db")
	if err := db.Restore(restoredPath1); err != nil {
		t.Fatalf("Primary restore failed: %v", err)
	}

	restored1 := &TestDB{Path: restoredPath1, t: t}
	count1, _ := restored1.GetRowCount("test_data")
	t.Logf("✓ Primary restore successful: %d rows", count1)

	t.Log("[5] Testing restore from secondary replica...")
	restoredPath2 := filepath.Join(db.TempDir, "failover-restored2.db")
	replica2DB.Path = db.Path
	if err := replica2DB.Restore(restoredPath2); err != nil {
		t.Fatalf("Secondary restore failed: %v", err)
	}

	restored2 := &TestDB{Path: restoredPath2, t: t}
	count2, _ := restored2.GetRowCount("test_data")
	t.Logf("✓ Secondary restore successful: %d rows", count2)

	if count1 != count2 {
		t.Fatalf("Replica data mismatch: replica1=%d, replica2=%d", count1, count2)
	}

	t.Log("TEST PASSED: Multiple replicas work correctly")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
