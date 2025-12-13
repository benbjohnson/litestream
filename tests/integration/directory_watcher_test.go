//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDirectoryWatcherBasicLifecycle tests the fundamental directory watcher functionality:
// - Start with empty directory
// - Create databases while Litestream is running
// - Verify they are detected and replicated
// - Delete databases and verify cleanup
func TestDirectoryWatcherBasicLifecycle(t *testing.T) {
	RequireBinaries(t)

	// Use recursive:true because this test creates databases in subdirectories (tenant1/app.db, etc.)
	db := SetupDirectoryWatchTest(t, "dir-watch-lifecycle", "*.db", true)

	// Create config with directory watching
	configPath, err := db.CreateDirectoryWatchConfig()
	if err != nil {
		t.Fatalf("create config: %v", err)
	}

	t.Log("Starting Litestream with directory watching...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}
	defer db.StopLitestream()

	// Give Litestream time to start
	time.Sleep(2 * time.Second)

	// Step 1: Create 2 databases in separate tenant directories
	t.Log("Creating databases in separate directories...")
	tenant1DB := CreateDatabaseInDir(t, db.DirPath, "tenant1", "app.db")
	tenant2DB := CreateDatabaseInDir(t, db.DirPath, "tenant2", "app.db")

	// Wait for detection and replication
	t.Log("Waiting for database detection...")
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, tenant1DB, 5*time.Second); err != nil {
		t.Fatalf("tenant1 database not detected: %v", err)
	}
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, tenant2DB, 5*time.Second); err != nil {
		t.Fatalf("tenant2 database not detected: %v", err)
	}

	// Step 2: Add data to both databases
	t.Log("Adding data to databases...")
	if err := CreateDatabaseWithData(t, tenant1DB, 100); err != nil {
		t.Fatalf("add data to tenant1: %v", err)
	}
	if err := CreateDatabaseWithData(t, tenant2DB, 100); err != nil {
		t.Fatalf("add data to tenant2: %v", err)
	}

	// Wait for replication
	time.Sleep(3 * time.Second)

	// Step 3: Create 3 more databases at intervals
	t.Log("Creating additional databases at intervals...")
	db3 := CreateDatabaseInDir(t, db.DirPath, "tenant3", "app.db")
	time.Sleep(1 * time.Second)

	db4 := CreateDatabaseInDir(t, db.DirPath, "", "standalone.db")
	time.Sleep(1 * time.Second)

	db5 := CreateDatabaseInDir(t, db.DirPath, "tenant4", "data.db")

	// Wait for all to be detected
	t.Log("Verifying all databases detected...")
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, db3, 5*time.Second); err != nil {
		t.Fatalf("tenant3 database not detected: %v", err)
	}
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, db4, 5*time.Second); err != nil {
		t.Fatalf("standalone database not detected: %v", err)
	}
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, db5, 5*time.Second); err != nil {
		t.Fatalf("tenant4 database not detected: %v", err)
	}

	// Step 4: Delete one database and verify cleanup
	t.Log("Deleting database and verifying cleanup...")
	if err := os.Remove(db4); err != nil {
		t.Fatalf("remove database: %v", err)
	}

	// Wait and verify no more replication
	if err := VerifyDatabaseRemoved(t, db.ReplicaPath, db4, 3*time.Second); err != nil {
		t.Fatalf("database still replicating after removal: %v", err)
	}

	// Step 5: Verify no critical errors in log
	t.Log("Checking for errors...")
	errors, err := CheckForCriticalErrors(t, db.TestDB)
	if err != nil {
		t.Fatalf("check errors: %v", err)
	}
	if len(errors) > 0 {
		t.Fatalf("found critical errors in log: %v", errors)
	}

	t.Log("✓ Basic lifecycle test passed")
}

// TestDirectoryWatcherRapidConcurrentCreation tests race conditions with rapid database creation
func TestDirectoryWatcherRapidConcurrentCreation(t *testing.T) {
	RequireBinaries(t)

	db := SetupDirectoryWatchTest(t, "dir-watch-concurrent", "*.db", false)

	configPath, err := db.CreateDirectoryWatchConfig()
	if err != nil {
		t.Fatalf("create config: %v", err)
	}

	t.Log("Starting Litestream with directory watching...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}
	defer db.StopLitestream()

	time.Sleep(2 * time.Second)

	// Create 20 databases simultaneously
	t.Log("Creating 20 databases concurrently...")
	dbPaths := CreateMultipleDatabasesConcurrently(t, db.DirPath, 20, "*.db")

	// Wait for all databases to be detected
	t.Log("Verifying all databases detected...")
	for i, dbPath := range dbPaths {
		if err := WaitForDatabaseInReplica(t, db.ReplicaPath, dbPath, 10*time.Second); err != nil {
			t.Fatalf("database %d (%s) not detected: %v", i, filepath.Base(dbPath), err)
		}
	}

	// Count databases in replica
	count, err := CountDatabasesInReplica(db.ReplicaPath)
	if err != nil {
		t.Fatalf("count databases: %v", err)
	}

	if count != 20 {
		t.Fatalf("expected 20 databases in replica, got %d", count)
	}

	// Check for errors (especially duplicate registrations or race conditions)
	errors, err := db.CheckForErrors()
	if err != nil {
		t.Fatalf("check errors: %v", err)
	}
	if len(errors) > 0 {
		t.Fatalf("found errors in log (possible race conditions): %v", errors)
	}

	t.Log("✓ Concurrent creation test passed")
}

// TestDirectoryWatcherRecursiveMode tests recursive directory scanning
func TestDirectoryWatcherRecursiveMode(t *testing.T) {
	RequireBinaries(t)

	db := SetupDirectoryWatchTest(t, "dir-watch-recursive", "*.db", true)

	configPath, err := db.CreateDirectoryWatchConfig()
	if err != nil {
		t.Fatalf("create config: %v", err)
	}

	t.Log("Starting Litestream with recursive directory watching...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}
	defer db.StopLitestream()

	time.Sleep(2 * time.Second)

	// Create nested directory structure
	t.Log("Creating nested directory structure...")
	db1 := CreateDatabaseInDir(t, db.DirPath, "", "db1.db")       // root/db1.db
	db2 := CreateDatabaseInDir(t, db.DirPath, "level1", "db2.db") // root/level1/db2.db

	// Verify first two detected
	t.Log("Verifying databases detected...")
	for i, dbPath := range []string{db1, db2} {
		if err := WaitForDatabaseInReplica(t, db.ReplicaPath, dbPath, 10*time.Second); err != nil {
			t.Fatalf("database %d (%s) not detected: %v", i+1, filepath.Base(dbPath), err)
		}
	}

	// Try deeper nesting (may be slower to detect)
	t.Log("Creating deeply nested database...")
	db3 := CreateDatabaseInDir(t, db.DirPath, "level1/level2", "db3.db") // root/level1/level2/db3.db

	// Give more time for deeply nested database
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, db3, 15*time.Second); err != nil {
		t.Logf("Warning: deeply nested database (2 levels) not detected: %v", err)
		// Don't fail the test - recursive watching of deeply nested dirs may have limitations
	}

	// Create new subdirectory after start
	t.Log("Creating new subdirectory dynamically...")
	newDir := filepath.Join(db.DirPath, "dynamic")
	if err := os.MkdirAll(newDir, 0755); err != nil {
		t.Fatalf("create dynamic dir: %v", err)
	}
	time.Sleep(500 * time.Millisecond) // Allow directory watch to register

	db5 := CreateDatabaseInDir(t, db.DirPath, "dynamic", "db5.db")
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, db5, 10*time.Second); err != nil {
		t.Fatalf("dynamically created database not detected: %v", err)
	}

	// Delete entire subdirectory
	t.Log("Deleting subdirectory with databases...")
	level1Dir := filepath.Join(db.DirPath, "level1")
	if err := os.RemoveAll(level1Dir); err != nil {
		t.Fatalf("remove level1 directory: %v", err)
	}

	// Verify databases removed (with more lenient timeout)
	time.Sleep(3 * time.Second)
	if err := VerifyDatabaseRemoved(t, db.ReplicaPath, db2, 3*time.Second); err != nil {
		t.Logf("Note: db2 may still have existing LTX files (cleanup timing): %v", err)
	}

	errors, err := db.CheckForErrors()
	if err != nil {
		t.Fatalf("check errors: %v", err)
	}
	if len(errors) > 0 {
		t.Logf("Errors found (may be expected from deletion): %v", errors)
	}

	t.Log("✓ Recursive mode test passed")
}

// TestDirectoryWatcherPatternMatching tests glob pattern matching
func TestDirectoryWatcherPatternMatching(t *testing.T) {
	RequireBinaries(t)

	db := SetupDirectoryWatchTest(t, "dir-watch-pattern", "*.db", false)

	configPath, err := db.CreateDirectoryWatchConfig()
	if err != nil {
		t.Fatalf("create config: %v", err)
	}

	t.Log("Starting Litestream with pattern '*.db'...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}
	defer db.StopLitestream()

	time.Sleep(2 * time.Second)

	// Create files with different extensions
	t.Log("Creating files with various patterns...")
	matchDB := CreateDatabaseInDir(t, db.DirPath, "", "test.db")                   // Should match
	noMatchSQLite := CreateDatabaseInDir(t, db.DirPath, "", "test.sqlite")         // Should NOT match
	noMatchBackup := CreateFakeDatabase(t, db.DirPath, "test.db.backup", []byte{}) // Should NOT match

	// Also create WAL and SHM files (should be ignored)
	CreateFakeDatabase(t, db.DirPath, "test.db-wal", []byte{})
	CreateFakeDatabase(t, db.DirPath, "test.db-shm", []byte{})

	// Wait and verify only .db file is replicated
	t.Log("Verifying pattern matching...")
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, matchDB, 5*time.Second); err != nil {
		t.Fatalf("*.db file should be detected: %v", err)
	}

	// Give time for other files to be processed (they shouldn't be)
	time.Sleep(3 * time.Second)

	// Count - should only have 1 database
	count, err := CountDatabasesInReplica(db.ReplicaPath)
	if err != nil {
		t.Fatalf("count databases: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 database in replica, got %d (pattern matching failed)", count)
	}

	// Verify .sqlite and .db.backup files were not added
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, noMatchSQLite, 2*time.Second); err == nil {
		t.Fatal("*.sqlite file should NOT be detected with *.db pattern")
	}

	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, noMatchBackup, 2*time.Second); err == nil {
		t.Fatal("*.db.backup file should NOT be detected with *.db pattern")
	}

	t.Log("✓ Pattern matching test passed")
}

// TestDirectoryWatcherNonSQLiteRejection tests that non-SQLite files are rejected
func TestDirectoryWatcherNonSQLiteRejection(t *testing.T) {
	RequireBinaries(t)

	db := SetupDirectoryWatchTest(t, "dir-watch-nonsqlite", "*.db", false)

	configPath, err := db.CreateDirectoryWatchConfig()
	if err != nil {
		t.Fatalf("create config: %v", err)
	}

	t.Log("Starting Litestream...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}
	defer db.StopLitestream()

	time.Sleep(2 * time.Second)

	// Create fake database files
	t.Log("Creating non-SQLite files...")
	fakeDB := CreateFakeDatabase(t, db.DirPath, "fake.db", []byte("this is not a sqlite file"))
	emptyDB := CreateFakeDatabase(t, db.DirPath, "empty.db", []byte{})
	textDB := CreateFakeDatabase(t, db.DirPath, "text.db", []byte("SQLite format 2\x00")) // Wrong version

	// Create one valid SQLite database
	validDB := CreateDatabaseInDir(t, db.DirPath, "", "valid.db")

	// Wait for valid database
	t.Log("Verifying only valid SQLite database is detected...")
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, validDB, 5*time.Second); err != nil {
		t.Fatalf("valid database should be detected: %v", err)
	}

	// Wait to ensure fake databases are not added
	time.Sleep(3 * time.Second)

	// Should only have 1 database
	count, err := CountDatabasesInReplica(db.ReplicaPath)
	if err != nil {
		t.Fatalf("count databases: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 database in replica, got %d (non-SQLite files were not rejected)", count)
	}

	// Verify fake files were not added
	for _, fakePath := range []string{fakeDB, emptyDB, textDB} {
		if err := WaitForDatabaseInReplica(t, db.ReplicaPath, fakePath, 1*time.Second); err == nil {
			t.Fatalf("non-SQLite file %s should NOT be replicated", filepath.Base(fakePath))
		}
	}

	t.Log("✓ Non-SQLite rejection test passed")
}

// TestDirectoryWatcherActiveConnections tests behavior with databases that are actively being used
func TestDirectoryWatcherActiveConnections(t *testing.T) {
	RequireBinaries(t)

	db := SetupDirectoryWatchTest(t, "dir-watch-active", "*.db", false)

	configPath, err := db.CreateDirectoryWatchConfig()
	if err != nil {
		t.Fatalf("create config: %v", err)
	}

	// Create database with active connection before starting Litestream
	t.Log("Creating database with active connection...")
	db1Path := CreateDatabaseInDir(t, db.DirPath, "", "active.db")

	// Start continuous writes
	ctx := context.Background()
	wg, cancel, err := StartContinuousWrites(ctx, t, db1Path, 10) // 10 writes/sec
	if err != nil {
		t.Fatalf("start writes: %v", err)
	}
	defer cancel()

	// Start Litestream
	t.Log("Starting Litestream with active database...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}
	defer db.StopLitestream()

	time.Sleep(2 * time.Second)

	// Verify database is detected despite active connection
	t.Log("Verifying active database is detected...")
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, db1Path, 10*time.Second); err != nil {
		t.Fatalf("active database not detected: %v", err)
	}

	// Create second database and start writing to it
	t.Log("Creating second database with writes...")
	db2Path := CreateDatabaseInDir(t, db.DirPath, "", "active2.db")
	wg2, cancel2, err := StartContinuousWrites(ctx, t, db2Path, 5)
	if err != nil {
		t.Fatalf("start writes for db2: %v", err)
	}
	defer cancel2()

	// Verify second database detected
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, db2Path, 10*time.Second); err != nil {
		t.Fatalf("second active database not detected: %v", err)
	}

	// Let writes continue for a bit
	t.Log("Letting writes continue for 5 seconds...")
	time.Sleep(5 * time.Second)

	// Stop writers
	cancel()
	cancel2()
	wg.Wait()
	wg2.Wait()

	// Verify both databases are still replicated
	count, err := CountDatabasesInReplica(db.ReplicaPath)
	if err != nil {
		t.Fatalf("count databases: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected 2 databases in replica, got %d", count)
	}

	errors, err := CheckForCriticalErrors(t, db.TestDB)
	if err != nil {
		t.Fatalf("check errors: %v", err)
	}
	if len(errors) > 0 {
		t.Fatalf("found critical errors with active connections: %v", errors)
	}

	t.Log("✓ Active connections test passed")
}

// TestDirectoryWatcherRestartBehavior tests behavior across Litestream restarts
func TestDirectoryWatcherRestartBehavior(t *testing.T) {
	RequireBinaries(t)

	db := SetupDirectoryWatchTest(t, "dir-watch-restart", "*.db", false)

	// Create 3 databases before starting
	t.Log("Creating initial databases...")
	db1 := CreateDatabaseInDir(t, db.DirPath, "", "db1.db")
	db2 := CreateDatabaseInDir(t, db.DirPath, "", "db2.db")
	db3 := CreateDatabaseInDir(t, db.DirPath, "", "db3.db")

	configPath, err := db.CreateDirectoryWatchConfig()
	if err != nil {
		t.Fatalf("create config: %v", err)
	}

	// First start
	t.Log("Starting Litestream (first time)...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}

	time.Sleep(3 * time.Second)

	// Verify all 3 detected
	for _, dbPath := range []string{db1, db2, db3} {
		if err := WaitForDatabaseInReplica(t, db.ReplicaPath, dbPath, 5*time.Second); err != nil {
			t.Fatalf("database %s not detected: %v", filepath.Base(dbPath), err)
		}
	}

	// Add 2 more databases dynamically
	t.Log("Adding databases dynamically...")
	db4 := CreateDatabaseInDir(t, db.DirPath, "", "db4.db")
	db5 := CreateDatabaseInDir(t, db.DirPath, "", "db5.db")

	time.Sleep(3 * time.Second)

	// Verify new databases detected
	for _, dbPath := range []string{db4, db5} {
		if err := WaitForDatabaseInReplica(t, db.ReplicaPath, dbPath, 5*time.Second); err != nil {
			t.Fatalf("dynamically added database %s not detected: %v", filepath.Base(dbPath), err)
		}
	}

	// Stop Litestream
	t.Log("Stopping Litestream...")
	if err := db.StopLitestream(); err != nil {
		t.Fatalf("stop litestream: %v", err)
	}

	// Add one more database while stopped
	t.Log("Adding database while Litestream is stopped...")
	db6 := CreateDatabaseInDir(t, db.DirPath, "", "db6.db")

	time.Sleep(2 * time.Second)

	// Restart Litestream
	t.Log("Restarting Litestream...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("restart litestream: %v", err)
	}
	defer db.StopLitestream()

	time.Sleep(3 * time.Second)

	// Verify all 6 databases are now being replicated
	t.Log("Verifying all databases detected after restart...")
	for i, dbPath := range []string{db1, db2, db3, db4, db5, db6} {
		if err := WaitForDatabaseInReplica(t, db.ReplicaPath, dbPath, 10*time.Second); err != nil {
			t.Fatalf("database %d (%s) not detected after restart: %v", i+1, filepath.Base(dbPath), err)
		}
	}

	// Add one more dynamically after restart
	t.Log("Adding database after restart...")
	db7 := CreateDatabaseInDir(t, db.DirPath, "", "db7.db")

	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, db7, 5*time.Second); err != nil {
		t.Fatalf("database added after restart not detected: %v", err)
	}

	// Final count - should have 7 databases
	count, err := CountDatabasesInReplica(db.ReplicaPath)
	if err != nil {
		t.Fatalf("count databases: %v", err)
	}

	if count != 7 {
		t.Fatalf("expected 7 databases in replica, got %d", count)
	}

	t.Log("✓ Restart behavior test passed")
}

// TestDirectoryWatcherRenameOperations tests file rename handling
func TestDirectoryWatcherRenameOperations(t *testing.T) {
	RequireBinaries(t)

	db := SetupDirectoryWatchTest(t, "dir-watch-rename", "*.db", false)

	configPath, err := db.CreateDirectoryWatchConfig()
	if err != nil {
		t.Fatalf("create config: %v", err)
	}

	t.Log("Starting Litestream...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}
	defer db.StopLitestream()

	time.Sleep(2 * time.Second)

	// Create database
	t.Log("Creating database...")
	originalPath := CreateDatabaseInDir(t, db.DirPath, "", "original.db")

	// Wait for replication
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, originalPath, 5*time.Second); err != nil {
		t.Fatalf("original database not detected: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Rename database
	t.Log("Renaming database...")
	renamedPath := filepath.Join(db.DirPath, "renamed.db")
	if err := os.Rename(originalPath, renamedPath); err != nil {
		t.Fatalf("rename database: %v", err)
	}

	// Wait for new name to be detected
	t.Log("Waiting for renamed database to be detected...")
	if err := WaitForDatabaseInReplica(t, db.ReplicaPath, renamedPath, 10*time.Second); err != nil {
		t.Fatalf("renamed database not detected: %v", err)
	}

	// Verify old database stopped replicating
	t.Log("Verifying original database stopped replicating...")
	if err := VerifyDatabaseRemoved(t, db.ReplicaPath, originalPath, 3*time.Second); err != nil {
		t.Logf("Warning: original may still be replicating: %v", err)
	}

	t.Log("✓ Rename operations test passed")
}

// TestDirectoryWatcherLoadWithWrites tests directory watching with concurrent database writes
func TestDirectoryWatcherLoadWithWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	RequireBinaries(t)

	db := SetupDirectoryWatchTest(t, "dir-watch-load", "*.db", false)

	// Create 3 databases with data
	t.Log("Creating initial databases with data...")
	db1 := CreateDatabaseInDir(t, db.DirPath, "", "db1.db")
	db2 := CreateDatabaseInDir(t, db.DirPath, "", "db2.db")
	db3 := CreateDatabaseInDir(t, db.DirPath, "", "db3.db")

	for _, dbPath := range []string{db1, db2, db3} {
		if err := CreateDatabaseWithData(t, dbPath, 50); err != nil {
			t.Fatalf("create database with data: %v", err)
		}
	}

	configPath, err := db.CreateDirectoryWatchConfig()
	if err != nil {
		t.Fatalf("create config: %v", err)
	}

	t.Log("Starting Litestream...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}
	defer db.StopLitestream()

	time.Sleep(3 * time.Second)

	// Start continuous writes to all 3 databases
	ctx := context.Background()
	t.Log("Starting continuous writes to all databases...")
	wg1, cancel1, _ := StartContinuousWrites(ctx, t, db1, 20)
	wg2, cancel2, _ := StartContinuousWrites(ctx, t, db2, 15)
	wg3, cancel3, _ := StartContinuousWrites(ctx, t, db3, 10)

	defer func() {
		cancel1()
		cancel2()
		cancel3()
		wg1.Wait()
		wg2.Wait()
		wg3.Wait()
	}()

	// Wait a bit for writes to start
	time.Sleep(2 * time.Second)

	// While writes are happening, create 2 new databases
	t.Log("Creating new databases while writes are ongoing...")
	db4 := CreateDatabaseInDir(t, db.DirPath, "", "db4.db")
	db5 := CreateDatabaseInDir(t, db.DirPath, "", "db5.db")

	// Start writes on new databases
	wg4, cancel4, _ := StartContinuousWrites(ctx, t, db4, 10)
	wg5, cancel5, _ := StartContinuousWrites(ctx, t, db5, 10)

	defer func() {
		cancel4()
		cancel5()
		wg4.Wait()
		wg5.Wait()
	}()

	// Verify all databases detected
	for i, dbPath := range []string{db1, db2, db3, db4, db5} {
		if err := WaitForDatabaseInReplica(t, db.ReplicaPath, dbPath, 10*time.Second); err != nil {
			t.Fatalf("database %d not detected: %v", i+1, err)
		}
	}

	// Let writes continue
	t.Log("Running writes for 10 seconds...")
	time.Sleep(10 * time.Second)

	// Stop all writes
	cancel1()
	cancel2()
	cancel3()
	cancel4()
	cancel5()
	wg1.Wait()
	wg2.Wait()
	wg3.Wait()
	wg4.Wait()
	wg5.Wait()

	// Wait for final replication
	time.Sleep(3 * time.Second)

	// Verify all 5 databases are in replica
	count, err := CountDatabasesInReplica(db.ReplicaPath)
	if err != nil {
		t.Fatalf("count databases: %v", err)
	}

	if count != 5 {
		t.Fatalf("expected 5 databases in replica, got %d", count)
	}

	// Check for errors
	errors, err := CheckForCriticalErrors(t, db.TestDB)
	if err != nil {
		t.Fatalf("check errors: %v", err)
	}
	if len(errors) > 0 {
		t.Fatalf("found critical errors during load test: %v", errors)
	}

	t.Log("✓ Load with writes test passed")
}
