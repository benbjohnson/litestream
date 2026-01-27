package main

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
)

func TestDirectoryWatcher_StressTest_PreCreated100(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const dbCount = 100
	const detectionTimeout = 2 * time.Minute

	dbDir := t.TempDir()
	replicaDir := t.TempDir()

	t.Logf("Creating %d databases...", dbCount)
	dbs := createTestDatabases(t, dbDir, dbCount)
	defer closeTestDatabases(dbs)

	store, monitors := startDirectoryMonitor(t, dbDir, replicaDir)
	defer stopDirectoryMonitor(store, monitors)

	t.Logf("Waiting for detection (timeout: %v)...", detectionTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), detectionTimeout)
	defer cancel()

	if err := waitForDBCount(ctx, store, dbCount); err != nil {
		t.Fatalf("Failed to detect all databases: %v (got %d, expected %d)",
			err, len(store.DBs()), dbCount)
	}

	t.Logf("All %d databases detected successfully", dbCount)
}

func TestDirectoryWatcher_StressTest_DynamicScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const initialDBs = 10
	const finalDBs = 100
	const batchSize = 10
	const batchDetectionTimeout = 30 * time.Second

	dbDir := t.TempDir()
	replicaDir := t.TempDir()

	t.Logf("Creating initial %d databases...", initialDBs)
	dbs := createTestDatabases(t, dbDir, initialDBs)

	store, monitors := startDirectoryMonitor(t, dbDir, replicaDir)
	defer stopDirectoryMonitor(store, monitors)

	ctx, cancel := context.WithTimeout(context.Background(), batchDetectionTimeout)
	if err := waitForDBCount(ctx, store, initialDBs); err != nil {
		cancel()
		closeTestDatabases(dbs)
		t.Fatalf("Failed to detect initial databases: %v", err)
	}
	cancel()
	t.Logf("Initial %d databases detected", initialDBs)

	currentCount := initialDBs
	for currentCount < finalDBs {
		batchCount := batchSize
		if currentCount+batchCount > finalDBs {
			batchCount = finalDBs - currentCount
		}

		t.Logf("Adding batch: %d -> %d databases", currentCount, currentCount+batchCount)
		newDBs := createTestDatabasesBatch(t, dbDir, currentCount, batchCount)
		dbs = append(dbs, newDBs...)

		ctx, cancel := context.WithTimeout(context.Background(), batchDetectionTimeout)
		expectedCount := currentCount + batchCount
		if err := waitForDBCount(ctx, store, expectedCount); err != nil {
			cancel()
			closeTestDatabases(dbs)
			t.Fatalf("Failed to detect batch (expected %d, got %d): %v",
				expectedCount, len(store.DBs()), err)
		}
		cancel()

		currentCount += batchCount
		time.Sleep(500 * time.Millisecond)
	}

	closeTestDatabases(dbs)
	t.Logf("Successfully scaled to %d databases", finalDBs)
}

func TestDirectoryWatcher_ConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	const dbCount = 50
	const writeDuration = 10 * time.Second
	const writesPerDBPerSec = 5

	dbDir := t.TempDir()
	replicaDir := t.TempDir()

	t.Logf("Creating %d databases...", dbCount)
	dbs := createTestDatabases(t, dbDir, dbCount)
	defer closeTestDatabases(dbs)

	store, monitors := startDirectoryMonitor(t, dbDir, replicaDir)
	defer stopDirectoryMonitor(store, monitors)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	if err := waitForDBCount(ctx, store, dbCount); err != nil {
		cancel()
		t.Fatalf("Failed to detect databases: %v", err)
	}
	cancel()

	t.Logf("Starting concurrent writes for %v...", writeDuration)
	var totalWrites int64
	var wg sync.WaitGroup

	writeCtx, writeCancel := context.WithTimeout(context.Background(), writeDuration)
	defer writeCancel()

	for i, db := range dbs {
		wg.Add(1)
		go func(idx int, db *sql.DB) {
			defer wg.Done()
			ticker := time.NewTicker(time.Second / time.Duration(writesPerDBPerSec))
			defer ticker.Stop()

			for {
				select {
				case <-writeCtx.Done():
					return
				case <-ticker.C:
					_, err := db.Exec("INSERT INTO data (value) VALUES (?)",
						fmt.Sprintf("db%d-%d", idx, time.Now().UnixNano()))
					if err == nil {
						atomic.AddInt64(&totalWrites, 1)
					}
				}
			}
		}(i, db)
	}

	wg.Wait()
	t.Logf("Completed %d total writes across %d databases", totalWrites, dbCount)

	if totalWrites == 0 {
		t.Fatal("Expected at least some writes to succeed")
	}
}

func createTestDatabases(t *testing.T, dir string, count int) []*sql.DB {
	return createTestDatabasesBatch(t, dir, 0, count)
}

func createTestDatabasesBatch(t *testing.T, dir string, startIdx, count int) []*sql.DB {
	t.Helper()
	dbs := make([]*sql.DB, 0, count)

	for i := 0; i < count; i++ {
		idx := startIdx + i
		dbPath := filepath.Join(dir, fmt.Sprintf("test_%04d.db", idx))

		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			closeTestDatabases(dbs)
			t.Fatalf("Failed to open database %d: %v", idx, err)
		}

		_, err = db.Exec(`
			PRAGMA journal_mode=WAL;
			CREATE TABLE IF NOT EXISTS data (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				value TEXT,
				created_at DATETIME DEFAULT CURRENT_TIMESTAMP
			);
		`)
		if err != nil {
			db.Close()
			closeTestDatabases(dbs)
			t.Fatalf("Failed to initialize database %d: %v", idx, err)
		}

		dbs = append(dbs, db)
	}

	return dbs
}

func closeTestDatabases(dbs []*sql.DB) {
	for _, db := range dbs {
		if db != nil {
			db.Close()
		}
	}
}

func startDirectoryMonitor(t *testing.T, dbDir, replicaDir string) (*litestream.Store, []*DirectoryMonitor) {
	t.Helper()

	syncInterval := time.Second
	dbConfig := &DBConfig{
		Dir:       dbDir,
		Pattern:   "*.db",
		Recursive: false,
		Watch:     true,
		Replica: &ReplicaConfig{
			Type: "file",
			Path: replicaDir,
			ReplicaSettings: ReplicaSettings{
				SyncInterval: &syncInterval,
			},
		},
	}

	dbs, err := NewDBsFromDirectoryConfig(dbConfig)
	if err != nil && !strings.Contains(err.Error(), "no SQLite databases found") {
		t.Fatalf("Failed to create DBs from directory config: %v", err)
	}

	store := litestream.NewStore(dbs, litestream.DefaultCompactionLevels)
	if err := store.Open(context.Background()); err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}

	monitor, err := NewDirectoryMonitor(context.Background(), store, dbConfig, dbs)
	if err != nil {
		store.Close(context.Background())
		t.Fatalf("Failed to create directory monitor: %v", err)
	}

	return store, []*DirectoryMonitor{monitor}
}

func stopDirectoryMonitor(store *litestream.Store, monitors []*DirectoryMonitor) {
	for _, m := range monitors {
		m.Close()
	}
	store.Close(context.Background())
}

func waitForDBCount(ctx context.Context, store *litestream.Store, expected int) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if len(store.DBs()) >= expected {
				return nil
			}
		}
	}
}
