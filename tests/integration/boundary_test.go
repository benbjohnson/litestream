//go:build integration

package integration

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func Test1GBBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	RequireBinaries(t)

	t.Log("Testing: SQLite 1GB lock page boundary handling")
	t.Log("This tests database growth beyond 1GB with 4KB pages (lock page at #262145)")

	db := SetupTestDB(t, "1gb-boundary")
	defer db.Cleanup()

	t.Log("[1] Creating database with 4KB page size...")
	if err := db.CreateWithPageSize(4096); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	t.Log("✓ Database created with 4KB pages")

	t.Log("[2] Populating to 1.5GB to cross lock page boundary...")
	if err := db.PopulateWithOptions("1.5GB", 4096, 1024); err != nil {
		t.Fatalf("Failed to populate database: %v", err)
	}

	dbSize, err := db.GetDatabaseSize()
	if err != nil {
		t.Fatalf("Failed to get database size: %v", err)
	}

	sizeGB := float64(dbSize) / (1024 * 1024 * 1024)
	t.Logf("✓ Database populated: %.2f GB", sizeGB)

	if sizeGB < 1.0 {
		t.Fatalf("Database did not reach 1GB threshold: %.2f GB", sizeGB)
	}

	t.Log("[3] Starting Litestream...")
	if err := db.StartLitestream(); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}

	time.Sleep(30 * time.Second)

	t.Log("[4] Checking replication across lock page boundary...")
	fileCount, err := db.GetReplicaFileCount()
	if err != nil {
		t.Fatalf("Failed to check replica: %v", err)
	}

	if fileCount == 0 {
		t.Fatal("No LTX files created!")
	}

	t.Logf("✓ Replication started: %d LTX files", fileCount)

	t.Log("[5] Checking for lock page errors...")
	errors, err := db.CheckForErrors()
	if err != nil {
		t.Fatalf("Failed to check errors: %v", err)
	}

	lockPageErrors := 0
	for _, errMsg := range errors {
		if containsAny(errMsg, []string{"lock page", "page 262145", "locking page"}) {
			lockPageErrors++
			t.Logf("Lock page error: %s", errMsg)
		}
	}

	if lockPageErrors > 0 {
		t.Fatalf("Found %d lock page errors!", lockPageErrors)
	}

	t.Log("✓ No lock page errors detected")

	db.StopLitestream()
	time.Sleep(2 * time.Second)

	t.Log("[6] Testing restore of large database...")
	restoredPath := filepath.Join(db.TempDir, "1gb-restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	t.Log("✓ Restore successful")

	t.Log("[7] Validating restored database integrity...")
	if err := db.QuickValidate(restoredPath); err != nil {
		t.Fatalf("Validation failed: %v", err)
	}

	restoredDB := &TestDB{Path: restoredPath, t: t}
	restoredSize, _ := restoredDB.GetDatabaseSize()
	restoredSizeGB := float64(restoredSize) / (1024 * 1024 * 1024)

	t.Logf("✓ Restored database size: %.2f GB", restoredSizeGB)

	if restoredSizeGB < 0.9 {
		t.Fatalf("Restored database too small: %.2f GB (expected ~%.2f GB)", restoredSizeGB, sizeGB)
	}

	t.Log("TEST PASSED: 1GB lock page boundary handled correctly")
}

func TestLockPageWithDifferentPageSizes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	RequireBinaries(t)

	t.Log("Testing: Lock page handling with different SQLite page sizes")

	pageSizes := []struct {
		size         int
		lockPageNum  int
		targetSizeMB int
	}{
		{4096, 262145, 1200},
		{8192, 131073, 1200},
	}

	for _, ps := range pageSizes {
		t.Run(fmt.Sprintf("PageSize%d", ps.size), func(t *testing.T) {
			db := SetupTestDB(t, fmt.Sprintf("lockpage-%d", ps.size))
			defer db.Cleanup()

			t.Logf("[1] Creating database with %d byte page size (lock page at #%d)...", ps.size, ps.lockPageNum)
			if err := db.CreateWithPageSize(ps.size); err != nil {
				t.Fatalf("Failed to create database: %v", err)
			}

			t.Logf("[2] Populating to %dMB...", ps.targetSizeMB)
			if err := db.PopulateWithOptions(fmt.Sprintf("%dMB", ps.targetSizeMB), ps.size, 1024); err != nil {
				t.Fatalf("Failed to populate database: %v", err)
			}

			dbSize, _ := db.GetDatabaseSize()
			t.Logf("✓ Database: %.2f MB", float64(dbSize)/(1024*1024))

			t.Log("[3] Starting replication...")
			if err := db.StartLitestream(); err != nil {
				t.Fatalf("Failed to start Litestream: %v", err)
			}

			time.Sleep(20 * time.Second)

			fileCount, _ := db.GetReplicaFileCount()
			t.Logf("✓ LTX files: %d", fileCount)

			db.StopLitestream()

			t.Log("[4] Testing restore...")
			restoredPath := filepath.Join(db.TempDir, fmt.Sprintf("lockpage-%d-restored.db", ps.size))
			if err := db.Restore(restoredPath); err != nil {
				t.Fatalf("Restore failed: %v", err)
			}

			t.Log("✓ Test passed for page size", ps.size)
		})
	}

	t.Log("TEST PASSED: All page sizes handled correctly")
}

func containsAny(s string, substrs []string) bool {
	for _, substr := range substrs {
		if contains(s, substr) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || anySubstring(s, substr)))
}

func anySubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
