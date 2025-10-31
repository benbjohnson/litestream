//go:build integration

package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestQuickValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	RequireBinaries(t)

	startTime := time.Now()
	duration := GetTestDuration(t, 30*time.Minute)
	t.Logf("Testing: Quick validation test (duration: %v)", duration)
	t.Log("Default: 30 minutes, configurable via test duration")

	db := SetupTestDB(t, "quick-validation")
	defer db.Cleanup()
	defer db.PrintTestSummary(t, "Quick Validation Test", startTime)

	t.Log("[1] Creating and populating database...")
	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	if err := db.Populate("10MB"); err != nil {
		t.Fatalf("Failed to populate database: %v", err)
	}

	t.Log("✓ Database populated to 10MB")

	t.Log("[2] Starting Litestream...")
	if err := db.StartLitestream(); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}

	time.Sleep(5 * time.Second)

	t.Log("[3] Generating wave pattern load...")
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	config := DefaultLoadConfig()
	config.WriteRate = 100
	config.Duration = duration
	config.Pattern = LoadPatternWave
	config.PayloadSize = 4 * 1024
	config.Workers = 4

	if err := db.GenerateLoad(ctx, config.WriteRate, config.Duration, string(config.Pattern)); err != nil && ctx.Err() == nil {
		t.Fatalf("Load generation failed: %v", err)
	}

	t.Log("✓ Load generation complete")

	time.Sleep(10 * time.Second)

	t.Log("[4] Checking replica status...")
	fileCount, err := db.GetReplicaFileCount()
	if err != nil {
		t.Fatalf("Failed to check replica: %v", err)
	}

	if fileCount == 0 {
		t.Fatal("No LTX segments created!")
	}

	t.Logf("✓ LTX segments created: %d files", fileCount)

	dbSize, err := db.GetDatabaseSize()
	if err != nil {
		t.Fatalf("Failed to get database size: %v", err)
	}

	t.Logf("Database size: %.2f MB", float64(dbSize)/(1024*1024))

	t.Log("[5] Checking for errors...")
	errors, err := db.CheckForErrors()
	if err != nil {
		t.Fatalf("Failed to check errors: %v", err)
	}

	if len(errors) > 10 {
		t.Fatalf("Too many critical errors (%d), showing first 5:\n%v", len(errors), errors[:5])
	} else if len(errors) > 0 {
		t.Logf("Found %d errors (showing first 3):", len(errors))
		for i := 0; i < min(len(errors), 3); i++ {
			t.Logf("  %s", errors[i])
		}
	} else {
		t.Log("✓ No errors detected")
	}

	db.StopLitestream()
	time.Sleep(2 * time.Second)

	t.Log("[6] Testing restore...")
	restoredPath := filepath.Join(db.TempDir, "quick-restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	t.Log("✓ Restore successful")

	t.Log("[7] Validating restoration...")
	if err := db.QuickValidate(restoredPath); err != nil {
		t.Fatalf("Validation failed: %v", err)
	}

	t.Log("✓ Validation passed")
	t.Log("TEST PASSED: Quick validation successful")
}
