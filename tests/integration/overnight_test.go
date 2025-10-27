//go:build integration && long

package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestOvernightFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long integration test in short mode")
	}

	RequireBinaries(t)

	startTime := time.Now()
	duration := GetTestDuration(t, 8*time.Hour)
	t.Logf("Testing: Overnight file-based replication (duration: %v)", duration)
	t.Log("Default: 8 hours, configurable via test duration")

	db := SetupTestDB(t, "overnight-file")
	defer db.Cleanup()
	defer db.PrintTestSummary(t, "Overnight File Replication", startTime)

	t.Log("[1] Creating and populating database...")
	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	if err := db.Populate("100MB"); err != nil {
		t.Fatalf("Failed to populate database: %v", err)
	}

	t.Log("✓ Database populated to 100MB")

	t.Log("[2] Starting Litestream...")
	if err := db.StartLitestream(); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}

	time.Sleep(10 * time.Second)

	t.Log("[3] Generating sustained load...")
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	config := DefaultLoadConfig()
	config.WriteRate = 50
	config.Duration = duration
	config.Pattern = LoadPatternWave
	config.PayloadSize = 2 * 1024
	config.Workers = 4

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fileCount, _ := db.GetReplicaFileCount()
				dbSize, _ := db.GetDatabaseSize()
				t.Logf("[Progress] Files: %d, DB Size: %.2f MB, Elapsed: %v",
					fileCount, float64(dbSize)/(1024*1024), time.Since(time.Now().Add(-duration)))
			}
		}
	}()

	if err := db.GenerateLoad(ctx, config.WriteRate, config.Duration, string(config.Pattern)); err != nil && ctx.Err() == nil {
		t.Fatalf("Load generation failed: %v", err)
	}

	t.Log("✓ Load generation complete")

	time.Sleep(1 * time.Minute)

	t.Log("[4] Final statistics...")
	fileCount, err := db.GetReplicaFileCount()
	if err != nil {
		t.Fatalf("Failed to check replica: %v", err)
	}

	dbSize, err := db.GetDatabaseSize()
	if err != nil {
		t.Fatalf("Failed to get database size: %v", err)
	}

	t.Logf("Final LTX files: %d", fileCount)
	t.Logf("Final DB size: %.2f MB", float64(dbSize)/(1024*1024))

	t.Log("[5] Checking for errors...")
	errors, err := db.CheckForErrors()
	if err != nil {
		t.Fatalf("Failed to check errors: %v", err)
	}

	if len(errors) > 20 {
		t.Fatalf("Too many errors (%d), test may be unstable", len(errors))
	} else if len(errors) > 0 {
		t.Logf("Found %d errors (acceptable for long test)", len(errors))
	} else {
		t.Log("✓ No errors detected")
	}

	db.StopLitestream()
	time.Sleep(2 * time.Second)

	t.Log("[6] Testing final restore...")
	restoredPath := filepath.Join(db.TempDir, "overnight-restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	t.Log("✓ Restore successful")

	t.Log("[7] Full validation...")
	if err := db.Validate(restoredPath); err != nil {
		t.Fatalf("Validation failed: %v", err)
	}

	t.Log("✓ Validation passed")
	t.Log("TEST PASSED: Overnight file replication successful")
}

func TestOvernightComprehensive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long integration test in short mode")
	}

	RequireBinaries(t)

	startTime := time.Now()
	duration := GetTestDuration(t, 8*time.Hour)
	t.Logf("Testing: Comprehensive overnight test (duration: %v)", duration)

	db := SetupTestDB(t, "overnight-comprehensive")
	defer db.Cleanup()
	defer db.PrintTestSummary(t, "Overnight Comprehensive Test", startTime)

	t.Log("[1] Creating large database...")
	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	if err := db.Populate("500MB"); err != nil {
		t.Fatalf("Failed to populate database: %v", err)
	}

	t.Log("✓ Database populated to 500MB")

	t.Log("[2] Starting Litestream...")
	if err := db.StartLitestream(); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}

	time.Sleep(10 * time.Second)

	t.Log("[3] Generating mixed workload...")
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	config := DefaultLoadConfig()
	config.WriteRate = 100
	config.Duration = duration
	config.Pattern = LoadPatternWave
	config.PayloadSize = 4 * 1024
	config.ReadRatio = 0.3
	config.Workers = 8

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fileCount, _ := db.GetReplicaFileCount()
				dbSize, _ := db.GetDatabaseSize()
				t.Logf("[Progress] Files: %d, DB Size: %.2f MB", fileCount, float64(dbSize)/(1024*1024))
			}
		}
	}()

	if err := db.GenerateLoad(ctx, config.WriteRate, config.Duration, string(config.Pattern)); err != nil && ctx.Err() == nil {
		t.Fatalf("Load generation failed: %v", err)
	}

	t.Log("✓ Load generation complete")

	time.Sleep(2 * time.Minute)

	db.StopLitestream()

	t.Log("[4] Final validation...")
	restoredPath := filepath.Join(db.TempDir, "comprehensive-restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	if err := db.Validate(restoredPath); err != nil {
		t.Fatalf("Validation failed: %v", err)
	}

	t.Log("✓ Comprehensive test passed")
	t.Log("TEST PASSED: Overnight comprehensive test successful")
}
