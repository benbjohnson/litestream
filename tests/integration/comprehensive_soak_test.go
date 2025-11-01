//go:build integration && soak

package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestComprehensiveSoak runs a comprehensive soak test with aggressive settings
// to validate all Litestream features: replication, snapshots, compaction, checkpoints.
//
// Default duration: 2 hours
// Can be shortened with: go test -test.short (runs for 30 minutes)
//
// This test exercises:
// - Continuous replication
// - Snapshot generation (every 10m)
// - Compaction (30s/1m/5m/15m/30m intervals)
// - Checkpoint operations
// - Database restoration
func TestComprehensiveSoak(t *testing.T) {
	RequireBinaries(t)

	// Determine test duration
	duration := GetTestDuration(t, 2*time.Hour)
	shortMode := testing.Short()
	if shortMode {
		duration = 2 * time.Minute
	}

	targetSize := "50MB"
	writeRate := 500
	if shortMode {
		targetSize = "5MB"
		writeRate = 100
	}

	t.Logf("================================================")
	t.Logf("Litestream Comprehensive Soak Test")
	t.Logf("================================================")
	t.Logf("Duration: %v", duration)
	t.Logf("Start time: %s", time.Now().Format(time.RFC3339))
	t.Log("")
	t.Log("This test uses aggressive settings to validate:")
	t.Log("  - Continuous replication")
	t.Log("  - Snapshot generation (every 10m)")
	t.Log("  - Compaction (30s/1m/5m intervals)")
	t.Log("  - Checkpoint operations")
	t.Log("  - Database restoration")
	t.Log("")

	startTime := time.Now()

	// Setup test database
	db := SetupTestDB(t, "comprehensive-soak")
	defer db.Cleanup()

	// Create database
	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Populate database
	t.Logf("Populating database (%s initial data)...", targetSize)
	if err := db.Populate(targetSize); err != nil {
		t.Fatalf("Failed to populate database: %v", err)
	}
	t.Log("✓ Database populated")
	t.Log("")

	// Create aggressive configuration for testing
	t.Log("Creating aggressive test configuration...")
	replicaURL := fmt.Sprintf("file://%s", filepath.ToSlash(db.ReplicaPath))
	configPath := CreateSoakConfig(db.Path, replicaURL, nil, shortMode)
	db.ConfigPath = configPath
	t.Logf("✓ Configuration created: %s", configPath)
	t.Log("")

	// Start Litestream
	t.Log("Starting Litestream replication...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}
	t.Logf("✓ Litestream running (PID: %d)", db.LitestreamPID)
	t.Log("")

	// Start load generator with heavy sustained load
	t.Log("Starting load generator (heavy sustained load)...")
	t.Logf("  Write rate: %d writes/second", writeRate)
	t.Logf("  Pattern: wave (simulates varying load)")
	t.Logf("  Payload size: 4KB")
	t.Logf("  Workers: 8")
	t.Log("")

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Setup signal handler for graceful interruption
	testInfo := &TestInfo{
		StartTime: startTime,
		Duration:  duration,
		DB:        db,
		cancel:    cancel,
	}
	setupSignalHandler(t, cancel, testInfo)

	// Run load generation in background
	loadDone := make(chan error, 1)
	go func() {
		loadDone <- db.GenerateLoad(ctx, writeRate, duration, "wave")
	}()

	// Monitor every 60 seconds
	t.Log("Running comprehensive test...")
	t.Log("Monitor will report every 60 seconds")
	t.Log("Press Ctrl+C twice within 5 seconds to stop early")
	t.Log("================================================")
	t.Log("")

	refreshStats := func() {
		testInfo.RowCount, _ = db.GetRowCount("load_test")
		if testInfo.RowCount == 0 {
			testInfo.RowCount, _ = db.GetRowCount("test_table_0")
		}
		if testInfo.RowCount == 0 {
			testInfo.RowCount, _ = db.GetRowCount("test_data")
		}
		testInfo.FileCount, _ = db.GetReplicaFileCount()
	}

	logMetrics := func() {
		LogSoakMetrics(t, db, "comprehensive")
		if db.LitestreamCmd != nil && db.LitestreamCmd.ProcessState != nil {
			t.Error("✗ Litestream stopped unexpectedly!")
			if testInfo.cancel != nil {
				testInfo.cancel()
			}
		}
	}

	MonitorSoakTest(t, db, ctx, testInfo, refreshStats, logMetrics)

	// Wait for load generation to complete
	if err := <-loadDone; err != nil {
		t.Logf("Load generation completed: %v", err)
	}

	if err := db.WaitForSnapshots(30 * time.Second); err != nil {
		t.Fatalf("Failed waiting for snapshot: %v", err)
	}

	t.Log("")
	t.Log("================================================")
	t.Log("Final Test Results")
	t.Log("================================================")
	t.Log("")

	// Stop Litestream
	t.Log("Stopping Litestream...")
	if err := db.StopLitestream(); err != nil {
		t.Logf("Warning: Failed to stop Litestream cleanly: %v", err)
	}

	// Final statistics
	t.Log("Database Statistics:")
	if dbSize, err := db.GetDatabaseSize(); err == nil {
		t.Logf("  Final size: %.2f MB", float64(dbSize)/(1024*1024))
	}

	// Count rows using different table name possibilities
	var rowCount int
	var err error
	if rowCount, err = db.GetRowCount("load_test"); err != nil {
		if rowCount, err = db.GetRowCount("test_table_0"); err != nil {
			if rowCount, err = db.GetRowCount("test_data"); err != nil {
				t.Logf("  Warning: Could not get row count: %v", err)
			}
		}
	}
	if err == nil {
		t.Logf("  Total rows: %d", rowCount)
	}
	t.Log("")

	// Replica statistics
	t.Log("Replication Statistics:")
	if fileCount, err := db.GetReplicaFileCount(); err == nil {
		t.Logf("  LTX segments: %d", fileCount)
	}

	// Check for errors
	errors, _ := db.CheckForErrors()
	criticalErrors := 0
	for _, errLine := range errors {
		// Filter out known non-critical errors
		if !containsAny(errLine, []string{"page size not initialized"}) {
			criticalErrors++
		}
	}
	t.Logf("  Critical errors: %d", criticalErrors)
	t.Log("")

	// Test restoration
	t.Log("Testing restoration...")
	restoredPath := filepath.Join(db.TempDir, "restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restoration failed: %v", err)
	}
	t.Log("✓ Restoration successful!")

	// Validate
	t.Log("")
	t.Log("Validating restored database integrity...")
	restoredDB := &TestDB{Path: restoredPath, t: t}
	if err := restoredDB.IntegrityCheck(); err != nil {
		t.Fatalf("Integrity check failed: %v", err)
	}
	t.Log("✓ Integrity check passed!")

	// Analyze test results
	analysis := AnalyzeSoakTest(t, db, duration)
	PrintSoakTestAnalysis(t, analysis)

	// Test Summary
	t.Log("================================================")
	t.Log("Test Summary")
	t.Log("================================================")

	testPassed := true
	issues := []string{}

	if criticalErrors > 0 {
		testPassed = false
		issues = append(issues, fmt.Sprintf("Critical errors detected: %d", criticalErrors))
	}

	if analysis.FinalFileCount == 0 {
		testPassed = false
		issues = append(issues, "No files created (replication not working)")
	}

	if testPassed {
		t.Log("✓ TEST PASSED!")
		t.Log("")
		t.Log("The configuration is ready for production use.")
	} else {
		t.Log("⚠ TEST COMPLETED WITH ISSUES:")
		for _, issue := range issues {
			t.Logf("  - %s", issue)
		}
		t.Log("")
		t.Log("Review the logs for details:")
		logPath, _ := db.GetLitestreamLog()
		t.Logf("  %s", logPath)
		t.Fail()
	}

	t.Log("")
	t.Logf("Test duration: %v", time.Since(startTime).Round(time.Second))
	t.Logf("Results available in: %s", db.TempDir)
	t.Log("================================================")
}
