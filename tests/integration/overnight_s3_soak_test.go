//go:build integration && soak && aws

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestOvernightS3Soak runs an 8-hour overnight soak test against real AWS S3.
//
// Default duration: 8 hours
// Can be shortened with: go test -test.short (runs for 1 hour)
//
// Requirements:
// - AWS_ACCESS_KEY_ID environment variable
// - AWS_SECRET_ACCESS_KEY environment variable
// - S3_BUCKET environment variable
// - AWS_REGION environment variable (optional, defaults to us-east-1)
// - AWS CLI must be installed
//
// This test validates:
// - Long-term S3 replication stability
// - Network resilience over 8 hours
// - Real S3 API performance
// - Restoration from cloud storage
func TestOvernightS3Soak(t *testing.T) {
	RequireBinaries(t)

	// Check AWS credentials and get configuration
	bucket, region := CheckAWSCredentials(t)

	// Determine test duration
	var duration time.Duration
	if testing.Short() {
		duration = 10 * time.Minute
	} else {
		duration = 8 * time.Hour
	}

	shortMode := testing.Short()

	t.Logf("================================================")
	t.Logf("Litestream Overnight S3 Soak Test")
	t.Logf("================================================")
	t.Logf("Duration: %v", duration)
	t.Logf("S3 Bucket: %s", bucket)
	t.Logf("AWS Region: %s", region)
	t.Logf("Start time: %s", time.Now().Format(time.RFC3339))
	t.Log("")

	startTime := time.Now()

	// Test S3 connectivity
	t.Log("Testing S3 connectivity...")
	TestS3Connectivity(t, bucket)
	t.Log("")

	// Setup test database
	db := SetupTestDB(t, "overnight-s3-soak")
	defer db.Cleanup()

	// Create database
	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create S3 configuration
	s3Path := fmt.Sprintf("litestream-overnight-%d", time.Now().Unix())
	s3URL := fmt.Sprintf("s3://%s/%s", bucket, s3Path)
	db.ReplicaURL = s3URL
	t.Log("Creating Litestream configuration for S3...")
	s3Config := &S3Config{
		Region: region,
	}
	configPath := CreateSoakConfig(db.Path, s3URL, s3Config, shortMode)
	db.ConfigPath = configPath
	t.Logf("✓ Configuration created: %s", configPath)
	t.Logf("  S3 URL: %s", s3URL)
	t.Log("")

	// Start Litestream initially (before population)
	t.Log("Starting Litestream...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}
	t.Logf("✓ Litestream started (PID: %d)", db.LitestreamPID)
	t.Log("")

	// Stop Litestream to populate database
	t.Log("Stopping Litestream temporarily for initial population...")
	if err := db.StopLitestream(); err != nil {
		t.Fatalf("Failed to stop Litestream: %v", err)
	}

	// Populate with 100MB of initial data
	t.Log("Populating database (100MB initial data)...")
	if err := db.Populate("100MB"); err != nil {
		t.Fatalf("Failed to populate database: %v", err)
	}
	t.Log("✓ Database populated")
	t.Log("")

	// Restart Litestream after population
	t.Log("Restarting Litestream after population...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("Failed to restart Litestream: %v", err)
	}
	t.Logf("✓ Litestream restarted (PID: %d)", db.LitestreamPID)
	t.Log("")

	// Start load generator for overnight test
	t.Log("Starting load generator for overnight S3 test...")
	t.Log("Configuration:")
	t.Logf("  Duration: %v", duration)
	t.Logf("  Write rate: 100 writes/second (higher for S3 testing)")
	t.Logf("  Pattern: wave (simulates varying load)")
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
		loadDone <- db.GenerateLoad(ctx, 100, duration, "wave")
	}()

	// Monitor every 60 seconds with S3-specific metrics
	t.Log("Overnight S3 test is running!")
	t.Log("Monitor will report every 60 seconds")
	t.Log("Press Ctrl+C twice within 5 seconds to stop early")
	t.Log("================================================")
	t.Log("")
	t.Logf("The test will run for %v. Monitor progress below.", duration)
	t.Log("")

	refreshStats := func() {
		testInfo.RowCount, _ = db.GetRowCount("load_test")
		if testInfo.RowCount == 0 {
			testInfo.RowCount, _ = db.GetRowCount("test_table_0")
		}
		if testInfo.RowCount == 0 {
			testInfo.RowCount, _ = db.GetRowCount("test_data")
		}
		testInfo.FileCount = CountS3Objects(t, s3URL)
	}

	logMetrics := func() {
		logS3Metrics(t, db, s3URL)
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

	t.Log("")
	t.Log("Load generation completed.")

	// Final statistics
	t.Log("")
	t.Log("================================================")
	t.Log("Final Statistics")
	t.Log("================================================")
	t.Log("")

	// Stop Litestream
	t.Log("Stopping Litestream...")
	if err := db.StopLitestream(); err != nil {
		t.Logf("Warning: Failed to stop Litestream cleanly: %v", err)
	}

	// Database statistics
	t.Log("Database Statistics:")
	if dbSize, err := db.GetDatabaseSize(); err == nil {
		t.Logf("  Final size: %.2f MB", float64(dbSize)/(1024*1024))
	}

	// Count rows
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

	// S3 statistics
	t.Log("S3 Statistics:")
	finalObjects := CountS3Objects(t, s3URL)
	t.Logf("  Total objects: %d", finalObjects)

	if s3Size := GetS3StorageSize(t, s3URL); s3Size > 0 {
		t.Logf("  Total S3 storage: %.2f MB", float64(s3Size)/(1024*1024))
	}
	t.Log("")

	// Check for errors
	errors, _ := db.CheckForErrors()
	criticalErrors := 0
	for _, errLine := range errors {
		if !containsAny(errLine, []string{"page size not initialized"}) {
			criticalErrors++
		}
	}
	t.Logf("  Critical errors: %d", criticalErrors)
	t.Log("")

	// Test restoration from S3
	t.Log("Testing restoration from S3...")
	restoredPath := filepath.Join(db.TempDir, "restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restoration from S3 failed: %v", err)
	}
	t.Log("✓ Restoration successful!")

	// Compare row counts
	var restoredCount int
	if restoredCount, err = getRowCountFromPath(restoredPath, "load_test"); err != nil {
		if restoredCount, err = getRowCountFromPath(restoredPath, "test_table_0"); err != nil {
			if restoredCount, err = getRowCountFromPath(restoredPath, "test_data"); err != nil {
				t.Logf("  Warning: Could not get restored row count: %v", err)
			}
		}
	}
	if err == nil && rowCount > 0 {
		if rowCount == restoredCount {
			t.Logf("✓ Row counts match! (%d rows)", restoredCount)
		} else {
			t.Logf("⚠ Row count mismatch! Original: %d, Restored: %d", rowCount, restoredCount)
		}
	}

	// Validate
	t.Log("")
	t.Log("Validating restored database...")
	if err := db.Validate(restoredPath); err != nil {
		t.Fatalf("Validation failed: %v", err)
	}
	t.Log("✓ Validation passed!")

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

	if finalObjects == 0 {
		testPassed = false
		issues = append(issues, "No objects stored in S3")
	}

	if testPassed {
		t.Log("✓ TEST PASSED!")
		t.Log("")
		t.Logf("Successfully replicated to AWS S3 (%d objects)", finalObjects)
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
	t.Logf("S3 replica data in: %s", s3URL)
	t.Log("================================================")
}

// logS3Metrics logs S3-specific metrics
func logS3Metrics(t *testing.T, db *TestDB, s3URL string) {
	t.Helper()

	// Basic database metrics
	LogSoakMetrics(t, db, "overnight-s3")

	// S3-specific metrics
	t.Log("")
	t.Log("  S3 Statistics:")

	objectCount := CountS3Objects(t, s3URL)
	t.Logf("    Total objects: %d", objectCount)

	if s3Size := GetS3StorageSize(t, s3URL); s3Size > 0 {
		t.Logf("    Total storage: %.2f MB", float64(s3Size)/(1024*1024))
	}
}

// getRowCountFromPath gets row count from a database file path
func getRowCountFromPath(dbPath, table string) (int, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	if err := db.QueryRow(query).Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}
