//go:build integration && soak && docker

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestMinIOSoak runs a soak test against local MinIO S3-compatible server using Docker.
//
// Default duration: 2 hours
// Can be shortened with: go test -test.short (runs for 30 minutes)
//
// Requirements:
// - Docker must be running
// - docker command must be in PATH
//
// This test validates:
// - S3-compatible replication to MinIO
// - Docker container lifecycle management
// - Heavy sustained load (500 writes/sec)
// - Restoration from S3-compatible storage
func TestMinIOSoak(t *testing.T) {
	RequireBinaries(t)
	RequireDocker(t)

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
	t.Logf("Litestream MinIO S3 Soak Test")
	t.Logf("================================================")
	t.Logf("Duration: %v", duration)
	t.Logf("Start time: %s", time.Now().Format(time.RFC3339))
	t.Log("")

	startTime := time.Now()

	// Start MinIO container
	t.Log("Starting MinIO container...")
	containerID, endpoint, dataVolume := StartMinIOContainer(t)
	defer StopMinIOContainer(t, containerID, dataVolume)
	t.Logf("✓ MinIO running at: %s", endpoint)
	t.Log("")

	// Create MinIO bucket
	bucket := "litestream-test"
	CreateMinIOBucket(t, containerID, bucket)
	t.Log("")

	// Setup test database
	db := SetupTestDB(t, "minio-soak")
	defer db.Cleanup()

	// Create database
	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Populate with initial data
	t.Logf("Populating database (%s initial data)...", targetSize)
	if err := db.Populate(targetSize); err != nil {
		t.Fatalf("Failed to populate database: %v", err)
	}
	t.Log("✓ Database populated")
	t.Log("")

	// Create S3 configuration for MinIO
	s3Path := fmt.Sprintf("litestream-test-%d", time.Now().Unix())
	s3URL := fmt.Sprintf("s3://%s/%s", bucket, s3Path)
	db.ReplicaURL = s3URL
	t.Log("Creating Litestream configuration for MinIO S3...")
	s3Config := &S3Config{
		Endpoint:       endpoint,
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin",
		Region:         "us-east-1",
		ForcePathStyle: true,
		SkipVerify:     true,
	}
	configPath := CreateSoakConfig(db.Path, s3URL, s3Config, shortMode)
	db.ConfigPath = configPath
	t.Logf("✓ Configuration created: %s", configPath)
	t.Logf("  S3 URL: %s", s3URL)
	t.Log("")

	// Start Litestream
	t.Log("Starting Litestream with MinIO backend...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}
	t.Logf("✓ Litestream running (PID: %d)", db.LitestreamPID)
	t.Log("")

	// Start load generator
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

	// Monitor every 60 seconds with MinIO-specific metrics
	t.Log("Running MinIO S3 test...")
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
		testInfo.FileCount = CountMinIOObjects(t, containerID, bucket)
	}

	logMetrics := func() {
		logMinIOMetrics(t, db, containerID, bucket)
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

	// MinIO statistics
	t.Log("MinIO S3 Statistics:")
	finalObjects := CountMinIOObjects(t, containerID, bucket)
	t.Logf("  Total objects in MinIO: %d", finalObjects)
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

	// Test restoration from MinIO
	t.Log("Testing restoration from MinIO S3...")
	restoredPath := filepath.Join(db.TempDir, "restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restoration from MinIO failed: %v", err)
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

	// Validate integrity
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

	if finalObjects == 0 {
		testPassed = false
		issues = append(issues, "No objects stored in MinIO")
	}

	if testPassed {
		t.Log("✓ TEST PASSED!")
		t.Log("")
		t.Logf("Successfully replicated to MinIO (%d objects)", finalObjects)
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

// logMinIOMetrics logs MinIO-specific metrics
func logMinIOMetrics(t *testing.T, db *TestDB, containerID, bucket string) {
	t.Helper()

	// Basic database metrics
	LogSoakMetrics(t, db, "minio")

	// MinIO-specific metrics
	t.Log("")
	t.Log("  MinIO S3 Statistics:")

	objectCount := CountMinIOObjects(t, containerID, bucket)
	t.Logf("    Total objects: %d", objectCount)

	// Count LTX files specifically
	ltxCount := countMinIOLTXFiles(t, containerID, bucket)
	t.Logf("    LTX segments: %d", ltxCount)
}

// countMinIOLTXFiles counts LTX files in MinIO bucket
func countMinIOLTXFiles(t *testing.T, containerID, bucket string) int {
	t.Helper()

	cmd := exec.Command("docker", "run", "--rm",
		"--link", containerID+":minio",
		"-e", "MC_HOST_minio=http://minioadmin:minioadmin@minio:9000",
		"minio/mc", "ls", "minio/"+bucket+"/", "--recursive")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	ltxCount := 0
	for _, line := range lines {
		if strings.Contains(line, ".ltx") {
			ltxCount++
		}
	}

	return ltxCount
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
