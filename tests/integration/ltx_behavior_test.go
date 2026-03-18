//go:build integration && soak

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestLTXBehavior runs behavioral assertions against LTX output across all load profiles.
// This is the core test Ben asked for: verifying that LTX files "look right" —
// right size, right frequency, no excessive snapshots.
//
// Default duration: 30 minutes per profile (90 minutes total)
// Short mode: 2 minutes per profile (6 minutes total)
//
// Run: go test -tags 'integration,soak' -run TestLTXBehavior -v ./tests/integration/
// Short: go test -tags 'integration,soak' -run TestLTXBehavior -short -v ./tests/integration/
func TestLTXBehavior(t *testing.T) {
	RequireBinaries(t)

	shortMode := testing.Short()
	profiles := DefaultLoadProfiles(shortMode)
	duration := ProfileDuration(shortMode)
	snapshotInterval := ProfileSnapshotInterval(shortMode)
	compactionIntervals := ProfileCompactionIntervals(shortMode)

	t.Logf("================================================")
	t.Logf("LTX Behavioral Test Suite")
	t.Logf("================================================")
	t.Logf("Mode: %s", modeString(shortMode))
	t.Logf("Duration per profile: %v", duration)
	t.Logf("Profiles: %d", len(profiles))
	t.Logf("Snapshot interval: %v", snapshotInterval)
	t.Log("")

	for _, profile := range profiles {
		t.Run(profile.Name, func(t *testing.T) {
			runProfileBehaviorTest(t, profile, duration, snapshotInterval, compactionIntervals, shortMode)
		})
	}
}

// TestLTXBehavior_NoExcessiveSnapshots is a focused test for the specific bug
// where checkpoints trigger unwanted full snapshots. It runs at moderate write
// rate and forces checkpoints via WAL growth, then asserts no unexpected snapshots.
//
// Run: go test -tags 'integration,soak' -run TestLTXBehavior_NoExcessiveSnapshots -v ./tests/integration/
func TestLTXBehavior_NoExcessiveSnapshots(t *testing.T) {
	RequireBinaries(t)

	shortMode := testing.Short()
	duration := 5 * time.Minute
	if shortMode {
		duration = 2 * time.Minute
	}

	// Use the same snapshot interval as CreateSoakConfig to keep assertions aligned
	snapshotInterval := ProfileSnapshotInterval(shortMode)

	profile := LoadProfile{
		Name:            "checkpoint-snapshot-regression",
		Description:     "Moderate writes with low checkpoint threshold to trigger frequent checkpoints",
		WriteRate:       50,
		Pattern:         "constant",
		PayloadSize:     4096,
		Workers:         2,
		MaxL0Pages:      100,
		MaxWALSizeMB:    200,
		SnapshotWindow:  2.0,
		CompactionSlack: 0.5,
		InitialSize:     "5MB",
	}

	compactionIntervals := ProfileCompactionIntervals(shortMode)

	t.Logf("================================================")
	t.Logf("Snapshot-on-Checkpoint Regression Test")
	t.Logf("================================================")
	t.Logf("Duration: %v", duration)
	t.Logf("Snapshot interval: %v (should see 0-1 snapshots)", snapshotInterval)
	t.Logf("Write rate: %d writes/sec (moderate)", profile.WriteRate)
	t.Log("")

	runProfileBehaviorTest(t, profile, duration, snapshotInterval, compactionIntervals, shortMode)
}

func runProfileBehaviorTest(t *testing.T, profile LoadProfile, duration, snapshotInterval time.Duration, compactionIntervals map[int]time.Duration, shortMode bool) {
	t.Helper()

	t.Logf("--- Profile: %s ---", profile.Name)
	t.Logf("  %s", profile.Description)
	t.Logf("  Write rate: %d/sec, Pattern: %s, Workers: %d", profile.WriteRate, profile.Pattern, profile.Workers)
	t.Log("")

	startTime := time.Now()

	// Setup test database
	db := SetupTestDB(t, fmt.Sprintf("ltx-behavior-%s", profile.Name))
	defer db.Cleanup()

	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Populate with initial data
	t.Logf("Populating database (%s)...", profile.InitialSize)
	if err := db.Populate(profile.InitialSize); err != nil {
		t.Fatalf("Failed to populate database: %v", err)
	}

	// Create configuration with test intervals
	replicaURL := fmt.Sprintf("file://%s", filepath.ToSlash(db.ReplicaPath))
	configPath := CreateSoakConfig(db.Path, replicaURL, nil, shortMode)
	db.ConfigPath = configPath

	// Start Litestream (LOG_LEVEL=DEBUG is set automatically by StartLitestreamWithConfig)
	t.Log("Starting Litestream...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}

	// Run load generation with profile-specific workers and payload size
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	testInfo := &TestInfo{
		StartTime: startTime,
		Duration:  duration,
		DB:        db,
		cancel:    cancel,
	}
	setupSignalHandler(t, cancel, testInfo)

	loadDone := make(chan error, 1)
	go func() {
		loadDone <- db.GenerateLoadWithOptions(ctx, profile.WriteRate, duration, profile.Pattern, profile.Workers, profile.PayloadSize)
	}()

	// Track peak WAL size during monitoring
	walPath := db.Path + "-wal"
	var peakWALSize int64
	var walMu sync.Mutex

	refreshStats := func() {
		testInfo.RowCount, _ = db.GetRowCount("load_test")
		if testInfo.RowCount == 0 {
			testInfo.RowCount, _ = db.GetRowCount("test_table_0")
		}
		if testInfo.RowCount == 0 {
			testInfo.RowCount, _ = db.GetRowCount("test_data")
		}
		testInfo.FileCount, _ = db.GetReplicaFileCount()

		// Sample WAL size for peak tracking
		if info, err := os.Stat(walPath); err == nil {
			walMu.Lock()
			if info.Size() > peakWALSize {
				peakWALSize = info.Size()
			}
			walMu.Unlock()
		}
	}

	logMetrics := func() {
		LogSoakMetrics(t, db, profile.Name)
	}

	// Monitor load gen errors concurrently — cancel the context if the
	// load generator exits early so MonitorSoakTest stops immediately
	// instead of waiting for the full duration. This prevents masking
	// early crashes as expected context cancellations.
	var loadErr error
	var loadCtxDone bool
	loadErrCh := make(chan struct{})
	go func() {
		loadErr = <-loadDone
		loadCtxDone = ctx.Err() != nil
		if loadErr != nil && !loadCtxDone {
			t.Logf("Load generator exited early with error, cancelling test: %v", loadErr)
			cancel()
		}
		close(loadErrCh)
	}()

	MonitorSoakTest(t, db, ctx, testInfo, refreshStats, logMetrics)

	<-loadErrCh
	if loadErr != nil {
		if loadCtxDone {
			t.Logf("Load generation stopped (context done): %v", loadErr)
		} else {
			t.Fatalf("Load generation failed unexpectedly: %v", loadErr)
		}
	}

	// Give Litestream time to flush final syncs and run gap recovery.
	// Under high write load, the last checkpoints can create TOCTOU gaps
	// that need one more sync + compaction cycle to heal.
	t.Log("Waiting for final sync/compaction cycle...")
	time.Sleep(45 * time.Second)

	// Stop Litestream
	t.Log("Stopping Litestream...")
	if err := db.StopLitestream(); err != nil {
		t.Logf("Warning: %v", err)
	}

	actualDuration := time.Since(startTime)

	// Parse events and build behavioral report
	t.Log("")
	t.Log("Analyzing LTX behavior...")

	logContent, err := db.GetLitestreamLog()
	if err != nil {
		t.Fatalf("Failed to read Litestream log: %v", err)
	}

	// Write log to file for parsing
	logPath := filepath.Join(db.TempDir, "litestream.log")
	events, err := ParseLTXEvents(logPath)
	if err != nil {
		t.Fatalf("Failed to parse LTX events: %v", err)
	}

	if len(events) == 0 && logContent != "" {
		t.Log("Warning: log file exists but no events parsed — check log format compatibility")
	}

	report := BuildBehaviorReport(events, actualDuration)
	PrintBehaviorReport(t, report)

	// Run behavioral assertions
	t.Log("")
	t.Log("================================================")
	t.Logf("Behavioral Assertions: %s", profile.Name)
	t.Log("================================================")

	// 1. Snapshot cadence — use profile's snapshot window for tolerance
	snapshotTolerance := time.Duration(float64(snapshotInterval) * (1 - 1/profile.SnapshotWindow))
	AssertSnapshotCadence(t, report, snapshotInterval, snapshotTolerance)

	// 2. No excessive snapshots
	AssertNoExcessiveSnapshots(t, report, snapshotInterval)

	// 3. L0 file page counts should be small (not snapshot-sized)
	pageSize := 4096 // default SQLite page size
	AssertL0PageCount(t, report, pageSize, profile.MaxL0Pages, db.ReplicaPath)

	// 4. Compaction timing should match configured intervals
	AssertCompactionTiming(t, report, compactionIntervals, profile.CompactionSlack)

	// 5. WAL should stay bounded (check both current and peak size)
	walMu.Lock()
	peak := peakWALSize
	walMu.Unlock()
	AssertWALBounded(t, walPath, profile.MaxWALSizeMB, peak)

	// 6. No snapshot-on-checkpoint bug
	AssertNoSnapshotOnCheckpoint(t, report)

	// 7. Ensure at least one checkpoint occurred so assertion 6 is non-vacuous
	if len(report.CheckpointTimes) == 0 {
		t.Errorf("  [checkpoint-observed] FAIL: no checkpoints occurred during test — snapshot-on-checkpoint assertion is vacuous")
	} else {
		t.Logf("  [checkpoint-observed] PASS: %d checkpoints observed", len(report.CheckpointTimes))
	}

	// Verify restoration
	t.Log("")
	t.Log("Testing restoration...")
	restoredPath := filepath.Join(db.TempDir, "restored.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("Restoration failed: %v", err)
	}
	t.Log("  Restoration successful")

	restoredDB := &TestDB{Path: restoredPath, t: t}
	if err := restoredDB.IntegrityCheck(); err != nil {
		t.Fatalf("Integrity check failed: %v", err)
	}
	t.Log("  Integrity check passed")

	t.Log("")
	t.Logf("Profile %s completed in %v", profile.Name, actualDuration.Round(time.Second))
	t.Log("================================================")
}

func modeString(shortMode bool) string {
	if shortMode {
		return "short (CI gate)"
	}
	return "full (nightly)"
}
