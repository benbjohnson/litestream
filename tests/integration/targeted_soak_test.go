//go:build integration && soak

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestRapidCheckpointSoak(t *testing.T) {
	RequireBinaries(t)

	shortMode := testing.Short()
	duration := 30 * time.Minute
	initialSize := "20MB"
	writeRate := 150
	workers := 4
	checkpointInterval := 15 * time.Second
	if shortMode {
		duration = 2 * time.Minute
		initialSize = "5MB"
		writeRate = 75
		workers = 2
		checkpointInterval = 5 * time.Second
	}

	db, configPath := setupLocalSoakDB(t, "rapid-checkpoint-soak", initialSize, shortMode)

	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	testInfo := &TestInfo{
		StartTime: time.Now(),
		Duration:  duration,
		DB:        db,
		cancel:    cancel,
	}
	setupSignalHandler(t, cancel, testInfo)

	loadDone := make(chan error, 1)
	go func() {
		loadDone <- db.GenerateLoadWithOptions(ctx, writeRate, duration, "wave", workers, 4096)
	}()

	var attemptedCheckpoints atomic.Int64
	var progressedCheckpoints atomic.Int64
	checkpointDone := make(chan error, 1)
	go func() {
		checkpointDone <- runCheckpointLoop(ctx, db.Path, checkpointInterval, &attemptedCheckpoints, &progressedCheckpoints)
	}()

	monitorFileSoak(t, db, ctx, testInfo, "rapid-checkpoint-soak", func() {
		t.Logf("  External checkpoints: %d attempted, %d progressed", attemptedCheckpoints.Load(), progressedCheckpoints.Load())
	})

	if err := <-loadDone; err != nil && ctx.Err() == nil {
		t.Fatalf("load generation failed: %v", err)
	}
	if err := <-checkpointDone; err != nil {
		t.Fatalf("checkpoint loop failed: %v", err)
	}

	waitForFinalFlush(shortMode)

	if err := db.StopLitestream(); err != nil {
		t.Fatalf("stop litestream: %v", err)
	}

	if attemptedCheckpoints.Load() == 0 {
		t.Fatal("no external checkpoints were attempted during soak")
	}

	errStats := getErrorStats(db)
	if errStats.CriticalCount > 0 {
		t.Fatalf("critical errors detected: %d", errStats.CriticalCount)
	}

	events, err := ParseLTXEvents(db.LitestreamLogPath())
	if err != nil {
		t.Fatalf("parse LTX events: %v", err)
	}
	report := BuildBehaviorReport(events, time.Since(testInfo.StartTime))
	PrintBehaviorReport(t, report)

	restoredPath := filepath.Join(db.TempDir, "rapid-checkpoint-restored.db")
	verifyRestoredLoadTest(t, db, restoredPath)
	PrintSoakTestAnalysis(t, AnalyzeSoakTest(t, db, duration))
}

func TestRestartRecoverySoak(t *testing.T) {
	RequireBinaries(t)

	shortMode := testing.Short()
	duration := 30 * time.Minute
	initialSize := "20MB"
	writeRate := 200
	workers := 4
	restartInterval := 3 * time.Minute
	if shortMode {
		duration = 2 * time.Minute
		initialSize = "5MB"
		writeRate = 100
		workers = 2
		restartInterval = 20 * time.Second
	}

	db, configPath := setupLocalSoakDB(t, "restart-recovery-soak", initialSize, shortMode)

	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	testInfo := &TestInfo{
		StartTime: time.Now(),
		Duration:  duration,
		DB:        db,
		cancel:    cancel,
	}
	setupSignalHandler(t, cancel, testInfo)

	loadDone := make(chan error, 1)
	go func() {
		loadDone <- db.GenerateLoadWithOptions(ctx, writeRate, duration, "wave", workers, 4096)
	}()

	var restartCount atomic.Int64
	restartDone := make(chan error, 1)
	go func() {
		restartDone <- runRestartLoop(ctx, db, configPath, restartInterval, &restartCount)
	}()

	monitorFileSoak(t, db, ctx, testInfo, "restart-recovery-soak", func() {
		t.Logf("  Litestream restarts: %d", restartCount.Load())
	})

	if err := <-loadDone; err != nil && ctx.Err() == nil {
		t.Fatalf("load generation failed: %v", err)
	}
	if err := <-restartDone; err != nil {
		t.Fatalf("restart loop failed: %v", err)
	}

	waitForFinalFlush(shortMode)

	if err := db.StopLitestream(); err != nil {
		t.Fatalf("stop litestream: %v", err)
	}

	if restartCount.Load() == 0 {
		t.Fatal("no Litestream restarts completed during soak")
	}

	errStats := getErrorStats(db)
	if errStats.CriticalCount > 0 {
		t.Fatalf("critical errors detected: %d", errStats.CriticalCount)
	}

	restoredPath := filepath.Join(db.TempDir, "restart-recovery-restored.db")
	verifyRestoredLoadTest(t, db, restoredPath)
	PrintSoakTestAnalysis(t, AnalyzeSoakTest(t, db, duration))
}

func setupLocalSoakDB(t *testing.T, name, initialSize string, shortMode bool) (*TestDB, string) {
	t.Helper()

	db := SetupTestDB(t, name)
	t.Cleanup(db.Cleanup)

	if err := db.Create(); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := db.Populate(initialSize); err != nil {
		t.Fatalf("populate database: %v", err)
	}

	replicaURL := fmt.Sprintf("file://%s", filepath.ToSlash(db.ReplicaPath))
	configPath := CreateSoakConfig(db.Path, replicaURL, nil, shortMode)
	db.ConfigPath = configPath
	return db, configPath
}

func monitorFileSoak(t *testing.T, db *TestDB, ctx context.Context, testInfo *TestInfo, testName string, extraLog func()) {
	t.Helper()

	refreshStats := func() {
		testInfo.RowCount, _ = db.GetRowCount("load_test")
		testInfo.FileCount, _ = db.GetReplicaFileCount()
	}

	logMetrics := func() {
		LogSoakMetrics(t, db, testName)
		if extraLog != nil {
			extraLog()
		}
	}

	MonitorSoakTest(t, db, ctx, testInfo, refreshStats, logMetrics)
}

func runCheckpointLoop(ctx context.Context, dbPath string, interval time.Duration, attempted, progressed *atomic.Int64) error {
	sqlDB, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=1000")
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			attempted.Add(1)

			var busy, logFrameN, checkpointedFrameN int
			if err := sqlDB.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&busy, &logFrameN, &checkpointedFrameN); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			if busy == 0 && checkpointedFrameN > 0 {
				progressed.Add(1)
			}
		}
	}
}

func runRestartLoop(ctx context.Context, db *TestDB, configPath string, restartInterval time.Duration, restartCount *atomic.Int64) error {
	ticker := time.NewTicker(restartInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := db.RestartLitestreamWithConfig(configPath); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			restartCount.Add(1)
		}
	}
}

func waitForFinalFlush(shortMode bool) {
	if shortMode {
		time.Sleep(20 * time.Second)
		return
	}
	time.Sleep(45 * time.Second)
}

func verifyRestoredLoadTest(t *testing.T, db *TestDB, restoredPath string) {
	t.Helper()

	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	sourceCount, err := db.GetRowCount("load_test")
	if err != nil {
		t.Fatalf("source row count: %v", err)
	}
	restoredCount, err := getRowCountFromPath(restoredPath, "load_test")
	if err != nil {
		t.Fatalf("restored row count: %v", err)
	}

	if sourceCount != restoredCount {
		t.Fatalf("row count mismatch: source=%d restored=%d", sourceCount, restoredCount)
	}

	restoredDB := &TestDB{Path: restoredPath, t: t}
	if err := restoredDB.IntegrityCheck(); err != nil {
		t.Fatalf("integrity check failed: %v", err)
	}
}
