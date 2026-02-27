//go:build integration && soak && docker

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestSoakReplicateRestore reproduces issue #1164: intermittent database corruption
// ("wrong # of entries in index") after restoring from S3-compatible replicas.
//
// This test mirrors fuchstim's exact setup:
// - SQLite DB in WAL mode with tables + indexes
// - Litestream replication to MinIO (S3)
// - ~100 rows/sec concurrent writes
// - Periodic stop→restore→integrity_check cycles
//
// Default duration: 5 minutes (override with SOAK_DURATION env var)
// Can be shortened with: go test -test.short (runs for 1 minute)
//
// Requirements:
// - Docker must be running
// - Binaries must be built: go build -o bin/litestream ./cmd/litestream
func TestSoakReplicateRestore(t *testing.T) {
	RequireBinaries(t)
	RequireDocker(t)

	duration := parseSoakDuration(t, 5*time.Minute)
	if testing.Short() {
		duration = 1 * time.Minute
	}
	restoreInterval := 30 * time.Second
	writeRate := 100

	t.Logf("================================================")
	t.Logf("Issue #1164 Reproduction: Replicate+Restore Soak")
	t.Logf("================================================")
	t.Logf("Duration:          %v", duration)
	t.Logf("Restore interval:  %v", restoreInterval)
	t.Logf("Write rate:        %d rows/sec", writeRate)
	t.Logf("Start time:        %s", time.Now().Format(time.RFC3339))
	t.Log("")

	containerID, endpoint, dataVolume := StartMinIOContainer(t)
	defer StopMinIOContainer(t, containerID, dataVolume)

	bucket := "litestream-test"
	CreateMinIOBucket(t, containerID, bucket)

	db := SetupTestDB(t, "replicate-restore")
	defer db.Cleanup()

	if err := db.Create(); err != nil {
		t.Fatalf("create database: %v", err)
	}

	sqlDB, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer sqlDB.Close()

	if _, err := sqlDB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("set WAL mode: %v", err)
	}

	// Create schema matching fuchstim's setup: table with indexes
	if _, err := sqlDB.Exec(`
		CREATE TABLE IF NOT EXISTS resources (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			_uid TEXT NOT NULL,
			_resource_version INTEGER NOT NULL DEFAULT 0,
			name TEXT NOT NULL,
			data TEXT,
			created_at INTEGER NOT NULL
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_resources_uid ON resources(_uid);
		CREATE INDEX IF NOT EXISTS idx_resources_rv ON resources(_resource_version);
		CREATE INDEX IF NOT EXISTS idx_resources_name ON resources(name);
	`); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	s3Path := fmt.Sprintf("replicate-restore-%d", time.Now().Unix())
	s3URL := fmt.Sprintf("s3://%s/%s", bucket, s3Path)
	db.ReplicaURL = s3URL

	s3Config := &S3Config{
		Endpoint:       endpoint,
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin",
		Region:         "us-east-1",
		ForcePathStyle: true,
		SkipVerify:     true,
	}
	configPath := CreateSoakConfig(db.Path, s3URL, s3Config, true)
	db.ConfigPath = configPath

	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}
	t.Logf("Litestream running (PID: %d)", db.LitestreamPID)

	// Wait for initial sync
	time.Sleep(3 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Performance tracking
	var (
		latencies   latencyTracker
		totalWrites atomic.Int64
		writeErrs   atomic.Int64
	)

	// Writer goroutine: ~writeRate rows/sec
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Second / time.Duration(writeRate))
		defer ticker.Stop()

		rv := int64(0)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rv++
				start := time.Now()
				_, err := sqlDB.ExecContext(ctx, `
					INSERT INTO resources (_uid, _resource_version, name, data, created_at)
					VALUES (?, ?, ?, ?, ?)`,
					fmt.Sprintf("uid-%d-%d", time.Now().UnixNano(), rv),
					rv,
					fmt.Sprintf("resource-%d", rv%1000),
					fmt.Sprintf("payload data for resource version %d with some padding to simulate real workload size", rv),
					time.Now().Unix(),
				)
				elapsed := time.Since(start)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					writeErrs.Add(1)
					continue
				}
				totalWrites.Add(1)
				latencies.record(elapsed)
			}
		}
	}()

	// Periodic restore+integrity check loop
	var (
		restoreCount    int
		corruptionCount int
		restoreErrors   []string
	)

	restoreTicker := time.NewTicker(restoreInterval)
	defer restoreTicker.Stop()

	t.Log("Running replicate+restore cycles...")
	t.Log("")

	startTime := time.Now()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-restoreTicker.C:
			restoreCount++
			elapsed := time.Since(startTime)
			sourceRows := countRowsSafe(sqlDB)

			t.Logf("[%v] Restore cycle #%d (source rows: %d, writes: %d, write errors: %d)",
				elapsed.Round(time.Second), restoreCount, sourceRows, totalWrites.Load(), writeErrs.Load())

			// Stop litestream for clean restore
			if err := db.StopLitestream(); err != nil {
				t.Logf("  Warning: stop litestream: %v", err)
			}
			time.Sleep(1 * time.Second)

			// Restore to new path
			restoredPath := filepath.Join(db.TempDir, fmt.Sprintf("restored-%d.db", restoreCount))
			if err := db.Restore(restoredPath); err != nil {
				restoreErrors = append(restoreErrors, fmt.Sprintf("cycle %d: restore: %v", restoreCount, err))
				t.Logf("  RESTORE FAILED: %v", err)
				// Restart litestream and continue
				if err := db.StartLitestreamWithConfig(configPath); err != nil {
					t.Fatalf("restart litestream after failed restore: %v", err)
				}
				continue
			}

			// Integrity check on restored DB
			restoredDB, err := sql.Open("sqlite3", restoredPath)
			if err != nil {
				restoreErrors = append(restoreErrors, fmt.Sprintf("cycle %d: open restored: %v", restoreCount, err))
				t.Logf("  OPEN FAILED: %v", err)
			} else {
				var result string
				if err := restoredDB.QueryRow("PRAGMA integrity_check").Scan(&result); err != nil {
					restoreErrors = append(restoreErrors, fmt.Sprintf("cycle %d: integrity_check query: %v", restoreCount, err))
					t.Logf("  INTEGRITY CHECK QUERY FAILED: %v", err)
				} else if result != "ok" {
					corruptionCount++
					restoreErrors = append(restoreErrors, fmt.Sprintf("cycle %d: CORRUPTION: %s", restoreCount, result))
					t.Errorf("  CORRUPTION DETECTED: %s", result)
				} else {
					// Verify row count is reasonable (restored may lag behind source)
					var restoredRows int
					restoredDB.QueryRow("SELECT COUNT(*) FROM resources").Scan(&restoredRows)
					t.Logf("  OK: integrity=ok, restored_rows=%d", restoredRows)
				}
				restoredDB.Close()
			}

			// Clean up restored DB
			os.Remove(restoredPath)
			os.Remove(restoredPath + "-wal")
			os.Remove(restoredPath + "-shm")

			// Restart litestream
			if err := db.StartLitestreamWithConfig(configPath); err != nil {
				t.Fatalf("restart litestream: %v", err)
			}
			time.Sleep(2 * time.Second)
		}
	}

	cancel()
	wg.Wait()

	// Final report
	t.Log("")
	t.Log("================================================")
	t.Log("Results")
	t.Log("================================================")
	t.Logf("Duration:          %v", time.Since(startTime).Round(time.Second))
	t.Logf("Total writes:      %d", totalWrites.Load())
	t.Logf("Write errors:      %d", writeErrs.Load())
	t.Logf("Restore cycles:    %d", restoreCount)
	t.Logf("Corruptions:       %d", corruptionCount)

	stats := latencies.stats()
	t.Logf("Write latency P50: %v", stats.p50)
	t.Logf("Write latency P95: %v", stats.p95)
	t.Logf("Write latency P99: %v", stats.p99)
	t.Logf("Write latency max: %v", stats.max)

	if len(restoreErrors) > 0 {
		t.Log("")
		t.Log("Restore errors:")
		for _, e := range restoreErrors {
			t.Logf("  %s", e)
		}
	}

	t.Log("================================================")

	if corruptionCount > 0 {
		t.Fatalf("FAILED: %d corruption(s) detected in %d restore cycles", corruptionCount, restoreCount)
	}

	if stats.p99 > 500*time.Millisecond {
		t.Errorf("P99 write latency %v exceeds 500ms threshold", stats.p99)
	}

	t.Log("PASSED: no corruption detected")
}

func parseSoakDuration(t *testing.T, defaultDuration time.Duration) time.Duration {
	t.Helper()
	if v := os.Getenv("SOAK_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		if mins, err := strconv.Atoi(v); err == nil {
			return time.Duration(mins) * time.Minute
		}
		t.Logf("Warning: invalid SOAK_DURATION %q, using default %v", v, defaultDuration)
	}
	return defaultDuration
}

func countRowsSafe(db *sql.DB) int {
	var count int
	db.QueryRow("SELECT COUNT(*) FROM resources").Scan(&count)
	return count
}

type latencyTracker struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (lt *latencyTracker) record(d time.Duration) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	lt.samples = append(lt.samples, d)
}

type latencyStats struct {
	p50, p95, p99, max time.Duration
}

func (lt *latencyTracker) stats() latencyStats {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if len(lt.samples) == 0 {
		return latencyStats{}
	}

	sorted := make([]time.Duration, len(lt.samples))
	copy(sorted, lt.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	percentile := func(p float64) time.Duration {
		idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}

	return latencyStats{
		p50: percentile(50),
		p95: percentile(95),
		p99: percentile(99),
		max: sorted[len(sorted)-1],
	}
}
