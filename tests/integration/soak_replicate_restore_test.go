//go:build integration && soak && docker

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
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

	// An explicit SOAK_DURATION wins over the -short default.
	defaultDuration := 5 * time.Minute
	if testing.Short() {
		defaultDuration = 1 * time.Minute
	}
	duration := parseSoakDuration(t, defaultDuration)
	restoreInterval := 30 * time.Second
	if testing.Short() {
		restoreInterval = 15 * time.Second
	}
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

	configPath := writeReplicateRestoreConfig(t, db.Path, s3URL, endpoint)
	db.ConfigPath = configPath

	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}
	t.Logf("Litestream running (PID: %d)", db.LitestreamPID)

	// Wait until the replica is restorable rather than sleeping a fixed time.
	initialSyncDeadline := time.Now().Add(30 * time.Second)
	for {
		probePath := filepath.Join(db.TempDir, "restore-probe.db")
		err := db.Restore(probePath)
		os.Remove(probePath)
		os.Remove(probePath + "-wal")
		os.Remove(probePath + "-shm")
		if err == nil {
			break
		}
		if time.Now().After(initialSyncDeadline) {
			t.Fatalf("replica not restorable after initial sync window: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Register before cancel so the deferred cancel stops the writer
	// before wg.Wait runs, even on t.Fatalf paths.
	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithTimeout(t.Context(), duration)
	defer cancel()

	// Performance tracking
	var (
		latencies   latencyTracker
		totalWrites atomic.Int64
		writeErrs   atomic.Int64
	)

	// Writer goroutine: ~writeRate rows/sec
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
		restoreCount         int
		corruptionCount      int
		restoreErrors        []string
		lastRestoredRows     int
		prevSourceRows       int
		firstCycleSourceRows int
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
			cycleErrsBefore := len(restoreErrors)
			elapsed := time.Since(startTime)
			sourceRows, err := countRows(sqlDB)
			if err != nil {
				restoreErrors = append(restoreErrors, fmt.Sprintf("cycle %d: count source rows: %v", restoreCount, err))
				t.Errorf("  COUNT SOURCE ROWS FAILED: %v", err)
			}
			if restoreCount == 1 {
				firstCycleSourceRows = sourceRows
			}

			t.Logf("[%v] Restore cycle #%d (source rows: %d, writes: %d, write errors: %d)",
				elapsed.Round(time.Second), restoreCount, sourceRows, totalWrites.Load(), writeErrs.Load())

			// Stop litestream for clean restore. A failed stop usually
			// means the process died mid-soak, which is itself a failure.
			// StopLitestream waits for the process to exit.
			if err := db.StopLitestream(); err != nil {
				restoreErrors = append(restoreErrors, fmt.Sprintf("cycle %d: stop litestream: %v", restoreCount, err))
				t.Errorf("  STOP LITESTREAM FAILED: %v", err)
			}

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
					// The restore may lag behind the source, but row count
					// must never be zero with data written nor shrink
					// between cycles — either means lost data that passes
					// integrity_check.
					var restoredRows int
					if err := restoredDB.QueryRow("SELECT COUNT(*) FROM resources").Scan(&restoredRows); err != nil {
						restoreErrors = append(restoreErrors, fmt.Sprintf("cycle %d: count restored rows: %v", restoreCount, err))
						t.Errorf("  COUNT RESTORED ROWS FAILED: %v", err)
					} else {
						if restoredRows == 0 && sourceRows > 0 {
							restoreErrors = append(restoreErrors, fmt.Sprintf("cycle %d: restored 0 rows, source has %d", restoreCount, sourceRows))
							t.Errorf("  EMPTY RESTORE: source has %d rows", sourceRows)
						}
						if restoredRows < lastRestoredRows {
							restoreErrors = append(restoreErrors, fmt.Sprintf("cycle %d: restored rows shrank %d -> %d", restoreCount, lastRestoredRows, restoredRows))
							t.Errorf("  RESTORED ROWS SHRANK: %d -> %d", lastRestoredRows, restoredRows)
						}
						// The restore must contain at least everything the
						// source held a full cycle ago: bigger lag means
						// silent data loss that still passes integrity_check.
						if restoredRows < prevSourceRows {
							restoreErrors = append(restoreErrors, fmt.Sprintf("cycle %d: restored rows %d behind source count %d from previous cycle", restoreCount, restoredRows, prevSourceRows))
							t.Errorf("  RESTORE LAGGED A FULL CYCLE: restored=%d, source at previous cycle=%d", restoredRows, prevSourceRows)
						}
						lastRestoredRows = restoredRows
						t.Logf("  OK: integrity=ok, restored_rows=%d", restoredRows)
					}
				}
				restoredDB.Close()
			}

			// Clean up restored DB, but keep the files for any cycle that
			// recorded an error or corruption: they live in db.TempDir,
			// which the nightly workflow uploads for post-mortem when
			// SOAK_KEEP_TEMP is set. Healthy restores are removed to bound
			// disk use over long soaks.
			if len(restoreErrors) > cycleErrsBefore {
				t.Logf("  keeping restored DB for post-mortem: %s", restoredPath)
			} else {
				os.Remove(restoredPath)
				os.Remove(restoredPath + "-wal")
				os.Remove(restoredPath + "-shm")
			}

			// Restart litestream. The helper waits for startup internally.
			if err := db.StartLitestreamWithConfig(configPath); err != nil {
				t.Fatalf("restart litestream: %v", err)
			}
			prevSourceRows = sourceRows
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

	// The test exists to prove restores work; a failed restore, open, or
	// verification is a failure even when no corruption was detected.
	if len(restoreErrors) > 0 {
		t.Fatalf("FAILED: %d restore error(s) in %d restore cycles", len(restoreErrors), restoreCount)
	}

	if restoreCount == 0 {
		t.Fatalf("FAILED: no restore cycles ran; duration %v must exceed the %v restore interval", duration, restoreInterval)
	}

	if totalWrites.Load() == 0 {
		t.Fatal("FAILED: no writes succeeded; soak applied no load")
	}

	// Write-path liveness: a writer that dies mid-soak leaves counts flat,
	// so the shrink/lag checks above pass trivially.
	if attempts := totalWrites.Load() + writeErrs.Load(); writeErrs.Load()*100 > attempts {
		t.Fatalf("FAILED: %d write error(s) in %d attempts exceeds 1%% threshold", writeErrs.Load(), attempts)
	}

	finalSourceRows, err := countRows(sqlDB)
	if err != nil {
		t.Fatalf("count final source rows: %v", err)
	}
	if finalSourceRows <= firstCycleSourceRows {
		t.Fatalf("FAILED: source rows did not grow after first restore cycle (first=%d, final=%d); write path died mid-soak", firstCycleSourceRows, finalSourceRows)
	}

	if threshold := parseSoakP99Threshold(t, 500*time.Millisecond); stats.p99 > threshold {
		t.Errorf("P99 write latency %v exceeds %v threshold", stats.p99, threshold)
	}

	t.Log("PASSED: no corruption detected")
}

func parseSoakP99Threshold(t *testing.T, defaultThreshold time.Duration) time.Duration {
	t.Helper()
	if v := os.Getenv("SOAK_P99_THRESHOLD"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("invalid SOAK_P99_THRESHOLD %q: %v", v, err)
		}
		return d
	}
	return defaultThreshold
}

func writeReplicateRestoreConfig(t *testing.T, dbPath, s3URL, endpoint string) string {
	t.Helper()
	configPath := filepath.Join(filepath.Dir(dbPath), "litestream.yml")
	config := fmt.Sprintf(`access-key-id: minioadmin
secret-access-key: minioadmin

dbs:
  - path: %s
    checkpoint-interval: 1m
    min-checkpoint-page-count: 100
    replicas:
      - url: %s
        endpoint: %s
        region: us-east-1
        force-path-style: true
        skip-verify: true
        sync-interval: 1s
        snapshot-interval: 30s
`, filepath.ToSlash(dbPath), s3URL, endpoint)
	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return configPath
}

// parseSoakDuration reads SOAK_DURATION; unlike GetTestDuration, an explicit
// env value wins over the -short default so CI can pin exact soak lengths.
func parseSoakDuration(t *testing.T, defaultDuration time.Duration) time.Duration {
	t.Helper()
	if v := os.Getenv("SOAK_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		if mins, err := strconv.Atoi(v); err == nil {
			return time.Duration(mins) * time.Minute
		}
		t.Fatalf("invalid SOAK_DURATION %q: must be a duration (e.g. 30m) or whole minutes", v)
	}
	return defaultDuration
}

func countRows(db *sql.DB) (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM resources").Scan(&count)
	return count, err
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

	sorted := slices.Clone(lt.samples)
	slices.Sort(sorted)

	percentile := func(p float64) time.Duration {
		idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
		return sorted[max(0, min(idx, len(sorted)-1))]
	}

	return latencyStats{
		p50: percentile(50),
		p95: percentile(95),
		p99: percentile(99),
		max: sorted[len(sorted)-1],
	}
}
