//go:build profile

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"

	_ "modernc.org/sqlite"
)

// TestIdleCPUProfile starts N databases with file-based replicas and no writes,
// then exposes pprof and fgprof endpoints for interactive profiling.
//
// This test is designed for manual CPU profiling to understand idle overhead
// when running many Litestream instances on a single machine.
//
// Usage:
//
//	# Start with 100 idle databases on default port:
//	PROFILE_DB_COUNT=100 go test -tags=profile -run TestIdleCPUProfile -timeout=0 -v ./tests/integration/
//
//	# Custom listen address:
//	PROFILE_ADDR=:9090 PROFILE_DB_COUNT=50 go test -tags=profile -run TestIdleCPUProfile -timeout=0 -v ./tests/integration/
//
//	# Then in another terminal:
//	go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
//	go tool pprof http://localhost:6060/debug/pprof/goroutine
//	go tool pprof http://localhost:6060/debug/pprof/heap
func TestIdleCPUProfile(t *testing.T) {
	dbCount := 10
	if s := os.Getenv("PROFILE_DB_COUNT"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("invalid PROFILE_DB_COUNT: %v", err)
		}
		dbCount = n
	}

	addr := ":6060"
	if s := os.Getenv("PROFILE_ADDR"); s != "" {
		addr = s
	}

	// Start HTTP server for profiling (net/http/pprof registered via blank import).
	go func() {
		log.Printf("pprof server listening on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("pprof server error: %v", err)
		}
	}()

	// Create temporary root directory for all databases.
	rootDir := t.TempDir()

	// Start N databases with monitoring enabled (the idle hot path).
	type instance struct {
		db    *litestream.DB
		sqldb *sql.DB
	}
	instances := make([]instance, 0, dbCount)

	for i := range dbCount {
		dbPath := filepath.Join(rootDir, fmt.Sprintf("db%d", i), "db")
		replicaDir := filepath.Join(rootDir, fmt.Sprintf("db%d", i), "replica")

		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		// Create database with WAL mode and seed data.
		sqldb, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("open sql db %d: %v", i, err)
		}
		if _, err := sqldb.Exec(`PRAGMA journal_mode = wal`); err != nil {
			t.Fatalf("set wal mode db %d: %v", i, err)
		}
		if _, err := sqldb.Exec(`CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)`); err != nil {
			t.Fatalf("create table db %d: %v", i, err)
		}
		if _, err := sqldb.Exec(`INSERT INTO data (value) VALUES ('seed')`); err != nil {
			t.Fatalf("insert seed db %d: %v", i, err)
		}

		// Configure Litestream DB with monitoring enabled.
		db := litestream.NewDB(dbPath)
		db.Replica = litestream.NewReplica(db)
		db.Replica.Client = file.NewReplicaClient(replicaDir)

		if err := db.Open(); err != nil {
			t.Fatalf("open litestream db %d: %v", i, err)
		}

		// Do an initial sync so there's a valid LTX baseline.
		if err := db.Sync(context.Background()); err != nil {
			t.Fatalf("initial sync db %d: %v", i, err)
		}

		instances = append(instances, instance{db: db, sqldb: sqldb})
	}

	t.Logf("started %d idle databases with monitoring (interval=%s)", dbCount, litestream.DefaultMonitorInterval)
	t.Logf("")
	t.Logf("profiling endpoints:")
	t.Logf("  CPU (on-cpu):     go tool pprof http://localhost%s/debug/pprof/profile?seconds=30", addr)
	t.Logf("  goroutines:       go tool pprof http://localhost%s/debug/pprof/goroutine", addr)
	t.Logf("  heap:             go tool pprof http://localhost%s/debug/pprof/heap", addr)
	t.Logf("")
	t.Logf("press Ctrl+C to stop")

	// Block until interrupted.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()

	t.Logf("shutting down %d databases...", dbCount)
	for i, inst := range instances {
		if err := inst.db.Close(context.Background()); err != nil {
			t.Logf("close litestream db %d: %v", i, err)
		}
		if err := inst.sqldb.Close(); err != nil {
			t.Logf("close sql db %d: %v", i, err)
		}
	}
}

// MetricsSnapshot captures runtime metrics at a point in time.
type MetricsSnapshot struct {
	Timestamp       time.Time `json:"timestamp"`
	ElapsedSec      float64   `json:"elapsed_sec"`
	GoroutineCount  int       `json:"goroutine_count"`
	HeapAllocBytes  uint64    `json:"heap_alloc_bytes"`
	HeapObjects     uint64    `json:"heap_objects"`
	SysBytes        uint64    `json:"sys_bytes"`
	NumGC           uint32    `json:"num_gc"`
	PauseTotalNs    uint64    `json:"pause_total_ns"`
	HeapInuseBytes  uint64    `json:"heap_inuse_bytes"`
	StackInuseBytes uint64    `json:"stack_inuse_bytes"`
}

// ProfileReport holds all metrics collected during a profile run.
type ProfileReport struct {
	DBCount   int               `json:"db_count"`
	Duration  string            `json:"duration"`
	StartTime time.Time         `json:"start_time"`
	EndTime   time.Time         `json:"end_time"`
	Snapshots []MetricsSnapshot `json:"snapshots"`
}

func collectMetricsSnapshot(start time.Time) MetricsSnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return MetricsSnapshot{
		Timestamp:       time.Now(),
		ElapsedSec:      time.Since(start).Seconds(),
		GoroutineCount:  runtime.NumGoroutine(),
		HeapAllocBytes:  m.HeapAlloc,
		HeapObjects:     m.HeapObjects,
		SysBytes:        m.Sys,
		NumGC:           m.NumGC,
		PauseTotalNs:    m.PauseTotalNs,
		HeapInuseBytes:  m.HeapInuse,
		StackInuseBytes: m.StackInuse,
	}
}

// TestIdleProfileSuite runs automated profiling with metrics collection.
// Unlike TestIdleCPUProfile (interactive), this test runs for a fixed duration,
// collects profiles and metrics automatically, and saves them for comparison.
//
// Environment variables:
//
//	PROFILE_DB_COUNT    - number of idle databases (default: 100)
//	PROFILE_DURATION    - profiling duration after warmup (default: 60s)
//	PROFILE_OUTPUT_DIR  - output directory for profiles (default: ./profiles)
//	PROFILE_WARMUP      - warmup period before profiling (default: 10s)
//	PROFILE_TRACE       - set to "1" to capture runtime/trace (default: off)
//	PROFILE_CPU_SECS    - CPU profile duration in seconds (default: 30)
//
// Usage:
//
//	PROFILE_DB_COUNT=400 PROFILE_DURATION=60s PROFILE_OUTPUT_DIR=./profiles/pr1211 \
//	  go test -tags=profile -run TestIdleProfileSuite -timeout=5m -v ./tests/integration/
func TestIdleProfileSuite(t *testing.T) {
	// Parse configuration from environment.
	dbCount := envInt(t, "PROFILE_DB_COUNT", 100)
	duration := envDuration(t, "PROFILE_DURATION", 60*time.Second)
	outputDir := envString("PROFILE_OUTPUT_DIR", "./profiles")
	warmup := envDuration(t, "PROFILE_WARMUP", 10*time.Second)
	enableTrace := os.Getenv("PROFILE_TRACE") == "1"
	cpuSecs := envInt(t, "PROFILE_CPU_SECS", 30)

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("create output dir: %v", err)
	}

	t.Logf("=== Idle Profile Suite ===")
	t.Logf("  databases:  %d", dbCount)
	t.Logf("  warmup:     %s", warmup)
	t.Logf("  duration:   %s", duration)
	t.Logf("  cpu_secs:   %d", cpuSecs)
	t.Logf("  trace:      %v", enableTrace)
	t.Logf("  output:     %s", outputDir)
	t.Logf("")

	// Create temporary root directory for all databases.
	rootDir := t.TempDir()

	// Start N databases with monitoring enabled.
	type instance struct {
		db    *litestream.DB
		sqldb *sql.DB
	}
	instances := make([]instance, 0, dbCount)

	t.Logf("creating %d databases...", dbCount)
	createStart := time.Now()
	for i := range dbCount {
		dbPath := filepath.Join(rootDir, fmt.Sprintf("db%04d", i), "db")
		replicaDir := filepath.Join(rootDir, fmt.Sprintf("db%04d", i), "replica")

		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		sqldb, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("open sql db %d: %v", i, err)
		}
		if _, err := sqldb.Exec(`PRAGMA journal_mode = wal`); err != nil {
			t.Fatalf("set wal mode db %d: %v", i, err)
		}
		if _, err := sqldb.Exec(`CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)`); err != nil {
			t.Fatalf("create table db %d: %v", i, err)
		}
		if _, err := sqldb.Exec(`INSERT INTO data (value) VALUES ('seed')`); err != nil {
			t.Fatalf("insert seed db %d: %v", i, err)
		}

		db := litestream.NewDB(dbPath)
		db.Replica = litestream.NewReplica(db)
		db.Replica.Client = file.NewReplicaClient(replicaDir)

		if err := db.Open(); err != nil {
			t.Fatalf("open litestream db %d: %v", i, err)
		}

		if err := db.Sync(context.Background()); err != nil {
			t.Fatalf("initial sync db %d: %v", i, err)
		}

		instances = append(instances, instance{db: db, sqldb: sqldb})
	}
	t.Logf("created %d databases in %s", dbCount, time.Since(createStart).Round(time.Millisecond))

	// Warmup: let monitors settle into steady state.
	t.Logf("warming up for %s...", warmup)
	time.Sleep(warmup)

	// Collect metrics during the profiling window.
	t.Logf("profiling for %s...", duration)
	profileStart := time.Now()
	report := ProfileReport{
		DBCount:   dbCount,
		Duration:  duration.String(),
		StartTime: profileStart,
	}

	// Start CPU profile.
	cpuFile, err := os.Create(filepath.Join(outputDir, "cpu.pprof"))
	if err != nil {
		t.Fatalf("create cpu profile: %v", err)
	}
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		t.Fatalf("start cpu profile: %v", err)
	}

	// Start runtime/trace if enabled.
	var traceFile *os.File
	if enableTrace {
		traceFile, err = os.Create(filepath.Join(outputDir, "trace.out"))
		if err != nil {
			t.Fatalf("create trace file: %v", err)
		}
		if err := trace.Start(traceFile); err != nil {
			t.Fatalf("start trace: %v", err)
		}
	}

	// Collect metrics snapshots every 5 seconds.
	metricsTicker := time.NewTicker(5 * time.Second)
	defer metricsTicker.Stop()

	// Initial snapshot.
	report.Snapshots = append(report.Snapshots, collectMetricsSnapshot(profileStart))

	deadline := time.After(duration)

	// Stop CPU profile after cpuSecs (or at end if shorter).
	cpuDeadline := time.After(time.Duration(cpuSecs) * time.Second)
	cpuStopped := false

	// Stop trace after 10 seconds (traces get large quickly).
	traceDeadline := time.After(10 * time.Second)
	traceStopped := false

loop:
	for {
		select {
		case <-deadline:
			break loop
		case <-metricsTicker.C:
			snap := collectMetricsSnapshot(profileStart)
			report.Snapshots = append(report.Snapshots, snap)
			t.Logf("  t=%5.0fs  goroutines=%d  heap=%dMB  stack=%dKB  gc=%d",
				snap.ElapsedSec,
				snap.GoroutineCount,
				snap.HeapAllocBytes/(1024*1024),
				snap.StackInuseBytes/1024,
				snap.NumGC)
		case <-cpuDeadline:
			if !cpuStopped {
				pprof.StopCPUProfile()
				cpuFile.Close()
				cpuStopped = true
				t.Logf("  CPU profile saved (%ds)", cpuSecs)
			}
		case <-traceDeadline:
			if enableTrace && !traceStopped {
				trace.Stop()
				traceFile.Close()
				traceStopped = true
				t.Log("  runtime trace saved (10s)")
			}
		}
	}

	// Stop profiles if still running.
	if !cpuStopped {
		pprof.StopCPUProfile()
		cpuFile.Close()
	}
	if enableTrace && !traceStopped {
		trace.Stop()
		traceFile.Close()
	}

	// Final snapshot.
	report.Snapshots = append(report.Snapshots, collectMetricsSnapshot(profileStart))
	report.EndTime = time.Now()

	// Save goroutine profile.
	goroutineFile, err := os.Create(filepath.Join(outputDir, "goroutine.pprof"))
	if err != nil {
		t.Fatalf("create goroutine profile: %v", err)
	}
	if err := pprof.Lookup("goroutine").WriteTo(goroutineFile, 0); err != nil {
		t.Fatalf("write goroutine profile: %v", err)
	}
	goroutineFile.Close()

	// Save heap profile.
	heapFile, err := os.Create(filepath.Join(outputDir, "heap.pprof"))
	if err != nil {
		t.Fatalf("create heap profile: %v", err)
	}
	runtime.GC()
	if err := pprof.Lookup("heap").WriteTo(heapFile, 0); err != nil {
		t.Fatalf("write heap profile: %v", err)
	}
	heapFile.Close()

	// Save metrics report as JSON.
	metricsFile, err := os.Create(filepath.Join(outputDir, "metrics.json"))
	if err != nil {
		t.Fatalf("create metrics file: %v", err)
	}
	enc := json.NewEncoder(metricsFile)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		t.Fatalf("encode metrics: %v", err)
	}
	metricsFile.Close()

	// Print summary.
	first := report.Snapshots[0]
	last := report.Snapshots[len(report.Snapshots)-1]
	t.Logf("")
	t.Logf("=== Profile Summary ===")
	t.Logf("  databases:       %d", dbCount)
	t.Logf("  duration:        %s", duration)
	t.Logf("  goroutines:      %d → %d", first.GoroutineCount, last.GoroutineCount)
	t.Logf("  heap alloc:      %dMB → %dMB", first.HeapAllocBytes/(1024*1024), last.HeapAllocBytes/(1024*1024))
	t.Logf("  heap objects:    %d → %d", first.HeapObjects, last.HeapObjects)
	t.Logf("  stack inuse:     %dKB → %dKB", first.StackInuseBytes/1024, last.StackInuseBytes/1024)
	t.Logf("  GC cycles:       %d → %d (+%d)", first.NumGC, last.NumGC, last.NumGC-first.NumGC)
	t.Logf("  GC pause total:  %dms → %dms", first.PauseTotalNs/1e6, last.PauseTotalNs/1e6)
	t.Logf("")
	t.Logf("=== Output Files ===")
	t.Logf("  %s/cpu.pprof", outputDir)
	t.Logf("  %s/goroutine.pprof", outputDir)
	t.Logf("  %s/heap.pprof", outputDir)
	t.Logf("  %s/metrics.json", outputDir)
	if enableTrace {
		t.Logf("  %s/trace.out", outputDir)
	}
	t.Logf("")
	t.Logf("Analyze with:")
	t.Logf("  go tool pprof -http=:8181 %s/cpu.pprof", outputDir)
	t.Logf("  go tool pprof -http=:8182 %s/heap.pprof", outputDir)
	if enableTrace {
		t.Logf("  go tool trace %s/trace.out", outputDir)
	}

	// Shutdown.
	t.Logf("shutting down %d databases...", dbCount)
	for i, inst := range instances {
		if err := inst.db.Close(context.Background()); err != nil {
			t.Logf("close litestream db %d: %v", i, err)
		}
		if err := inst.sqldb.Close(); err != nil {
			t.Logf("close sql db %d: %v", i, err)
		}
	}
}

func envInt(t *testing.T, key string, defaultVal int) int {
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("invalid %s: %v", key, err)
	}
	return n
}

func envDuration(t *testing.T, key string, defaultVal time.Duration) time.Duration {
	s := os.Getenv(key)
	if s == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		t.Fatalf("invalid %s: %v", key, err)
	}
	return d
}

func envString(key, defaultVal string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return defaultVal
}
