package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

const (
	cpuKillThreshold    = 100.0
	cpuSustainedSeconds = 10
	memoryKillThreshold = 1024 * 1024 * 1024 // 1GB in bytes
)

type Config struct {
	TestType    string
	DBCount     int
	InitialDBs  int
	FinalDBs    int
	BatchSize   int
	WritesPerDB int
	Duration    time.Duration
	DBDir       string
	ReplicaDir  string
	ConfigPath  string
	Verbose     bool
}

type Metrics struct {
	startTime      time.Time
	peakCPU        float64
	peakMemory     int64
	cpuSamples     []float64
	cpuSampleIndex int
	detectedDBs    int32
	totalWrites    int64
	mu             sync.RWMutex
}

func main() {
	cfg := parseFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n[SIGNAL] Interrupt received, shutting down...")
		cancel()
	}()

	if err := run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.TestType, "test", "precreated", "Test type: precreated or dynamic")
	flag.IntVar(&cfg.DBCount, "dbs", 500, "Number of databases for precreated test")
	flag.IntVar(&cfg.InitialDBs, "initial-dbs", 20, "Initial databases for dynamic test")
	flag.IntVar(&cfg.FinalDBs, "final-dbs", 500, "Final databases for dynamic test")
	flag.IntVar(&cfg.BatchSize, "batch-size", 20, "Batch size for dynamic test")
	flag.IntVar(&cfg.WritesPerDB, "writes-per-db", 5, "Writes per database per second")
	flag.DurationVar(&cfg.Duration, "duration", 60*time.Second, "Test duration after all DBs are created")
	flag.StringVar(&cfg.DBDir, "db-dir", "", "Database directory (default: temp dir)")
	flag.StringVar(&cfg.ReplicaDir, "replica-dir", "", "Replica directory (default: temp dir)")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "Enable verbose output")

	flag.Parse()

	return cfg
}

func run(ctx context.Context, cfg *Config) error {
	if cfg.DBDir == "" {
		tmpDir, err := os.MkdirTemp("", "stress-test-dbs-*")
		if err != nil {
			return fmt.Errorf("create temp db dir: %w", err)
		}
		cfg.DBDir = tmpDir
		defer os.RemoveAll(tmpDir)
	}

	if cfg.ReplicaDir == "" {
		tmpDir, err := os.MkdirTemp("", "stress-test-replica-*")
		if err != nil {
			return fmt.Errorf("create temp replica dir: %w", err)
		}
		cfg.ReplicaDir = tmpDir
		defer os.RemoveAll(tmpDir)
	}

	configPath, err := writeConfig(cfg)
	if err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	cfg.ConfigPath = configPath
	defer os.Remove(configPath)

	fmt.Printf("=== Directory Watcher Stress Test ===\n")
	fmt.Printf("Test type:     %s\n", cfg.TestType)
	fmt.Printf("DB directory:  %s\n", cfg.DBDir)
	fmt.Printf("Replica dir:   %s\n", cfg.ReplicaDir)
	fmt.Printf("Config file:   %s\n", cfg.ConfigPath)
	fmt.Printf("Kill thresholds: CPU >%.0f%% for %ds, Memory >%dMB\n",
		cpuKillThreshold, cpuSustainedSeconds, memoryKillThreshold/1024/1024)
	fmt.Println()

	switch cfg.TestType {
	case "precreated":
		return runPreCreatedTest(ctx, cfg)
	case "dynamic":
		return runDynamicTest(ctx, cfg)
	default:
		return fmt.Errorf("unknown test type: %s", cfg.TestType)
	}
}

func writeConfig(cfg *Config) (string, error) {
	configContent := fmt.Sprintf(`dbs:
  - dir: %s
    pattern: "*.db"
    recursive: false
    watch: true
    replica:
      type: file
      path: %s
      sync-interval: 1s
logging:
  level: info
`, cfg.DBDir, cfg.ReplicaDir)

	f, err := os.CreateTemp("", "litestream-stress-*.yml")
	if err != nil {
		return "", err
	}
	defer f.Close()

	if _, err := f.WriteString(configContent); err != nil {
		os.Remove(f.Name())
		return "", err
	}

	return f.Name(), nil
}

func runPreCreatedTest(ctx context.Context, cfg *Config) error {
	fmt.Printf("[PHASE 1] Creating %d databases upfront...\n", cfg.DBCount)

	dbs, err := createDatabases(ctx, cfg.DBDir, cfg.DBCount)
	if err != nil {
		return fmt.Errorf("create databases: %w", err)
	}
	defer closeDatabases(dbs)

	fmt.Printf("[PHASE 1] Created %d databases\n\n", len(dbs))

	metrics := &Metrics{
		startTime:  time.Now(),
		cpuSamples: make([]float64, cpuSustainedSeconds),
	}

	fmt.Println("[PHASE 2] Starting litestream...")

	cmd, err := startLitestream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("start litestream: %w", err)
	}

	monitorCtx, monitorCancel := context.WithCancel(ctx)
	defer monitorCancel()

	killCh := make(chan string, 1)
	go monitorResources(monitorCtx, cmd, metrics, killCh)

	fmt.Println("[PHASE 2] Waiting for database detection (timeout: 2m)...")

	detectionCtx, detectionCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer detectionCancel()

	if err := waitForDetection(detectionCtx, cfg.ReplicaDir, cfg.DBCount, metrics); err != nil {
		stopLitestream(cmd)
		return fmt.Errorf("detection failed: %w", err)
	}

	fmt.Printf("[PHASE 2] All %d databases detected\n\n", cfg.DBCount)

	fmt.Printf("[PHASE 3] Starting writes (%d writes/db/sec for %v)...\n", cfg.WritesPerDB, cfg.Duration)

	writeCtx, writeCancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i, db := range dbs {
		wg.Add(1)
		go func(idx int, db *sql.DB) {
			defer wg.Done()
			runWrites(writeCtx, db, idx, cfg.WritesPerDB, metrics)
		}(i, db)
	}

	select {
	case <-ctx.Done():
		fmt.Println("\n[CANCELLED] Test interrupted")
	case reason := <-killCh:
		fmt.Printf("\n[KILLED] %s\n", reason)
		stopLitestream(cmd)
		writeCancel()
		wg.Wait()
		printFinalReport(metrics, false, reason)
		return fmt.Errorf("threshold exceeded: %s", reason)
	case <-time.After(cfg.Duration):
		fmt.Println("\n[PHASE 3] Write test completed")
	}

	writeCancel()
	wg.Wait()

	monitorCancel()
	stopLitestream(cmd)

	printFinalReport(metrics, true, "")
	return nil
}

func runDynamicTest(ctx context.Context, cfg *Config) error {
	fmt.Printf("[PHASE 1] Creating initial %d databases...\n", cfg.InitialDBs)

	dbs, err := createDatabases(ctx, cfg.DBDir, cfg.InitialDBs)
	if err != nil {
		return fmt.Errorf("create initial databases: %w", err)
	}
	defer closeDatabases(dbs)

	metrics := &Metrics{
		startTime:  time.Now(),
		cpuSamples: make([]float64, cpuSustainedSeconds),
	}

	fmt.Println("[PHASE 2] Starting litestream...")

	cmd, err := startLitestream(ctx, cfg)
	if err != nil {
		return fmt.Errorf("start litestream: %w", err)
	}

	monitorCtx, monitorCancel := context.WithCancel(ctx)
	defer monitorCancel()

	killCh := make(chan string, 1)
	go monitorResources(monitorCtx, cmd, metrics, killCh)

	fmt.Println("[PHASE 2] Waiting for initial database detection...")

	detectionCtx, detectionCancel := context.WithTimeout(ctx, 30*time.Second)
	if err := waitForDetection(detectionCtx, cfg.ReplicaDir, cfg.InitialDBs, metrics); err != nil {
		detectionCancel()
		stopLitestream(cmd)
		return fmt.Errorf("initial detection failed: %w", err)
	}
	detectionCancel()

	fmt.Printf("[PHASE 2] Initial %d databases detected\n\n", cfg.InitialDBs)

	writeCtx, writeCancel := context.WithCancel(ctx)
	var writeWg sync.WaitGroup
	var dbMu sync.Mutex

	startWritesForDBs := func(databases []*sql.DB, startIdx int) {
		for i, db := range databases {
			writeWg.Add(1)
			go func(idx int, db *sql.DB) {
				defer writeWg.Done()
				runWrites(writeCtx, db, idx, cfg.WritesPerDB, metrics)
			}(startIdx+i, db)
		}
	}

	fmt.Printf("[PHASE 3] Starting writes on initial %d databases...\n", cfg.InitialDBs)
	startWritesForDBs(dbs, 0)

	fmt.Printf("[PHASE 4] Scaling from %d to %d databases (batch size: %d)...\n",
		cfg.InitialDBs, cfg.FinalDBs, cfg.BatchSize)

	currentCount := cfg.InitialDBs
	batchNum := 0
	for currentCount < cfg.FinalDBs {
		select {
		case <-ctx.Done():
			writeCancel()
			writeWg.Wait()
			stopLitestream(cmd)
			return ctx.Err()
		case reason := <-killCh:
			fmt.Printf("\n[KILLED] %s\n", reason)
			writeCancel()
			writeWg.Wait()
			stopLitestream(cmd)
			printFinalReport(metrics, false, reason)
			return fmt.Errorf("threshold exceeded: %s", reason)
		default:
		}

		batchNum++
		batchSize := cfg.BatchSize
		if currentCount+batchSize > cfg.FinalDBs {
			batchSize = cfg.FinalDBs - currentCount
		}

		fmt.Printf("  [Batch %d] Creating %d databases (%d -> %d)...\n",
			batchNum, batchSize, currentCount, currentCount+batchSize)

		newDBs, err := createDatabasesBatch(ctx, cfg.DBDir, currentCount, batchSize)
		if err != nil {
			writeCancel()
			writeWg.Wait()
			stopLitestream(cmd)
			return fmt.Errorf("create batch %d: %w", batchNum, err)
		}

		dbMu.Lock()
		dbs = append(dbs, newDBs...)
		dbMu.Unlock()

		detectionTimeout := 30 * time.Second
		if currentCount+batchSize >= cfg.FinalDBs-50 {
			detectionTimeout = 60 * time.Second
		}
		detectionCtx, detectionCancel := context.WithTimeout(ctx, detectionTimeout)
		expectedCount := currentCount + batchSize
		if err := waitForDetection(detectionCtx, cfg.ReplicaDir, expectedCount, metrics); err != nil {
			detectionCancel()
			writeCancel()
			writeWg.Wait()
			stopLitestream(cmd)
			return fmt.Errorf("batch %d detection failed (expected %d, got %d): %w",
				batchNum, expectedCount, atomic.LoadInt32(&metrics.detectedDBs), err)
		}
		detectionCancel()

		startWritesForDBs(newDBs, currentCount)
		currentCount += batchSize

		fmt.Printf("  [Batch %d] Complete. Total: %d databases\n", batchNum, currentCount)

		time.Sleep(2 * time.Second)
	}

	fmt.Printf("\n[PHASE 5] All %d databases active. Running sustained load for %v...\n",
		cfg.FinalDBs, cfg.Duration)

	select {
	case <-ctx.Done():
		fmt.Println("\n[CANCELLED] Test interrupted")
	case reason := <-killCh:
		fmt.Printf("\n[KILLED] %s\n", reason)
		stopLitestream(cmd)
		writeCancel()
		writeWg.Wait()
		printFinalReport(metrics, false, reason)
		return fmt.Errorf("threshold exceeded: %s", reason)
	case <-time.After(cfg.Duration):
		fmt.Println("\n[PHASE 5] Sustained load test completed")
	}

	writeCancel()
	writeWg.Wait()
	monitorCancel()
	stopLitestream(cmd)

	printFinalReport(metrics, true, "")
	return nil
}

func createDatabases(ctx context.Context, dir string, count int) ([]*sql.DB, error) {
	return createDatabasesBatch(ctx, dir, 0, count)
}

func createDatabasesBatch(ctx context.Context, dir string, startIdx, count int) ([]*sql.DB, error) {
	dbs := make([]*sql.DB, 0, count)

	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			closeDatabases(dbs)
			return nil, ctx.Err()
		default:
		}

		idx := startIdx + i
		dbPath := filepath.Join(dir, fmt.Sprintf("test_%04d.db", idx))

		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			closeDatabases(dbs)
			return nil, fmt.Errorf("open db %d: %w", idx, err)
		}

		if _, err := db.ExecContext(ctx, `
			PRAGMA journal_mode=WAL;
			CREATE TABLE IF NOT EXISTS data (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				value TEXT,
				created_at DATETIME DEFAULT CURRENT_TIMESTAMP
			);
		`); err != nil {
			db.Close()
			closeDatabases(dbs)
			return nil, fmt.Errorf("init db %d: %w", idx, err)
		}

		dbs = append(dbs, db)

		if (i+1)%100 == 0 || i+1 == count {
			fmt.Printf("  Created %d/%d databases\n", i+1, count)
		}
	}

	return dbs, nil
}

func closeDatabases(dbs []*sql.DB) {
	for _, db := range dbs {
		if db != nil {
			db.Close()
		}
	}
}

func startLitestream(ctx context.Context, cfg *Config) (*exec.Cmd, error) {
	litestreamPath := "./bin/litestream"
	if _, err := os.Stat(litestreamPath); os.IsNotExist(err) {
		litestreamPath = "litestream"
	}

	cmd := exec.CommandContext(ctx, litestreamPath, "replicate", "-config", cfg.ConfigPath)
	cmd.Stdout = nil
	cmd.Stderr = nil

	if cfg.Verbose {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	time.Sleep(500 * time.Millisecond)

	return cmd, nil
}

func stopLitestream(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}

	cmd.Process.Signal(syscall.SIGTERM)

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		cmd.Process.Kill()
	}
}

func monitorResources(ctx context.Context, cmd *exec.Cmd, metrics *Metrics, killCh chan<- string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if cmd.Process == nil {
				return
			}

			cpu, mem := sampleProcess(cmd.Process.Pid)

			metrics.mu.Lock()
			if cpu > metrics.peakCPU {
				metrics.peakCPU = cpu
			}
			if mem > metrics.peakMemory {
				metrics.peakMemory = mem
			}
			metrics.cpuSamples[metrics.cpuSampleIndex] = cpu
			metrics.cpuSampleIndex = (metrics.cpuSampleIndex + 1) % len(metrics.cpuSamples)
			metrics.mu.Unlock()

			elapsed := int(time.Since(metrics.startTime).Seconds())
			detected := atomic.LoadInt32(&metrics.detectedDBs)
			writes := atomic.LoadInt64(&metrics.totalWrites)

			fmt.Printf("\r[%3ds] CPU: %5.1f%% | Mem: %4dMB | Detected: %4d | Writes: %6d | Peak: %5.1f%% / %4dMB",
				elapsed, cpu, mem/1024/1024, detected, writes, metrics.peakCPU, metrics.peakMemory/1024/1024)

			if mem > memoryKillThreshold {
				select {
				case killCh <- fmt.Sprintf("Memory exceeded %dMB (current: %dMB)",
					memoryKillThreshold/1024/1024, mem/1024/1024):
				default:
				}
				return
			}

			if elapsed >= cpuSustainedSeconds {
				avgCPU := averageCPU(metrics)
				if avgCPU > cpuKillThreshold {
					select {
					case killCh <- fmt.Sprintf("CPU exceeded %.0f%% for %d seconds (avg: %.1f%%)",
						cpuKillThreshold, cpuSustainedSeconds, avgCPU):
					default:
					}
					return
				}
			}
		}
	}
}

func averageCPU(metrics *Metrics) float64 {
	metrics.mu.RLock()
	defer metrics.mu.RUnlock()

	var sum float64
	for _, v := range metrics.cpuSamples {
		sum += v
	}
	return sum / float64(len(metrics.cpuSamples))
}

func sampleProcess(pid int) (cpu float64, mem int64) {
	out, err := exec.Command("ps", "-o", "pcpu=,rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, 0
	}

	fields := strings.Fields(string(out))
	if len(fields) < 2 {
		return 0, 0
	}

	if v, err := strconv.ParseFloat(fields[0], 64); err == nil {
		cpu = v
	}
	if v, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
		mem = v * 1024 // RSS is in KB
	}

	return cpu, mem
}

func waitForDetection(ctx context.Context, replicaDir string, expectedCount int, metrics *Metrics) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			count := countDetectedDBs(replicaDir)
			atomic.StoreInt32(&metrics.detectedDBs, int32(count))

			if count >= expectedCount {
				return nil
			}
		}
	}
}

func countDetectedDBs(replicaDir string) int {
	entries, err := os.ReadDir(replicaDir)
	if err != nil {
		return 0
	}

	count := 0
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "test_") {
			count++
		}
	}
	return count
}

func runWrites(ctx context.Context, db *sql.DB, dbIdx, writesPerSec int, metrics *Metrics) {
	if writesPerSec <= 0 {
		return
	}

	interval := time.Second / time.Duration(writesPerSec)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := db.ExecContext(ctx,
				"INSERT INTO data (value) VALUES (?)",
				fmt.Sprintf("db%d-%d", dbIdx, time.Now().UnixNano()))
			if err == nil {
				atomic.AddInt64(&metrics.totalWrites, 1)
			}
		}
	}
}

func printFinalReport(metrics *Metrics, passed bool, failReason string) {
	fmt.Println()
	fmt.Println()
	fmt.Println("=" + strings.Repeat("=", 50))
	fmt.Println("                    FINAL REPORT")
	fmt.Println("=" + strings.Repeat("=", 50))

	elapsed := time.Since(metrics.startTime)
	fmt.Printf("Duration:        %v\n", elapsed.Round(time.Second))
	fmt.Printf("Databases:       %d detected\n", atomic.LoadInt32(&metrics.detectedDBs))
	fmt.Printf("Total writes:    %d\n", atomic.LoadInt64(&metrics.totalWrites))
	fmt.Printf("Peak CPU:        %.1f%%\n", metrics.peakCPU)
	fmt.Printf("Peak Memory:     %d MB\n", metrics.peakMemory/1024/1024)

	fmt.Println()
	if passed {
		fmt.Println("Result:          ✓ PASSED")
		fmt.Println()
		fmt.Println("All thresholds met:")
		fmt.Printf("  • CPU stayed below %.0f%% sustained\n", cpuKillThreshold)
		fmt.Printf("  • Memory stayed below %d MB\n", memoryKillThreshold/1024/1024)
		fmt.Println("  • All databases detected within timeout")
	} else {
		fmt.Println("Result:          ✗ FAILED")
		fmt.Printf("Reason:          %s\n", failReason)
	}
	fmt.Println("=" + strings.Repeat("=", 50))
}
