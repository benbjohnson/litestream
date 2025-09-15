package main

import (
	"context"
	cryptorand "crypto/rand"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type LoadCommand struct {
	Main *Main

	DB          string
	WriteRate   int
	Duration    time.Duration
	Pattern     string
	PayloadSize int
	ReadRatio   float64
	Workers     int
}

type LoadStats struct {
	writes     int64
	reads      int64
	errors     int64
	startTime  time.Time
	lastReport time.Time
	mu         sync.Mutex
}

func (c *LoadCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-test load", flag.ExitOnError)
	fs.StringVar(&c.DB, "db", "", "Database path (required)")
	fs.IntVar(&c.WriteRate, "write-rate", 100, "Writes per second")
	fs.DurationVar(&c.Duration, "duration", 1*time.Minute, "How long to run")
	fs.StringVar(&c.Pattern, "pattern", "constant", "Write pattern (constant, burst, random, wave)")
	fs.IntVar(&c.PayloadSize, "payload-size", 1024, "Size of each write operation in bytes")
	fs.Float64Var(&c.ReadRatio, "read-ratio", 0.2, "Read/write ratio (0.0-1.0)")
	fs.IntVar(&c.Workers, "workers", 1, "Number of concurrent workers")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if c.DB == "" {
		return fmt.Errorf("database path required")
	}

	if _, err := os.Stat(c.DB); err != nil {
		return fmt.Errorf("database does not exist: %w", err)
	}

	slog.Info("Starting load generation",
		"db", c.DB,
		"write_rate", c.WriteRate,
		"duration", c.Duration,
		"pattern", c.Pattern,
		"payload_size", c.PayloadSize,
		"read_ratio", c.ReadRatio,
		"workers", c.Workers,
	)

	return c.generateLoad(ctx)
}

func (c *LoadCommand) generateLoad(ctx context.Context) error {
	db, err := sql.Open("sqlite3", c.DB+"?_journal_mode=WAL")
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(c.Workers + 1)
	db.SetMaxIdleConns(c.Workers)

	if err := c.ensureTestTable(db); err != nil {
		return fmt.Errorf("ensure test table: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, c.Duration)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("Received interrupt signal, stopping load generation")
		cancel()
	}()

	stats := &LoadStats{
		startTime:  time.Now(),
		lastReport: time.Now(),
	}

	var wg sync.WaitGroup
	for i := 0; i < c.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c.worker(ctx, db, workerID, stats)
		}(i)
	}

	go c.reportStats(ctx, stats)

	wg.Wait()

	c.finalReport(stats)
	return nil
}

func (c *LoadCommand) worker(ctx context.Context, db *sql.DB, workerID int, stats *LoadStats) {
	ticker := time.NewTicker(time.Second / time.Duration(c.WriteRate/c.Workers))
	defer ticker.Stop()

	data := make([]byte, c.PayloadSize)
	cryptorand.Read(data)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rate := c.calculateRate(stats)
			if rate == 0 {
				continue
			}

			if rand.Float64() < c.ReadRatio {
				if err := c.performRead(db); err != nil {
					atomic.AddInt64(&stats.errors, 1)
					slog.Error("Read failed", "error", err)
				} else {
					atomic.AddInt64(&stats.reads, 1)
				}
			} else {
				if err := c.performWrite(db, data); err != nil {
					atomic.AddInt64(&stats.errors, 1)
					slog.Error("Write failed", "error", err)
				} else {
					atomic.AddInt64(&stats.writes, 1)
				}
			}
		}
	}
}

func (c *LoadCommand) calculateRate(stats *LoadStats) float64 {
	elapsed := time.Since(stats.startTime).Seconds()

	switch c.Pattern {
	case "burst":
		if int(elapsed)%10 < 3 {
			return 2.0
		}
		return 0.0
	case "random":
		return rand.Float64() * 2.0
	case "wave":
		return (1.0 + 0.5*waveFunction(elapsed/10.0))
	default:
		return 1.0
	}
}

func waveFunction(t float64) float64 {
	return (1.0 + 0.8*sinApprox(t)) / 2.0
}

func sinApprox(x float64) float64 {
	const twoPi = 2 * 3.14159265359
	x = x - float64(int(x/twoPi))*twoPi

	if x < 3.14159265359 {
		return 4 * x * (3.14159265359 - x) / (3.14159265359 * 3.14159265359)
	}
	x = x - 3.14159265359
	return -4 * x * (3.14159265359 - x) / (3.14159265359 * 3.14159265359)
}

func (c *LoadCommand) ensureTestTable(db *sql.DB) error {
	createSQL := `
		CREATE TABLE IF NOT EXISTS load_test (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			data BLOB,
			text_field TEXT,
			int_field INTEGER,
			timestamp INTEGER
		)
	`
	_, err := db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_load_test_timestamp ON load_test(timestamp)")
	return err
}

func (c *LoadCommand) performWrite(db *sql.DB, data []byte) error {
	textField := fmt.Sprintf("load_%d", time.Now().UnixNano())
	intField := rand.Int63()
	timestamp := time.Now().Unix()

	_, err := db.Exec(`
		INSERT INTO load_test (data, text_field, int_field, timestamp)
		VALUES (?, ?, ?, ?)
	`, data, textField, intField, timestamp)

	return err
}

func (c *LoadCommand) performRead(db *sql.DB) error {
	var count int
	query := `SELECT COUNT(*) FROM load_test WHERE timestamp > ?`
	timestamp := time.Now().Add(-1 * time.Hour).Unix()

	return db.QueryRow(query, timestamp).Scan(&count)
}

func (c *LoadCommand) reportStats(ctx context.Context, stats *LoadStats) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats.mu.Lock()
			elapsed := time.Since(stats.lastReport).Seconds()
			writes := atomic.LoadInt64(&stats.writes)
			reads := atomic.LoadInt64(&stats.reads)
			errors := atomic.LoadInt64(&stats.errors)

			writeRate := float64(writes) / elapsed
			readRate := float64(reads) / elapsed

			slog.Info("Load statistics",
				"writes_per_sec", fmt.Sprintf("%.1f", writeRate),
				"reads_per_sec", fmt.Sprintf("%.1f", readRate),
				"total_writes", writes,
				"total_reads", reads,
				"errors", errors,
				"elapsed", time.Since(stats.startTime).Round(time.Second),
			)

			atomic.StoreInt64(&stats.writes, 0)
			atomic.StoreInt64(&stats.reads, 0)
			stats.lastReport = time.Now()
			stats.mu.Unlock()
		}
	}
}

func (c *LoadCommand) finalReport(stats *LoadStats) {
	totalTime := time.Since(stats.startTime)
	writes := atomic.LoadInt64(&stats.writes)
	reads := atomic.LoadInt64(&stats.reads)
	errors := atomic.LoadInt64(&stats.errors)

	slog.Info("Load generation complete",
		"duration", totalTime.Round(time.Second),
		"total_writes", writes,
		"total_reads", reads,
		"total_errors", errors,
		"avg_writes_per_sec", fmt.Sprintf("%.1f", float64(writes)/totalTime.Seconds()),
		"avg_reads_per_sec", fmt.Sprintf("%.1f", float64(reads)/totalTime.Seconds()),
	)
}

func (c *LoadCommand) Usage() {
	fmt.Fprintln(c.Main.Stdout, `
Generate continuous load on a SQLite database for testing.

Usage:

	litestream-test load [options]

Options:

	-db PATH
	    Database path (required)

	-write-rate RATE
	    Target writes per second
	    Default: 100

	-duration DURATION
	    How long to run (e.g., "10m", "1h")
	    Default: 1m

	-pattern PATTERN
	    Write pattern: constant, burst, random, wave
	    Default: constant

	-payload-size SIZE
	    Size of each write operation in bytes
	    Default: 1024

	-read-ratio RATIO
	    Read/write ratio (0.0-1.0)
	    Default: 0.2

	-workers COUNT
	    Number of concurrent workers
	    Default: 1

Examples:

	# Generate constant load for 10 minutes
	litestream-test load -db /tmp/test.db -write-rate 100 -duration 10m

	# Generate burst pattern load
	litestream-test load -db /tmp/test.db -pattern burst -duration 1h

	# Heavy write load with multiple workers
	litestream-test load -db /tmp/test.db -write-rate 1000 -workers 4 -read-ratio 0.1
`[1:])
}
