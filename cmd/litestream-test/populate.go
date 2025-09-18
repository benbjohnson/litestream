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
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type PopulateCommand struct {
	Main *Main

	DB         string
	TargetSize string
	RowSize    int
	BatchSize  int
	TableCount int
	IndexRatio float64
	PageSize   int
}

func (c *PopulateCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-test populate", flag.ExitOnError)
	fs.StringVar(&c.DB, "db", "", "Database path (required)")
	fs.StringVar(&c.TargetSize, "target-size", "100MB", "Target database size (e.g., 1GB, 500MB)")
	fs.IntVar(&c.RowSize, "row-size", 1024, "Average row size in bytes")
	fs.IntVar(&c.BatchSize, "batch-size", 1000, "Rows per transaction")
	fs.IntVar(&c.TableCount, "table-count", 1, "Number of tables to create")
	fs.Float64Var(&c.IndexRatio, "index-ratio", 0.2, "Percentage of columns to index (0.0-1.0)")
	fs.IntVar(&c.PageSize, "page-size", 4096, "SQLite page size in bytes")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if c.DB == "" {
		return fmt.Errorf("database path required")
	}

	targetBytes, err := parseSize(c.TargetSize)
	if err != nil {
		return fmt.Errorf("invalid target size: %w", err)
	}

	slog.Info("Starting database population",
		"db", c.DB,
		"target_size", c.TargetSize,
		"row_size", c.RowSize,
		"batch_size", c.BatchSize,
		"table_count", c.TableCount,
		"page_size", c.PageSize,
	)

	if err := c.populateDatabase(ctx, targetBytes); err != nil {
		return fmt.Errorf("populate database: %w", err)
	}

	slog.Info("Database population complete", "db", c.DB)
	return nil
}

func (c *PopulateCommand) populateDatabase(ctx context.Context, targetBytes int64) error {
	if err := os.Remove(c.DB); err != nil && !os.IsNotExist(err) {
		slog.Warn("Could not remove existing database", "error", err)
	}

	db, err := sql.Open("sqlite3", c.DB)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	if _, err := db.Exec(fmt.Sprintf("PRAGMA page_size = %d", c.PageSize)); err != nil {
		return fmt.Errorf("set page size: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		return fmt.Errorf("set journal mode: %w", err)
	}

	if _, err := db.Exec("PRAGMA synchronous = NORMAL"); err != nil {
		return fmt.Errorf("set synchronous: %w", err)
	}

	for i := 0; i < c.TableCount; i++ {
		tableName := fmt.Sprintf("test_table_%d", i)

		createSQL := fmt.Sprintf(`
			CREATE TABLE %s (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				data BLOB,
				text_field TEXT,
				int_field INTEGER,
				float_field REAL,
				timestamp INTEGER
			)
		`, tableName)

		if _, err := db.Exec(createSQL); err != nil {
			return fmt.Errorf("create table %s: %w", tableName, err)
		}

		if c.IndexRatio > 0 {
			if rand.Float64() < c.IndexRatio {
				indexSQL := fmt.Sprintf("CREATE INDEX idx_%s_timestamp ON %s(timestamp)", tableName, tableName)
				if _, err := db.Exec(indexSQL); err != nil {
					return fmt.Errorf("create index: %w", err)
				}
			}
			if rand.Float64() < c.IndexRatio {
				indexSQL := fmt.Sprintf("CREATE INDEX idx_%s_int ON %s(int_field)", tableName, tableName)
				if _, err := db.Exec(indexSQL); err != nil {
					return fmt.Errorf("create index: %w", err)
				}
			}
		}
	}

	totalRows := int(targetBytes / int64(c.RowSize))
	rowsPerTable := totalRows / c.TableCount
	if rowsPerTable == 0 {
		rowsPerTable = 1
	}

	slog.Info("Populating database",
		"target_bytes", targetBytes,
		"total_rows", totalRows,
		"rows_per_table", rowsPerTable,
	)

	startTime := time.Now()
	for tableIdx := 0; tableIdx < c.TableCount; tableIdx++ {
		tableName := fmt.Sprintf("test_table_%d", tableIdx)

		if err := c.populateTable(ctx, db, tableName, rowsPerTable); err != nil {
			return fmt.Errorf("populate table %s: %w", tableName, err)
		}

		currentSize, _ := getDatabaseSize(c.DB)
		progress := float64(currentSize) / float64(targetBytes) * 100
		slog.Info("Progress",
			"table", tableName,
			"current_size_mb", currentSize/1024/1024,
			"progress_percent", fmt.Sprintf("%.1f", progress),
		)

		if currentSize >= targetBytes {
			break
		}
	}

	duration := time.Since(startTime)
	finalSize, _ := getDatabaseSize(c.DB)

	slog.Info("Population complete",
		"duration", duration,
		"final_size_mb", finalSize/1024/1024,
		"throughput_mb_per_sec", fmt.Sprintf("%.2f", float64(finalSize)/1024/1024/duration.Seconds()),
	)

	return nil
}

func (c *PopulateCommand) populateTable(ctx context.Context, db *sql.DB, tableName string, rowCount int) error {
	data := make([]byte, c.RowSize)

	for i := 0; i < rowCount; i += c.BatchSize {
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin transaction: %w", err)
		}

		stmt, err := tx.Prepare(fmt.Sprintf(`
			INSERT INTO %s (data, text_field, int_field, float_field, timestamp)
			VALUES (?, ?, ?, ?, ?)
		`, tableName))
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("prepare statement: %w", err)
		}

		batchEnd := i + c.BatchSize
		if batchEnd > rowCount {
			batchEnd = rowCount
		}

		for j := i; j < batchEnd; j++ {
			cryptorand.Read(data)
			textField := fmt.Sprintf("row_%d_%d", i, j)
			intField := rand.Int63()
			floatField := rand.Float64() * 1000
			timestamp := time.Now().Unix()

			if _, err := stmt.Exec(data, textField, intField, floatField, timestamp); err != nil {
				stmt.Close()
				tx.Rollback()
				return fmt.Errorf("insert row: %w", err)
			}
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return nil
}

func (c *PopulateCommand) Usage() {
	fmt.Fprintln(c.Main.Stdout, `
Populate a SQLite database to a target size for testing.

Usage:

	litestream-test populate [options]

Options:

	-db PATH
	    Database path (required)

	-target-size SIZE
	    Target database size (e.g., "1GB", "500MB")
	    Default: 100MB

	-row-size SIZE
	    Average row size in bytes
	    Default: 1024

	-batch-size COUNT
	    Number of rows per transaction
	    Default: 1000

	-table-count COUNT
	    Number of tables to create
	    Default: 1

	-index-ratio RATIO
	    Percentage of columns to index (0.0-1.0)
	    Default: 0.2

	-page-size SIZE
	    SQLite page size in bytes
	    Default: 4096

Examples:

	# Create a 1GB database with default settings
	litestream-test populate -db /tmp/test.db -target-size 1GB

	# Create a 2GB database with larger rows
	litestream-test populate -db /tmp/test.db -target-size 2GB -row-size 4096

	# Test lock page with different page sizes
	litestream-test populate -db /tmp/test.db -target-size 1.5GB -page-size 8192
`[1:])
}

func parseSize(s string) (int64, error) {
	// Check suffixes in order from longest to shortest to avoid "B" matching before "MB"
	suffixes := []struct {
		suffix     string
		multiplier int64
	}{
		{"TB", 1024 * 1024 * 1024 * 1024},
		{"GB", 1024 * 1024 * 1024},
		{"MB", 1024 * 1024},
		{"KB", 1024},
		{"B", 1},
	}

	for _, sf := range suffixes {
		if len(s) > len(sf.suffix) && s[len(s)-len(sf.suffix):] == sf.suffix {
			var value float64
			if _, err := fmt.Sscanf(s[:len(s)-len(sf.suffix)], "%f", &value); err != nil {
				return 0, err
			}
			return int64(value * float64(sf.multiplier)), nil
		}
	}

	var value int64
	if _, err := fmt.Sscanf(s, "%d", &value); err != nil {
		return 0, err
	}
	return value, nil
}

func getDatabaseSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}

	size := info.Size()

	walPath := path + "-wal"
	if walInfo, err := os.Stat(walPath); err == nil {
		size += walInfo.Size()
	}

	shmPath := path + "-shm"
	if shmInfo, err := os.Stat(shmPath); err == nil {
		size += shmInfo.Size()
	}

	return size, nil
}
