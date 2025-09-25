package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type ShrinkCommand struct {
	Main *Main

	DB               string
	DeletePercentage float64
	Vacuum           bool
	Checkpoint       bool
	CheckpointMode   string
}

func (c *ShrinkCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-test shrink", flag.ExitOnError)
	fs.StringVar(&c.DB, "db", "", "Database path (required)")
	fs.Float64Var(&c.DeletePercentage, "delete-percentage", 50, "Percentage of data to delete (0-100)")
	fs.BoolVar(&c.Vacuum, "vacuum", false, "Run VACUUM after deletion")
	fs.BoolVar(&c.Checkpoint, "checkpoint", false, "Run checkpoint after deletion")
	fs.StringVar(&c.CheckpointMode, "checkpoint-mode", "PASSIVE", "Checkpoint mode (PASSIVE, FULL, RESTART, TRUNCATE)")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if c.DB == "" {
		return fmt.Errorf("database path required")
	}

	if c.DeletePercentage < 0 || c.DeletePercentage > 100 {
		return fmt.Errorf("delete percentage must be between 0 and 100")
	}

	if _, err := os.Stat(c.DB); err != nil {
		return fmt.Errorf("database does not exist: %w", err)
	}

	slog.Info("Starting database shrink operation",
		"db", c.DB,
		"delete_percentage", c.DeletePercentage,
		"vacuum", c.Vacuum,
		"checkpoint", c.Checkpoint,
	)

	return c.shrinkDatabase(ctx)
}

func (c *ShrinkCommand) shrinkDatabase(ctx context.Context) error {
	initialSize, err := getDatabaseSize(c.DB)
	if err != nil {
		return fmt.Errorf("get initial size: %w", err)
	}

	slog.Info("Initial database size",
		"size_mb", initialSize/1024/1024,
	)

	db, err := sql.Open("sqlite3", c.DB+"?_journal_mode=WAL")
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	tables, err := c.getTableList(db)
	if err != nil {
		return fmt.Errorf("get table list: %w", err)
	}

	slog.Info("Found tables", "count", len(tables))

	totalDeleted := int64(0)
	for _, table := range tables {
		deleted, err := c.deleteFromTable(db, table)
		if err != nil {
			slog.Error("Failed to delete from table", "table", table, "error", err)
			continue
		}
		totalDeleted += deleted
		slog.Info("Deleted rows from table",
			"table", table,
			"rows_deleted", deleted,
		)
	}

	slog.Info("Deletion complete", "total_rows_deleted", totalDeleted)

	sizeAfterDelete, err := getDatabaseSize(c.DB)
	if err != nil {
		return fmt.Errorf("get size after delete: %w", err)
	}

	slog.Info("Size after deletion",
		"size_mb", sizeAfterDelete/1024/1024,
		"change_mb", (initialSize-sizeAfterDelete)/1024/1024,
	)

	if c.Checkpoint {
		if err := c.runCheckpoint(db); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}

		sizeAfterCheckpoint, _ := getDatabaseSize(c.DB)
		slog.Info("Size after checkpoint",
			"size_mb", sizeAfterCheckpoint/1024/1024,
			"change_from_delete_mb", (sizeAfterDelete-sizeAfterCheckpoint)/1024/1024,
		)
	}

	if c.Vacuum {
		if err := c.runVacuum(db); err != nil {
			return fmt.Errorf("vacuum: %w", err)
		}

		sizeAfterVacuum, _ := getDatabaseSize(c.DB)
		slog.Info("Size after VACUUM",
			"size_mb", sizeAfterVacuum/1024/1024,
			"total_reduction_mb", (initialSize-sizeAfterVacuum)/1024/1024,
		)
	}

	finalSize, err := getDatabaseSize(c.DB)
	if err != nil {
		return fmt.Errorf("get final size: %w", err)
	}

	reductionPercent := float64(initialSize-finalSize) / float64(initialSize) * 100
	slog.Info("Shrink operation complete",
		"initial_size_mb", initialSize/1024/1024,
		"final_size_mb", finalSize/1024/1024,
		"reduction_percent", fmt.Sprintf("%.1f", reductionPercent),
	)

	return nil
}

func (c *ShrinkCommand) getTableList(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`
		SELECT name FROM sqlite_master
		WHERE type='table'
		AND name NOT LIKE 'sqlite_%'
		AND name NOT LIKE 'load_test'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

func (c *ShrinkCommand) deleteFromTable(db *sql.DB, table string) (int64, error) {
	var totalRows int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	if err := db.QueryRow(countQuery).Scan(&totalRows); err != nil {
		return 0, fmt.Errorf("count rows: %w", err)
	}

	if totalRows == 0 {
		return 0, nil
	}

	rowsToDelete := int(float64(totalRows) * (c.DeletePercentage / 100))
	if rowsToDelete == 0 {
		return 0, nil
	}

	var hasID bool
	columnQuery := fmt.Sprintf("PRAGMA table_info(%s)", table)
	rows, err := db.Query(columnQuery)
	if err != nil {
		return 0, fmt.Errorf("get table info: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, dtype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &dtype, &notnull, &dflt, &pk); err != nil {
			continue
		}
		if name == "id" || pk == 1 {
			hasID = true
			break
		}
	}

	var deleteQuery string
	if hasID {
		deleteQuery = fmt.Sprintf(`
			DELETE FROM %s
			WHERE id IN (
				SELECT id FROM %s
				ORDER BY RANDOM()
				LIMIT %d
			)
		`, table, table, rowsToDelete)
	} else {
		deleteQuery = fmt.Sprintf(`
			DELETE FROM %s
			WHERE rowid IN (
				SELECT rowid FROM %s
				ORDER BY RANDOM()
				LIMIT %d
			)
		`, table, table, rowsToDelete)
	}

	startTime := time.Now()
	result, err := db.Exec(deleteQuery)
	if err != nil {
		return 0, fmt.Errorf("delete rows: %w", err)
	}

	rowsDeleted, _ := result.RowsAffected()
	duration := time.Since(startTime)

	slog.Debug("Deleted rows from table",
		"table", table,
		"rows_deleted", rowsDeleted,
		"duration", duration,
	)

	return rowsDeleted, nil
}

func (c *ShrinkCommand) runCheckpoint(db *sql.DB) error {
	slog.Info("Running checkpoint", "mode", c.CheckpointMode)

	startTime := time.Now()
	query := fmt.Sprintf("PRAGMA wal_checkpoint(%s)", c.CheckpointMode)

	var busy, written, total int
	err := db.QueryRow(query).Scan(&busy, &written, &total)
	if err != nil {
		return fmt.Errorf("checkpoint failed: %w", err)
	}

	duration := time.Since(startTime)
	slog.Info("Checkpoint complete",
		"mode", c.CheckpointMode,
		"busy", busy,
		"pages_written", written,
		"total_pages", total,
		"duration", duration,
	)

	return nil
}

func (c *ShrinkCommand) runVacuum(db *sql.DB) error {
	slog.Info("Running VACUUM (this may take a while)")

	startTime := time.Now()
	_, err := db.Exec("VACUUM")
	if err != nil {
		return fmt.Errorf("vacuum failed: %w", err)
	}

	duration := time.Since(startTime)
	slog.Info("VACUUM complete", "duration", duration)

	return nil
}

func (c *ShrinkCommand) Usage() {
	fmt.Fprintln(c.Main.Stdout, `
Shrink a database by deleting data and optionally running VACUUM.

Usage:

	litestream-test shrink [options]

Options:

	-db PATH
	    Database path (required)

	-delete-percentage PCT
	    Percentage of data to delete (0-100)
	    Default: 50

	-vacuum
	    Run VACUUM after deletion
	    Default: false

	-checkpoint
	    Run checkpoint after deletion
	    Default: false

	-checkpoint-mode MODE
	    Checkpoint mode (PASSIVE, FULL, RESTART, TRUNCATE)
	    Default: PASSIVE

Examples:

	# Delete 50% of data
	litestream-test shrink -db /tmp/test.db -delete-percentage 50

	# Delete 75% and run VACUUM
	litestream-test shrink -db /tmp/test.db -delete-percentage 75 -vacuum

	# Delete 30%, checkpoint, then VACUUM
	litestream-test shrink -db /tmp/test.db -delete-percentage 30 -checkpoint -vacuum

	# Test with FULL checkpoint mode
	litestream-test shrink -db /tmp/test.db -delete-percentage 50 -checkpoint -checkpoint-mode FULL
`[1:])
}
