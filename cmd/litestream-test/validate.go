package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type ValidateCommand struct {
	Main *Main

	SourceDB      string
	ReplicaURL    string
	RestoredDB    string
	CheckType     string
	LTXContinuity bool
	ConfigPath    string
}

type ValidationResult struct {
	CheckType    string
	Passed       bool
	Duration     time.Duration
	ErrorMessage string
	Details      map[string]interface{}
}

func (c *ValidateCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-test validate", flag.ExitOnError)
	fs.StringVar(&c.SourceDB, "source-db", "", "Original database path")
	fs.StringVar(&c.ReplicaURL, "replica-url", "", "Replica URL to validate")
	fs.StringVar(&c.RestoredDB, "restored-db", "", "Path for restored database")
	fs.StringVar(&c.CheckType, "check-type", "quick", "Type of check (quick, integrity, checksum, full)")
	fs.BoolVar(&c.LTXContinuity, "ltx-continuity", false, "Check LTX file continuity")
	fs.StringVar(&c.ConfigPath, "config", "", "Litestream config file path")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if c.SourceDB == "" {
		return fmt.Errorf("source database path required")
	}

	if c.ReplicaURL == "" && c.ConfigPath == "" {
		return fmt.Errorf("replica URL or config file required")
	}

	if c.RestoredDB == "" {
		c.RestoredDB = c.SourceDB + ".restored"
	}

	slog.Info("Starting validation",
		"source_db", c.SourceDB,
		"replica_url", c.ReplicaURL,
		"check_type", c.CheckType,
		"ltx_continuity", c.LTXContinuity,
	)

	results := []ValidationResult{}

	if c.LTXContinuity && c.ReplicaURL != "" {
		result := c.validateLTXContinuity(ctx)
		results = append(results, result)
	}

	restoreResult := c.performRestore(ctx)
	results = append(results, restoreResult)

	if restoreResult.Passed {
		switch c.CheckType {
		case "quick":
			results = append(results, c.performQuickCheck(ctx))
		case "integrity":
			results = append(results, c.performIntegrityCheck(ctx))
		case "checksum":
			results = append(results, c.performChecksumCheck(ctx))
		case "full":
			results = append(results, c.performQuickCheck(ctx))
			results = append(results, c.performIntegrityCheck(ctx))
			results = append(results, c.performChecksumCheck(ctx))
			results = append(results, c.performDataValidation(ctx))
		}
	}

	return c.reportResults(results)
}

func (c *ValidateCommand) performRestore(ctx context.Context) ValidationResult {
	startTime := time.Now()
	result := ValidationResult{
		CheckType: "restore",
		Details:   make(map[string]interface{}),
	}

	if err := os.Remove(c.RestoredDB); err != nil && !os.IsNotExist(err) {
		slog.Warn("Could not remove existing restored database", "error", err)
	}

	var cmd *exec.Cmd
	if c.ConfigPath != "" {
		cmd = exec.CommandContext(ctx, "litestream", "restore",
			"-config", c.ConfigPath,
			"-o", c.RestoredDB,
			c.SourceDB,
		)
	} else {
		cmd = exec.CommandContext(ctx, "litestream", "restore",
			"-o", c.RestoredDB,
			c.ReplicaURL,
		)
	}

	output, err := cmd.CombinedOutput()
	result.Duration = time.Since(startTime)

	if err != nil {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("restore failed: %v\nOutput: %s", err, string(output))
		return result
	}

	if _, err := os.Stat(c.RestoredDB); err != nil {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("restored database not found: %v", err)
		return result
	}

	result.Passed = true
	result.Details["restored_path"] = c.RestoredDB

	if info, err := os.Stat(c.RestoredDB); err == nil {
		result.Details["restored_size"] = info.Size()
	}

	slog.Info("Restore completed",
		"duration", result.Duration,
		"restored_db", c.RestoredDB,
	)

	return result
}

func (c *ValidateCommand) performQuickCheck(ctx context.Context) ValidationResult {
	startTime := time.Now()
	result := ValidationResult{
		CheckType: "quick_check",
		Details:   make(map[string]interface{}),
	}

	db, err := sql.Open("sqlite3", c.RestoredDB)
	if err != nil {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("failed to open database: %v", err)
		return result
	}
	defer db.Close()

	var checkResult string
	err = db.QueryRow("PRAGMA quick_check").Scan(&checkResult)
	result.Duration = time.Since(startTime)

	if err != nil {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("quick check failed: %v", err)
		return result
	}

	result.Passed = checkResult == "ok"
	result.Details["check_result"] = checkResult

	if !result.Passed {
		result.ErrorMessage = fmt.Sprintf("quick check returned: %s", checkResult)
	}

	slog.Info("Quick check completed",
		"passed", result.Passed,
		"duration", result.Duration,
	)

	return result
}

func (c *ValidateCommand) performIntegrityCheck(ctx context.Context) ValidationResult {
	startTime := time.Now()
	result := ValidationResult{
		CheckType: "integrity_check",
		Details:   make(map[string]interface{}),
	}

	db, err := sql.Open("sqlite3", c.RestoredDB)
	if err != nil {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("failed to open database: %v", err)
		return result
	}
	defer db.Close()

	rows, err := db.Query("PRAGMA integrity_check")
	if err != nil {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("integrity check failed: %v", err)
		result.Duration = time.Since(startTime)
		return result
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			result.Passed = false
			result.ErrorMessage = fmt.Sprintf("failed to scan result: %v", err)
			result.Duration = time.Since(startTime)
			return result
		}
		results = append(results, line)
	}

	result.Duration = time.Since(startTime)
	result.Details["check_results"] = results

	if len(results) == 1 && results[0] == "ok" {
		result.Passed = true
	} else {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("integrity check found issues: %v", results)
	}

	slog.Info("Integrity check completed",
		"passed", result.Passed,
		"duration", result.Duration,
		"issues", len(results)-1,
	)

	return result
}

func (c *ValidateCommand) performChecksumCheck(ctx context.Context) ValidationResult {
	startTime := time.Now()
	result := ValidationResult{
		CheckType: "checksum",
		Details:   make(map[string]interface{}),
	}

	sourceChecksum, err := c.calculateDBChecksum(c.SourceDB)
	if err != nil {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("failed to calculate source checksum: %v", err)
		result.Duration = time.Since(startTime)
		return result
	}

	restoredChecksum, err := c.calculateDBChecksum(c.RestoredDB)
	if err != nil {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("failed to calculate restored checksum: %v", err)
		result.Duration = time.Since(startTime)
		return result
	}

	result.Duration = time.Since(startTime)
	result.Details["source_checksum"] = fmt.Sprintf("%x", sourceChecksum)
	result.Details["restored_checksum"] = fmt.Sprintf("%x", restoredChecksum)

	if string(sourceChecksum) == string(restoredChecksum) {
		result.Passed = true
	} else {
		result.Passed = false
		result.ErrorMessage = "checksums do not match"
	}

	slog.Info("Checksum check completed",
		"passed", result.Passed,
		"duration", result.Duration,
		"match", result.Passed,
	)

	return result
}

func (c *ValidateCommand) performDataValidation(ctx context.Context) ValidationResult {
	startTime := time.Now()
	result := ValidationResult{
		CheckType: "data_validation",
		Details:   make(map[string]interface{}),
	}

	sourceDB, err := sql.Open("sqlite3", c.SourceDB)
	if err != nil {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("failed to open source database: %v", err)
		return result
	}
	defer sourceDB.Close()

	restoredDB, err := sql.Open("sqlite3", c.RestoredDB)
	if err != nil {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("failed to open restored database: %v", err)
		return result
	}
	defer restoredDB.Close()

	tables, err := c.getTableList(sourceDB)
	if err != nil {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("failed to get table list: %v", err)
		result.Duration = time.Since(startTime)
		return result
	}

	result.Details["tables_checked"] = len(tables)
	allMatch := true

	for _, table := range tables {
		sourceCount, err := c.getRowCount(sourceDB, table)
		if err != nil {
			result.Passed = false
			result.ErrorMessage = fmt.Sprintf("failed to count rows in source table %s: %v", table, err)
			result.Duration = time.Since(startTime)
			return result
		}

		restoredCount, err := c.getRowCount(restoredDB, table)
		if err != nil {
			result.Passed = false
			result.ErrorMessage = fmt.Sprintf("failed to count rows in restored table %s: %v", table, err)
			result.Duration = time.Since(startTime)
			return result
		}

		if sourceCount != restoredCount {
			allMatch = false
			result.Details[fmt.Sprintf("table_%s_mismatch", table)] = fmt.Sprintf("source=%d, restored=%d", sourceCount, restoredCount)
		}
	}

	result.Duration = time.Since(startTime)
	result.Passed = allMatch

	if !allMatch {
		result.ErrorMessage = "row count mismatch between source and restored databases"
	}

	slog.Info("Data validation completed",
		"passed", result.Passed,
		"duration", result.Duration,
		"tables_checked", len(tables),
	)

	return result
}

func (c *ValidateCommand) validateLTXContinuity(ctx context.Context) ValidationResult {
	startTime := time.Now()
	result := ValidationResult{
		CheckType: "ltx_continuity",
		Details:   make(map[string]interface{}),
	}

	cmd := exec.CommandContext(ctx, "litestream", "ltx", c.ReplicaURL)
	output, err := cmd.Output()
	if err != nil {
		result.Passed = false
		result.ErrorMessage = fmt.Sprintf("failed to list LTX files: %v", err)
		result.Duration = time.Since(startTime)
		return result
	}

	lines := strings.Split(string(output), "\n")
	if len(lines) < 2 {
		result.Passed = false
		result.ErrorMessage = "no LTX files found"
		result.Duration = time.Since(startTime)
		return result
	}

	result.Passed = true
	result.Duration = time.Since(startTime)
	result.Details["ltx_files_checked"] = len(lines) - 2

	slog.Info("LTX continuity check completed",
		"passed", result.Passed,
		"duration", result.Duration,
	)

	return result
}

func (c *ValidateCommand) calculateDBChecksum(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return nil, err
	}

	return hash.Sum(nil), nil
}

func (c *ValidateCommand) getTableList(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
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

func (c *ValidateCommand) getRowCount(db *sql.DB, table string) (int, error) {
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	err := db.QueryRow(query).Scan(&count)
	return count, err
}

func (c *ValidateCommand) reportResults(results []ValidationResult) error {
	allPassed := true
	for _, result := range results {
		if !result.Passed {
			allPassed = false
			slog.Error("Validation failed",
				"check_type", result.CheckType,
				"error", result.ErrorMessage,
			)
		}
	}

	if allPassed {
		slog.Info("All validation checks passed")
		return nil
	}

	return fmt.Errorf("validation failed")
}

func (c *ValidateCommand) Usage() {
	fmt.Fprintln(c.Main.Stdout, `
Validate replication integrity by restoring and checking databases.

Usage:

	litestream-test validate [options]

Options:

	-source-db PATH
	    Original database path (required)

	-replica-url URL
	    Replica URL to validate

	-restored-db PATH
	    Path for restored database
	    Default: source-db.restored

	-check-type TYPE
	    Type of check: quick, integrity, checksum, full
	    Default: quick

	-ltx-continuity
	    Check LTX file continuity
	    Default: false

	-config PATH
	    Litestream config file path

Examples:

	# Quick validation
	litestream-test validate -source-db /tmp/test.db -replica-url s3://bucket/test

	# Full validation with all checks
	litestream-test validate -source-db /tmp/test.db -replica-url s3://bucket/test -check-type full

	# Validate with config file
	litestream-test validate -source-db /tmp/test.db -config /etc/litestream.yml -check-type integrity
`[1:])
}
