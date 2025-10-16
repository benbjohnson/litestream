//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type TestDB struct {
	Path          string
	ReplicaPath   string
	ConfigPath    string
	TempDir       string
	LitestreamCmd *exec.Cmd
	LitestreamPID int
	t             *testing.T
}

func SetupTestDB(t *testing.T, name string) *TestDB {
	t.Helper()

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, fmt.Sprintf("%s.db", name))
	replicaPath := filepath.Join(tempDir, "replica")

	return &TestDB{
		Path:        dbPath,
		ReplicaPath: replicaPath,
		TempDir:     tempDir,
		t:           t,
	}
}

func (db *TestDB) Create() error {
	sqlDB, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer sqlDB.Close()

	if _, err := sqlDB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("set WAL mode: %w", err)
	}

	return nil
}

func (db *TestDB) CreateWithPageSize(pageSize int) error {
	sqlDB, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer sqlDB.Close()

	if _, err := sqlDB.Exec(fmt.Sprintf("PRAGMA page_size = %d", pageSize)); err != nil {
		return fmt.Errorf("set page size: %w", err)
	}

	if _, err := sqlDB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("set WAL mode: %w", err)
	}

	return nil
}

func (db *TestDB) Populate(targetSize string) error {
	cmd := exec.Command("../../bin/litestream-test", "populate",
		"-db", db.Path,
		"-target-size", targetSize,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("populate failed: %w\nOutput: %s", err, string(output))
	}
	return nil
}

func (db *TestDB) PopulateWithOptions(targetSize string, pageSize int, rowSize int) error {
	cmd := exec.Command("../../bin/litestream-test", "populate",
		"-db", db.Path,
		"-target-size", targetSize,
		"-page-size", fmt.Sprintf("%d", pageSize),
		"-row-size", fmt.Sprintf("%d", rowSize),
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("populate failed: %w\nOutput: %s", err, string(output))
	}
	return nil
}

func (db *TestDB) GenerateLoad(ctx context.Context, writeRate int, duration time.Duration, pattern string) error {
	cmd := exec.CommandContext(ctx, "../../bin/litestream-test", "load",
		"-db", db.Path,
		"-write-rate", fmt.Sprintf("%d", writeRate),
		"-duration", duration.String(),
		"-pattern", pattern,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("load generation failed: %w\nOutput: %s", err, string(output))
	}
	return nil
}

func (db *TestDB) StartLitestream() error {
	logPath := filepath.Join(db.TempDir, "litestream.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("create log file: %w", err)
	}

	cmd := exec.Command("../../bin/litestream", "replicate",
		db.Path,
		fmt.Sprintf("file://%s", db.ReplicaPath),
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("start litestream: %w", err)
	}

	db.LitestreamCmd = cmd
	db.LitestreamPID = cmd.Process.Pid

	time.Sleep(2 * time.Second)

	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		logFile.Close()
		return fmt.Errorf("litestream exited immediately")
	}

	return nil
}

func (db *TestDB) StartLitestreamWithConfig(configPath string) error {
	logPath := filepath.Join(db.TempDir, "litestream.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("create log file: %w", err)
	}

	db.ConfigPath = configPath
	cmd := exec.Command("../../bin/litestream", "replicate",
		"-config", configPath,
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("start litestream: %w", err)
	}

	db.LitestreamCmd = cmd
	db.LitestreamPID = cmd.Process.Pid

	time.Sleep(2 * time.Second)

	return nil
}

func (db *TestDB) StopLitestream() error {
	if db.LitestreamCmd == nil || db.LitestreamCmd.Process == nil {
		return nil
	}

	if err := db.LitestreamCmd.Process.Kill(); err != nil {
		return fmt.Errorf("kill litestream: %w", err)
	}

	db.LitestreamCmd.Wait()
	time.Sleep(1 * time.Second)

	return nil
}

func (db *TestDB) Restore(outputPath string) error {
	cmd := exec.Command("../../bin/litestream", "restore",
		"-o", outputPath,
		fmt.Sprintf("file://%s", db.ReplicaPath),
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("restore failed: %w\nOutput: %s", err, string(output))
	}
	return nil
}

func (db *TestDB) Validate(restoredPath string) error {
	cmd := exec.Command("../../bin/litestream-test", "validate",
		"-source-db", db.Path,
		"-replica-url", fmt.Sprintf("file://%s", db.ReplicaPath),
		"-restored-db", restoredPath,
		"-check-type", "full",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("validation failed: %w\nOutput: %s", err, string(output))
	}
	return nil
}

func (db *TestDB) QuickValidate(restoredPath string) error {
	cmd := exec.Command("../../bin/litestream-test", "validate",
		"-source-db", db.Path,
		"-replica-url", fmt.Sprintf("file://%s", db.ReplicaPath),
		"-restored-db", restoredPath,
		"-check-type", "quick",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("validation failed: %w\nOutput: %s", err, string(output))
	}
	return nil
}

func (db *TestDB) GetRowCount(table string) (int, error) {
	sqlDB, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		return 0, fmt.Errorf("open database: %w", err)
	}
	defer sqlDB.Close()

	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	if err := sqlDB.QueryRow(query).Scan(&count); err != nil {
		return 0, fmt.Errorf("query count: %w", err)
	}

	return count, nil
}

func (db *TestDB) GetDatabaseSize() (int64, error) {
	info, err := os.Stat(db.Path)
	if err != nil {
		return 0, err
	}

	size := info.Size()

	walPath := db.Path + "-wal"
	if walInfo, err := os.Stat(walPath); err == nil {
		size += walInfo.Size()
	}

	return size, nil
}

func (db *TestDB) GetReplicaFileCount() (int, error) {
	ltxPath := filepath.Join(db.ReplicaPath, "ltx", "0")
	files, err := filepath.Glob(filepath.Join(ltxPath, "*.ltx"))
	if err != nil {
		return 0, err
	}
	return len(files), nil
}

func (db *TestDB) GetLitestreamLog() (string, error) {
	logPath := filepath.Join(db.TempDir, "litestream.log")
	content, err := os.ReadFile(logPath)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func (db *TestDB) CheckForErrors() ([]string, error) {
	log, err := db.GetLitestreamLog()
	if err != nil {
		return nil, err
	}

	var errors []string
	lines := strings.Split(log, "\n")
	for _, line := range lines {
		if strings.Contains(strings.ToUpper(line), "ERROR") {
			errors = append(errors, line)
		}
	}

	return errors, nil
}

func (db *TestDB) Cleanup() {
	db.StopLitestream()
}

func GetTestDuration(t *testing.T, defaultDuration time.Duration) time.Duration {
	t.Helper()

	if testing.Short() {
		return defaultDuration / 10
	}

	return defaultDuration
}

func RequireBinaries(t *testing.T) {
	t.Helper()

	if _, err := os.Stat("../../bin/litestream"); err != nil {
		t.Skip("litestream binary not found, run: go build -o bin/litestream ./cmd/litestream")
	}

	if _, err := os.Stat("../../bin/litestream-test"); err != nil {
		t.Skip("litestream-test binary not found, run: go build -o bin/litestream-test ./cmd/litestream-test")
	}
}

func CreateTestTable(t *testing.T, dbPath string) error {
	t.Helper()

	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	_, err = sqlDB.Exec(`
		CREATE TABLE IF NOT EXISTS test_data (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			data TEXT,
			created_at INTEGER
		)
	`)
	return err
}

func InsertTestData(t *testing.T, dbPath string, count int) error {
	t.Helper()

	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	tx, err := sqlDB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO test_data (data, created_at) VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := 0; i < count; i++ {
		if _, err := stmt.Exec(fmt.Sprintf("test data %d", i), time.Now().Unix()); err != nil {
			return err
		}
	}

	return tx.Commit()
}
