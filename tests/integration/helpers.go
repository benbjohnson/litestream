//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

// getBinaryPath returns the cross-platform path to a binary.
// On Windows, it adds the .exe extension.
func getBinaryPath(name string) string {
	binPath := filepath.Join("..", "..", "bin", name)
	if runtime.GOOS == "windows" {
		binPath += ".exe"
	}
	return binPath
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
	cmd := exec.Command(getBinaryPath("litestream-test"), "populate",
		"-db", db.Path,
		"-target-size", targetSize,
	)

	// Stream output to see progress
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	db.t.Logf("Populating database to %s...", targetSize)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("populate failed: %w", err)
	}
	return nil
}

func (db *TestDB) PopulateWithOptions(targetSize string, pageSize int, rowSize int) error {
	cmd := exec.Command(getBinaryPath("litestream-test"), "populate",
		"-db", db.Path,
		"-target-size", targetSize,
		"-page-size", fmt.Sprintf("%d", pageSize),
		"-row-size", fmt.Sprintf("%d", rowSize),
	)

	// Stream output to see progress
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	db.t.Logf("Populating database to %s (page size: %d, row size: %d)...", targetSize, pageSize, rowSize)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("populate failed: %w", err)
	}
	return nil
}

func (db *TestDB) GenerateLoad(ctx context.Context, writeRate int, duration time.Duration, pattern string) error {
	cmd := exec.CommandContext(ctx, getBinaryPath("litestream-test"), "load",
		"-db", db.Path,
		"-write-rate", fmt.Sprintf("%d", writeRate),
		"-duration", duration.String(),
		"-pattern", pattern,
	)

	// Stream output to see progress in real-time
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	db.t.Logf("Starting load generation: %d writes/sec for %v (%s pattern)", writeRate, duration, pattern)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("load generation failed: %w", err)
	}
	return nil
}

func (db *TestDB) StartLitestream() error {
	logPath := filepath.Join(db.TempDir, "litestream.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("create log file: %w", err)
	}

	replicaURL := fmt.Sprintf("file://%s", filepath.ToSlash(db.ReplicaPath))
	cmd := exec.Command(getBinaryPath("litestream"), "replicate",
		db.Path,
		replicaURL,
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
	cmd := exec.Command(getBinaryPath("litestream"), "replicate",
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
	replicaURL := fmt.Sprintf("file://%s", filepath.ToSlash(db.ReplicaPath))
	cmd := exec.Command(getBinaryPath("litestream"), "restore",
		"-o", outputPath,
		replicaURL,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("restore failed: %w\nOutput: %s", err, string(output))
	}
	return nil
}

func (db *TestDB) Validate(restoredPath string) error {
	replicaURL := fmt.Sprintf("file://%s", filepath.ToSlash(db.ReplicaPath))
	cmd := exec.Command(getBinaryPath("litestream-test"), "validate",
		"-source-db", db.Path,
		"-replica-url", replicaURL,
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
	replicaURL := fmt.Sprintf("file://%s", filepath.ToSlash(db.ReplicaPath))
	cmd := exec.Command(getBinaryPath("litestream-test"), "validate",
		"-source-db", db.Path,
		"-replica-url", replicaURL,
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

	litestreamBin := getBinaryPath("litestream")
	if _, err := os.Stat(litestreamBin); err != nil {
		t.Skip("litestream binary not found, run: go build -o bin/litestream ./cmd/litestream")
	}

	litestreamTestBin := getBinaryPath("litestream-test")
	if _, err := os.Stat(litestreamTestBin); err != nil {
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

// PrintTestSummary prints a summary of the test results
func (db *TestDB) PrintTestSummary(t *testing.T, testName string, startTime time.Time) {
	t.Helper()

	duration := time.Since(startTime)
	dbSize, _ := db.GetDatabaseSize()
	fileCount, _ := db.GetReplicaFileCount()
	errors, _ := db.CheckForErrors()

	t.Log("\n" + strings.Repeat("=", 80))
	t.Logf("TEST SUMMARY: %s", testName)
	t.Log(strings.Repeat("=", 80))
	t.Logf("Duration:           %v", duration.Round(time.Second))
	t.Logf("Database Size:      %.2f MB", float64(dbSize)/(1024*1024))
	t.Logf("Replica Files:      %d LTX files", fileCount)
	t.Logf("Litestream Errors:  %d", len(errors))
	t.Log(strings.Repeat("=", 80))
}
