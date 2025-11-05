//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/benbjohnson/litestream"
)

type TestDB struct {
	Path          string
	ReplicaPath   string
	ReplicaURL    string
	ReplicaEnv    []string
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

func streamCommandOutput() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("SOAK_DEBUG")))
	switch v {
	case "", "0", "false", "off", "no":
		return false
	default:
		return true
	}
}

func configureCmdIO(cmd *exec.Cmd) (bool, *bytes.Buffer, *bytes.Buffer) {
	stream := streamCommandOutput()
	stdoutBuf := &bytes.Buffer{}
	stderrBuf := &bytes.Buffer{}
	if stream {
		cmd.Stdout = io.MultiWriter(os.Stdout, stdoutBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, stderrBuf)
	} else {
		cmd.Stdout = stdoutBuf
		cmd.Stderr = stderrBuf
	}
	return stream, stdoutBuf, stderrBuf
}

func combinedOutput(stdoutBuf, stderrBuf *bytes.Buffer) string {
	var sb strings.Builder
	if stdoutBuf != nil && stdoutBuf.Len() > 0 {
		sb.Write(stdoutBuf.Bytes())
	}
	if stderrBuf != nil && stderrBuf.Len() > 0 {
		sb.Write(stderrBuf.Bytes())
	}
	return strings.TrimSpace(sb.String())
}

func SetupTestDB(t *testing.T, name string) *TestDB {
	t.Helper()

	var tempDir string
	if os.Getenv("SOAK_KEEP_TEMP") != "" {
		dir, err := os.MkdirTemp("", fmt.Sprintf("litestream-%s-", name))
		if err != nil {
			t.Fatalf("create temp dir: %v", err)
		}
		tempDir = dir
		t.Cleanup(func() {
			t.Logf("SOAK_KEEP_TEMP set, preserving test artifacts at: %s", tempDir)
		})
	} else {
		tempDir = t.TempDir()
	}
	dbPath := filepath.Join(tempDir, fmt.Sprintf("%s.db", name))
	replicaPath := filepath.Join(tempDir, "replica")

	return &TestDB{
		Path:        dbPath,
		ReplicaPath: replicaPath,
		ReplicaURL:  fmt.Sprintf("file://%s", filepath.ToSlash(replicaPath)),
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

	_, stdoutBuf, stderrBuf := configureCmdIO(cmd)

	db.t.Logf("Populating database to %s...", targetSize)

	if err := cmd.Run(); err != nil {
		if output := combinedOutput(stdoutBuf, stderrBuf); output != "" {
			return fmt.Errorf("populate failed: %w\nOutput: %s", err, output)
		}
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

	_, stdoutBuf, stderrBuf := configureCmdIO(cmd)

	db.t.Logf("Populating database to %s (page size: %d, row size: %d)...", targetSize, pageSize, rowSize)

	if err := cmd.Run(); err != nil {
		if output := combinedOutput(stdoutBuf, stderrBuf); output != "" {
			return fmt.Errorf("populate failed: %w\nOutput: %s", err, output)
		}
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

	_, stdoutBuf, stderrBuf := configureCmdIO(cmd)

	db.t.Logf("Starting load generation: %d writes/sec for %v (%s pattern)", writeRate, duration, pattern)

	if err := cmd.Run(); err != nil {
		if output := combinedOutput(stdoutBuf, stderrBuf); output != "" {
			return fmt.Errorf("load generation failed: %w\nOutput: %s", err, output)
		}
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
	replicaURL := db.ReplicaURL
	if replicaURL == "" {
		replicaURL = fmt.Sprintf("file://%s", filepath.ToSlash(db.ReplicaPath))
	}
	var cmd *exec.Cmd
	if db.ConfigPath != "" && (strings.HasPrefix(replicaURL, "s3://") || strings.HasPrefix(replicaURL, "abs://") || strings.HasPrefix(replicaURL, "nats://")) {
		cmd = exec.Command(getBinaryPath("litestream"), "restore",
			"-config", db.ConfigPath,
			"-o", outputPath,
			db.Path,
		)
	} else {
		cmd = exec.Command(getBinaryPath("litestream"), "restore",
			"-o", outputPath,
			replicaURL,
		)
	}
	cmd.Env = append(os.Environ(), db.ReplicaEnv...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("restore failed: %w\nOutput: %s", err, string(output))
	}
	return nil
}

func (db *TestDB) Validate(restoredPath string) error {
	replicaURL := db.ReplicaURL
	if replicaURL == "" {
		replicaURL = fmt.Sprintf("file://%s", filepath.ToSlash(db.ReplicaPath))
	}
	cmd := exec.Command(getBinaryPath("litestream-test"), "validate",
		"-source-db", db.Path,
		"-replica-url", replicaURL,
		"-restored-db", restoredPath,
		"-check-type", "full",
	)
	cmd.Env = append(os.Environ(), db.ReplicaEnv...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("validation failed: %w\nOutput: %s", err, string(output))
	}
	return nil
}

func (db *TestDB) QuickValidate(restoredPath string) error {
	replicaURL := db.ReplicaURL
	if replicaURL == "" {
		replicaURL = fmt.Sprintf("file://%s", filepath.ToSlash(db.ReplicaPath))
	}
	cmd := exec.Command(getBinaryPath("litestream-test"), "validate",
		"-source-db", db.Path,
		"-replica-url", replicaURL,
		"-restored-db", restoredPath,
		"-check-type", "quick",
	)
	cmd.Env = append(os.Environ(), db.ReplicaEnv...)
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

// WaitForSnapshots waits for snapshots & WAL segments to appear on file replicas.
func (db *TestDB) WaitForSnapshots(timeout time.Duration) error {
	if !strings.HasPrefix(db.ReplicaURL, "file://") {
		return nil
	}

	snapshotDir := filepath.Join(db.ReplicaPath, "ltx", fmt.Sprintf("%d", litestream.SnapshotLevel))
	walDir := filepath.Join(db.ReplicaPath, "ltx", "0")

	deadline := time.Now().Add(timeout)
	for {
		snapshotCount := countLTXFiles(snapshotDir)
		walCount := countLTXFiles(walDir)

		if snapshotCount > 0 && walCount > 0 {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for replica data: snapshots=%d wal=%d", snapshotCount, walCount)
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func countLTXFiles(dir string) int {
	matches, err := filepath.Glob(filepath.Join(dir, "*.ltx"))
	if err != nil {
		return 0
	}
	return len(matches)
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

// WriteS3AccessPointConfig writes a minimal configuration file for S3 access point tests.
func WriteS3AccessPointConfig(t *testing.T, dbPath, replicaURL, endpoint string, forcePathStyle bool, accessKey, secretKey string) string {
	t.Helper()

	dir := filepath.Dir(dbPath)
	configPath := filepath.Join(dir, "litestream-access-point.yml")

	config := fmt.Sprintf(`access-key-id: %s
secret-access-key: %s

dbs:
  - path: %s
    replicas:
      - url: %s
        endpoint: %s
        region: us-east-1
        force-path-style: %t
        skip-verify: true
        sync-interval: 1s
`, accessKey, secretKey, filepath.ToSlash(dbPath), replicaURL, endpoint, forcePathStyle)

	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	return configPath
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

// IntegrityCheck runs PRAGMA integrity_check on the database.
func (db *TestDB) IntegrityCheck() error {
	sqlDB, err := sql.Open("sqlite3", db.Path)
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	var result string
	if err := sqlDB.QueryRow("PRAGMA integrity_check").Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("integrity check failed: %s", result)
	}
	return nil
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
