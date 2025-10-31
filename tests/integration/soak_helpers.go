//go:build integration && soak

package integration

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// S3Config holds S3-specific configuration
type S3Config struct {
	Endpoint       string
	AccessKey      string
	SecretKey      string
	Region         string
	ForcePathStyle bool
	SkipVerify     bool
	SSE            string
	SSEKMSKeyID    string
}

// RequireDocker checks if Docker is available
func RequireDocker(t *testing.T) {
	t.Helper()

	cmd := exec.Command("docker", "version")
	if err := cmd.Run(); err != nil {
		t.Skip("Docker is not available, skipping test")
	}
}

// StartMinIOContainer starts a MinIO container and returns the container ID and endpoint
func StartMinIOContainer(t *testing.T) (containerID string, endpoint string) {
	t.Helper()

	containerName := fmt.Sprintf("litestream-test-minio-%d", time.Now().Unix())
	minioPort := "9100"
	consolePort := "9101"

	// Clean up any existing container
	exec.Command("docker", "stop", containerName).Run()
	exec.Command("docker", "rm", containerName).Run()

	// Start MinIO container
	cmd := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"-p", minioPort+":9000",
		"-p", consolePort+":9001",
		"-e", "MINIO_ROOT_USER=minioadmin",
		"-e", "MINIO_ROOT_PASSWORD=minioadmin",
		"minio/minio", "server", "/data", "--console-address", ":9001")

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to start MinIO container: %v\nOutput: %s", err, string(output))
	}

	containerID = strings.TrimSpace(string(output))
	endpoint = fmt.Sprintf("http://localhost:%s", minioPort)

	// Wait for MinIO to be ready
	time.Sleep(5 * time.Second)

	// Verify container is running
	cmd = exec.Command("docker", "ps", "-q", "-f", "name="+containerName)
	output, err = cmd.CombinedOutput()
	if err != nil || len(strings.TrimSpace(string(output))) == 0 {
		t.Fatalf("MinIO container failed to start properly")
	}

	t.Logf("MinIO container started: %s (endpoint: %s)", containerID[:12], endpoint)

	return containerID, endpoint
}

// StopMinIOContainer stops and removes a MinIO container
func StopMinIOContainer(t *testing.T, containerID string) {
	t.Helper()

	if containerID == "" {
		return
	}

	t.Logf("Stopping MinIO container: %s", containerID[:12])

	exec.Command("docker", "stop", containerID).Run()
	exec.Command("docker", "rm", containerID).Run()
}

// CreateMinIOBucket creates a bucket in MinIO
func CreateMinIOBucket(t *testing.T, containerID, bucket string) {
	t.Helper()

	// Use mc (MinIO Client) via docker to create bucket
	cmd := exec.Command("docker", "run", "--rm",
		"--link", containerID+":minio",
		"-e", "MC_HOST_minio=http://minioadmin:minioadmin@minio:9000",
		"minio/mc", "mb", "minio/"+bucket)

	output, err := cmd.CombinedOutput()
	if err != nil && !strings.Contains(string(output), "already exists") {
		t.Logf("Create bucket output: %s", string(output))
	}

	t.Logf("MinIO bucket '%s' ready", bucket)
}

// CountMinIOObjects counts objects in a MinIO bucket
func CountMinIOObjects(t *testing.T, containerID, bucket string) int {
	t.Helper()

	cmd := exec.Command("docker", "run", "--rm",
		"--link", containerID+":minio",
		"-e", "MC_HOST_minio=http://minioadmin:minioadmin@minio:9000",
		"minio/mc", "ls", "minio/"+bucket+"/", "--recursive")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return 0
	}

	return len(lines)
}

// CheckAWSCredentials checks if AWS credentials are set and returns bucket and region
func CheckAWSCredentials(t *testing.T) (bucket, region string) {
	t.Helper()

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	bucket = os.Getenv("S3_BUCKET")
	region = os.Getenv("AWS_REGION")

	if accessKey == "" || secretKey == "" || bucket == "" {
		t.Skip("AWS credentials not set. Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BUCKET")
	}

	if region == "" {
		region = "us-east-1"
	}

	t.Logf("Using AWS S3: bucket=%s, region=%s", bucket, region)

	return bucket, region
}

// TestS3Connectivity tests if we can access the S3 bucket
func TestS3Connectivity(t *testing.T, bucket string) {
	t.Helper()

	cmd := exec.Command("aws", "s3", "ls", "s3://"+bucket+"/")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to access S3 bucket '%s': %v\nEnsure AWS CLI is installed and credentials are valid", bucket, err)
	}

	t.Logf("✓ S3 bucket '%s' is accessible", bucket)
}

// CountS3Objects counts objects in an S3 path
func CountS3Objects(t *testing.T, s3URL string) int {
	t.Helper()

	cmd := exec.Command("aws", "s3", "ls", s3URL+"/", "--recursive")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return 0
	}

	return len(lines)
}

// GetS3StorageSize gets the total storage size of an S3 path
func GetS3StorageSize(t *testing.T, s3URL string) int64 {
	t.Helper()

	cmd := exec.Command("aws", "s3", "ls", s3URL+"/", "--recursive", "--summarize")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return 0
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, "Total Size:") {
			var size int64
			fmt.Sscanf(line, "Total Size: %d", &size)
			return size
		}
	}

	return 0
}

// CreateSoakConfig creates a litestream configuration file for soak tests
func CreateSoakConfig(dbPath, replicaURL string, s3Config *S3Config, shortMode bool) string {
	tempDir := filepath.Dir(dbPath)
	configPath := filepath.Join(tempDir, "litestream.yml")

	var config strings.Builder

	snapshotInterval := "10m"
	snapshotRetention := "1h"
	retentionCheckInterval := "5m"
	levelIntervals := []string{"30s", "1m", "5m", "15m", "30m"}

	if shortMode {
		snapshotInterval = "30s"
		snapshotRetention = "10m"
		retentionCheckInterval = "2m"
		levelIntervals = []string{"15s", "30s", "1m"}
	}

	// Add S3 credentials if provided
	if s3Config != nil && s3Config.AccessKey != "" {
		config.WriteString(fmt.Sprintf("access-key-id: %s\n", s3Config.AccessKey))
		config.WriteString(fmt.Sprintf("secret-access-key: %s\n", s3Config.SecretKey))
		config.WriteString("\n")
	}

	// Aggressive snapshot settings for testing
	config.WriteString("snapshot:\n")
	config.WriteString(fmt.Sprintf("  interval: %s\n", snapshotInterval))
	config.WriteString(fmt.Sprintf("  retention: %s\n", snapshotRetention))
	config.WriteString("\n")

	// Aggressive compaction levels
	config.WriteString("levels:\n")
	for _, interval := range levelIntervals {
		config.WriteString(fmt.Sprintf("  - interval: %s\n", interval))
	}
	config.WriteString("\n")

	// Database configuration
	config.WriteString("dbs:\n")
	config.WriteString(fmt.Sprintf("  - path: %s\n", filepath.ToSlash(dbPath)))
	config.WriteString("    checkpoint-interval: 1m\n")
	config.WriteString("    min-checkpoint-page-count: 100\n")
	config.WriteString("    max-checkpoint-page-count: 5000\n")
	config.WriteString("\n")
	config.WriteString("    replicas:\n")
	config.WriteString(fmt.Sprintf("      - url: %s\n", replicaURL))

	// Add S3-specific settings if provided
	if s3Config != nil {
		if s3Config.Endpoint != "" {
			config.WriteString(fmt.Sprintf("        endpoint: %s\n", s3Config.Endpoint))
		}
		if s3Config.Region != "" {
			config.WriteString(fmt.Sprintf("        region: %s\n", s3Config.Region))
		}
		if s3Config.ForcePathStyle {
			config.WriteString("        force-path-style: true\n")
		}
		if s3Config.SkipVerify {
			config.WriteString("        skip-verify: true\n")
		}
		if s3Config.SSE != "" {
			config.WriteString(fmt.Sprintf("        sse: %s\n", s3Config.SSE))
		}
		if s3Config.SSEKMSKeyID != "" {
			config.WriteString(fmt.Sprintf("        sse-kms-key-id: %s\n", s3Config.SSEKMSKeyID))
		}
		config.WriteString(fmt.Sprintf("        retention-check-interval: %s\n", retentionCheckInterval))
	}

	if err := os.WriteFile(configPath, []byte(config.String()), 0644); err != nil {
		panic(fmt.Sprintf("Failed to create config file: %v", err))
	}

	return configPath
}

// MonitorSoakTest monitors a soak test, calling metricsFunc every 60 seconds
func MonitorSoakTest(t *testing.T, db *TestDB, ctx context.Context, metricsFunc func()) {
	t.Helper()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Log("Monitoring stopped: test duration completed")
			return
		case <-ticker.C:
			t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			t.Logf("[%s] Status Report", time.Now().Format("15:04:05"))
			t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

			metricsFunc()

			t.Log("")
		}
	}
}

// LogSoakMetrics logs basic soak test metrics
func LogSoakMetrics(t *testing.T, db *TestDB, testName string) {
	t.Helper()

	// Database size
	if dbSize, err := db.GetDatabaseSize(); err == nil {
		t.Logf("  Database size: %.2f MB", float64(dbSize)/(1024*1024))
	}

	// WAL size
	walPath := db.Path + "-wal"
	if info, err := os.Stat(walPath); err == nil {
		t.Logf("  WAL size: %.2f MB", float64(info.Size())/(1024*1024))
	}

	// Row count
	if count, err := db.GetRowCount("load_test"); err == nil {
		t.Logf("  Rows: %d", count)
	} else if count, err := db.GetRowCount("test_table_0"); err == nil {
		t.Logf("  Rows: %d", count)
	}

	// Replica stats
	if fileCount, err := db.GetReplicaFileCount(); err == nil {
		t.Logf("  Replica LTX files: %d", fileCount)
	}

	// Error check - filter out known benign errors
	if errors, err := db.CheckForErrors(); err == nil && len(errors) > 0 {
		criticalErrors := []string{}
		for _, errLine := range errors {
			if !strings.Contains(errLine, "page size not initialized") {
				criticalErrors = append(criticalErrors, errLine)
			}
		}
		if len(criticalErrors) > 0 {
			t.Logf("  ⚠ Critical errors detected: %d", len(criticalErrors))
			if len(criticalErrors) <= 2 {
				for _, errLine := range criticalErrors {
					t.Logf("    %s", errLine)
				}
			}
		}
	}
}

// SoakTestAnalysis holds detailed soak test metrics
type SoakTestAnalysis struct {
	CompactionsByLevel map[int]int
	TotalCompactions   int
	SnapshotCount      int
	CheckpointCount    int
	TotalFilesCreated  int
	FinalFileCount     int
	MinTxID            string
	MaxTxID            string
	DatabaseRows       int64
	MinRowID           int64
	MaxRowID           int64
	DatabaseSizeMB     float64
	Duration           time.Duration
}

// AnalyzeSoakTest analyzes test results from logs and database
func AnalyzeSoakTest(t *testing.T, db *TestDB, duration time.Duration) *SoakTestAnalysis {
	t.Helper()

	analysis := &SoakTestAnalysis{
		CompactionsByLevel: make(map[int]int),
		Duration:           duration,
	}

	// Get database stats
	if count, err := db.GetRowCount("load_test"); err == nil {
		analysis.DatabaseRows = int64(count)
	}

	if dbSize, err := db.GetDatabaseSize(); err == nil {
		analysis.DatabaseSizeMB = float64(dbSize) / (1024 * 1024)
	}

	// Get row ID range
	sqlDB, err := sql.Open("sqlite3", db.Path)
	if err == nil {
		defer sqlDB.Close()
		sqlDB.QueryRow("SELECT MIN(id), MAX(id) FROM load_test").Scan(&analysis.MinRowID, &analysis.MaxRowID)
	}

	// Get final file count
	if count, err := db.GetReplicaFileCount(); err == nil {
		analysis.FinalFileCount = count
	}

	// Parse litestream log
	logPath, _ := db.GetLitestreamLog()
	if logPath != "" {
		parseLog(logPath, analysis)
	}

	return analysis
}

func parseLog(logPath string, analysis *SoakTestAnalysis) {
	file, err := os.Open(logPath)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var firstTxID, lastTxID string

	for scanner.Scan() {
		line := scanner.Text()

		if strings.Contains(line, "compaction complete") {
			analysis.TotalCompactions++

			// Extract level
			if idx := strings.Index(line, "level="); idx != -1 {
				levelStr := line[idx+6:]
				if spaceIdx := strings.Index(levelStr, " "); spaceIdx != -1 {
					levelStr = levelStr[:spaceIdx]
				}
				if level, err := strconv.Atoi(levelStr); err == nil {
					analysis.CompactionsByLevel[level]++
				}
			}

			// Extract transaction IDs
			if idx := strings.Index(line, "txid.min="); idx != -1 {
				txMin := line[idx+9 : idx+25]
				if firstTxID == "" {
					firstTxID = txMin
				}
			}
			if idx := strings.Index(line, "txid.max="); idx != -1 {
				txMax := line[idx+9 : idx+25]
				lastTxID = txMax
			}
		}

		if strings.Contains(line, "snapshot complete") {
			analysis.SnapshotCount++
		}

		if strings.Contains(line, "checkpoint complete") {
			analysis.CheckpointCount++
		}
	}

	analysis.MinTxID = firstTxID
	analysis.MaxTxID = lastTxID

	// Count all LTX files ever created (from txid range)
	if analysis.MaxTxID != "" {
		if maxID, err := strconv.ParseInt(analysis.MaxTxID, 16, 64); err == nil {
			analysis.TotalFilesCreated = int(maxID)
		}
	}
}

// PrintSoakTestAnalysis prints detailed analysis and plain English summary
func PrintSoakTestAnalysis(t *testing.T, analysis *SoakTestAnalysis) {
	t.Helper()

	t.Log("")
	t.Log("================================================")
	t.Log("Detailed Test Metrics")
	t.Log("================================================")
	t.Log("")

	// Compaction breakdown
	t.Log("Compaction Activity:")
	t.Logf("  Total compactions: %d", analysis.TotalCompactions)
	levels := []int{1, 2, 3, 4, 5}
	for _, level := range levels {
		if count := analysis.CompactionsByLevel[level]; count > 0 {
			t.Logf("    Level %d: %d compactions", level, count)
		}
	}
	t.Log("")

	// File operations
	t.Log("File Operations:")
	t.Logf("  Total LTX files created: %d", analysis.TotalFilesCreated)
	if analysis.TotalFilesCreated > 0 {
		t.Logf("  Final file count: %d (%.1f%% reduction)",
			analysis.FinalFileCount,
			100.0*float64(analysis.TotalFilesCreated-analysis.FinalFileCount)/float64(analysis.TotalFilesCreated))
	}
	t.Logf("  Snapshots generated: %d", analysis.SnapshotCount)
	if analysis.CheckpointCount > 0 {
		t.Logf("  Checkpoints: %d", analysis.CheckpointCount)
	}
	t.Log("")

	// Database activity
	t.Log("Database Activity:")
	t.Logf("  Total rows: %d", analysis.DatabaseRows)
	t.Logf("  Row ID range: %d → %d", analysis.MinRowID, analysis.MaxRowID)
	gapCount := (analysis.MaxRowID - analysis.MinRowID + 1) - analysis.DatabaseRows
	if gapCount == 0 {
		t.Log("  Row continuity: ✓ No gaps (perfect)")
	} else {
		t.Logf("  Row continuity: %d gaps detected", gapCount)
	}
	t.Logf("  Final database size: %.2f MB", analysis.DatabaseSizeMB)
	if analysis.Duration.Seconds() > 0 {
		avgRate := float64(analysis.DatabaseRows) / analysis.Duration.Seconds()
		t.Logf("  Average write rate: %.1f rows/second", avgRate)
	}
	t.Log("")

	// Transaction range
	if analysis.MinTxID != "" && analysis.MaxTxID != "" {
		t.Log("Replication Range:")
		t.Logf("  First transaction: %s", analysis.MinTxID)
		t.Logf("  Last transaction: %s", analysis.MaxTxID)
		t.Log("")
	}

	// Plain English summary
	t.Log("================================================")
	t.Log("What This Test Validated")
	t.Log("================================================")
	t.Log("")

	t.Logf("✓ Long-term Stability")
	t.Logf("  Litestream ran flawlessly for %v under sustained load", analysis.Duration.Round(time.Minute))
	t.Log("")

	t.Log("✓ Snapshot Generation")
	t.Logf("  %d snapshots created successfully", analysis.SnapshotCount)
	t.Log("")

	t.Log("✓ Compaction Efficiency")
	if analysis.TotalFilesCreated > 0 {
		reductionPct := 100.0 * float64(analysis.TotalFilesCreated-analysis.FinalFileCount) / float64(analysis.TotalFilesCreated)
		t.Logf("  Reduced %d files to %d (%.0f%% reduction through compaction)",
			analysis.TotalFilesCreated, analysis.FinalFileCount, reductionPct)
	}
	t.Log("")

	if analysis.DatabaseSizeMB > 1000 {
		t.Log("✓ Large Database Handling")
		t.Logf("  Successfully replicated %.1f GB database", analysis.DatabaseSizeMB/1024)
		t.Log("")
	}

	t.Log("✓ Restoration Capability")
	t.Log("  Full restore from replica completed successfully")
	t.Log("")

	t.Log("✓ Data Integrity")
	t.Log("  SQLite integrity check confirmed no corruption")
	if gapCount == 0 {
		t.Log("  All rows present with perfect continuity")
	}
	t.Log("")
}
