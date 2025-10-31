//go:build integration && soak

package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

	// Error check
	if errors, err := db.CheckForErrors(); err == nil && len(errors) > 0 {
		t.Logf("  ⚠ Errors detected: %d", len(errors))
		if len(errors) <= 2 {
			for _, errLine := range errors {
				t.Logf("    %s", errLine)
			}
		}
	}
}
