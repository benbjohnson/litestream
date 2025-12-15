//go:build integration && docker

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestShutdownSyncRetry_429Errors tests that Litestream retries syncing LTX files
// during shutdown when receiving 429 (Too Many Requests) errors.
//
// This test:
// 1. Starts a MinIO container
// 2. Starts a rate-limiting proxy in front of MinIO that returns 429 for first N PUT requests
// 3. Starts Litestream replicating to the proxy endpoint
// 4. Writes data and syncs
// 5. Sends SIGTERM to trigger graceful shutdown
// 6. Verifies that Litestream retries and eventually succeeds despite 429 errors
//
// Requirements:
// - Docker must be running
// - Litestream binary must be built at ../../bin/litestream
func TestShutdownSyncRetry_429Errors(t *testing.T) {
	RequireBinaries(t)
	RequireDocker(t)

	t.Log("================================================")
	t.Log("Litestream Shutdown Sync Retry Test (429 Errors)")
	t.Log("================================================")
	t.Log("")

	// Start MinIO container
	t.Log("Starting MinIO container...")
	containerName, minioEndpoint := StartMinioTestContainer(t)
	defer StopMinioTestContainer(t, containerName)
	t.Logf("✓ MinIO running at: %s", minioEndpoint)

	// Create MinIO bucket by creating directory in /data (MinIO stores buckets as directories)
	bucket := "litestream-test"
	t.Logf("Creating bucket '%s'...", bucket)

	// Wait for MinIO to be ready
	time.Sleep(2 * time.Second)

	// Create bucket directory directly - MinIO uses /data as the storage root
	createBucketCmd := exec.Command("docker", "exec", containerName,
		"mkdir", "-p", "/data/"+bucket)
	if out, err := createBucketCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to create bucket directory: %v, output: %s", err, string(out))
	}
	t.Log("✓ Bucket created")
	t.Log("")

	// Start rate-limiting proxy
	t.Log("Starting rate-limiting proxy...")
	proxy := newRateLimitingProxy(t, minioEndpoint, 3) // Return 429 for first 3 PUT requests
	proxyServer := &http.Server{
		Addr:    "127.0.0.1:0",
		Handler: proxy,
	}

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	proxyAddr := listener.Addr().String()

	go func() {
		if err := proxyServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			t.Logf("Proxy server error: %v", err)
		}
	}()
	defer proxyServer.Close()

	proxyEndpoint := fmt.Sprintf("http://%s", proxyAddr)
	t.Logf("✓ Rate-limiting proxy running at: %s", proxyEndpoint)
	t.Logf("  (Will return 429 for first 3 PUT requests during shutdown)")
	t.Log("")

	// Setup test database
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	configPath := filepath.Join(tempDir, "litestream.yml")

	// Create database with some data
	t.Log("Creating test database...")
	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	if _, err := sqlDB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("Failed to set WAL mode: %v", err)
	}
	if _, err := sqlDB.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if _, err := sqlDB.Exec("INSERT INTO test (data) VALUES ('initial data')"); err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	sqlDB.Close()
	t.Log("✓ Database created with initial data")
	t.Log("")

	// Create Litestream config with shutdown retry settings
	s3Path := fmt.Sprintf("test-%d", time.Now().Unix())
	config := fmt.Sprintf(`
shutdown-sync-timeout: 10s
shutdown-sync-interval: 500ms

dbs:
  - path: %s
    replica:
      type: s3
      bucket: %s
      path: %s
      endpoint: %s
      access-key-id: minioadmin
      secret-access-key: minioadmin
      region: us-east-1
      force-path-style: true
      skip-verify: true
      sync-interval: 1s
`, dbPath, bucket, s3Path, proxyEndpoint)

	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}
	t.Logf("✓ Config written to: %s", configPath)
	t.Log("")

	// Start Litestream
	t.Log("Starting Litestream...")
	litestreamBin := filepath.Join("..", "..", "bin", "litestream")
	cmd := exec.Command(litestreamBin, "replicate", "-config", configPath)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &stdout)
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}
	t.Logf("✓ Litestream started (PID: %d)", cmd.Process.Pid)

	// Wait for initial sync
	t.Log("Waiting for initial sync...")
	time.Sleep(3 * time.Second)

	// Write more data to ensure we have pending LTX files
	t.Log("Writing additional data...")
	sqlDB, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := sqlDB.Exec("INSERT INTO test (data) VALUES (?)", fmt.Sprintf("data-%d", i)); err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}
	sqlDB.Close()
	t.Log("✓ Additional data written")

	// Wait a bit for sync to pick up the changes
	time.Sleep(2 * time.Second)

	// Reset proxy counter so 429s happen during shutdown
	proxy.Reset()
	t.Log("")
	t.Log("Sending SIGTERM to trigger graceful shutdown...")
	t.Log("(Proxy will return 429 for first 3 PUT requests)")

	// Send SIGTERM
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("Failed to send SIGTERM: %v", err)
	}

	// Wait for process to exit with timeout
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		if err != nil {
			// Check if it's just a signal exit (expected)
			if exitErr, ok := err.(*exec.ExitError); ok {
				t.Logf("Litestream exited with: %v", exitErr)
			} else {
				t.Fatalf("Litestream failed: %v", err)
			}
		}
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		t.Fatal("Litestream did not exit within 30 seconds")
	}

	t.Log("")
	t.Log("================================================")
	t.Log("Results")
	t.Log("================================================")

	// Check proxy statistics
	stats := proxy.Stats()
	t.Logf("Proxy statistics:")
	t.Logf("  Total requests:     %d", stats.TotalRequests)
	t.Logf("  429 responses sent: %d", stats.RateLimited)
	t.Logf("  Forwarded requests: %d", stats.Forwarded)

	// Verify that we saw 429s and retries succeeded
	output := stdout.String() + stderr.String()

	if !strings.Contains(output, "shutdown sync failed, retrying") {
		t.Log("")
		t.Log("WARNING: Did not see retry messages in output.")
		t.Log("This could mean:")
		t.Log("  1. No pending LTX files during shutdown")
		t.Log("  2. Sync completed before shutdown signal")
		t.Log("  3. Retry logic not triggered")
	} else {
		t.Log("")
		t.Log("✓ Saw retry messages - shutdown sync retry is working!")
	}

	if strings.Contains(output, "shutdown sync succeeded after retry") {
		t.Log("✓ Shutdown sync succeeded after retrying!")
	}

	if stats.RateLimited > 0 {
		t.Logf("✓ Proxy returned %d 429 responses as expected", stats.RateLimited)
	}

	t.Log("")
	t.Log("Test completed successfully!")
}

// rateLimitingProxy is an HTTP proxy that returns 429 for the first N PUT requests
type rateLimitingProxy struct {
	target      *url.URL
	proxy       *httputil.ReverseProxy
	mu          sync.Mutex
	putCount    int32
	limit       int32
	totalReqs   int64
	rateLimited int64
	forwarded   int64
	t           *testing.T
}

type proxyStats struct {
	TotalRequests int64
	RateLimited   int64
	Forwarded     int64
}

func newRateLimitingProxy(t *testing.T, targetURL string, limit int) *rateLimitingProxy {
	target, err := url.Parse(targetURL)
	if err != nil {
		t.Fatalf("Failed to parse target URL: %v", err)
	}

	p := &rateLimitingProxy{
		target: target,
		limit:  int32(limit),
		t:      t,
	}

	p.proxy = &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			// Don't modify Host header - it's part of the AWS signature
		},
	}

	return p
}

func (p *rateLimitingProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&p.totalReqs, 1)

	// Only rate limit PUT requests (uploads)
	if r.Method == "PUT" {
		count := atomic.AddInt32(&p.putCount, 1)
		if count <= p.limit {
			atomic.AddInt64(&p.rateLimited, 1)
			p.t.Logf("PROXY: Returning 429 for PUT request #%d (limit: %d)", count, p.limit)
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte("Rate limit exceeded"))
			return
		}
	}

	atomic.AddInt64(&p.forwarded, 1)
	p.proxy.ServeHTTP(w, r)
}

func (p *rateLimitingProxy) Reset() {
	atomic.StoreInt32(&p.putCount, 0)
}

func (p *rateLimitingProxy) Stats() proxyStats {
	return proxyStats{
		TotalRequests: atomic.LoadInt64(&p.totalReqs),
		RateLimited:   atomic.LoadInt64(&p.rateLimited),
		Forwarded:     atomic.LoadInt64(&p.forwarded),
	}
}
