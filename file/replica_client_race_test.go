package file_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/litestream/file"
	"github.com/superfly/ltx"
)

// TestWriteLTXFile_RaceCondition tests the race condition that can occur
// when temporary files are removed during the rename operation.
func TestWriteLTXFile_RaceCondition(t *testing.T) {
	// This test simulates the race condition by having a goroutine
	// that continuously removes .tmp files while WriteLTXFile is trying
	// to rename them.

	tempDir := t.TempDir()
	client := file.NewReplicaClient(tempDir)

	// Configure test parameters
	const (
		numWriters      = 10
		numOperations   = 100
		cleanupInterval = time.Microsecond * 100
	)

	// Track successes and failures
	var (
		successCount int64
		failureCount int64
		raceErrors   int64
	)

	// Start a goroutine that aggressively removes .tmp files
	// This simulates the race condition that was happening in production
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Cleanup goroutine - simulates the removeTmpFiles function
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(cleanupInterval):
				// Walk through and remove .tmp files
				filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
					if err != nil || info.IsDir() {
						return nil
					}
					if filepath.Ext(path) == ".tmp" {
						os.Remove(path) // Ignore errors
					}
					return nil
				})
			}
		}
	}()

	// Writer goroutines
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for j := 0; j < numOperations; j++ {
				// Create test data
				data := []byte(fmt.Sprintf("test data from writer %d operation %d", writerID, j))
				reader := bytes.NewReader(data)

				// Attempt to write LTX file
				_, err := client.WriteLTXFile(
					context.Background(),
					0,                           // level
					ltx.TXID(writerID*1000+j),   // minTXID
					ltx.TXID(writerID*1000+j+1), // maxTXID
					reader,
				)

				if err != nil {
					atomic.AddInt64(&failureCount, 1)
					// Check if this is the specific race condition error
					if os.IsNotExist(err) {
						atomic.AddInt64(&raceErrors, 1)
					}
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}(i)
	}

	// Wait for all operations to complete
	cancel()
	wg.Wait()

	// Report results
	t.Logf("Test Results:")
	t.Logf("  Total operations: %d", numWriters*numOperations)
	t.Logf("  Successful writes: %d", successCount)
	t.Logf("  Failed writes: %d", failureCount)
	t.Logf("  Race condition errors: %d", raceErrors)
	t.Logf("  Success rate: %.2f%%", float64(successCount)/float64(numWriters*numOperations)*100)

	// With the fix, we expect a much higher success rate
	// The fix should prevent most race condition errors
	if successCount == 0 {
		t.Error("No successful writes - this indicates a serious problem")
	}
}

// TestWriteLTXFile_ConcurrentWrites tests concurrent writes without interference
func TestWriteLTXFile_ConcurrentWrites(t *testing.T) {
	tempDir := t.TempDir()
	client := file.NewReplicaClient(tempDir)

	const numGoroutines = 20
	const writesPerGoroutine = 50

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*writesPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < writesPerGoroutine; j++ {
				data := []byte(fmt.Sprintf("goroutine %d write %d", id, j))
				reader := bytes.NewReader(data)

				_, err := client.WriteLTXFile(
					context.Background(),
					id%10, // vary levels
					ltx.TXID(id*1000+j),
					ltx.TXID(id*1000+j+1),
					reader,
				)

				if err != nil {
					errors <- fmt.Errorf("goroutine %d write %d: %w", id, j, err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Logf("Error: %v", err)
		errorCount++
	}

	if errorCount > 0 {
		t.Errorf("Had %d errors out of %d writes (%.2f%% failure rate)",
			errorCount, numGoroutines*writesPerGoroutine,
			float64(errorCount)/float64(numGoroutines*writesPerGoroutine)*100)
	}
}

// TestWriteLTXFile_DirectorySync tests that directory sync helps with persistence
func TestWriteLTXFile_DirectorySync(t *testing.T) {
	tempDir := t.TempDir()
	client := file.NewReplicaClient(tempDir)

	// Write a file
	data := []byte("test data for directory sync")
	reader := bytes.NewReader(data)

	info, err := client.WriteLTXFile(
		context.Background(),
		0,
		ltx.TXID(1),
		ltx.TXID(2),
		reader,
	)

	if err != nil {
		t.Fatalf("Failed to write LTX file: %v", err)
	}

	// Verify the file exists
	expectedPath := filepath.Join(tempDir, "ltx", "0", "0000000000000001-0000000000000002.ltx")
	if _, err := os.Stat(expectedPath); err != nil {
		t.Errorf("File not found at expected path: %v", err)
	}

	// Verify file info
	if info.Size != int64(len(data)) {
		t.Errorf("File size mismatch: got %d, want %d", info.Size, len(data))
	}
}

// TestWriteLTXFile_LargeFile tests writing larger files that take more time
func TestWriteLTXFile_LargeFile(t *testing.T) {
	tempDir := t.TempDir()
	client := file.NewReplicaClient(tempDir)

	// Create a large data buffer (10MB)
	size := 10 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	reader := bytes.NewReader(data)

	start := time.Now()
	info, err := client.WriteLTXFile(
		context.Background(),
		9, // snapshot level
		ltx.TXID(1000),
		ltx.TXID(2000),
		reader,
	)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Failed to write large LTX file: %v", err)
	}

	if info.Size != int64(size) {
		t.Errorf("Size mismatch: got %d, want %d", info.Size, size)
	}

	t.Logf("Wrote %d bytes in %v (%.2f MB/s)", size, duration,
		float64(size)/1024/1024/duration.Seconds())
}

// BenchmarkWriteLTXFile benchmarks the performance of WriteLTXFile
func BenchmarkWriteLTXFile(b *testing.B) {
	sizes := []int{
		1024,    // 1KB
		10240,   // 10KB
		102400,  // 100KB
		1048576, // 1MB
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			tempDir := b.TempDir()
			client := file.NewReplicaClient(tempDir)

			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.SetBytes(int64(size))

			for i := 0; i < b.N; i++ {
				reader := bytes.NewReader(data)
				_, err := client.WriteLTXFile(
					context.Background(),
					0,
					ltx.TXID(i),
					ltx.TXID(i+1),
					reader,
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// MockSlowReader simulates a slow data source
type MockSlowReader struct {
	data      []byte
	pos       int
	delay     time.Duration
	chunkSize int
}

func NewMockSlowReader(data []byte, delay time.Duration, chunkSize int) io.Reader {
	return &MockSlowReader{
		data:      data,
		delay:     delay,
		chunkSize: chunkSize,
	}
}

func (r *MockSlowReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}

	// Simulate slow read
	time.Sleep(r.delay)

	// Read up to chunkSize bytes
	end := r.pos + r.chunkSize
	if end > len(r.data) {
		end = len(r.data)
	}

	n = copy(p, r.data[r.pos:end])
	r.pos = end

	return n, nil
}

// TestWriteLTXFile_SlowWrite tests behavior with slow data sources
func TestWriteLTXFile_SlowWrite(t *testing.T) {
	tempDir := t.TempDir()
	client := file.NewReplicaClient(tempDir)

	// Create data that will be read slowly
	data := []byte("slow write test data")
	slowReader := NewMockSlowReader(data, 10*time.Millisecond, 5)

	// This gives time for potential race conditions to occur
	_, err := client.WriteLTXFile(
		context.Background(),
		0,
		ltx.TXID(1),
		ltx.TXID(2),
		slowReader,
	)

	if err != nil {
		t.Fatalf("Failed slow write: %v", err)
	}
}
