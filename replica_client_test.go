package litestream_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
	"github.com/benbjohnson/litestream/s3"
)

// createLTXData creates a minimal valid LTX file with a header for testing.
// The data parameter is appended after the header for testing purposes.
func createLTXData(minTXID, maxTXID ltx.TXID, data []byte) []byte {
	hdr := ltx.Header{
		Version:   1,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: time.Now().UnixMilli(),
	}

	headerBytes, _ := hdr.MarshalBinary()
	return append(headerBytes, data...)
}

func TestReplicaClient_LTX(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Helper()
		t.Parallel()

		// Write files out of order to check for sorting.
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(4), ltx.TXID(8), bytes.NewReader(createLTXData(4, 8, []byte(`67`)))); err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(1), bytes.NewReader(createLTXData(1, 1, []byte(``)))); err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(9), ltx.TXID(9), bytes.NewReader(createLTXData(9, 9, []byte(`xyz`)))); err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(2), ltx.TXID(3), bytes.NewReader(createLTXData(2, 3, []byte(`12345`)))); err != nil {
			t.Fatal(err)
		}

		itr, err := c.LTXFiles(context.Background(), 0, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		// Read all items and ensure they are sorted.
		a, err := ltx.SliceFileIterator(itr)
		if err != nil {
			t.Fatal(err)
		} else if got, want := len(a), 4; got != want {
			t.Fatalf("len=%v, want %v", got, want)
		}

		// Check that files are sorted by MinTXID (Size no longer checked since we add LTX headers)
		if got, want := a[0].MinTXID, ltx.TXID(1); got != want {
			t.Fatalf("Index[0].MinTXID=%v, want %v", got, want)
		}
		if got, want := a[0].MaxTXID, ltx.TXID(1); got != want {
			t.Fatalf("Index[0].MaxTXID=%v, want %v", got, want)
		}
		if got, want := a[1].MinTXID, ltx.TXID(2); got != want {
			t.Fatalf("Index[1].MinTXID=%v, want %v", got, want)
		}
		if got, want := a[1].MaxTXID, ltx.TXID(3); got != want {
			t.Fatalf("Index[1].MaxTXID=%v, want %v", got, want)
		}
		if got, want := a[2].MinTXID, ltx.TXID(4); got != want {
			t.Fatalf("Index[2].MinTXID=%v, want %v", got, want)
		}
		if got, want := a[2].MaxTXID, ltx.TXID(8); got != want {
			t.Fatalf("Index[2].MaxTXID=%v, want %v", got, want)
		}
		if got, want := a[3].MinTXID, ltx.TXID(9); got != want {
			t.Fatalf("Index[3].MinTXID=%v, want %v", got, want)
		}
		if got, want := a[3].MaxTXID, ltx.TXID(9); got != want {
			t.Fatalf("Index[3].MaxTXID=%v, want %v", got, want)
		}

		if err := itr.Close(); err != nil {
			t.Fatal(err)
		}
	})

	RunWithReplicaClient(t, "NoWALs", func(t *testing.T, c litestream.ReplicaClient) {
		t.Helper()
		t.Parallel()

		itr, err := c.LTXFiles(context.Background(), 0, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatal("expected no wal files")
		}
	})
}

func TestReplicaClient_WriteLTXFile(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Helper()
		t.Parallel()

		testData := []byte(`foobar`)
		ltxData := createLTXData(1, 2, testData)
		expectedContent := ltxData

		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2), bytes.NewReader(expectedContent)); err != nil {
			t.Fatal(err)
		}

		r, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2), 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()

		buf, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}

		if err := r.Close(); err != nil {
			t.Fatal(err)
		}

		if got, want := string(buf), string(expectedContent); got != want {
			t.Fatalf("data=%q, want %q", got, want)
		}
	})
}

func TestReplicaClient_OpenLTXFile(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Helper()
		t.Parallel()

		testData := []byte(`foobar`)
		ltxData := createLTXData(1, 2, testData)
		expectedContent := ltxData

		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2), bytes.NewReader(expectedContent)); err != nil {
			t.Fatal(err)
		}

		r, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2), 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if buf, err := io.ReadAll(r); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), string(expectedContent); got != want {
			t.Fatalf("ReadAll=%v, want %v", got, want)
		}
	})

	RunWithReplicaClient(t, "ErrNotFound", func(t *testing.T, c litestream.ReplicaClient) {
		t.Helper()
		t.Parallel()

		if _, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(1), 0, 0); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
	})
}

func TestReplicaClient_DeleteWALSegments(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Helper()
		t.Parallel()

		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2), bytes.NewReader(createLTXData(1, 2, []byte(`foo`)))); err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(3), ltx.TXID(4), bytes.NewReader(createLTXData(3, 4, []byte(`bar`)))); err != nil {
			t.Fatal(err)
		}

		if err := c.DeleteLTXFiles(context.Background(), []*ltx.FileInfo{
			{Level: 0, MinTXID: 1, MaxTXID: 2},
			{Level: 0, MinTXID: 3, MaxTXID: 4},
		}); err != nil {
			t.Fatal(err)
		}

		if _, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2), 0, 0); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
		if _, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(3), ltx.TXID(4), 0, 0); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
	})
}

// RunWithReplicaClient executes fn with each replica specified by the -integration flag
func RunWithReplicaClient(t *testing.T, name string, fn func(*testing.T, litestream.ReplicaClient)) {
	t.Run(name, func(t *testing.T) {
		for _, typ := range testingutil.ReplicaClientTypes() {
			t.Run(typ, func(t *testing.T) {
				if !testingutil.Integration() {
					t.Skip("skipping integration test, use -integration flag to run")
				}

				c := testingutil.NewReplicaClient(t, typ)
				defer testingutil.MustDeleteAll(t, c)

				fn(t, c)
			})
		}
	})
}

// TestReplicaClient_TimestampPreservation verifies that LTX file timestamps are preserved
// during write and read operations. This is critical for point-in-time restoration (#771).
func TestReplicaClient_TimestampPreservation(t *testing.T) {
	RunWithReplicaClient(t, "PreservesTimestamp", func(t *testing.T, c litestream.ReplicaClient) {
		t.Helper()
		t.Parallel()

		ctx := context.Background()

		// Create an LTX file with a specific timestamp
		// Use a timestamp from the past to ensure it's different from write time
		expectedTimestamp := time.Now().Add(-1 * time.Hour).Truncate(time.Millisecond)

		// Create a minimal LTX file with header containing the timestamp
		hdr := ltx.Header{
			Version:   1,
			PageSize:  4096,
			Commit:    1,
			MinTXID:   1,
			MaxTXID:   1,
			Timestamp: expectedTimestamp.UnixMilli(),
		}

		// Marshal LTX header
		headerBytes, err := hdr.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}

		// Combine header with minimal body to create valid LTX file
		buf := bytes.NewReader(headerBytes)

		// Write to storage backend
		info, err := c.WriteLTXFile(ctx, 0, ltx.TXID(1), ltx.TXID(1), buf)
		if err != nil {
			t.Fatal(err)
		}

		// For File backend, timestamp should be preserved immediately
		// For cloud backends (S3, GCS, Azure, NATS), timestamp is stored in metadata
		// Verify the returned FileInfo has correct timestamp
		if info.CreatedAt.IsZero() {
			t.Fatal("WriteLTXFile returned zero timestamp")
		}

		// Read back via LTXFiles and verify timestamp is preserved
		itr, err := c.LTXFiles(ctx, 0, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		var found *ltx.FileInfo
		for itr.Next() {
			item := itr.Item()
			if item.MinTXID == 1 && item.MaxTXID == 1 {
				found = item
				break
			}
		}
		if err := itr.Close(); err != nil {
			t.Fatal(err)
		}

		if found == nil {
			t.Fatal("LTX file not found in iteration")
		}

		// Verify timestamp was preserved (allow 1 second drift for precision)
		timeDiff := found.CreatedAt.Sub(expectedTimestamp)
		if timeDiff.Abs() > time.Second {
			t.Errorf("Timestamp not preserved: expected %v, got %v (diff: %v)",
				expectedTimestamp, found.CreatedAt, timeDiff)
		}
	})
}

// TestReplicaClient_S3_UploaderConfig tests S3 uploader configuration for large files
func TestReplicaClient_S3_UploaderConfig(t *testing.T) {
	// Only run for S3 integration tests
	if !slices.Contains(testingutil.ReplicaClientTypes(), "s3") {
		t.Skip("Skipping S3-specific uploader config test")
	}

	RunWithReplicaClient(t, "LargeFileWithCustomConfig", func(t *testing.T, c litestream.ReplicaClient) {
		t.Helper()
		t.Parallel()

		// Type assert to S3 client to set custom config
		s3Client, ok := c.(*s3.ReplicaClient)
		if !ok {
			t.Skip("Not an S3 client")
		}

		// Set custom upload configuration
		s3Client.PartSize = 5 * 1024 * 1024 // 5MB parts
		s3Client.Concurrency = 3            // 3 concurrent parts

		// Determine file size based on whether we're testing against moto or real S3
		// Moto has issue #8762 where composite checksums for multipart uploads
		// don't have the -X suffix, causing checksum validation to fail.
		// Reference: https://github.com/getmoto/moto/issues/8762
		size := 10 * 1024 * 1024 // 10MB - triggers multipart upload

		// If we're using moto (localhost endpoint), use smaller file to avoid multipart
		if s3Client.Endpoint != "" && strings.Contains(s3Client.Endpoint, "127.0.0.1") {
			size = 4 * 1024 * 1024 // 4MB - avoids multipart upload with moto
			t.Log("Using 4MB file size to work around moto multipart checksum issue")
		} else {
			t.Log("Using 10MB file size to test multipart upload")
		}
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		// Upload the file using bytes.Reader to avoid string conversion issues
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(100), bytes.NewReader(data)); err != nil {
			t.Fatalf("failed to write large file: %v", err)
		}

		// Read it back and verify size
		r, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(100), 0, 0)
		if err != nil {
			t.Fatalf("failed to open large file: %v", err)
		}
		defer r.Close()

		buf, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("failed to read large file: %v", err)
		}

		if len(buf) != size {
			t.Errorf("size mismatch: got %d, want %d", len(buf), size)
		}

		// Verify the data matches what we uploaded
		if !bytes.Equal(buf, data) {
			t.Errorf("data mismatch: uploaded and downloaded data do not match")
		}
	})
}

// TestReplicaClient_S3_ErrorContext tests that S3 errors include helpful context
func TestReplicaClient_S3_ErrorContext(t *testing.T) {
	// Only run for S3 integration tests
	if !slices.Contains(testingutil.ReplicaClientTypes(), "s3") {
		t.Skip("Skipping S3-specific error context test")
	}

	RunWithReplicaClient(t, "ErrorContext", func(t *testing.T, c litestream.ReplicaClient) {
		t.Helper()
		t.Parallel()

		// Test OpenLTXFile with non-existent file
		_, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(999), ltx.TXID(999), 0, 0)
		if err == nil {
			t.Fatal("expected error for non-existent file")
		}

		// Should return os.ErrNotExist for S3 NoSuchKey
		if !os.IsNotExist(err) {
			t.Errorf("expected os.ErrNotExist, got %v", err)
		}
	})
}

// TestReplicaClient_S3_BucketValidation tests bucket validation in S3 client
func TestReplicaClient_S3_BucketValidation(t *testing.T) {
	// Only run for S3 integration tests
	if !slices.Contains(testingutil.ReplicaClientTypes(), "s3") {
		t.Skip("Skipping S3-specific bucket validation test")
	}

	// Create a new S3 client with empty bucket
	c := testingutil.NewS3ReplicaClient(t)
	c.Bucket = ""

	// Should fail with bucket validation error
	err := c.Init(context.Background())
	if err == nil {
		t.Fatal("expected error for empty bucket name")
	}
	if !strings.Contains(err.Error(), "bucket name is required") {
		t.Errorf("expected bucket validation error, got: %v", err)
	}
}
