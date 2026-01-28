package litestream_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"
	"golang.org/x/crypto/ssh"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
	"github.com/benbjohnson/litestream/s3"
)

// createLTXData creates a minimal valid LTX file with a header for testing.
// The data parameter is appended after the header for testing purposes.
func createLTXData(minTXID, maxTXID ltx.TXID, data []byte) []byte {
	return createLTXDataWithTimestamp(minTXID, maxTXID, time.Now(), data)
}

func createLTXDataWithTimestamp(minTXID, maxTXID ltx.TXID, ts time.Time, data []byte) []byte {
	hdr := ltx.Header{
		Version:   ltx.Version,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: ts.UnixMilli(),
	}
	if minTXID == 1 {
		// Snapshot files do not include a checksum.
		hdr.PreApplyChecksum = 0
	} else {
		hdr.PreApplyChecksum = ltx.ChecksumFlag
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

	// Regression test: LTXFiles should return files that span across the seek TXID.
	// A file with minTXID < seek but maxTXID >= seek should be included because
	// it contains TXIDs that are >= seek. This bug caused compaction gaps when
	// a compacted file at level N spanned a range that started before the seek
	// point but contained newer TXIDs that level N+1 needed.
	RunWithReplicaClient(t, "SeekWithSpanningFile", func(t *testing.T, c litestream.ReplicaClient) {
		t.Helper()
		t.Parallel()

		// Write a file that spans a large range (simulates a compacted file)
		// minTXID=100, maxTXID=200
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(100), ltx.TXID(200), bytes.NewReader(createLTXData(100, 200, []byte(`spanning`)))); err != nil {
			t.Fatal(err)
		}

		// Write a file after the spanning file
		// minTXID=201, maxTXID=210
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(201), ltx.TXID(210), bytes.NewReader(createLTXData(201, 210, []byte(`after`)))); err != nil {
			t.Fatal(err)
		}

		// Seek to TXID 150, which is in the middle of the spanning file.
		// The spanning file (100-200) should be returned because it contains
		// TXIDs >= 150 (specifically 150-200).
		itr, err := c.LTXFiles(context.Background(), 0, ltx.TXID(150), false)
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		a, err := ltx.SliceFileIterator(itr)
		if err != nil {
			t.Fatal(err)
		}

		// Should return 2 files: the spanning file (100-200) and the file after (201-210)
		if got, want := len(a), 2; got != want {
			t.Fatalf("len=%v, want %v; spanning file should be included when seek is within its range", got, want)
		}

		// First file should be the spanning file
		if got, want := a[0].MinTXID, ltx.TXID(100); got != want {
			t.Fatalf("a[0].MinTXID=%v, want %v", got, want)
		}
		if got, want := a[0].MaxTXID, ltx.TXID(200); got != want {
			t.Fatalf("a[0].MaxTXID=%v, want %v", got, want)
		}

		// Second file should be the one after
		if got, want := a[1].MinTXID, ltx.TXID(201); got != want {
			t.Fatalf("a[1].MinTXID=%v, want %v", got, want)
		}
		if got, want := a[1].MaxTXID, ltx.TXID(210); got != want {
			t.Fatalf("a[1].MaxTXID=%v, want %v", got, want)
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

		if _, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(1), 0, 0); !errors.Is(err, os.ErrNotExist) {
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

		if _, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2), 0, 0); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected not exist, got %#v", err)
		}
		if _, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(3), ltx.TXID(4), 0, 0); !errors.Is(err, os.ErrNotExist) {
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

		ltxData := createLTXDataWithTimestamp(1, 1, expectedTimestamp, []byte("payload"))
		info, err := c.WriteLTXFile(ctx, 0, ltx.TXID(1), ltx.TXID(1), bytes.NewReader(ltxData))
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
		itr, err := c.LTXFiles(ctx, 0, 0, true)
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

		// All backends preserve timestamps in metadata (see issue #771)
		// Verify timestamp was preserved (allow 1 second drift for precision)
		timeDiff := found.CreatedAt.Sub(expectedTimestamp)
		if timeDiff.Abs() > time.Second {
			t.Errorf("Timestamp not preserved for backend %T: expected %v, got %v (diff: %v)",
				c, expectedTimestamp, found.CreatedAt, timeDiff)
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
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i % 256)
		}
		ltxData := createLTXData(1, 100, payload)

		// Upload the file using bytes.Reader to avoid string conversion issues
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(100), bytes.NewReader(ltxData)); err != nil {
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

		if len(buf) != len(ltxData) {
			t.Errorf("size mismatch: got %d, want %d", len(buf), len(ltxData))
		}

		// Verify the data matches what we uploaded
		if !bytes.Equal(buf, ltxData) {
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
		if !errors.Is(err, os.ErrNotExist) {
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

// TestReplicaClient_S3_UnsignedPayloadRejected verifies that unsigned payloads
// are rejected by real AWS S3. This is a negative test that documents the
// expected behavior and ensures we don't accidentally ship unsigned payload
// support for AWS S3.
//
// See issue #911 - AWS S3 requires signed payloads and returns
// SignatureDoesNotMatch for unsigned payload requests.
func TestReplicaClient_S3_UnsignedPayloadRejected(t *testing.T) {
	// Only run for S3 integration tests
	if !slices.Contains(testingutil.ReplicaClientTypes(), "s3") {
		t.Skip("Skipping S3-specific test")
	}

	// Skip if using mock endpoint (moto accepts unsigned payloads)
	if endpoint := os.Getenv("LITESTREAM_S3_ENDPOINT"); endpoint != "" {
		t.Skip("Skipping negative test with mock endpoint (moto accepts unsigned)")
	}

	// Create client directly (not via test helper) to control SignPayload
	c := s3.NewReplicaClient()
	c.AccessKeyID = os.Getenv("LITESTREAM_S3_ACCESS_KEY_ID")
	c.SecretAccessKey = os.Getenv("LITESTREAM_S3_SECRET_ACCESS_KEY")
	c.Region = os.Getenv("LITESTREAM_S3_REGION")
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	c.Bucket = os.Getenv("LITESTREAM_S3_BUCKET")
	c.Path = fmt.Sprintf("negative-test/%016x", rand.Uint64())

	// Force unsigned payloads - this should fail with real AWS
	c.SignPayload = false

	ctx := context.Background()
	if err := c.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Attempt to write - should fail with signature error
	ltxData := createLTXData(1, 1, []byte("test"))
	_, err := c.WriteLTXFile(ctx, 0, ltx.TXID(1), ltx.TXID(1), bytes.NewReader(ltxData))

	if err == nil {
		t.Fatal("expected unsigned payload to be rejected by AWS S3, but upload succeeded")
	}

	// Verify it's a signature-related error
	errStr := strings.ToLower(err.Error())
	if !strings.Contains(errStr, "signature") && !strings.Contains(errStr, "accessdenied") {
		t.Errorf("expected signature-related error, got: %v", err)
	}

	t.Logf("Correctly rejected unsigned payload with error: %v", err)
}

func TestReplicaClient_SFTP_HostKeyValidation(t *testing.T) {
	testHostKeyPEM := `-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACAJytPhncDnpV5QF3ai8f6r0u1hzK96x+81tvtA7ZiuawAAAJAIcGGVCHBh
lQAAAAtzc2gtZWQyNTUxOQAAACAJytPhncDnpV5QF3ai8f6r0u1hzK96x+81tvtA7Ziuaw
AAAEDzV1D6COyvFGhSiZa6ll9aXZ2IMWED3KGrvCNjEEtYHwnK0+GdwOelXlAXdqLx/qvS
7WHMr3rH7zW2+0DtmK5rAAAADGZlbGl4QGJvcmVhcwE=
-----END OPENSSH PRIVATE KEY-----`
	privateKey, err := ssh.ParsePrivateKey([]byte(testHostKeyPEM))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("ValidHostKey", func(t *testing.T) {
		addr := testingutil.MockSFTPServer(t, privateKey)
		expectedHostKey := string(ssh.MarshalAuthorizedKey(privateKey.PublicKey()))

		c := testingutil.NewSFTPReplicaClient(t)
		c.User = "foo"
		c.Host = addr
		c.HostKey = expectedHostKey

		err = c.Init(context.Background())
		if err != nil {
			t.Fatalf("SFTP connection failed: %v", err)
		}
	})
	t.Run("InvalidHostKey", func(t *testing.T) {
		addr := testingutil.MockSFTPServer(t, privateKey)
		invalidHostKey := "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEqM2NkGvKKhR1oiKO0E72L3tOsYk+aX7H8Xn4bbZKsa"

		c := testingutil.NewSFTPReplicaClient(t)
		c.User = "foo"
		c.Host = addr
		c.HostKey = invalidHostKey

		err = c.Init(context.Background())
		if err == nil {
			t.Fatalf("SFTP connection established despite invalid host key")
		}
		if !strings.Contains(err.Error(), "ssh: host key mismatch") {
			t.Errorf("expected host key validation error, got: %v", err)
		}
	})
	t.Run("IgnoreHostKey", func(t *testing.T) {
		var captured []string
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
			Level: slog.LevelWarn,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.MessageKey {
					captured = append(captured, a.Value.String())
				}
				return a
			},
		})))

		addr := testingutil.MockSFTPServer(t, privateKey)

		c := testingutil.NewSFTPReplicaClient(t)
		c.User = "foo"
		c.Host = addr

		err = c.Init(context.Background())
		if err != nil {
			t.Fatalf("SFTP connection failed: %v", err)
		}

		if !slices.ContainsFunc(captured, func(msg string) bool {
			return strings.Contains(msg, "sftp host key not verified")
		}) {
			t.Errorf("Expected warning not found")
		}

	})
}

// TestReplicaClient_S3_MultipartThresholds tests multipart upload behavior at various
// size thresholds. These tests are critical for catching S3-compatible provider issues
// like #940, #941, #947 where multipart uploads fail with certain providers.
//
// NOTE: These tests skip moto due to multipart checksum validation issues (moto#8762).
// They should be run against real cloud providers using the manual integration workflow.
func TestReplicaClient_S3_MultipartThresholds(t *testing.T) {
	if !slices.Contains(testingutil.ReplicaClientTypes(), "s3") {
		t.Skip("Skipping S3-specific multipart threshold tests")
	}

	// Skip if using mock endpoint (moto has multipart checksum issues)
	if endpoint := os.Getenv("LITESTREAM_S3_ENDPOINT"); endpoint != "" {
		if strings.Contains(endpoint, "127.0.0.1") || strings.Contains(endpoint, "localhost") {
			t.Skip("Skipping multipart tests with mock endpoint (moto has checksum issues)")
		}
	}

	tests := []struct {
		name     string
		sizeMB   int
		partSize int64
	}{
		{
			name:     "AtThreshold_5MB",
			sizeMB:   5,
			partSize: 5 * 1024 * 1024,
		},
		{
			name:     "AboveThreshold_10MB",
			sizeMB:   10,
			partSize: 5 * 1024 * 1024,
		},
		{
			name:     "Large_50MB",
			sizeMB:   50,
			partSize: 10 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !testingutil.Integration() {
				t.Skip("skipping integration test, use -integration flag to run")
			}

			c := testingutil.NewS3ReplicaClient(t)
			c.Path = fmt.Sprintf("multipart-test/%016x", rand.Uint64())
			c.PartSize = tt.partSize
			c.Concurrency = 3
			defer testingutil.MustDeleteAll(t, c)

			ctx := context.Background()
			if err := c.Init(ctx); err != nil {
				t.Fatalf("Init() error: %v", err)
			}

			size := tt.sizeMB * 1024 * 1024
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 256)
			}
			ltxData := createLTXData(1, 100, payload)

			t.Logf("Testing %dMB file with %dMB parts", tt.sizeMB, tt.partSize/(1024*1024))

			if _, err := c.WriteLTXFile(ctx, 0, ltx.TXID(1), ltx.TXID(100), bytes.NewReader(ltxData)); err != nil {
				t.Fatalf("WriteLTXFile() error: %v", err)
			}

			r, err := c.OpenLTXFile(ctx, 0, ltx.TXID(1), ltx.TXID(100), 0, 0)
			if err != nil {
				t.Fatalf("OpenLTXFile() error: %v", err)
			}
			defer r.Close()

			buf, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("ReadAll() error: %v", err)
			}

			if len(buf) != len(ltxData) {
				t.Errorf("size mismatch: got %d, want %d", len(buf), len(ltxData))
			}

			if !bytes.Equal(buf, ltxData) {
				t.Errorf("data mismatch: uploaded and downloaded data do not match")
			}
		})
	}
}

// TestReplicaClient_S3_ConcurrencyLimits tests that concurrency limits are respected
// during multipart uploads. This is important for providers like Cloudflare R2 that
// have strict concurrent upload limits (issue #948).
func TestReplicaClient_S3_ConcurrencyLimits(t *testing.T) {
	if !slices.Contains(testingutil.ReplicaClientTypes(), "s3") {
		t.Skip("Skipping S3-specific concurrency test")
	}

	// Skip if using mock endpoint
	if endpoint := os.Getenv("LITESTREAM_S3_ENDPOINT"); endpoint != "" {
		if strings.Contains(endpoint, "127.0.0.1") || strings.Contains(endpoint, "localhost") {
			t.Skip("Skipping concurrency test with mock endpoint")
		}
	}

	if !testingutil.Integration() {
		t.Skip("skipping integration test, use -integration flag to run")
	}

	concurrencyLevels := []int{1, 2, 5}

	for _, concurrency := range concurrencyLevels {
		t.Run(fmt.Sprintf("Concurrency_%d", concurrency), func(t *testing.T) {
			c := testingutil.NewS3ReplicaClient(t)
			c.Path = fmt.Sprintf("concurrency-test/%016x", rand.Uint64())
			c.PartSize = 5 * 1024 * 1024
			c.Concurrency = concurrency
			defer testingutil.MustDeleteAll(t, c)

			ctx := context.Background()
			if err := c.Init(ctx); err != nil {
				t.Fatalf("Init() error: %v", err)
			}

			size := 15 * 1024 * 1024
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 256)
			}
			ltxData := createLTXData(1, 100, payload)

			t.Logf("Testing 15MB file with concurrency=%d", concurrency)

			if _, err := c.WriteLTXFile(ctx, 0, ltx.TXID(1), ltx.TXID(100), bytes.NewReader(ltxData)); err != nil {
				t.Fatalf("WriteLTXFile() with concurrency=%d error: %v", concurrency, err)
			}

			r, err := c.OpenLTXFile(ctx, 0, ltx.TXID(1), ltx.TXID(100), 0, 0)
			if err != nil {
				t.Fatalf("OpenLTXFile() error: %v", err)
			}
			defer r.Close()

			buf, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("ReadAll() error: %v", err)
			}

			if !bytes.Equal(buf, ltxData) {
				t.Errorf("data mismatch at concurrency=%d", concurrency)
			}
		})
	}
}

// TestReplicaClient_PITR_ManyLTXFiles tests point-in-time restore with many LTX files.
// This is a regression test for issue #930 where HeadObject calls with 100+ LTX files
// caused the restore operation to hang.
func TestReplicaClient_PITR_ManyLTXFiles(t *testing.T) {
	tests := []struct {
		name      string
		fileCount int
		timeout   time.Duration
	}{
		{"100_Files", 100, 2 * time.Minute},
		{"500_Files", 500, 5 * time.Minute},
		{"1000_Files", 1000, 10 * time.Minute},
	}

	for _, tt := range tests {
		RunWithReplicaClient(t, tt.name, func(t *testing.T, c litestream.ReplicaClient) {
			t.Helper()

			// Skip very long tests unless explicitly enabled
			if tt.fileCount > 100 && os.Getenv("LITESTREAM_PITR_STRESS_TEST") == "" {
				t.Skipf("Skipping %d file stress test (set LITESTREAM_PITR_STRESS_TEST=1 to enable)", tt.fileCount)
			}

			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()

			baseTime := time.Now().Add(-time.Duration(tt.fileCount) * time.Minute)
			t.Logf("Creating %d LTX files starting from %v", tt.fileCount, baseTime)

			// Create snapshot at TXID 1
			snapshot := createLTXDataWithTimestamp(1, 1, baseTime, []byte("snapshot"))
			if _, err := c.WriteLTXFile(ctx, litestream.SnapshotLevel, 1, 1, bytes.NewReader(snapshot)); err != nil {
				t.Fatalf("WriteLTXFile(snapshot): %v", err)
			}

			// Create many L0 files with incrementing timestamps
			for i := 2; i <= tt.fileCount; i++ {
				ts := baseTime.Add(time.Duration(i-1) * time.Minute)
				data := createLTXDataWithTimestamp(ltx.TXID(i), ltx.TXID(i), ts, []byte(fmt.Sprintf("file-%d", i)))
				if _, err := c.WriteLTXFile(ctx, 0, ltx.TXID(i), ltx.TXID(i), bytes.NewReader(data)); err != nil {
					t.Fatalf("WriteLTXFile(%d): %v", i, err)
				}
				if i%100 == 0 {
					t.Logf("Created %d/%d files", i, tt.fileCount)
				}
			}

			// Test 1: Iterate all L0 files without metadata (fast path)
			t.Log("Testing L0 file iteration without metadata")
			startFast := time.Now()
			itr, err := c.LTXFiles(ctx, 0, 0, false)
			if err != nil {
				t.Fatalf("LTXFiles(useMetadata=false): %v", err)
			}
			var countFast int
			for itr.Next() {
				countFast++
			}
			if err := itr.Close(); err != nil {
				t.Fatalf("Iterator close: %v", err)
			}
			durationFast := time.Since(startFast)
			t.Logf("Fast iteration: %d files in %v", countFast, durationFast)

			if countFast != tt.fileCount-1 {
				t.Errorf("Fast iteration count: got %d, want %d", countFast, tt.fileCount-1)
			}

			// Test 2: Iterate all L0 files with metadata (required for PITR)
			// This is the path that was hanging in issue #930
			t.Log("Testing L0 file iteration with metadata (PITR path)")
			startMeta := time.Now()
			itrMeta, err := c.LTXFiles(ctx, 0, 0, true)
			if err != nil {
				t.Fatalf("LTXFiles(useMetadata=true): %v", err)
			}
			var countMeta int
			for itrMeta.Next() {
				countMeta++
			}
			if err := itrMeta.Close(); err != nil {
				t.Fatalf("Iterator close: %v", err)
			}
			durationMeta := time.Since(startMeta)
			t.Logf("Metadata iteration: %d files in %v", countMeta, durationMeta)

			if countMeta != tt.fileCount-1 {
				t.Errorf("Metadata iteration count: got %d, want %d", countMeta, tt.fileCount-1)
			}

			// Verify metadata iteration completed within reasonable time
			// (issue #930 caused this to hang indefinitely)
			if durationMeta > tt.timeout/2 {
				t.Errorf("Metadata iteration took too long: %v (should be < %v)", durationMeta, tt.timeout/2)
			}
		})
	}
}

// TestReplicaClient_PITR_TimestampFiltering tests that PITR correctly filters files
// by timestamp across a range of LTX files.
func TestReplicaClient_PITR_TimestampFiltering(t *testing.T) {
	RunWithReplicaClient(t, "TimestampFilter", func(t *testing.T, c litestream.ReplicaClient) {
		t.Helper()

		ctx := context.Background()
		fileCount := 50
		baseTime := time.Now().Add(-time.Duration(fileCount) * time.Minute)

		// Create snapshot at TXID 1
		snapshot := createLTXDataWithTimestamp(1, 1, baseTime, []byte("snapshot"))
		if _, err := c.WriteLTXFile(ctx, litestream.SnapshotLevel, 1, 1, bytes.NewReader(snapshot)); err != nil {
			t.Fatalf("WriteLTXFile(snapshot): %v", err)
		}

		// Create L0 files with known timestamps
		for i := 2; i <= fileCount; i++ {
			ts := baseTime.Add(time.Duration(i-1) * time.Minute)
			data := createLTXDataWithTimestamp(ltx.TXID(i), ltx.TXID(i), ts, []byte(fmt.Sprintf("file-%d", i)))
			if _, err := c.WriteLTXFile(ctx, 0, ltx.TXID(i), ltx.TXID(i), bytes.NewReader(data)); err != nil {
				t.Fatalf("WriteLTXFile(%d): %v", i, err)
			}
		}

		// Test filtering at various timestamp points
		testPoints := []struct {
			name        string
			offsetMins  int
			expectCount int
		}{
			{"Beginning", 5, 4},                   // Files 2-5 (4 files)
			{"Quarter", 12, 11},                   // Files 2-12 (11 files)
			{"Middle", 25, 24},                    // Files 2-25 (24 files)
			{"ThreeQuarters", 37, 36},             // Files 2-37 (36 files)
			{"End", fileCount - 1, fileCount - 2}, // All but last
		}

		for _, tp := range testPoints {
			t.Run(tp.name, func(t *testing.T) {
				targetTime := baseTime.Add(time.Duration(tp.offsetMins) * time.Minute)
				t.Logf("Filtering files before %v (offset: %d mins)", targetTime, tp.offsetMins)

				// Use LTXFiles with metadata to get accurate timestamps
				itr, err := c.LTXFiles(ctx, 0, 0, true)
				if err != nil {
					t.Fatalf("LTXFiles: %v", err)
				}
				defer itr.Close()

				var count int
				for itr.Next() {
					info := itr.Item()
					if info.CreatedAt.Before(targetTime) {
						count++
					}
				}

				// Allow for timestamp precision variance
				if count < tp.expectCount-1 || count > tp.expectCount+1 {
					t.Errorf("Files before %v: got %d, expected ~%d", targetTime, count, tp.expectCount)
				}
			})
		}
	})
}

// TestReplicaClient_PITR_CalcRestorePlanWithManyFiles tests CalcRestorePlan with
// a large number of LTX files. This ensures restore planning doesn't hang.
func TestReplicaClient_PITR_CalcRestorePlanWithManyFiles(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	RunWithReplicaClient(t, "RestorePlan", func(t *testing.T, c litestream.ReplicaClient) {
		t.Helper()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		fileCount := 100
		baseTime := time.Now().Add(-time.Duration(fileCount) * time.Minute)

		// Create snapshot
		snapshot := createLTXDataWithTimestamp(1, 1, baseTime, []byte("snapshot"))
		if _, err := c.WriteLTXFile(ctx, litestream.SnapshotLevel, 1, 1, bytes.NewReader(snapshot)); err != nil {
			t.Fatalf("WriteLTXFile(snapshot): %v", err)
		}

		// Create L0 files
		for i := 2; i <= fileCount; i++ {
			ts := baseTime.Add(time.Duration(i-1) * time.Minute)
			data := createLTXDataWithTimestamp(ltx.TXID(i), ltx.TXID(i), ts, []byte(fmt.Sprintf("file-%d", i)))
			if _, err := c.WriteLTXFile(ctx, 0, ltx.TXID(i), ltx.TXID(i), bytes.NewReader(data)); err != nil {
				t.Fatalf("WriteLTXFile(%d): %v", i, err)
			}
		}

		// Test restore plan calculation at various points
		testTargets := []struct {
			name     string
			txID     ltx.TXID
			minFiles int
		}{
			{"EarlyTXID", 10, 2},                   // snapshot + some L0
			{"MidTXID", 50, 2},                     // snapshot + more L0
			{"LateTXID", 90, 2},                    // snapshot + most L0
			{"LatestTXID", ltx.TXID(fileCount), 2}, // all files
		}

		logger := slog.Default()

		for _, target := range testTargets {
			t.Run(target.name, func(t *testing.T) {
				startTime := time.Now()

				plan, err := litestream.CalcRestorePlan(ctx, c, target.txID, time.Time{}, logger)
				if err != nil {
					t.Fatalf("CalcRestorePlan(%d): %v", target.txID, err)
				}

				duration := time.Since(startTime)
				t.Logf("CalcRestorePlan(txid=%d): %d files in %v", target.txID, len(plan), duration)

				if len(plan) < target.minFiles {
					t.Errorf("Plan has too few files: got %d, want >= %d", len(plan), target.minFiles)
				}

				// Verify plan doesn't take excessively long
				if duration > 30*time.Second {
					t.Errorf("CalcRestorePlan took too long: %v (should be < 30s)", duration)
				}
			})
		}

		// Test timestamp-based restore plan
		t.Run("TimestampBased", func(t *testing.T) {
			// Target halfway through the files
			targetTime := baseTime.Add(time.Duration(fileCount/2) * time.Minute)
			startTime := time.Now()

			plan, err := litestream.CalcRestorePlan(ctx, c, 0, targetTime, logger)
			if err != nil {
				t.Fatalf("CalcRestorePlan(timestamp=%v): %v", targetTime, err)
			}

			duration := time.Since(startTime)
			t.Logf("CalcRestorePlan(timestamp=%v): %d files in %v", targetTime, len(plan), duration)

			if len(plan) < 2 {
				t.Errorf("Plan has too few files: got %d, want >= 2", len(plan))
			}

			if duration > 60*time.Second {
				t.Errorf("Timestamp-based CalcRestorePlan took too long: %v", duration)
			}
		})
	})
}
