package litestream_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gs"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
)

var (
	// Enables integration tests.
	integration = flag.String("integration", "file", "")
)

// S3 settings
var (
	// Replica client settings
	s3AccessKeyID     = flag.String("s3-access-key-id", os.Getenv("LITESTREAM_S3_ACCESS_KEY_ID"), "")
	s3SecretAccessKey = flag.String("s3-secret-access-key", os.Getenv("LITESTREAM_S3_SECRET_ACCESS_KEY"), "")
	s3Region          = flag.String("s3-region", os.Getenv("LITESTREAM_S3_REGION"), "")
	s3Bucket          = flag.String("s3-bucket", os.Getenv("LITESTREAM_S3_BUCKET"), "")
	s3Path            = flag.String("s3-path", os.Getenv("LITESTREAM_S3_PATH"), "")
	s3Endpoint        = flag.String("s3-endpoint", os.Getenv("LITESTREAM_S3_ENDPOINT"), "")
	s3ForcePathStyle  = flag.Bool("s3-force-path-style", os.Getenv("LITESTREAM_S3_FORCE_PATH_STYLE") == "true", "")
	s3SkipVerify      = flag.Bool("s3-skip-verify", os.Getenv("LITESTREAM_S3_SKIP_VERIFY") == "true", "")
)

// Google cloud storage settings
var (
	gsBucket = flag.String("gs-bucket", os.Getenv("LITESTREAM_GS_BUCKET"), "")
	gsPath   = flag.String("gs-path", os.Getenv("LITESTREAM_GS_PATH"), "")
)

// Azure blob storage settings
var (
	absAccountName = flag.String("abs-account-name", os.Getenv("LITESTREAM_ABS_ACCOUNT_NAME"), "")
	absAccountKey  = flag.String("abs-account-key", os.Getenv("LITESTREAM_ABS_ACCOUNT_KEY"), "")
	absBucket      = flag.String("abs-bucket", os.Getenv("LITESTREAM_ABS_BUCKET"), "")
	absPath        = flag.String("abs-path", os.Getenv("LITESTREAM_ABS_PATH"), "")
)

// SFTP settings
var (
	sftpHost     = flag.String("sftp-host", os.Getenv("LITESTREAM_SFTP_HOST"), "")
	sftpUser     = flag.String("sftp-user", os.Getenv("LITESTREAM_SFTP_USER"), "")
	sftpPassword = flag.String("sftp-password", os.Getenv("LITESTREAM_SFTP_PASSWORD"), "")
	sftpKeyPath  = flag.String("sftp-key-path", os.Getenv("LITESTREAM_SFTP_KEY_PATH"), "")
	sftpPath     = flag.String("sftp-path", os.Getenv("LITESTREAM_SFTP_PATH"), "")
)

func TestReplicaClient_LTX(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		// Write files out of order to check for sorting.
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(4), ltx.TXID(8), strings.NewReader(`67`)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(1), strings.NewReader(``)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(9), ltx.TXID(9), strings.NewReader(`xyz`)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(2), ltx.TXID(3), strings.NewReader(`12345`)); err != nil {
			t.Fatal(err)
		}

		itr, err := c.LTXFiles(context.Background(), 0, 0)
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

		if got, want := stripLTXFileInfo(a[0]), (&ltx.FileInfo{MinTXID: 1, MaxTXID: 1, Size: 0}); *got != *want {
			t.Fatalf("Index=%v, want %v", got, want)
		}
		if got, want := stripLTXFileInfo(a[1]), (&ltx.FileInfo{MinTXID: 2, MaxTXID: 3, Size: 5}); *got != *want {
			t.Fatalf("Index=%v, want %v", got, want)
		}
		if got, want := stripLTXFileInfo(a[2]), (&ltx.FileInfo{MinTXID: 4, MaxTXID: 8, Size: 2}); *got != *want {
			t.Fatalf("Index=%v, want %v", got, want)
		}
		if got, want := stripLTXFileInfo(a[3]), (&ltx.FileInfo{MinTXID: 9, MaxTXID: 9, Size: 3}); *got != *want {
			t.Fatalf("Index=%v, want %v", got, want)
		}

		if err := itr.Close(); err != nil {
			t.Fatal(err)
		}
	})

	RunWithReplicaClient(t, "NoWALs", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		itr, err := c.LTXFiles(context.Background(), 0, 0)
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
		t.Parallel()

		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2), strings.NewReader(`foobar`)); err != nil {
			t.Fatal(err)
		}

		r, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2))
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

		if got, want := string(buf), `foobar`; got != want {
			t.Fatalf("data=%q, want %q", got, want)
		}
	})
}

func TestReplicaClient_OpenLTXFile(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2), strings.NewReader(`foobar`)); err != nil {
			t.Fatal(err)
		}

		r, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if buf, err := io.ReadAll(r); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), "foobar"; got != want {
			t.Fatalf("ReadAll=%v, want %v", got, want)
		}
	})

	RunWithReplicaClient(t, "ErrNotFound", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		if _, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(1)); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
	})
}

func TestReplicaClient_DeleteWALSegments(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2), strings.NewReader(`foo`)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(3), ltx.TXID(4), strings.NewReader(`bar`)); err != nil {
			t.Fatal(err)
		}

		if err := c.DeleteLTXFiles(context.Background(), []*ltx.FileInfo{
			{Level: 0, MinTXID: 1, MaxTXID: 2},
			{Level: 0, MinTXID: 3, MaxTXID: 4},
		}); err != nil {
			t.Fatal(err)
		}

		if _, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(2)); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
		if _, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(3), ltx.TXID(4)); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
	})
}

// RunWithReplicaClient executes fn with each replica specified by the -integration flag
func RunWithReplicaClient(t *testing.T, name string, fn func(*testing.T, litestream.ReplicaClient)) {
	t.Run(name, func(t *testing.T) {
		for _, typ := range strings.Split(*integration, ",") {
			t.Run(typ, func(t *testing.T) {
				c := NewReplicaClient(t, typ)
				defer MustDeleteAll(t, c)

				fn(t, c)
			})
		}
	})
}

// NewReplicaClient returns a new client for integration testing by type name.
func NewReplicaClient(tb testing.TB, typ string) litestream.ReplicaClient {
	tb.Helper()

	switch typ {
	case file.ReplicaClientType:
		return NewFileReplicaClient(tb)
	case s3.ReplicaClientType:
		return NewS3ReplicaClient(tb)
	case gs.ReplicaClientType:
		return NewGSReplicaClient(tb)
	case abs.ReplicaClientType:
		return NewABSReplicaClient(tb)
	case sftp.ReplicaClientType:
		return NewSFTPReplicaClient(tb)
	default:
		tb.Fatalf("invalid replica client type: %q", typ)
		return nil
	}
}

// NewFileReplicaClient returns a new client for integration testing.
func NewFileReplicaClient(tb testing.TB) *file.ReplicaClient {
	tb.Helper()
	return file.NewReplicaClient(tb.TempDir())
}

// NewS3ReplicaClient returns a new client for integration testing.
func NewS3ReplicaClient(tb testing.TB) *s3.ReplicaClient {
	tb.Helper()

	c := s3.NewReplicaClient()
	c.AccessKeyID = *s3AccessKeyID
	c.SecretAccessKey = *s3SecretAccessKey
	c.Region = *s3Region
	c.Bucket = *s3Bucket
	c.Path = path.Join(*s3Path, fmt.Sprintf("%016x", rand.Uint64()))
	c.Endpoint = *s3Endpoint
	c.ForcePathStyle = *s3ForcePathStyle
	c.SkipVerify = *s3SkipVerify
	return c
}

// NewGSReplicaClient returns a new client for integration testing.
func NewGSReplicaClient(tb testing.TB) *gs.ReplicaClient {
	tb.Helper()

	c := gs.NewReplicaClient()
	c.Bucket = *gsBucket
	c.Path = path.Join(*gsPath, fmt.Sprintf("%016x", rand.Uint64()))
	return c
}

// NewABSReplicaClient returns a new client for integration testing.
func NewABSReplicaClient(tb testing.TB) *abs.ReplicaClient {
	tb.Helper()

	c := abs.NewReplicaClient()
	c.AccountName = *absAccountName
	c.AccountKey = *absAccountKey
	c.Bucket = *absBucket
	c.Path = path.Join(*absPath, fmt.Sprintf("%016x", rand.Uint64()))
	return c
}

// NewSFTPReplicaClient returns a new client for integration testing.
func NewSFTPReplicaClient(tb testing.TB) *sftp.ReplicaClient {
	tb.Helper()

	c := sftp.NewReplicaClient()
	c.Host = *sftpHost
	c.User = *sftpUser
	c.Password = *sftpPassword
	c.KeyPath = *sftpKeyPath
	c.Path = path.Join(*sftpPath, fmt.Sprintf("%016x", rand.Uint64()))
	return c
}

// MustDeleteAll deletes all objects under the client's path.
func MustDeleteAll(tb testing.TB, c litestream.ReplicaClient) {
	tb.Helper()

	if err := c.DeleteAll(context.Background()); err != nil {
		tb.Fatalf("cannot delete all: %s", err)
	}

	switch c := c.(type) {
	case *sftp.ReplicaClient:
		if err := c.Cleanup(context.Background()); err != nil {
			tb.Fatalf("cannot cleanup sftp: %s", err)
		}
	}
}

func stripLTXFileInfo(info *ltx.FileInfo) *ltx.FileInfo {
	other := *info
	other.CreatedAt = time.Time{}
	return &other
}

// TestReplicaClient_S3_UploaderConfig tests S3 uploader configuration for large files
func TestReplicaClient_S3_UploaderConfig(t *testing.T) {
	// Only run for S3 integration tests
	if !strings.Contains(*integration, "s3") {
		t.Skip("Skipping S3-specific uploader config test")
	}

	RunWithReplicaClient(t, "LargeFileWithCustomConfig", func(t *testing.T, c litestream.ReplicaClient) {
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
		r, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(1), ltx.TXID(100))
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
	if !strings.Contains(*integration, "s3") {
		t.Skip("Skipping S3-specific error context test")
	}

	RunWithReplicaClient(t, "ErrorContext", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		// Test OpenLTXFile with non-existent file
		_, err := c.OpenLTXFile(context.Background(), 0, ltx.TXID(999), ltx.TXID(999))
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
	if !strings.Contains(*integration, "s3") {
		t.Skip("Skipping S3-specific bucket validation test")
	}

	// Create a new S3 client with empty bucket
	c := s3.NewReplicaClient()
	c.AccessKeyID = *s3AccessKeyID
	c.SecretAccessKey = *s3SecretAccessKey
	c.Region = *s3Region
	c.Bucket = "" // Empty bucket name

	// Should fail with bucket validation error
	err := c.Init(context.Background())
	if err == nil {
		t.Fatal("expected error for empty bucket name")
	}
	if !strings.Contains(err.Error(), "bucket name is required") {
		t.Errorf("expected bucket validation error, got: %v", err)
	}
}
