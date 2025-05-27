package litestream_test

import (
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

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gcs"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
	"github.com/superfly/ltx"
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
	gcsBucket = flag.String("gcs-bucket", os.Getenv("LITESTREAM_GCS_BUCKET"), "")
	gcsPath   = flag.String("gcs-path", os.Getenv("LITESTREAM_GCS_PATH"), "")
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

		itr, err := c.LTXFiles(context.Background(), 0)
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

		itr, err := c.LTXFiles(context.Background(), 0)
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
	case gcs.ReplicaClientType:
		return NewGCSReplicaClient(tb)
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

// NewGCSReplicaClient returns a new client for integration testing.
func NewGCSReplicaClient(tb testing.TB) *gcs.ReplicaClient {
	tb.Helper()

	c := gcs.NewReplicaClient()
	c.Bucket = *gcsBucket
	c.Path = path.Join(*gcsPath, fmt.Sprintf("%016x", rand.Uint64()))
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
