package litestream_test

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gcs"
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

func TestReplicaClient_Generations(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		// Write snapshots.
		if _, err := c.WriteSnapshot(context.Background(), "5efbd8d042012dca", 0, strings.NewReader(`foo`)); err != nil {
			t.Fatal(err)
		} else if _, err := c.WriteSnapshot(context.Background(), "b16ddcf5c697540f", 0, strings.NewReader(`bar`)); err != nil {
			t.Fatal(err)
		} else if _, err := c.WriteSnapshot(context.Background(), "155fe292f8333c72", 0, strings.NewReader(`baz`)); err != nil {
			t.Fatal(err)
		}

		// Fetch and sort generations.
		got, err := c.Generations(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(got)

		if want := []string{"155fe292f8333c72", "5efbd8d042012dca", "b16ddcf5c697540f"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Generations()=%v, want %v", got, want)
		}
	})

	RunWithReplicaClient(t, "NoGenerationsDir", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		if generations, err := c.Generations(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := len(generations), 0; got != want {
			t.Fatalf("len(Generations())=%v, want %v", got, want)
		}
	})
}

func TestReplicaClient_Snapshots(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		// Write snapshots.
		if _, err := c.WriteSnapshot(context.Background(), "5efbd8d042012dca", 1, strings.NewReader(``)); err != nil {
			t.Fatal(err)
		} else if _, err := c.WriteSnapshot(context.Background(), "b16ddcf5c697540f", 5, strings.NewReader(`x`)); err != nil {
			t.Fatal(err)
		} else if _, err := c.WriteSnapshot(context.Background(), "b16ddcf5c697540f", 10, strings.NewReader(`xyz`)); err != nil {
			t.Fatal(err)
		}

		// Fetch all snapshots by generation.
		itr, err := c.Snapshots(context.Background(), "b16ddcf5c697540f")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		// Read all snapshots into a slice so they can be sorted.
		a, err := litestream.SliceSnapshotIterator(itr)
		if err != nil {
			t.Fatal(err)
		} else if got, want := len(a), 2; got != want {
			t.Fatalf("len=%v, want %v", got, want)
		}
		sort.Sort(litestream.SnapshotInfoSlice(a))

		// Verify first snapshot metadata.
		if got, want := a[0].Generation, "b16ddcf5c697540f"; got != want {
			t.Fatalf("Generation=%v, want %v", got, want)
		} else if got, want := a[0].Index, 5; got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		} else if got, want := a[0].Size, int64(1); got != want {
			t.Fatalf("Size=%v, want %v", got, want)
		} else if a[0].CreatedAt.IsZero() {
			t.Fatalf("expected CreatedAt")
		}

		// Verify second snapshot metadata.
		if got, want := a[1].Generation, "b16ddcf5c697540f"; got != want {
			t.Fatalf("Generation=%v, want %v", got, want)
		} else if got, want := a[1].Index, 0xA; got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		} else if got, want := a[1].Size, int64(3); got != want {
			t.Fatalf("Size=%v, want %v", got, want)
		} else if a[1].CreatedAt.IsZero() {
			t.Fatalf("expected CreatedAt")
		}

		// Ensure close is clean.
		if err := itr.Close(); err != nil {
			t.Fatal(err)
		}
	})

	RunWithReplicaClient(t, "NoGenerationDir", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		itr, err := c.Snapshots(context.Background(), "5efbd8d042012dca")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatal("expected no snapshots")
		}
	})

	RunWithReplicaClient(t, "ErrNoGeneration", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		itr, err := c.Snapshots(context.Background(), "")
		if err == nil {
			err = itr.Close()
		}
		if err == nil || err.Error() != `cannot determine snapshots path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WriteSnapshot(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		if _, err := c.WriteSnapshot(context.Background(), "b16ddcf5c697540f", 1000, strings.NewReader(`foobar`)); err != nil {
			t.Fatal(err)
		}

		if r, err := c.SnapshotReader(context.Background(), "b16ddcf5c697540f", 1000); err != nil {
			t.Fatal(err)
		} else if buf, err := io.ReadAll(r); err != nil {
			t.Fatal(err)
		} else if err := r.Close(); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), `foobar`; got != want {
			t.Fatalf("data=%q, want %q", got, want)
		}
	})

	RunWithReplicaClient(t, "ErrNoGeneration", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()
		if _, err := c.WriteSnapshot(context.Background(), "", 0, nil); err == nil || err.Error() != `cannot determine snapshot path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_SnapshotReader(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		if _, err := c.WriteSnapshot(context.Background(), "5efbd8d042012dca", 10, strings.NewReader(`foo`)); err != nil {
			t.Fatal(err)
		}

		r, err := c.SnapshotReader(context.Background(), "5efbd8d042012dca", 10)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if buf, err := io.ReadAll(r); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), "foo"; got != want {
			t.Fatalf("ReadAll=%v, want %v", got, want)
		}
	})

	RunWithReplicaClient(t, "ErrNotFound", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		if _, err := c.SnapshotReader(context.Background(), "5efbd8d042012dca", 1); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
	})

	RunWithReplicaClient(t, "ErrNoGeneration", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		if _, err := c.SnapshotReader(context.Background(), "", 1); err == nil || err.Error() != `cannot determine snapshot path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WALs(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		if _, err := c.WriteWALSegment(context.Background(), litestream.Pos{Generation: "5efbd8d042012dca", Index: 1, Offset: 0}, strings.NewReader(``)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteWALSegment(context.Background(), litestream.Pos{Generation: "b16ddcf5c697540f", Index: 2, Offset: 0}, strings.NewReader(`12345`)); err != nil {
			t.Fatal(err)
		} else if _, err := c.WriteWALSegment(context.Background(), litestream.Pos{Generation: "b16ddcf5c697540f", Index: 2, Offset: 5}, strings.NewReader(`67`)); err != nil {
			t.Fatal(err)
		} else if _, err := c.WriteWALSegment(context.Background(), litestream.Pos{Generation: "b16ddcf5c697540f", Index: 3, Offset: 0}, strings.NewReader(`xyz`)); err != nil {
			t.Fatal(err)
		}

		itr, err := c.WALSegments(context.Background(), "b16ddcf5c697540f")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		// Read all WAL segment files into a slice so they can be sorted.
		a, err := litestream.SliceWALSegmentIterator(itr)
		if err != nil {
			t.Fatal(err)
		} else if got, want := len(a), 3; got != want {
			t.Fatalf("len=%v, want %v", got, want)
		}
		sort.Sort(litestream.WALSegmentInfoSlice(a))

		// Verify first WAL segment metadata.
		if got, want := a[0].Generation, "b16ddcf5c697540f"; got != want {
			t.Fatalf("Generation=%v, want %v", got, want)
		} else if got, want := a[0].Index, 2; got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		} else if got, want := a[0].Offset, int64(0); got != want {
			t.Fatalf("Offset=%v, want %v", got, want)
		} else if got, want := a[0].Size, int64(5); got != want {
			t.Fatalf("Size=%v, want %v", got, want)
		} else if a[0].CreatedAt.IsZero() {
			t.Fatalf("expected CreatedAt")
		}

		// Verify first WAL segment metadata.
		if got, want := a[1].Generation, "b16ddcf5c697540f"; got != want {
			t.Fatalf("Generation=%v, want %v", got, want)
		} else if got, want := a[1].Index, 2; got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		} else if got, want := a[1].Offset, int64(5); got != want {
			t.Fatalf("Offset=%v, want %v", got, want)
		} else if got, want := a[1].Size, int64(2); got != want {
			t.Fatalf("Size=%v, want %v", got, want)
		} else if a[1].CreatedAt.IsZero() {
			t.Fatalf("expected CreatedAt")
		}

		// Verify third WAL segment metadata.
		if got, want := a[2].Generation, "b16ddcf5c697540f"; got != want {
			t.Fatalf("Generation=%v, want %v", got, want)
		} else if got, want := a[2].Index, 3; got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		} else if got, want := a[2].Offset, int64(0); got != want {
			t.Fatalf("Offset=%v, want %v", got, want)
		} else if got, want := a[2].Size, int64(3); got != want {
			t.Fatalf("Size=%v, want %v", got, want)
		} else if a[1].CreatedAt.IsZero() {
			t.Fatalf("expected CreatedAt")
		}

		// Ensure close is clean.
		if err := itr.Close(); err != nil {
			t.Fatal(err)
		}
	})

	RunWithReplicaClient(t, "NoGenerationDir", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		itr, err := c.WALSegments(context.Background(), "5efbd8d042012dca")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatal("expected no wal files")
		}
	})

	RunWithReplicaClient(t, "NoWALs", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		if _, err := c.WriteSnapshot(context.Background(), "5efbd8d042012dca", 0, strings.NewReader(`foo`)); err != nil {
			t.Fatal(err)
		}

		itr, err := c.WALSegments(context.Background(), "5efbd8d042012dca")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatal("expected no wal files")
		}
	})

	RunWithReplicaClient(t, "ErrNoGeneration", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		itr, err := c.WALSegments(context.Background(), "")
		if err == nil {
			err = itr.Close()
		}
		if err == nil || err.Error() != `cannot determine wal path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WriteWALSegment(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		if _, err := c.WriteWALSegment(context.Background(), litestream.Pos{Generation: "b16ddcf5c697540f", Index: 1000, Offset: 2000}, strings.NewReader(`foobar`)); err != nil {
			t.Fatal(err)
		}

		if r, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: "b16ddcf5c697540f", Index: 1000, Offset: 2000}); err != nil {
			t.Fatal(err)
		} else if buf, err := io.ReadAll(r); err != nil {
			t.Fatal(err)
		} else if err := r.Close(); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), `foobar`; got != want {
			t.Fatalf("data=%q, want %q", got, want)
		}
	})

	RunWithReplicaClient(t, "ErrNoGeneration", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()
		if _, err := c.WriteWALSegment(context.Background(), litestream.Pos{Generation: "", Index: 0, Offset: 0}, nil); err == nil || err.Error() != `cannot determine wal segment path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WALReader(t *testing.T) {

	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()
		if _, err := c.WriteWALSegment(context.Background(), litestream.Pos{Generation: "5efbd8d042012dca", Index: 10, Offset: 5}, strings.NewReader(`foobar`)); err != nil {
			t.Fatal(err)
		}

		r, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: "5efbd8d042012dca", Index: 10, Offset: 5})
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

		if _, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: "5efbd8d042012dca", Index: 1, Offset: 0}); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
	})
}

func TestReplicaClient_DeleteWALSegments(t *testing.T) {
	RunWithReplicaClient(t, "OK", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()

		if _, err := c.WriteWALSegment(context.Background(), litestream.Pos{Generation: "b16ddcf5c697540f", Index: 1, Offset: 2}, strings.NewReader(`foo`)); err != nil {
			t.Fatal(err)
		} else if _, err := c.WriteWALSegment(context.Background(), litestream.Pos{Generation: "5efbd8d042012dca", Index: 3, Offset: 4}, strings.NewReader(`bar`)); err != nil {
			t.Fatal(err)
		}

		if err := c.DeleteWALSegments(context.Background(), []litestream.Pos{
			{Generation: "b16ddcf5c697540f", Index: 1, Offset: 2},
			{Generation: "5efbd8d042012dca", Index: 3, Offset: 4},
		}); err != nil {
			t.Fatal(err)
		}

		if _, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: "b16ddcf5c697540f", Index: 1, Offset: 2}); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		} else if _, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: "5efbd8d042012dca", Index: 3, Offset: 4}); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
	})

	RunWithReplicaClient(t, "ErrNoGeneration", func(t *testing.T, c litestream.ReplicaClient) {
		t.Parallel()
		if err := c.DeleteWALSegments(context.Background(), []litestream.Pos{{}}); err == nil || err.Error() != `cannot determine wal segment path: generation required` {
			t.Fatalf("unexpected error: %v", err)
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

	generations, err := c.Generations(context.Background())
	if err != nil {
		tb.Fatalf("cannot list generations for deletion: %s", err)
	}

	for _, generation := range generations {
		if err := c.DeleteGeneration(context.Background(), generation); err != nil {
			tb.Fatalf("cannot delete generation: %s", err)
		}
	}

	switch c := c.(type) {
	case *sftp.ReplicaClient:
		if err := c.Cleanup(context.Background()); err != nil {
			tb.Fatalf("cannot cleanup sftp: %s", err)
		}
	}
}
