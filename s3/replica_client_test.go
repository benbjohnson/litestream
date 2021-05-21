package s3_test

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/s3"
)

var (
	// Enables integration tests.
	integration = flag.Bool("integration", false, "")

	// Replica client settings
	accessKeyID     = flag.String("access-key-id", os.Getenv("LITESTREAM_S3_ACCESS_KEY_ID"), "")
	secretAccessKey = flag.String("secret-access-key", os.Getenv("LITESTREAM_S3_SECRET_ACCESS_KEY"), "")
	region          = flag.String("region", os.Getenv("LITESTREAM_S3_REGION"), "")
	bucket          = flag.String("bucket", os.Getenv("LITESTREAM_S3_BUCKET"), "")
	pathFlag        = flag.String("path", os.Getenv("LITESTREAM_S3_PATH"), "")
	endpoint        = flag.String("endpoint", os.Getenv("LITESTREAM_S3_ENDPOINT"), "")
	forcePathStyle  = flag.Bool("force-path-style", os.Getenv("LITESTREAM_S3_FORCE_PATH_STYLE") == "true", "")
	skipVerify      = flag.Bool("skip-verify", os.Getenv("LITESTREAM_S3_SKIP_VERIFY") == "true", "")
)

func TestReplicaClient_Type(t *testing.T) {
	if got, want := s3.NewReplicaClient().Type(), "s3"; got != want {
		t.Fatalf("Type()=%v, want %v", got, want)
	}
}

func TestReplicaClient_Generations(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

		// Write snapshots.
		if _, err := c.WriteSnapshot(context.Background(), "5efbd8d042012dca", 0, strings.NewReader(`foo`)); err != nil {
			t.Fatal(err)
		} else if _, err := c.WriteSnapshot(context.Background(), "b16ddcf5c697540f", 0, strings.NewReader(`bar`)); err != nil {
			t.Fatal(err)
		} else if _, err := c.WriteSnapshot(context.Background(), "155fe292f8333c72", 0, strings.NewReader(`baz`)); err != nil {
			t.Fatal(err)
		}

		// Verify returned generations.
		if got, err := c.Generations(context.Background()); err != nil {
			t.Fatal(err)
		} else if want := []string{"155fe292f8333c72", "5efbd8d042012dca", "b16ddcf5c697540f"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Generations()=%v, want %v", got, want)
		}
	})

	t.Run("NoGenerationsDir", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)
		if generations, err := c.Generations(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := len(generations), 0; got != want {
			t.Fatalf("len(Generations())=%v, want %v", got, want)
		}
	})
}

func TestReplicaClient_Snapshots(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

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

	t.Run("NoGenerationDir", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

		itr, err := c.Snapshots(context.Background(), "5efbd8d042012dca")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatal("expected no snapshots")
		}
	})

	t.Run("ErrNoGeneration", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

		if itr, err := c.Snapshots(context.Background(), ""); err != nil {
			t.Fatal(err)
		} else if err := itr.Close(); err == nil || err.Error() != `cannot determine snapshot directory path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WriteSnapshot(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

		if _, err := c.WriteSnapshot(context.Background(), "b16ddcf5c697540f", 1000, strings.NewReader(`foobar`)); err != nil {
			t.Fatal(err)
		}

		if r, err := c.SnapshotReader(context.Background(), "b16ddcf5c697540f", 1000); err != nil {
			t.Fatal(err)
		} else if buf, err := ioutil.ReadAll(r); err != nil {
			t.Fatal(err)
		} else if err := r.Close(); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), `foobar`; got != want {
			t.Fatalf("data=%q, want %q", got, want)
		}
	})

	t.Run("ErrNoGeneration", func(t *testing.T) {
		t.Parallel()
		if _, err := NewIntegrationReplicaClient(t).WriteSnapshot(context.Background(), "", 0, nil); err == nil || err.Error() != `cannot determine snapshot path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_SnapshotReader(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

		if _, err := c.WriteSnapshot(context.Background(), "5efbd8d042012dca", 10, strings.NewReader(`foo`)); err != nil {
			t.Fatal(err)
		}

		r, err := c.SnapshotReader(context.Background(), "5efbd8d042012dca", 10)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if buf, err := ioutil.ReadAll(r); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), "foo"; got != want {
			t.Fatalf("ReadAll=%v, want %v", got, want)
		}
	})

	t.Run("ErrNotFound", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

		if _, err := c.SnapshotReader(context.Background(), "5efbd8d042012dca", 1); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
	})

	t.Run("ErrGeneration", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

		if _, err := c.SnapshotReader(context.Background(), "", 1); err == nil || err.Error() != `cannot determine snapshot path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WALs(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

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

	t.Run("NoGenerationDir", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

		itr, err := c.WALSegments(context.Background(), "5efbd8d042012dca")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatal("expected no wal files")
		}
	})

	t.Run("NoWALs", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

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

	t.Run("ErrNoGeneration", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

		if itr, err := c.WALSegments(context.Background(), ""); err != nil {
			t.Fatal(err)
		} else if err := itr.Close(); err == nil || err.Error() != `cannot determine wal directory path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WriteWALSegment(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

		if _, err := c.WriteWALSegment(context.Background(), litestream.Pos{Generation: "b16ddcf5c697540f", Index: 1000, Offset: 2000}, strings.NewReader(`foobar`)); err != nil {
			t.Fatal(err)
		}

		if r, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: "b16ddcf5c697540f", Index: 1000, Offset: 2000}); err != nil {
			t.Fatal(err)
		} else if buf, err := ioutil.ReadAll(r); err != nil {
			t.Fatal(err)
		} else if err := r.Close(); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), `foobar`; got != want {
			t.Fatalf("data=%q, want %q", got, want)
		}
	})

	t.Run("ErrNoGeneration", func(t *testing.T) {
		t.Parallel()
		if _, err := NewIntegrationReplicaClient(t).WriteWALSegment(context.Background(), litestream.Pos{Generation: "", Index: 0, Offset: 0}, nil); err == nil || err.Error() != `cannot determine wal segment path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WALReader(t *testing.T) {
	t.Parallel()

	c := NewIntegrationReplicaClient(t)
	defer MustDeleteAll(t, c)

	if _, err := c.WriteWALSegment(context.Background(), litestream.Pos{Generation: "5efbd8d042012dca", Index: 10, Offset: 5}, strings.NewReader(`foobar`)); err != nil {
		t.Fatal(err)
	}

	t.Run("OK", func(t *testing.T) {
		r, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: "5efbd8d042012dca", Index: 10, Offset: 5})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if buf, err := ioutil.ReadAll(r); err != nil {
			t.Fatal(err)
		} else if got, want := string(buf), "foobar"; got != want {
			t.Fatalf("ReadAll=%v, want %v", got, want)
		}
	})

	t.Run("ErrNotFound", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

		if _, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: "5efbd8d042012dca", Index: 1, Offset: 0}); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
	})
}

func TestReplicaClient_DeleteWALSegments(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		t.Parallel()

		c := NewIntegrationReplicaClient(t)
		defer MustDeleteAll(t, c)

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

	t.Run("ErrNoGeneration", func(t *testing.T) {
		t.Parallel()
		if err := NewIntegrationReplicaClient(t).DeleteWALSegments(context.Background(), []litestream.Pos{{}}); err == nil || err.Error() != `cannot determine wal segment path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestParseHost(t *testing.T) {
	// Ensure non-specific hosts return as buckets.
	t.Run("S3", func(t *testing.T) {
		bucket, region, endpoint, forcePathStyle := s3.ParseHost(`test.litestream.io`)
		if got, want := bucket, `test.litestream.io`; got != want {
			t.Fatalf("bucket=%q, want %q", got, want)
		} else if got, want := region, ``; got != want {
			t.Fatalf("region=%q, want %q", got, want)
		} else if got, want := endpoint, ``; got != want {
			t.Fatalf("endpoint=%q, want %q", got, want)
		} else if got, want := forcePathStyle, false; got != want {
			t.Fatalf("forcePathStyle=%v, want %v", got, want)
		}
	})

	// Ensure localhosts use an HTTP endpoint and extract the bucket name.
	t.Run("Localhost", func(t *testing.T) {
		t.Run("WithPort", func(t *testing.T) {
			bucket, region, endpoint, forcePathStyle := s3.ParseHost(`test.localhost:9000`)
			if got, want := bucket, `test`; got != want {
				t.Fatalf("bucket=%q, want %q", got, want)
			} else if got, want := region, `us-east-1`; got != want {
				t.Fatalf("region=%q, want %q", got, want)
			} else if got, want := endpoint, `http://localhost:9000`; got != want {
				t.Fatalf("endpoint=%q, want %q", got, want)
			} else if got, want := forcePathStyle, true; got != want {
				t.Fatalf("forcePathStyle=%v, want %v", got, want)
			}
		})

		t.Run("WithoutPort", func(t *testing.T) {
			bucket, region, endpoint, forcePathStyle := s3.ParseHost(`test.localhost`)
			if got, want := bucket, `test`; got != want {
				t.Fatalf("bucket=%q, want %q", got, want)
			} else if got, want := region, `us-east-1`; got != want {
				t.Fatalf("region=%q, want %q", got, want)
			} else if got, want := endpoint, `http://localhost`; got != want {
				t.Fatalf("endpoint=%q, want %q", got, want)
			} else if got, want := forcePathStyle, true; got != want {
				t.Fatalf("forcePathStyle=%v, want %v", got, want)
			}
		})
	})

	// Ensure backblaze B2 URLs extract bucket, region, & endpoint from host.
	t.Run("Backblaze", func(t *testing.T) {
		bucket, region, endpoint, forcePathStyle := s3.ParseHost(`test-123.s3.us-west-000.backblazeb2.com`)
		if got, want := bucket, `test-123`; got != want {
			t.Fatalf("bucket=%q, want %q", got, want)
		} else if got, want := region, `us-west-000`; got != want {
			t.Fatalf("region=%q, want %q", got, want)
		} else if got, want := endpoint, `https://s3.us-west-000.backblazeb2.com`; got != want {
			t.Fatalf("endpoint=%q, want %q", got, want)
		} else if got, want := forcePathStyle, true; got != want {
			t.Fatalf("forcePathStyle=%v, want %v", got, want)
		}
	})

	// Ensure GCS URLs extract bucket & endpoint from host.
	t.Run("GCS", func(t *testing.T) {
		bucket, region, endpoint, forcePathStyle := s3.ParseHost(`litestream.io.storage.googleapis.com`)
		if got, want := bucket, `litestream.io`; got != want {
			t.Fatalf("bucket=%q, want %q", got, want)
		} else if got, want := region, `us-east-1`; got != want {
			t.Fatalf("region=%q, want %q", got, want)
		} else if got, want := endpoint, `https://storage.googleapis.com`; got != want {
			t.Fatalf("endpoint=%q, want %q", got, want)
		} else if got, want := forcePathStyle, true; got != want {
			t.Fatalf("forcePathStyle=%v, want %v", got, want)
		}
	})
}

// NewIntegrationReplicaClient returns a new client for integration testing.
// If integration flag is not set then test/benchmark is skipped.
func NewIntegrationReplicaClient(tb testing.TB) *s3.ReplicaClient {
	tb.Helper()

	if !*integration {
		tb.Skip("integration tests disabled")
	}

	c := s3.NewReplicaClient()
	c.AccessKeyID = *accessKeyID
	c.SecretAccessKey = *secretAccessKey
	c.Region = *region
	c.Bucket = *bucket
	c.Path = path.Join(*pathFlag, fmt.Sprintf("%016x", rand.Uint64()))
	c.Endpoint = *endpoint
	c.ForcePathStyle = *forcePathStyle
	c.SkipVerify = *skipVerify

	return c
}

// MustDeleteAll deletes all objects under the client's path.
func MustDeleteAll(tb testing.TB, c *s3.ReplicaClient) {
	tb.Helper()
	if err := c.DeleteAll(context.Background()); err != nil {
		tb.Fatal(err)
	}
}
