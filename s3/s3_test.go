package s3_test

import (
	"testing"

	"github.com/benbjohnson/litestream/s3"
)

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
