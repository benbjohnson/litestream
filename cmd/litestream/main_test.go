package main_test

import (
	"testing"

	"github.com/benbjohnson/litestream"
	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/s3"
)

func TestNewFileReplicaFromConfig(t *testing.T) {
	r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{Path: "/foo"}, nil)
	if err != nil {
		t.Fatal(err)
	} else if r, ok := r.(*litestream.FileReplica); !ok {
		t.Fatal("unexpected replica type")
	} else if got, want := r.Path(), "/foo"; got != want {
		t.Fatalf("Path=%s, want %s", got, want)
	}
}

func TestNewS3ReplicaFromConfig(t *testing.T) {
	t.Run("URL", func(t *testing.T) {
		r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{URL: "s3://foo/bar"}, nil)
		if err != nil {
			t.Fatal(err)
		} else if r, ok := r.(*s3.Replica); !ok {
			t.Fatal("unexpected replica type")
		} else if got, want := r.Bucket, "foo"; got != want {
			t.Fatalf("Bucket=%s, want %s", got, want)
		} else if got, want := r.Path, "bar"; got != want {
			t.Fatalf("Path=%s, want %s", got, want)
		} else if got, want := r.Region, ""; got != want {
			t.Fatalf("Region=%s, want %s", got, want)
		} else if got, want := r.Endpoint, ""; got != want {
			t.Fatalf("Endpoint=%s, want %s", got, want)
		} else if got, want := r.ForcePathStyle, false; got != want {
			t.Fatalf("ForcePathStyle=%v, want %v", got, want)
		}
	})

	t.Run("MinIO", func(t *testing.T) {
		r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{URL: "s3://foo.localhost:9000/bar"}, nil)
		if err != nil {
			t.Fatal(err)
		} else if r, ok := r.(*s3.Replica); !ok {
			t.Fatal("unexpected replica type")
		} else if got, want := r.Bucket, "foo"; got != want {
			t.Fatalf("Bucket=%s, want %s", got, want)
		} else if got, want := r.Path, "bar"; got != want {
			t.Fatalf("Path=%s, want %s", got, want)
		} else if got, want := r.Region, "us-east-1"; got != want {
			t.Fatalf("Region=%s, want %s", got, want)
		} else if got, want := r.Endpoint, "http://localhost:9000"; got != want {
			t.Fatalf("Endpoint=%s, want %s", got, want)
		} else if got, want := r.ForcePathStyle, true; got != want {
			t.Fatalf("ForcePathStyle=%v, want %v", got, want)
		}
	})

	t.Run("Backblaze", func(t *testing.T) {
		r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{URL: "s3://foo.s3.us-west-000.backblazeb2.com/bar"}, nil)
		if err != nil {
			t.Fatal(err)
		} else if r, ok := r.(*s3.Replica); !ok {
			t.Fatal("unexpected replica type")
		} else if got, want := r.Bucket, "foo"; got != want {
			t.Fatalf("Bucket=%s, want %s", got, want)
		} else if got, want := r.Path, "bar"; got != want {
			t.Fatalf("Path=%s, want %s", got, want)
		} else if got, want := r.Region, "us-west-000"; got != want {
			t.Fatalf("Region=%s, want %s", got, want)
		} else if got, want := r.Endpoint, "https://s3.us-west-000.backblazeb2.com"; got != want {
			t.Fatalf("Endpoint=%s, want %s", got, want)
		} else if got, want := r.ForcePathStyle, true; got != want {
			t.Fatalf("ForcePathStyle=%v, want %v", got, want)
		}
	})

	t.Run("GCS", func(t *testing.T) {
		r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{URL: "s3://foo.storage.googleapis.com/bar"}, nil)
		if err != nil {
			t.Fatal(err)
		} else if r, ok := r.(*s3.Replica); !ok {
			t.Fatal("unexpected replica type")
		} else if got, want := r.Bucket, "foo"; got != want {
			t.Fatalf("Bucket=%s, want %s", got, want)
		} else if got, want := r.Path, "bar"; got != want {
			t.Fatalf("Path=%s, want %s", got, want)
		} else if got, want := r.Region, "us-east-1"; got != want {
			t.Fatalf("Region=%s, want %s", got, want)
		} else if got, want := r.Endpoint, "https://storage.googleapis.com"; got != want {
			t.Fatalf("Endpoint=%s, want %s", got, want)
		} else if got, want := r.ForcePathStyle, true; got != want {
			t.Fatalf("ForcePathStyle=%v, want %v", got, want)
		}
	})
}
