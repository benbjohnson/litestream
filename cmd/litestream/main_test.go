package main_test

import (
	"os"
	"path/filepath"
	"testing"

	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gcs"
	"github.com/benbjohnson/litestream/s3"
)

func TestReadConfigFile(t *testing.T) {
	// Ensure global AWS settings are propagated down to replica configurations.
	t.Run("PropagateGlobalSettings", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
access-key-id: XXX
secret-access-key: YYY

dbs:
  - path: /path/to/db
    replicas:
      - url: s3://foo/bar
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, true)
		if err != nil {
			t.Fatal(err)
		} else if got, want := config.AccessKeyID, `XXX`; got != want {
			t.Fatalf("AccessKeyID=%v, want %v", got, want)
		} else if got, want := config.SecretAccessKey, `YYY`; got != want {
			t.Fatalf("SecretAccessKey=%v, want %v", got, want)
		} else if got, want := config.DBs[0].Replicas[0].AccessKeyID, `XXX`; got != want {
			t.Fatalf("Replica.AccessKeyID=%v, want %v", got, want)
		} else if got, want := config.DBs[0].Replicas[0].SecretAccessKey, `YYY`; got != want {
			t.Fatalf("Replica.SecretAccessKey=%v, want %v", got, want)
		}
	})

	// Ensure environment variables are expanded.
	t.Run("ExpandEnv", func(t *testing.T) {
		os.Setenv("LITESTREAM_TEST_0129380", "/path/to/db")
		os.Setenv("LITESTREAM_TEST_1872363", "s3://foo/bar")

		filename := filepath.Join(t.TempDir(), "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: $LITESTREAM_TEST_0129380
    replicas:
      - url: ${LITESTREAM_TEST_1872363}
      - url: ${LITESTREAM_TEST_NO_SUCH_ENV}
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, true)
		if err != nil {
			t.Fatal(err)
		} else if got, want := config.DBs[0].Path, `/path/to/db`; got != want {
			t.Fatalf("DB.Path=%v, want %v", got, want)
		} else if got, want := config.DBs[0].Replicas[0].URL, `s3://foo/bar`; got != want {
			t.Fatalf("Replica[0].URL=%v, want %v", got, want)
		} else if got, want := config.DBs[0].Replicas[1].URL, ``; got != want {
			t.Fatalf("Replica[1].URL=%v, want %v", got, want)
		}
	})

	// Ensure environment variables are not expanded.
	t.Run("NoExpandEnv", func(t *testing.T) {
		os.Setenv("LITESTREAM_TEST_9847533", "s3://foo/bar")

		filename := filepath.Join(t.TempDir(), "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: /path/to/db
    replicas:
      - url: ${LITESTREAM_TEST_9847533}
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, false)
		if err != nil {
			t.Fatal(err)
		} else if got, want := config.DBs[0].Replicas[0].URL, `${LITESTREAM_TEST_9847533}`; got != want {
			t.Fatalf("Replica.URL=%v, want %v", got, want)
		}
	})
}

func TestNewFileReplicaFromConfig(t *testing.T) {
	r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{Path: "/foo"}, nil)
	if err != nil {
		t.Fatal(err)
	} else if client, ok := r.Client.(*file.ReplicaClient); !ok {
		t.Fatal("unexpected replica type")
	} else if got, want := client.Path(), "/foo"; got != want {
		t.Fatalf("Path=%s, want %s", got, want)
	}
}

func TestNewS3ReplicaFromConfig(t *testing.T) {
	t.Run("URL", func(t *testing.T) {
		r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{URL: "s3://foo/bar"}, nil)
		if err != nil {
			t.Fatal(err)
		} else if client, ok := r.Client.(*s3.ReplicaClient); !ok {
			t.Fatal("unexpected replica type")
		} else if got, want := client.Bucket, "foo"; got != want {
			t.Fatalf("Bucket=%s, want %s", got, want)
		} else if got, want := client.Path, "bar"; got != want {
			t.Fatalf("Path=%s, want %s", got, want)
		} else if got, want := client.Region, ""; got != want {
			t.Fatalf("Region=%s, want %s", got, want)
		} else if got, want := client.Endpoint, ""; got != want {
			t.Fatalf("Endpoint=%s, want %s", got, want)
		} else if got, want := client.ForcePathStyle, false; got != want {
			t.Fatalf("ForcePathStyle=%v, want %v", got, want)
		}
	})

	t.Run("MinIO", func(t *testing.T) {
		r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{URL: "s3://foo.localhost:9000/bar"}, nil)
		if err != nil {
			t.Fatal(err)
		} else if client, ok := r.Client.(*s3.ReplicaClient); !ok {
			t.Fatal("unexpected replica type")
		} else if got, want := client.Bucket, "foo"; got != want {
			t.Fatalf("Bucket=%s, want %s", got, want)
		} else if got, want := client.Path, "bar"; got != want {
			t.Fatalf("Path=%s, want %s", got, want)
		} else if got, want := client.Region, "us-east-1"; got != want {
			t.Fatalf("Region=%s, want %s", got, want)
		} else if got, want := client.Endpoint, "http://localhost:9000"; got != want {
			t.Fatalf("Endpoint=%s, want %s", got, want)
		} else if got, want := client.ForcePathStyle, true; got != want {
			t.Fatalf("ForcePathStyle=%v, want %v", got, want)
		}
	})

	t.Run("Backblaze", func(t *testing.T) {
		r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{URL: "s3://foo.s3.us-west-000.backblazeb2.com/bar"}, nil)
		if err != nil {
			t.Fatal(err)
		} else if client, ok := r.Client.(*s3.ReplicaClient); !ok {
			t.Fatal("unexpected replica type")
		} else if got, want := client.Bucket, "foo"; got != want {
			t.Fatalf("Bucket=%s, want %s", got, want)
		} else if got, want := client.Path, "bar"; got != want {
			t.Fatalf("Path=%s, want %s", got, want)
		} else if got, want := client.Region, "us-west-000"; got != want {
			t.Fatalf("Region=%s, want %s", got, want)
		} else if got, want := client.Endpoint, "https://s3.us-west-000.backblazeb2.com"; got != want {
			t.Fatalf("Endpoint=%s, want %s", got, want)
		} else if got, want := client.ForcePathStyle, true; got != want {
			t.Fatalf("ForcePathStyle=%v, want %v", got, want)
		}
	})
}

func TestNewGCSReplicaFromConfig(t *testing.T) {
	r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{URL: "gcs://foo/bar"}, nil)
	if err != nil {
		t.Fatal(err)
	} else if client, ok := r.Client.(*gcs.ReplicaClient); !ok {
		t.Fatal("unexpected replica type")
	} else if got, want := client.Bucket, "foo"; got != want {
		t.Fatalf("Bucket=%s, want %s", got, want)
	} else if got, want := client.Path, "bar"; got != want {
		t.Fatalf("Path=%s, want %s", got, want)
	}
}
