package litestream_test

import (
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/s3"

	// Import backends to register factories
	_ "github.com/benbjohnson/litestream/abs"
	_ "github.com/benbjohnson/litestream/gs"
	_ "github.com/benbjohnson/litestream/nats"
	_ "github.com/benbjohnson/litestream/oss"
	_ "github.com/benbjohnson/litestream/sftp"
	_ "github.com/benbjohnson/litestream/webdav"
)

func TestNewReplicaClientFromURL(t *testing.T) {
	t.Run("S3", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("s3://mybucket/path/to/db")
		if err != nil {
			t.Fatal(err)
		}
		if client.Type() != "s3" {
			t.Errorf("expected type 's3', got %q", client.Type())
		}
		s3Client, ok := client.(*s3.ReplicaClient)
		if !ok {
			t.Fatalf("expected *s3.ReplicaClient, got %T", client)
		}
		if s3Client.Bucket != "mybucket" {
			t.Errorf("expected bucket 'mybucket', got %q", s3Client.Bucket)
		}
		if s3Client.Path != "path/to/db" {
			t.Errorf("expected path 'path/to/db', got %q", s3Client.Path)
		}
	})

	t.Run("S3WithQueryParams", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("s3://mybucket/db?endpoint=localhost:9000&region=us-west-2")
		if err != nil {
			t.Fatal(err)
		}
		s3Client, ok := client.(*s3.ReplicaClient)
		if !ok {
			t.Fatalf("expected *s3.ReplicaClient, got %T", client)
		}
		if s3Client.Endpoint != "http://localhost:9000" {
			t.Errorf("expected endpoint 'http://localhost:9000', got %q", s3Client.Endpoint)
		}
		if s3Client.Region != "us-west-2" {
			t.Errorf("expected region 'us-west-2', got %q", s3Client.Region)
		}
	})

	t.Run("File", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("file:///tmp/replica")
		if err != nil {
			t.Fatal(err)
		}
		if client.Type() != "file" {
			t.Errorf("expected type 'file', got %q", client.Type())
		}
		fileClient, ok := client.(*file.ReplicaClient)
		if !ok {
			t.Fatalf("expected *file.ReplicaClient, got %T", client)
		}
		if fileClient.Path() != "/tmp/replica" {
			t.Errorf("expected path '/tmp/replica', got %q", fileClient.Path())
		}
	})

	t.Run("GS", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("gs://mybucket/path")
		if err != nil {
			t.Fatal(err)
		}
		if client.Type() != "gs" {
			t.Errorf("expected type 'gs', got %q", client.Type())
		}
	})

	t.Run("UnsupportedScheme", func(t *testing.T) {
		_, err := litestream.NewReplicaClientFromURL("unknown://bucket/path")
		if err == nil {
			t.Fatal("expected error for unsupported scheme")
		}
	})

	t.Run("InvalidURL", func(t *testing.T) {
		_, err := litestream.NewReplicaClientFromURL("not-a-valid-url")
		if err == nil {
			t.Fatal("expected error for invalid URL")
		}
	})
}

func TestReplicaTypeFromURL(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{"s3://bucket/path", "s3"},
		{"gs://bucket/path", "gs"},
		{"abs://container/path", "abs"},
		{"file:///path/to/replica", "file"},
		{"sftp://host/path", "sftp"},
		{"webdav://host/path", "webdav"},
		{"webdavs://host/path", "webdav"},
		{"nats://host/bucket", "nats"},
		{"oss://bucket/path", "oss"},
		{"", ""},
		{"invalid", ""},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			got := litestream.ReplicaTypeFromURL(tt.url)
			if got != tt.expected {
				t.Errorf("ReplicaTypeFromURL(%q) = %q, want %q", tt.url, got, tt.expected)
			}
		})
	}
}

func TestIsURL(t *testing.T) {
	tests := []struct {
		s        string
		expected bool
	}{
		{"s3://bucket/path", true},
		{"file:///path", true},
		{"https://example.com", true},
		{"/path/to/file", false},
		{"relative/path", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			got := litestream.IsURL(tt.s)
			if got != tt.expected {
				t.Errorf("IsURL(%q) = %v, want %v", tt.s, got, tt.expected)
			}
		})
	}
}

func TestBoolQueryValue(t *testing.T) {
	t.Run("True values", func(t *testing.T) {
		for _, v := range []string{"true", "True", "TRUE", "1", "t", "yes"} {
			query := make(map[string][]string)
			query["key"] = []string{v}
			value, ok := litestream.BoolQueryValue(query, "key")
			if !ok {
				t.Errorf("BoolQueryValue with %q should be ok", v)
			}
			if !value {
				t.Errorf("BoolQueryValue with %q should be true", v)
			}
		}
	})

	t.Run("False values", func(t *testing.T) {
		for _, v := range []string{"false", "False", "FALSE", "0", "f", "no"} {
			query := make(map[string][]string)
			query["key"] = []string{v}
			value, ok := litestream.BoolQueryValue(query, "key")
			if !ok {
				t.Errorf("BoolQueryValue with %q should be ok", v)
			}
			if value {
				t.Errorf("BoolQueryValue with %q should be false", v)
			}
		}
	})

	t.Run("Missing key", func(t *testing.T) {
		query := make(map[string][]string)
		_, ok := litestream.BoolQueryValue(query, "key")
		if ok {
			t.Error("BoolQueryValue with missing key should not be ok")
		}
	})

	t.Run("Multiple keys", func(t *testing.T) {
		query := make(map[string][]string)
		query["key2"] = []string{"true"}
		value, ok := litestream.BoolQueryValue(query, "key1", "key2")
		if !ok {
			t.Error("BoolQueryValue should find second key")
		}
		if !value {
			t.Error("BoolQueryValue should return true for second key")
		}
	})
}
