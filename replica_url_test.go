package litestream_test

import (
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gs"
	"github.com/benbjohnson/litestream/nats"
	"github.com/benbjohnson/litestream/oss"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
	"github.com/benbjohnson/litestream/webdav"
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
		gsClient, ok := client.(*gs.ReplicaClient)
		if !ok {
			t.Fatalf("expected *gs.ReplicaClient, got %T", client)
		}
		if gsClient.Bucket != "mybucket" {
			t.Errorf("expected bucket 'mybucket', got %q", gsClient.Bucket)
		}
		if gsClient.Path != "path" {
			t.Errorf("expected path 'path', got %q", gsClient.Path)
		}
	})

	t.Run("GS_MissingBucket", func(t *testing.T) {
		_, err := litestream.NewReplicaClientFromURL("gs:///path")
		if err == nil {
			t.Fatal("expected error for missing bucket")
		}
	})

	t.Run("ABS", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("abs://mycontainer/path")
		if err != nil {
			t.Fatal(err)
		}
		if client.Type() != "abs" {
			t.Errorf("expected type 'abs', got %q", client.Type())
		}
		absClient, ok := client.(*abs.ReplicaClient)
		if !ok {
			t.Fatalf("expected *abs.ReplicaClient, got %T", client)
		}
		if absClient.Bucket != "mycontainer" {
			t.Errorf("expected bucket 'mycontainer', got %q", absClient.Bucket)
		}
		if absClient.Path != "path" {
			t.Errorf("expected path 'path', got %q", absClient.Path)
		}
	})

	t.Run("ABS_WithAccount", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("abs://myaccount@mycontainer/path")
		if err != nil {
			t.Fatal(err)
		}
		absClient, ok := client.(*abs.ReplicaClient)
		if !ok {
			t.Fatalf("expected *abs.ReplicaClient, got %T", client)
		}
		if absClient.AccountName != "myaccount" {
			t.Errorf("expected account 'myaccount', got %q", absClient.AccountName)
		}
		if absClient.Bucket != "mycontainer" {
			t.Errorf("expected bucket 'mycontainer', got %q", absClient.Bucket)
		}
	})

	t.Run("ABS_MissingBucket", func(t *testing.T) {
		_, err := litestream.NewReplicaClientFromURL("abs:///path")
		if err == nil {
			t.Fatal("expected error for missing bucket")
		}
	})

	t.Run("SFTP", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("sftp://myuser@host.example.com/path")
		if err != nil {
			t.Fatal(err)
		}
		if client.Type() != "sftp" {
			t.Errorf("expected type 'sftp', got %q", client.Type())
		}
		sftpClient, ok := client.(*sftp.ReplicaClient)
		if !ok {
			t.Fatalf("expected *sftp.ReplicaClient, got %T", client)
		}
		if sftpClient.Host != "host.example.com" {
			t.Errorf("expected host 'host.example.com', got %q", sftpClient.Host)
		}
		if sftpClient.User != "myuser" {
			t.Errorf("expected user 'myuser', got %q", sftpClient.User)
		}
		if sftpClient.Path != "path" {
			t.Errorf("expected path 'path', got %q", sftpClient.Path)
		}
	})

	t.Run("SFTP_WithPassword", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("sftp://myuser:secret@host.example.com/path")
		if err != nil {
			t.Fatal(err)
		}
		sftpClient, ok := client.(*sftp.ReplicaClient)
		if !ok {
			t.Fatalf("expected *sftp.ReplicaClient, got %T", client)
		}
		if sftpClient.User != "myuser" {
			t.Errorf("expected user 'myuser', got %q", sftpClient.User)
		}
		if sftpClient.Password != "secret" {
			t.Errorf("expected password 'secret', got %q", sftpClient.Password)
		}
	})

	t.Run("SFTP_RequiresUserError", func(t *testing.T) {
		_, err := litestream.NewReplicaClientFromURL("sftp://host.example.com/path")
		if err == nil {
			t.Fatal("expected error for missing user")
		}
	})

	t.Run("SFTP_MissingHost", func(t *testing.T) {
		_, err := litestream.NewReplicaClientFromURL("sftp:///path")
		if err == nil {
			t.Fatal("expected error for missing host")
		}
	})

	t.Run("WebDAV", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("webdav://host.example.com/path")
		if err != nil {
			t.Fatal(err)
		}
		if client.Type() != "webdav" {
			t.Errorf("expected type 'webdav', got %q", client.Type())
		}
		webdavClient, ok := client.(*webdav.ReplicaClient)
		if !ok {
			t.Fatalf("expected *webdav.ReplicaClient, got %T", client)
		}
		if webdavClient.URL != "http://host.example.com" {
			t.Errorf("expected URL 'http://host.example.com', got %q", webdavClient.URL)
		}
		if webdavClient.Path != "path" {
			t.Errorf("expected path 'path', got %q", webdavClient.Path)
		}
	})

	t.Run("WebDAVS", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("webdavs://host.example.com/path")
		if err != nil {
			t.Fatal(err)
		}
		if client.Type() != "webdav" {
			t.Errorf("expected type 'webdav', got %q", client.Type())
		}
		webdavClient, ok := client.(*webdav.ReplicaClient)
		if !ok {
			t.Fatalf("expected *webdav.ReplicaClient, got %T", client)
		}
		if webdavClient.URL != "https://host.example.com" {
			t.Errorf("expected URL 'https://host.example.com', got %q", webdavClient.URL)
		}
	})

	t.Run("WebDAV_WithCredentials", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("webdav://myuser:secret@host.example.com/path")
		if err != nil {
			t.Fatal(err)
		}
		webdavClient, ok := client.(*webdav.ReplicaClient)
		if !ok {
			t.Fatalf("expected *webdav.ReplicaClient, got %T", client)
		}
		if webdavClient.Username != "myuser" {
			t.Errorf("expected username 'myuser', got %q", webdavClient.Username)
		}
		if webdavClient.Password != "secret" {
			t.Errorf("expected password 'secret', got %q", webdavClient.Password)
		}
		if webdavClient.URL != "http://host.example.com" {
			t.Errorf("expected URL 'http://host.example.com', got %q", webdavClient.URL)
		}
	})

	t.Run("WebDAVS_WithCredentials", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("webdavs://myuser:secret@host.example.com/path")
		if err != nil {
			t.Fatal(err)
		}
		webdavClient, ok := client.(*webdav.ReplicaClient)
		if !ok {
			t.Fatalf("expected *webdav.ReplicaClient, got %T", client)
		}
		if webdavClient.Username != "myuser" {
			t.Errorf("expected username 'myuser', got %q", webdavClient.Username)
		}
		if webdavClient.Password != "secret" {
			t.Errorf("expected password 'secret', got %q", webdavClient.Password)
		}
		if webdavClient.URL != "https://host.example.com" {
			t.Errorf("expected URL 'https://host.example.com', got %q", webdavClient.URL)
		}
	})

	t.Run("WebDAV_MissingHost", func(t *testing.T) {
		_, err := litestream.NewReplicaClientFromURL("webdav:///path")
		if err == nil {
			t.Fatal("expected error for missing host")
		}
	})

	t.Run("NATS", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("nats://localhost:4222/mybucket")
		if err != nil {
			t.Fatal(err)
		}
		if client.Type() != "nats" {
			t.Errorf("expected type 'nats', got %q", client.Type())
		}
		natsClient, ok := client.(*nats.ReplicaClient)
		if !ok {
			t.Fatalf("expected *nats.ReplicaClient, got %T", client)
		}
		if natsClient.URL != "nats://localhost:4222" {
			t.Errorf("expected URL 'nats://localhost:4222', got %q", natsClient.URL)
		}
		if natsClient.BucketName != "mybucket" {
			t.Errorf("expected bucket 'mybucket', got %q", natsClient.BucketName)
		}
	})

	t.Run("NATS_WithCredentials", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("nats://myuser:secret@localhost:4222/mybucket")
		if err != nil {
			t.Fatal(err)
		}
		natsClient, ok := client.(*nats.ReplicaClient)
		if !ok {
			t.Fatalf("expected *nats.ReplicaClient, got %T", client)
		}
		if natsClient.Username != "myuser" {
			t.Errorf("expected username 'myuser', got %q", natsClient.Username)
		}
		if natsClient.Password != "secret" {
			t.Errorf("expected password 'secret', got %q", natsClient.Password)
		}
		if natsClient.URL != "nats://localhost:4222" {
			t.Errorf("expected URL 'nats://localhost:4222', got %q", natsClient.URL)
		}
	})

	t.Run("NATS_MissingBucket", func(t *testing.T) {
		_, err := litestream.NewReplicaClientFromURL("nats://localhost:4222/")
		if err == nil {
			t.Fatal("expected error for missing bucket")
		}
	})

	t.Run("OSS", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("oss://mybucket/path")
		if err != nil {
			t.Fatal(err)
		}
		if client.Type() != "oss" {
			t.Errorf("expected type 'oss', got %q", client.Type())
		}
		ossClient, ok := client.(*oss.ReplicaClient)
		if !ok {
			t.Fatalf("expected *oss.ReplicaClient, got %T", client)
		}
		if ossClient.Bucket != "mybucket" {
			t.Errorf("expected bucket 'mybucket', got %q", ossClient.Bucket)
		}
		if ossClient.Path != "path" {
			t.Errorf("expected path 'path', got %q", ossClient.Path)
		}
	})

	t.Run("OSS_WithRegion", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("oss://mybucket.oss-cn-shanghai.aliyuncs.com/path")
		if err != nil {
			t.Fatal(err)
		}
		ossClient, ok := client.(*oss.ReplicaClient)
		if !ok {
			t.Fatalf("expected *oss.ReplicaClient, got %T", client)
		}
		if ossClient.Bucket != "mybucket" {
			t.Errorf("expected bucket 'mybucket', got %q", ossClient.Bucket)
		}
		// Note: Region is extracted without the 'oss-' prefix
		if ossClient.Region != "cn-shanghai" {
			t.Errorf("expected region 'cn-shanghai', got %q", ossClient.Region)
		}
	})

	t.Run("OSS_MissingBucket", func(t *testing.T) {
		_, err := litestream.NewReplicaClientFromURL("oss:///path")
		if err == nil {
			t.Fatal("expected error for missing bucket")
		}
	})

	// Note: file:// with empty path returns "." due to path.Clean behavior.
	// This is technically valid but may not be the intended behavior.
	t.Run("File_EmptyPathReturnsDot", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("file://")
		if err != nil {
			t.Fatal(err)
		}
		fileClient, ok := client.(*file.ReplicaClient)
		if !ok {
			t.Fatalf("expected *file.ReplicaClient, got %T", client)
		}
		// path.Clean("") returns "." which passes the empty check
		if fileClient.Path() != "." {
			t.Errorf("expected path '.', got %q", fileClient.Path())
		}
	})

	t.Run("S3_ARN", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("s3://arn:aws:s3:us-east-1:123456789012:accesspoint/db-access/backups")
		if err != nil {
			t.Fatal(err)
		}
		s3Client, ok := client.(*s3.ReplicaClient)
		if !ok {
			t.Fatalf("expected *s3.ReplicaClient, got %T", client)
		}
		if s3Client.Bucket != "arn:aws:s3:us-east-1:123456789012:accesspoint/db-access" {
			t.Errorf("expected bucket ARN, got %q", s3Client.Bucket)
		}
		if s3Client.Path != "backups" {
			t.Errorf("expected path 'backups', got %q", s3Client.Path)
		}
	})

	t.Run("S3_ARN_WithQueryParams", func(t *testing.T) {
		client, err := litestream.NewReplicaClientFromURL("s3://arn:aws:s3:us-east-1:123456789012:accesspoint/db-access/backups?sign-payload=false")
		if err != nil {
			t.Fatal(err)
		}
		s3Client, ok := client.(*s3.ReplicaClient)
		if !ok {
			t.Fatalf("expected *s3.ReplicaClient, got %T", client)
		}
		if s3Client.Bucket != "arn:aws:s3:us-east-1:123456789012:accesspoint/db-access" {
			t.Errorf("expected bucket ARN, got %q", s3Client.Bucket)
		}
		if s3Client.Path != "backups" {
			t.Errorf("expected path 'backups', got %q", s3Client.Path)
		}
		if s3Client.SignPayload != false {
			t.Errorf("expected SignPayload=false from query param, got %v", s3Client.SignPayload)
		}
	})

	t.Run("S3_MissingBucket", func(t *testing.T) {
		_, err := litestream.NewReplicaClientFromURL("s3:///path")
		if err == nil {
			t.Fatal("expected error for missing bucket")
		}
	})

	t.Run("EmptyURL", func(t *testing.T) {
		_, err := litestream.NewReplicaClientFromURL("")
		if err == nil {
			t.Fatal("expected error for empty URL")
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

	t.Run("Nil query", func(t *testing.T) {
		_, ok := litestream.BoolQueryValue(nil, "key")
		if ok {
			t.Error("BoolQueryValue with nil query should not be ok")
		}
	})

	t.Run("Invalid value returns false with ok", func(t *testing.T) {
		query := make(map[string][]string)
		query["key"] = []string{"invalid"}
		value, ok := litestream.BoolQueryValue(query, "key")
		if !ok {
			t.Error("BoolQueryValue with invalid value should be ok")
		}
		if value {
			t.Error("BoolQueryValue with invalid value should be false")
		}
	})
}

func TestIsTigrisEndpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		expected bool
	}{
		{"fly.storage.tigris.dev", true},
		{"FLY.STORAGE.TIGRIS.DEV", true},
		{"https://fly.storage.tigris.dev", true},
		{"http://fly.storage.tigris.dev", true},
		{"t3.storage.dev", true},
		{"T3.STORAGE.DEV", true},
		{"https://t3.storage.dev", true},
		{"http://t3.storage.dev", true},
		{"s3.amazonaws.com", false},
		{"localhost:9000", false},
		{"", false},
		{"   ", false},
		{"https://s3.us-east-1.amazonaws.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			got := litestream.IsTigrisEndpoint(tt.endpoint)
			if got != tt.expected {
				t.Errorf("IsTigrisEndpoint(%q) = %v, want %v", tt.endpoint, got, tt.expected)
			}
		})
	}
}

func TestRegionFromS3ARN(t *testing.T) {
	tests := []struct {
		arn      string
		expected string
	}{
		{"arn:aws:s3:us-east-1:123456789012:accesspoint/db-access", "us-east-1"},
		{"arn:aws:s3:eu-west-1:123456789012:accesspoint/db-access", "eu-west-1"},
		{"arn:aws:s3:ap-southeast-2:123456789012:accesspoint/db-access", "ap-southeast-2"},
		{"arn:aws:s3::123456789012:accesspoint/db-access", ""},
		{"invalid-arn", ""},
		{"", ""},
		{"arn:aws:s3", ""},
	}

	for _, tt := range tests {
		t.Run(tt.arn, func(t *testing.T) {
			got := litestream.RegionFromS3ARN(tt.arn)
			if got != tt.expected {
				t.Errorf("RegionFromS3ARN(%q) = %q, want %q", tt.arn, got, tt.expected)
			}
		})
	}
}

func TestCleanReplicaURLPath(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"", ""},
		{"path", "path"},
		{"/path", "path"},
		{"path/", "path"},
		{"/path/", "path"},
		{"path/to/db", "path/to/db"},
		{"/path/to/db", "path/to/db"},
		{"//path//to//db", "path/to/db"},
		{".", ""},
		{"/.", ""},
		{"./path", "path"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := litestream.CleanReplicaURLPath(tt.path)
			if got != tt.expected {
				t.Errorf("CleanReplicaURLPath(%q) = %q, want %q", tt.path, got, tt.expected)
			}
		})
	}
}

func TestParseS3AccessPointURL(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		wantScheme string
		wantHost   string
		wantPath   string
		wantQuery  map[string]string
		wantErr    bool
	}{
		{
			name:       "BasicARN",
			url:        "s3://arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point",
			wantScheme: "s3",
			wantHost:   "arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point",
			wantPath:   "",
			wantQuery:  nil,
		},
		{
			name:       "ARNWithPath",
			url:        "s3://arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point/backups/db",
			wantScheme: "s3",
			wantHost:   "arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point",
			wantPath:   "backups/db",
			wantQuery:  nil,
		},
		{
			name:       "ARNWithSingleQueryParam",
			url:        "s3://arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point?sign-payload=true",
			wantScheme: "s3",
			wantHost:   "arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point",
			wantPath:   "",
			wantQuery:  map[string]string{"sign-payload": "true"},
		},
		{
			name:       "ARNWithMultipleQueryParams",
			url:        "s3://arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point?sign-payload=false&region=us-west-2",
			wantScheme: "s3",
			wantHost:   "arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point",
			wantPath:   "",
			wantQuery:  map[string]string{"sign-payload": "false", "region": "us-west-2"},
		},
		{
			name:       "ARNWithPathAndQuery",
			url:        "s3://arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point/backups?sign-payload=true",
			wantScheme: "s3",
			wantHost:   "arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point",
			wantPath:   "backups",
			wantQuery:  map[string]string{"sign-payload": "true"},
		},
		{
			name:       "CaseInsensitiveScheme",
			url:        "S3://arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point",
			wantScheme: "s3",
			wantHost:   "arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point",
			wantPath:   "",
			wantQuery:  nil,
		},
		{
			name:       "EmptyQueryValue",
			url:        "s3://arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point?key=",
			wantScheme: "s3",
			wantHost:   "arn:aws:s3:us-east-1:123456789012:accesspoint/my-access-point",
			wantPath:   "",
			wantQuery:  map[string]string{"key": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme, host, path, query, _, err := litestream.ParseReplicaURLWithQuery(tt.url)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if scheme != tt.wantScheme {
				t.Errorf("scheme = %q, want %q", scheme, tt.wantScheme)
			}
			if host != tt.wantHost {
				t.Errorf("host = %q, want %q", host, tt.wantHost)
			}
			if path != tt.wantPath {
				t.Errorf("path = %q, want %q", path, tt.wantPath)
			}

			if tt.wantQuery == nil {
				if len(query) > 0 {
					t.Errorf("query = %v, want nil/empty", query)
				}
			} else {
				for key, wantVal := range tt.wantQuery {
					if gotVal := query.Get(key); gotVal != wantVal {
						t.Errorf("query[%q] = %q, want %q", key, gotVal, wantVal)
					}
				}
			}
		})
	}
}

func TestIsDigitalOceanEndpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		expected bool
	}{
		{"https://sfo3.digitaloceanspaces.com", true},
		{"https://nyc3.digitaloceanspaces.com", true},
		{"sfo3.digitaloceanspaces.com", true},
		{"https://s3.amazonaws.com", false},
		{"https://s3.filebase.com", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			got := litestream.IsDigitalOceanEndpoint(tt.endpoint)
			if got != tt.expected {
				t.Errorf("IsDigitalOceanEndpoint(%q) = %v, want %v", tt.endpoint, got, tt.expected)
			}
		})
	}
}

func TestIsBackblazeEndpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		expected bool
	}{
		{"https://s3.us-west-002.backblazeb2.com", true},
		{"https://s3.eu-central-003.backblazeb2.com", true},
		{"s3.us-west-002.backblazeb2.com", true},
		{"https://s3.amazonaws.com", false},
		{"https://s3.filebase.com", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			got := litestream.IsBackblazeEndpoint(tt.endpoint)
			if got != tt.expected {
				t.Errorf("IsBackblazeEndpoint(%q) = %v, want %v", tt.endpoint, got, tt.expected)
			}
		})
	}
}

func TestIsFilebaseEndpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		expected bool
	}{
		{"https://s3.filebase.com", true},
		{"http://s3.filebase.com", true},
		{"s3.filebase.com", true},
		{"https://s3.amazonaws.com", false},
		{"https://sfo3.digitaloceanspaces.com", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			got := litestream.IsFilebaseEndpoint(tt.endpoint)
			if got != tt.expected {
				t.Errorf("IsFilebaseEndpoint(%q) = %v, want %v", tt.endpoint, got, tt.expected)
			}
		})
	}
}

func TestIsScalewayEndpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		expected bool
	}{
		{"https://s3.fr-par.scw.cloud", true},
		{"https://s3.nl-ams.scw.cloud", true},
		{"s3.fr-par.scw.cloud", true},
		{"https://s3.amazonaws.com", false},
		{"https://s3.filebase.com", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			got := litestream.IsScalewayEndpoint(tt.endpoint)
			if got != tt.expected {
				t.Errorf("IsScalewayEndpoint(%q) = %v, want %v", tt.endpoint, got, tt.expected)
			}
		})
	}
}

func TestIsCloudflareR2Endpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		expected bool
	}{
		{"https://abcdef123456.r2.cloudflarestorage.com", true},
		{"https://account-id.r2.cloudflarestorage.com", true},
		{"abcdef123456.r2.cloudflarestorage.com", true},
		{"https://s3.amazonaws.com", false},
		{"https://s3.filebase.com", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			got := litestream.IsCloudflareR2Endpoint(tt.endpoint)
			if got != tt.expected {
				t.Errorf("IsCloudflareR2Endpoint(%q) = %v, want %v", tt.endpoint, got, tt.expected)
			}
		})
	}
}

func TestIsMinIOEndpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		expected bool
	}{
		{"http://localhost:9000", true},
		{"http://192.168.1.100:9000", true},
		{"minio.local:9000", true},
		{"https://s3.amazonaws.com", false},
		{"https://s3.filebase.com", false},
		{"https://sfo3.digitaloceanspaces.com", false},
		{"s3.filebase.com", false}, // No port, not MinIO
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			got := litestream.IsMinIOEndpoint(tt.endpoint)
			if got != tt.expected {
				t.Errorf("IsMinIOEndpoint(%q) = %v, want %v", tt.endpoint, got, tt.expected)
			}
		})
	}
}
