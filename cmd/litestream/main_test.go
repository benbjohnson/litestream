package main_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gs"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
)

func TestOpenConfigFile(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		// Create a temporary file with test content
		dir := t.TempDir()
		testContent := "test: content\n"
		configPath := filepath.Join(dir, "test.yml")
		if err := os.WriteFile(configPath, []byte(testContent), 0644); err != nil {
			t.Fatal(err)
		}

		// Open the file
		rc, err := main.OpenConfigFile(configPath)
		if err != nil {
			t.Fatalf("failed to open config file: %v", err)
		}
		defer rc.Close()

		// Read and verify the content
		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(rc); err != nil {
			t.Fatalf("failed to read from config file: %v", err)
		}

		if got := buf.String(); got != testContent {
			t.Errorf("content mismatch: got %q, want %q", got, testContent)
		}
	})

	t.Run("FileNotFound", func(t *testing.T) {
		_, err := main.OpenConfigFile("/nonexistent/file.yml")
		if err == nil {
			t.Error("expected error for nonexistent file")
		} else if !errors.Is(err, main.ErrConfigFileNotFound) {
			t.Errorf("expected ErrConfigFileNotFound, got: %v", err)
		}
	})
}

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

func TestNewDBFromConfig_MetaPathExpansion(t *testing.T) {
	u, err := user.Current()
	if err != nil {
		t.Skipf("user.Current failed: %v", err)
	}
	if u.HomeDir == "" {
		t.Skip("no home directory available for expansion test")
	}

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "db.sqlite")
	replicaPath := filepath.Join(tmpDir, "replica")
	if err := os.MkdirAll(replicaPath, 0o755); err != nil {
		t.Fatalf("failed to create replica directory: %v", err)
	}

	metaPath := filepath.Join("~", "litestream-meta")
	config := &main.DBConfig{
		Path:     dbPath,
		MetaPath: &metaPath,
		Replica: &main.ReplicaConfig{
			Type: "file",
			Path: replicaPath,
		},
	}

	db, err := main.NewDBFromConfig(config)
	if err != nil {
		t.Fatalf("NewDBFromConfig failed: %v", err)
	}

	expectedMetaPath := filepath.Join(u.HomeDir, "litestream-meta")
	if got := db.MetaPath(); got != expectedMetaPath {
		t.Fatalf("MetaPath not expanded: got %s, want %s", got, expectedMetaPath)
	}
	if config.MetaPath == nil || *config.MetaPath != expectedMetaPath {
		t.Fatalf("config MetaPath not updated: got %v, want %s", config.MetaPath, expectedMetaPath)
	}
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

	t.Run("AccessPointARN", func(t *testing.T) {
		url := "s3://arn:aws:s3:us-east-2:123456789012:accesspoint/stream-replica"
		r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{URL: url}, nil)
		if err != nil {
			t.Fatal(err)
		}
		client, ok := r.Client.(*s3.ReplicaClient)
		if !ok {
			t.Fatal("unexpected replica type")
		}
		if got, want := client.Bucket, "arn:aws:s3:us-east-2:123456789012:accesspoint/stream-replica"; got != want {
			t.Fatalf("Bucket=%s, want %s", got, want)
		}
		if got, want := client.Path, ""; got != want {
			t.Fatalf("Path=%s, want %s", got, want)
		}
		if got, want := client.Region, "us-east-2"; got != want {
			t.Fatalf("Region=%s, want %s", got, want)
		}
		if got, want := client.Endpoint, ""; got != want {
			t.Fatalf("Endpoint=%s, want %s", got, want)
		}
		if got, want := client.ForcePathStyle, false; got != want {
			t.Fatalf("ForcePathStyle=%v, want %v", got, want)
		}
	})

	t.Run("AccessPointARNWithPrefix", func(t *testing.T) {
		url := "s3://arn:aws:s3:us-west-1:123456789012:accesspoint/stream-replica/backups/primary"
		r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{URL: url}, nil)
		if err != nil {
			t.Fatal(err)
		}
		client, ok := r.Client.(*s3.ReplicaClient)
		if !ok {
			t.Fatal("unexpected replica type")
		}
		if got, want := client.Bucket, "arn:aws:s3:us-west-1:123456789012:accesspoint/stream-replica"; got != want {
			t.Fatalf("Bucket=%s, want %s", got, want)
		}
		if got, want := client.Path, "backups/primary"; got != want {
			t.Fatalf("Path=%s, want %s", got, want)
		}
		if got, want := client.Region, "us-west-1"; got != want {
			t.Fatalf("Region=%s, want %s", got, want)
		}
		if got, want := client.Endpoint, ""; got != want {
			t.Fatalf("Endpoint=%s, want %s", got, want)
		}
		if got, want := client.ForcePathStyle, false; got != want {
			t.Fatalf("ForcePathStyle=%v, want %v", got, want)
		}
	})
}

func TestNewGSReplicaFromConfig(t *testing.T) {
	r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{URL: "gs://foo/bar"}, nil)
	if err != nil {
		t.Fatal(err)
	} else if client, ok := r.Client.(*gs.ReplicaClient); !ok {
		t.Fatal("unexpected replica type")
	} else if got, want := client.Bucket, "foo"; got != want {
		t.Fatalf("Bucket=%s, want %s", got, want)
	} else if got, want := client.Path, "bar"; got != want {
		t.Fatalf("Path=%s, want %s", got, want)
	}
}

func TestNewSFTPReplicaFromConfig(t *testing.T) {
	hostKey := "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIAnK0+GdwOelXlAXdqLx/qvS7WHMr3rH7zW2+0DtmK5r"
	r, err := main.NewReplicaFromConfig(&main.ReplicaConfig{
		URL: "sftp://user@example.com:2222/foo",
		ReplicaSettings: main.ReplicaSettings{
			HostKey: hostKey,
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	} else if client, ok := r.Client.(*sftp.ReplicaClient); !ok {
		t.Fatal("unexpected replica type")
	} else if got, want := client.HostKey, hostKey; got != want {
		t.Fatalf("HostKey=%s, want %s", got, want)
	} else if got, want := client.Host, "example.com:2222"; got != want {
		t.Fatalf("Host=%s, want %s", got, want)
	} else if got, want := client.User, "user"; got != want {
		t.Fatalf("User=%s, want %s", got, want)
	} else if got, want := client.Path, "/foo"; got != want {
		t.Fatalf("Path=%s, want %s", got, want)
	}
}

// TestNewReplicaFromConfig_AgeEncryption verifies that age encryption configuration is rejected.
// Age encryption is currently non-functional and would silently write plaintext data.
// See: https://github.com/benbjohnson/litestream/issues/790
func TestNewReplicaFromConfig_AgeEncryption(t *testing.T) {
	t.Run("RejectIdentities", func(t *testing.T) {
		config := &main.ReplicaConfig{
			URL: "s3://foo/bar",
		}
		config.Age.Identities = []string{"AGE-SECRET-KEY-1EXAMPLE"}

		_, err := main.NewReplicaFromConfig(config, nil)
		if err == nil {
			t.Fatal("expected error when age identities are configured")
		}
		if !strings.Contains(err.Error(), "age encryption is not currently supported") {
			t.Errorf("expected age encryption error, got: %v", err)
		}
		if !strings.Contains(err.Error(), "revert back to Litestream v0.3.x") {
			t.Errorf("expected error to reference v0.3.x, got: %v", err)
		}
	})

	t.Run("RejectRecipients", func(t *testing.T) {
		config := &main.ReplicaConfig{
			URL: "s3://foo/bar",
		}
		config.Age.Recipients = []string{"age1example"}

		_, err := main.NewReplicaFromConfig(config, nil)
		if err == nil {
			t.Fatal("expected error when age recipients are configured")
		}
		if !strings.Contains(err.Error(), "age encryption is not currently supported") {
			t.Errorf("expected age encryption error, got: %v", err)
		}
		if !strings.Contains(err.Error(), "revert back to Litestream v0.3.x") {
			t.Errorf("expected error to reference v0.3.x, got: %v", err)
		}
	})

	t.Run("RejectBoth", func(t *testing.T) {
		config := &main.ReplicaConfig{
			URL: "s3://foo/bar",
		}
		config.Age.Identities = []string{"AGE-SECRET-KEY-1EXAMPLE"}
		config.Age.Recipients = []string{"age1example"}

		_, err := main.NewReplicaFromConfig(config, nil)
		if err == nil {
			t.Fatal("expected error when both age identities and recipients are configured")
		}
		if !strings.Contains(err.Error(), "age encryption is not currently supported") {
			t.Errorf("expected age encryption error, got: %v", err)
		}
		if !strings.Contains(err.Error(), "revert back to Litestream v0.3.x") {
			t.Errorf("expected error to reference v0.3.x, got: %v", err)
		}
	})

	t.Run("AllowEmpty", func(t *testing.T) {
		config := &main.ReplicaConfig{
			URL: "s3://foo/bar",
		}

		_, err := main.NewReplicaFromConfig(config, nil)
		if err != nil {
			t.Fatalf("unexpected error when age configuration is not present: %v", err)
		}
	})
}

// TestConfig_Validate_SnapshotIntervals tests validation of snapshot intervals
func TestConfig_Validate_SnapshotIntervals(t *testing.T) {
	t.Run("ValidInterval", func(t *testing.T) {
		yaml := `
snapshot:
  interval: 1h
  retention: 24h
`
		config, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify the values were set as expected
		if config.Snapshot.Interval == nil {
			t.Fatal("expected snapshot interval to be set")
		}
		if *config.Snapshot.Interval != 1*time.Hour {
			t.Errorf("expected snapshot interval of 1h, got %v", *config.Snapshot.Interval)
		}

		if config.Snapshot.Retention == nil {
			t.Fatal("expected snapshot retention to be set")
		}
		if *config.Snapshot.Retention != 24*time.Hour {
			t.Errorf("expected snapshot retention of 24h, got %v", *config.Snapshot.Retention)
		}
	})

	t.Run("ZeroInterval", func(t *testing.T) {
		yaml := `
snapshot:
  interval: 0s
  retention: 24h
`
		_, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err == nil {
			t.Fatal("expected error for zero interval")
		}
		if !errors.Is(err, main.ErrInvalidSnapshotInterval) {
			t.Errorf("expected ErrInvalidSnapshotInterval, got %v", err)
		}
	})

	t.Run("ZeroRetention", func(t *testing.T) {
		yaml := `
snapshot:
  interval: 1h
  retention: 0s
`
		_, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err == nil {
			t.Fatal("expected error for zero retention")
		}
		if !errors.Is(err, main.ErrInvalidSnapshotRetention) {
			t.Errorf("expected ErrInvalidSnapshotRetention, got %v", err)
		}
	})

	t.Run("NegativeInterval", func(t *testing.T) {
		yaml := `
snapshot:
  interval: -1h
  retention: 24h
`
		_, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err == nil {
			t.Fatal("expected error for negative interval")
		}
		if !errors.Is(err, main.ErrInvalidSnapshotInterval) {
			t.Errorf("expected ErrInvalidSnapshotInterval, got %v", err)
		}
	})

	t.Run("NotSpecified", func(t *testing.T) {
		yaml := `
# snapshot section not specified
dbs:
  - path: /tmp/test.db
`
		config, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// When snapshot section is not specified, defaults should be applied
		if config.Snapshot.Interval == nil {
			t.Fatal("expected snapshot interval to have default value")
		}
		if *config.Snapshot.Interval != 24*time.Hour {
			t.Errorf("expected default snapshot interval of 24h, got %v", *config.Snapshot.Interval)
		}

		if config.Snapshot.Retention == nil {
			t.Fatal("expected snapshot retention to have default value")
		}
		if *config.Snapshot.Retention != 24*time.Hour {
			t.Errorf("expected default snapshot retention of 24h, got %v", *config.Snapshot.Retention)
		}
	})

	t.Run("EmptySection", func(t *testing.T) {
		yaml := `
snapshot:
`
		config, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// When snapshot section is empty, defaults should be preserved
		if config.Snapshot.Interval == nil {
			t.Fatal("expected snapshot interval to have default value")
		}
		if *config.Snapshot.Interval != 24*time.Hour {
			t.Errorf("expected default snapshot interval of 24h, got %v", *config.Snapshot.Interval)
		}

		if config.Snapshot.Retention == nil {
			t.Fatal("expected snapshot retention to have default value")
		}
		if *config.Snapshot.Retention != 24*time.Hour {
			t.Errorf("expected default snapshot retention of 24h, got %v", *config.Snapshot.Retention)
		}
	})
}

func TestParseReplicaURL_AccessPoint(t *testing.T) {
	t.Run("WithPrefix", func(t *testing.T) {
		scheme, host, urlPath, err := litestream.ParseReplicaURL("s3://arn:aws:s3:us-east-1:123456789012:accesspoint/db-access/backups/prod")
		if err != nil {
			t.Fatal(err)
		}
		if scheme != "s3" {
			t.Fatalf("scheme=%s, want s3", scheme)
		}
		if host != "arn:aws:s3:us-east-1:123456789012:accesspoint/db-access" {
			t.Fatalf("host=%s, want arn:aws:s3:us-east-1:123456789012:accesspoint/db-access", host)
		}
		if urlPath != "backups/prod" {
			t.Fatalf("path=%s, want backups/prod", urlPath)
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		if _, _, _, err := litestream.ParseReplicaURL("s3://arn:aws:s3:us-east-1:123456789012:accesspoint/"); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestConfig_Validate_L0Retention(t *testing.T) {
	t.Run("ZeroRetention", func(t *testing.T) {
		yaml := `
l0-retention: 0s
dbs:
  - path: /tmp/test.db
    replica:
      url: file:///tmp/replica
`
		_, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err == nil {
			t.Fatal("expected error for zero l0 retention")
		}
		if !errors.Is(err, main.ErrInvalidL0Retention) {
			t.Errorf("expected ErrInvalidL0Retention, got %v", err)
		}
	})

	t.Run("ZeroCheckInterval", func(t *testing.T) {
		yaml := `
l0-retention-check-interval: 0s
dbs:
  - path: /tmp/test.db
    replica:
      url: file:///tmp/replica
`
		_, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err == nil {
			t.Fatal("expected error for zero l0 retention check interval")
		}
		if !errors.Is(err, main.ErrInvalidL0RetentionCheckInterval) {
			t.Errorf("expected ErrInvalidL0RetentionCheckInterval, got %v", err)
		}
	})

	t.Run("DefaultsApplied", func(t *testing.T) {
		yaml := `
dbs:
  - path: /tmp/test.db
    replica:
      url: file:///tmp/replica
`
		config, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if config.L0Retention == nil {
			t.Fatal("expected default l0 retention to be set")
		}
		if *config.L0Retention != litestream.DefaultL0Retention {
			t.Errorf("expected default l0 retention %v, got %v", litestream.DefaultL0Retention, *config.L0Retention)
		}
		if config.L0RetentionCheckInterval == nil {
			t.Fatal("expected default l0 retention check interval to be set")
		}
		if *config.L0RetentionCheckInterval != litestream.DefaultL0RetentionCheckInterval {
			t.Errorf("expected default l0 retention check interval %v, got %v", litestream.DefaultL0RetentionCheckInterval, *config.L0RetentionCheckInterval)
		}
	})
}

// TestConfig_Validate_SyncIntervals tests validation of replica sync intervals
func TestConfig_Validate_SyncIntervals(t *testing.T) {
	t.Run("ValidSyncInterval", func(t *testing.T) {
		yaml := `
dbs:
  - path: /tmp/test.db
    replica:
      url: file:///tmp/replica
      sync-interval: 30s
`
		config, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify the sync interval was set correctly
		if len(config.DBs) != 1 {
			t.Fatal("expected one database")
		}
		if config.DBs[0].Replica == nil {
			t.Fatal("expected replica to be set")
		}
		if config.DBs[0].Replica.SyncInterval == nil {
			t.Fatal("expected sync interval to be set")
		}
		if *config.DBs[0].Replica.SyncInterval != 30*time.Second {
			t.Errorf("expected sync interval of 30s, got %v", *config.DBs[0].Replica.SyncInterval)
		}
	})

	t.Run("ZeroSyncInterval", func(t *testing.T) {
		yaml := `
dbs:
  - path: /tmp/test.db
    replica:
      url: file:///tmp/replica
      sync-interval: 0s
`
		_, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err == nil {
			t.Fatal("expected error for zero sync interval")
		}
		if !errors.Is(err, main.ErrInvalidSyncInterval) {
			t.Errorf("expected ErrInvalidSyncInterval, got %v", err)
		}
	})

	t.Run("NegativeSyncInterval", func(t *testing.T) {
		yaml := `
dbs:
  - path: /tmp/test.db
    replica:
      url: file:///tmp/replica
      sync-interval: -30s
`
		_, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err == nil {
			t.Fatal("expected error for negative sync interval")
		}
		if !errors.Is(err, main.ErrInvalidSyncInterval) {
			t.Errorf("expected ErrInvalidSyncInterval, got %v", err)
		}
	})

	t.Run("NotSpecifiedSyncInterval", func(t *testing.T) {
		yaml := `
dbs:
  - path: /tmp/test.db
    replica:
      url: file:///tmp/replica
`
		config, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// When sync-interval is not specified, it should remain nil
		// The default will be applied when the replica is created
		if len(config.DBs) != 1 {
			t.Fatal("expected one database")
		}
		if config.DBs[0].Replica == nil {
			t.Fatal("expected replica to be set")
		}
		if config.DBs[0].Replica.SyncInterval != nil {
			t.Errorf("expected sync-interval to be nil when not specified, got %v", *config.DBs[0].Replica.SyncInterval)
		}
	})

	t.Run("MultipleReplicasWithZero", func(t *testing.T) {
		yaml := `
dbs:
  - path: /tmp/test.db
    replicas:
      - url: file:///tmp/replica1
        sync-interval: 30s
      - url: file:///tmp/replica2
        sync-interval: 0s
`
		_, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err == nil {
			t.Fatal("expected error for zero sync interval in second replica")
		}
		if !errors.Is(err, main.ErrInvalidSyncInterval) {
			t.Errorf("expected ErrInvalidSyncInterval, got %v", err)
		}
	})

	t.Run("ValidMultipleReplicas", func(t *testing.T) {
		yaml := `
dbs:
  - path: /tmp/test.db
    replicas:
      - url: file:///tmp/replica1
        sync-interval: 30s
      - url: file:///tmp/replica2
        sync-interval: 1m
`
		config, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify both replicas have correct intervals
		if len(config.DBs) != 1 {
			t.Fatal("expected one database")
		}
		if len(config.DBs[0].Replicas) != 2 {
			t.Fatal("expected two replicas")
		}

		// Check first replica
		if config.DBs[0].Replicas[0].SyncInterval == nil {
			t.Fatal("expected first replica sync interval to be set")
		}
		if *config.DBs[0].Replicas[0].SyncInterval != 30*time.Second {
			t.Errorf("expected first replica sync interval of 30s, got %v", *config.DBs[0].Replicas[0].SyncInterval)
		}

		// Check second replica
		if config.DBs[0].Replicas[1].SyncInterval == nil {
			t.Fatal("expected second replica sync interval to be set")
		}
		if *config.DBs[0].Replicas[1].SyncInterval != 1*time.Minute {
			t.Errorf("expected second replica sync interval of 1m, got %v", *config.DBs[0].Replicas[1].SyncInterval)
		}
	})
}

// TestConfig_Validate_CompactionLevels tests validation of compaction level intervals
func TestConfig_Validate_CompactionLevels(t *testing.T) {
	t.Run("ValidLevels", func(t *testing.T) {
		yaml := `
levels:
  - interval: 5m
  - interval: 1h
`
		config, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify the levels were set correctly
		if len(config.Levels) != 2 {
			t.Fatalf("expected 2 compaction levels, got %d", len(config.Levels))
		}
		if config.Levels[0].Interval != 5*time.Minute {
			t.Errorf("expected level[0] interval of 5m, got %v", config.Levels[0].Interval)
		}
		if config.Levels[1].Interval != 1*time.Hour {
			t.Errorf("expected level[1] interval of 1h, got %v", config.Levels[1].Interval)
		}
	})

	t.Run("ZeroLevelInterval", func(t *testing.T) {
		yaml := `
levels:
  - interval: 0s
  - interval: 1h
`
		_, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err == nil {
			t.Fatal("expected error for zero level interval")
		}
		if !errors.Is(err, main.ErrInvalidCompactionInterval) {
			t.Errorf("expected ErrInvalidCompactionInterval, got %v", err)
		}
	})

	t.Run("NegativeLevelInterval", func(t *testing.T) {
		yaml := `
levels:
  - interval: 5m
  - interval: -1h
`
		_, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err == nil {
			t.Fatal("expected error for negative level interval")
		}
		if !errors.Is(err, main.ErrInvalidCompactionInterval) {
			t.Errorf("expected ErrInvalidCompactionInterval, got %v", err)
		}
	})

	t.Run("NotSpecified", func(t *testing.T) {
		yaml := `
# levels section not specified, should use defaults
dbs:
  - path: /tmp/test.db
`
		config, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// When levels are not specified, defaults should be applied
		if len(config.Levels) != 3 {
			t.Fatalf("expected 3 default compaction levels, got %d", len(config.Levels))
		}

		// Check default intervals: 30s, 5m and 1h
		if config.Levels[0].Interval != 30*time.Second {
			t.Errorf("expected default level[0] interval of 5m, got %v", config.Levels[0].Interval)
		}
		if config.Levels[1].Interval != 5*time.Minute {
			t.Errorf("expected default level[0] interval of 5m, got %v", config.Levels[0].Interval)
		}
		if config.Levels[2].Interval != 1*time.Hour {
			t.Errorf("expected default level[1] interval of 1h, got %v", config.Levels[1].Interval)
		}
	})

	t.Run("CustomLevels", func(t *testing.T) {
		yaml := `
levels:
  - interval: 10m
  - interval: 30m
  - interval: 2h
`
		config, err := main.ParseConfig(strings.NewReader(yaml), false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify three custom levels
		if len(config.Levels) != 3 {
			t.Fatalf("expected 3 compaction levels, got %d", len(config.Levels))
		}
		if config.Levels[0].Interval != 10*time.Minute {
			t.Errorf("expected level[0] interval of 10m, got %v", config.Levels[0].Interval)
		}
		if config.Levels[1].Interval != 30*time.Minute {
			t.Errorf("expected level[1] interval of 30m, got %v", config.Levels[1].Interval)
		}
		if config.Levels[2].Interval != 2*time.Hour {
			t.Errorf("expected level[2] interval of 2h, got %v", config.Levels[2].Interval)
		}
	})
}

// TestConfig_DefaultValues tests that default values are properly set
func TestConfig_DefaultValues(t *testing.T) {
	// Test empty config
	config, err := main.ParseConfig(strings.NewReader(""), false)
	if err != nil {
		t.Fatal(err)
	}

	// Check snapshot defaults
	if config.Snapshot.Interval == nil {
		t.Error("expected snapshot interval to have default value")
	} else if *config.Snapshot.Interval != 24*time.Hour {
		t.Errorf("expected default snapshot interval of 24h, got %v", *config.Snapshot.Interval)
	}

	if config.Snapshot.Retention == nil {
		t.Error("expected snapshot retention to have default value")
	} else if *config.Snapshot.Retention != 24*time.Hour {
		t.Errorf("expected default snapshot retention of 24h, got %v", *config.Snapshot.Retention)
	}
}

// TestParseByteSize tests the ParseByteSize function with various inputs,
// including IEC units (MiB, GiB) and decimal values that require proper rounding.
func TestParseByteSize(t *testing.T) {
	tests := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		// IEC units (base 1024) - the most important fix for AWS/B2 docs compatibility
		{"1MiB", 1024 * 1024, false},
		{"5MiB", 5 * 1024 * 1024, false},
		{"1GiB", 1024 * 1024 * 1024, false},
		{"1TiB", 1024 * 1024 * 1024 * 1024, false},
		{"1024KiB", 1024 * 1024, false},

		// SI units (base 1000) - traditional metric units
		{"1MB", 1000 * 1000, false},
		{"5MB", 5 * 1000 * 1000, false},
		{"1GB", 1000 * 1000 * 1000, false},
		{"1TB", 1000 * 1000 * 1000 * 1000, false},
		{"1000KB", 1000 * 1000, false},

		// Short forms (base 1000 - SI units without the 'B')
		{"1M", 1000 * 1000, false},
		{"1K", 1000, false},
		{"1G", 1000 * 1000 * 1000, false},
		{"1T", 1000 * 1000 * 1000 * 1000, false},

		// Decimal values with proper rounding (no more truncation issues)
		{"1.5MB", 1500000, false},     // 1.5 * 1000 * 1000
		{"1.5MiB", 1572864, false},    // 1.5 * 1024 * 1024
		{"0.5MB", 500000, false},      // Should round properly, not truncate
		{"2.5GiB", 2684354560, false}, // 2.5 * 1024^3
		{"100.5KB", 100500, false},    // Decimals work with any unit

		// Basic units
		{"100B", 100, false},
		{"100", 100, false}, // No unit defaults to bytes

		// Case insensitive
		{"1mib", 1024 * 1024, false},
		{"5MIB", 5 * 1024 * 1024, false},
		{"1gib", 1024 * 1024 * 1024, false},

		// With spaces (go-humanize handles this)
		{"1 MiB", 1024 * 1024, false},
		{"5 MB", 5 * 1000 * 1000, false},
		{"10 GiB", 10 * 1024 * 1024 * 1024, false},

		// Real-world examples from AWS/Backblaze documentation
		{"5MB", 5000000, false},     // AWS SDK default
		{"100MB", 100000000, false}, // B2 recommended size
		{"5MiB", 5242880, false},    // The value from the original error report
		{"1MiB", 1048576, false},    // B2 minimum (though actually they require 5MB)

		// Invalid inputs
		{"", 0, true},
		{"MB", 0, true},
		{"invalid", 0, true},
		{"1XB", 0, true},
		{"notanumber", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := main.ParseByteSize(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseByteSize(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ParseByteSize(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

// TestParseByteSizeOverflow tests that values larger than int64 are rejected.
func TestParseByteSizeOverflow(t *testing.T) {
	// 10 EB (exabytes) = 10,000,000,000,000,000,000 bytes, which exceeds int64 max (9,223,372,036,854,775,807)
	_, err := main.ParseByteSize("10EB")
	if err == nil {
		t.Error("expected error for value exceeding int64 max, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Errorf("expected overflow error, got: %v", err)
	}
}

// TestS3ReplicaConfig_PartSizeAndConcurrency tests that part-size and concurrency
// configuration values are properly parsed from YAML and applied to the S3 client.
// This test addresses issue #747 where Backblaze B2's 1MB chunk size limit was
// being exceeded due to part-size not being honored.
func TestS3ReplicaConfig_PartSizeAndConcurrency(t *testing.T) {
	t.Run("WithPartSize_IEC", func(t *testing.T) {
		// Test IEC unit (MiB) - the main fix addressing PR feedback
		filename := filepath.Join(t.TempDir(), "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: /path/to/db
    replicas:
      - type: s3
        bucket: mybucket
        path: mypath
        region: us-east-1
        part-size: 5MiB
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, false)
		if err != nil {
			t.Fatal(err)
		}

		if len(config.DBs) != 1 || len(config.DBs[0].Replicas) != 1 {
			t.Fatal("expected one database with one replica")
		}

		replicaConfig := config.DBs[0].Replicas[0]
		if replicaConfig.PartSize == nil {
			t.Fatal("expected part-size to be set")
		}
		// 5 MiB = 5 * 1024 * 1024 = 5242880 bytes
		if got, want := int64(*replicaConfig.PartSize), int64(5*1024*1024); got != want {
			t.Errorf("PartSize = %d, want %d", got, want)
		}

		// Test that the value is properly applied to the client
		r, err := main.NewReplicaFromConfig(replicaConfig, nil)
		if err != nil {
			t.Fatal(err)
		}

		client, ok := r.Client.(*s3.ReplicaClient)
		if !ok {
			t.Fatal("expected S3 replica client")
		}

		if got, want := client.PartSize, int64(5*1024*1024); got != want {
			t.Errorf("client.PartSize = %d, want %d", got, want)
		}
	})

	t.Run("WithPartSize_SI", func(t *testing.T) {
		// Test SI unit (MB) - uses base 1000
		filename := filepath.Join(t.TempDir(), "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: /path/to/db
    replicas:
      - type: s3
        bucket: mybucket
        path: mypath
        region: us-east-1
        part-size: 5MB
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, false)
		if err != nil {
			t.Fatal(err)
		}

		if len(config.DBs) != 1 || len(config.DBs[0].Replicas) != 1 {
			t.Fatal("expected one database with one replica")
		}

		replicaConfig := config.DBs[0].Replicas[0]
		if replicaConfig.PartSize == nil {
			t.Fatal("expected part-size to be set")
		}
		// 5 MB = 5 * 1000 * 1000 = 5000000 bytes (SI units use base 1000)
		if got, want := int64(*replicaConfig.PartSize), int64(5*1000*1000); got != want {
			t.Errorf("PartSize = %d, want %d", got, want)
		}

		// Test that the value is properly applied to the client
		r, err := main.NewReplicaFromConfig(replicaConfig, nil)
		if err != nil {
			t.Fatal(err)
		}

		client, ok := r.Client.(*s3.ReplicaClient)
		if !ok {
			t.Fatal("expected S3 replica client")
		}

		if got, want := client.PartSize, int64(5*1000*1000); got != want {
			t.Errorf("client.PartSize = %d, want %d", got, want)
		}
	})

	t.Run("WithConcurrency", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: /path/to/db
    replicas:
      - type: s3
        bucket: mybucket
        path: mypath
        region: us-east-1
        concurrency: 10
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, false)
		if err != nil {
			t.Fatal(err)
		}

		if len(config.DBs) != 1 || len(config.DBs[0].Replicas) != 1 {
			t.Fatal("expected one database with one replica")
		}

		replicaConfig := config.DBs[0].Replicas[0]
		if replicaConfig.Concurrency == nil {
			t.Fatal("expected concurrency to be set")
		}
		if got, want := *replicaConfig.Concurrency, 10; got != want {
			t.Errorf("Concurrency = %d, want %d", got, want)
		}

		// Test that the value is properly applied to the client
		r, err := main.NewReplicaFromConfig(replicaConfig, nil)
		if err != nil {
			t.Fatal(err)
		}

		client, ok := r.Client.(*s3.ReplicaClient)
		if !ok {
			t.Fatal("expected S3 replica client")
		}

		if got, want := client.Concurrency, 10; got != want {
			t.Errorf("client.Concurrency = %d, want %d", got, want)
		}
	})

	t.Run("WithBoth", func(t *testing.T) {
		// Test both part-size (using IEC unit) and concurrency together
		filename := filepath.Join(t.TempDir(), "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: /path/to/db
    replicas:
      - type: s3
        bucket: mybucket
        path: mypath
        region: us-east-1
        part-size: 10MiB
        concurrency: 10
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, false)
		if err != nil {
			t.Fatal(err)
		}

		if len(config.DBs) != 1 || len(config.DBs[0].Replicas) != 1 {
			t.Fatal("expected one database with one replica")
		}

		replicaConfig := config.DBs[0].Replicas[0]

		// Verify both values are parsed
		if replicaConfig.PartSize == nil {
			t.Fatal("expected part-size to be set")
		}
		// 10 MiB = 10 * 1024 * 1024 = 10485760 bytes
		if got, want := int64(*replicaConfig.PartSize), int64(10*1024*1024); got != want {
			t.Errorf("PartSize = %d, want %d", got, want)
		}

		if replicaConfig.Concurrency == nil {
			t.Fatal("expected concurrency to be set")
		}
		if got, want := *replicaConfig.Concurrency, 10; got != want {
			t.Errorf("Concurrency = %d, want %d", got, want)
		}

		// Test that both values are properly applied to the client
		r, err := main.NewReplicaFromConfig(replicaConfig, nil)
		if err != nil {
			t.Fatal(err)
		}

		client, ok := r.Client.(*s3.ReplicaClient)
		if !ok {
			t.Fatal("expected S3 replica client")
		}

		if got, want := client.PartSize, int64(10*1024*1024); got != want {
			t.Errorf("client.PartSize = %d, want %d", got, want)
		}
		if got, want := client.Concurrency, 10; got != want {
			t.Errorf("client.Concurrency = %d, want %d", got, want)
		}
	})

	t.Run("NotSpecified", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: /path/to/db
    replicas:
      - type: s3
        bucket: mybucket
        path: mypath
        region: us-east-1
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, false)
		if err != nil {
			t.Fatal(err)
		}

		if len(config.DBs) != 1 || len(config.DBs[0].Replicas) != 1 {
			t.Fatal("expected one database with one replica")
		}

		replicaConfig := config.DBs[0].Replicas[0]

		// When not specified, should be nil
		if replicaConfig.PartSize != nil {
			t.Errorf("expected PartSize to be nil when not specified, got %v", *replicaConfig.PartSize)
		}
		if replicaConfig.Concurrency != nil {
			t.Errorf("expected Concurrency to be nil when not specified, got %v", *replicaConfig.Concurrency)
		}

		// Test that the client is created successfully without these values
		r, err := main.NewReplicaFromConfig(replicaConfig, nil)
		if err != nil {
			t.Fatal(err)
		}

		client, ok := r.Client.(*s3.ReplicaClient)
		if !ok {
			t.Fatal("expected S3 replica client")
		}

		// When not specified, client should have default (0) values
		// The AWS SDK will use its own defaults
		if got, want := client.PartSize, int64(0); got != want {
			t.Errorf("client.PartSize = %d, want %d (AWS SDK default will be used)", got, want)
		}
		if got, want := client.Concurrency, 0; got != want {
			t.Errorf("client.Concurrency = %d, want %d (AWS SDK default will be used)", got, want)
		}
	})
}

// TestDBConfig_CheckpointFields tests that checkpoint-related configuration fields
// are properly parsed from YAML and applied to the DB instance.
func TestDBConfig_CheckpointFields(t *testing.T) {
	t.Run("MinCheckpointPageN", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: /tmp/test.db
    min-checkpoint-page-count: 2000
    replica:
      url: file:///tmp/replica
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, false)
		if err != nil {
			t.Fatal(err)
		}

		if len(config.DBs) != 1 {
			t.Fatal("expected one database config")
		}

		dbc := config.DBs[0]
		if dbc.MinCheckpointPageN == nil {
			t.Fatal("expected min-checkpoint-page-count to be set")
		}
		if got, want := *dbc.MinCheckpointPageN, 2000; got != want {
			t.Errorf("MinCheckpointPageN = %d, want %d", got, want)
		}

		// Test that the value is properly applied to the DB
		db, err := main.NewDBFromConfig(dbc)
		if err != nil {
			t.Fatal(err)
		}

		if got, want := db.MinCheckpointPageN, 2000; got != want {
			t.Errorf("db.MinCheckpointPageN = %d, want %d", got, want)
		}
	})

	t.Run("TruncatePageN", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: /tmp/test.db
    truncate-page-n: 100000
    replica:
      url: file:///tmp/replica
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, false)
		if err != nil {
			t.Fatal(err)
		}

		if len(config.DBs) != 1 {
			t.Fatal("expected one database config")
		}

		dbc := config.DBs[0]
		if dbc.TruncatePageN == nil {
			t.Fatal("expected truncate-page-n to be set")
		}
		if got, want := *dbc.TruncatePageN, 100000; got != want {
			t.Errorf("TruncatePageN = %d, want %d", got, want)
		}

		// Test that the value is properly applied to the DB
		db, err := main.NewDBFromConfig(dbc)
		if err != nil {
			t.Fatal(err)
		}

		if got, want := db.TruncatePageN, 100000; got != want {
			t.Errorf("db.TruncatePageN = %d, want %d", got, want)
		}
	})

	t.Run("BothCheckpointFields", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: /tmp/test.db
    min-checkpoint-page-count: 2000
    truncate-page-n: 100000
    replica:
      url: file:///tmp/replica
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, false)
		if err != nil {
			t.Fatal(err)
		}

		if len(config.DBs) != 1 {
			t.Fatal("expected one database config")
		}

		dbc := config.DBs[0]
		if dbc.MinCheckpointPageN == nil {
			t.Fatal("expected min-checkpoint-page-count to be set")
		}
		if got, want := *dbc.MinCheckpointPageN, 2000; got != want {
			t.Errorf("MinCheckpointPageN = %d, want %d", got, want)
		}

		if dbc.TruncatePageN == nil {
			t.Fatal("expected truncate-page-n to be set")
		}
		if got, want := *dbc.TruncatePageN, 100000; got != want {
			t.Errorf("TruncatePageN = %d, want %d", got, want)
		}

		// Test that both values are properly applied to the DB
		db, err := main.NewDBFromConfig(dbc)
		if err != nil {
			t.Fatal(err)
		}

		if got, want := db.MinCheckpointPageN, 2000; got != want {
			t.Errorf("db.MinCheckpointPageN = %d, want %d", got, want)
		}
		if got, want := db.TruncatePageN, 100000; got != want {
			t.Errorf("db.TruncatePageN = %d, want %d", got, want)
		}
	})

	t.Run("NotSpecified_UsesDefaults", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: /tmp/test.db
    replica:
      url: file:///tmp/replica
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, false)
		if err != nil {
			t.Fatal(err)
		}

		if len(config.DBs) != 1 {
			t.Fatal("expected one database config")
		}

		dbc := config.DBs[0]
		if dbc.MinCheckpointPageN != nil {
			t.Errorf("expected MinCheckpointPageN to be nil when not specified, got %v", *dbc.MinCheckpointPageN)
		}
		if dbc.TruncatePageN != nil {
			t.Errorf("expected TruncatePageN to be nil when not specified, got %v", *dbc.TruncatePageN)
		}

		// Test that the DB uses default values
		db, err := main.NewDBFromConfig(dbc)
		if err != nil {
			t.Fatal(err)
		}

		if got, want := db.MinCheckpointPageN, litestream.DefaultMinCheckpointPageN; got != want {
			t.Errorf("db.MinCheckpointPageN = %d, want default %d", got, want)
		}
		if got, want := db.TruncatePageN, litestream.DefaultTruncatePageN; got != want {
			t.Errorf("db.TruncatePageN = %d, want default %d", got, want)
		}
	})
}

func TestFindSQLiteDatabases(t *testing.T) {
	// Create a temporary directory using t.TempDir() - automatically cleaned up
	tmpDir := t.TempDir()

	// Create test files
	testFiles := []struct {
		path       string
		isSQLite   bool
		shouldFind bool
	}{
		{"test1.db", true, true},
		{"test2.sqlite", true, true},
		{"test3.db", false, false}, // Not a SQLite file
		{"test.txt", false, false},
		{"subdir/test4.db", true, true},
		{"subdir/test5.sqlite", true, true},
		{"subdir/deep/test6.db", true, true},
	}

	// Create test files
	for _, tf := range testFiles {
		fullPath := filepath.Join(tmpDir, tf.path)
		dir := filepath.Dir(fullPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}

		file, err := os.Create(fullPath)
		if err != nil {
			t.Fatal(err)
		}

		if tf.isSQLite {
			// Write SQLite header
			if _, err := file.Write([]byte("SQLite format 3\x00")); err != nil {
				t.Fatal(err)
			}
		} else {
			// Write non-SQLite content
			if _, err := file.Write([]byte("not a sqlite file")); err != nil {
				t.Fatal(err)
			}
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("non-recursive *.db pattern", func(t *testing.T) {
		dbs, err := main.FindSQLiteDatabases(tmpDir, "*.db", false)
		if err != nil {
			t.Fatal(err)
		}

		// Should only find test1.db in root directory
		if len(dbs) != 1 {
			t.Errorf("expected 1 database, got %d", len(dbs))
		}
	})

	t.Run("recursive *.db pattern", func(t *testing.T) {
		dbs, err := main.FindSQLiteDatabases(tmpDir, "*.db", true)
		if err != nil {
			t.Fatal(err)
		}

		// Should find test1.db, test4.db, and test6.db
		if len(dbs) != 3 {
			t.Errorf("expected 3 databases, got %d", len(dbs))
		}
	})

	t.Run("recursive *.sqlite pattern", func(t *testing.T) {
		dbs, err := main.FindSQLiteDatabases(tmpDir, "*.sqlite", true)
		if err != nil {
			t.Fatal(err)
		}

		// Should find test2.sqlite and test5.sqlite
		if len(dbs) != 2 {
			t.Errorf("expected 2 databases, got %d", len(dbs))
		}
	})

	t.Run("recursive * pattern", func(t *testing.T) {
		dbs, err := main.FindSQLiteDatabases(tmpDir, "*", true)
		if err != nil {
			t.Fatal(err)
		}

		// Should find all 5 SQLite databases
		if len(dbs) != 5 {
			t.Errorf("expected 5 databases, got %d", len(dbs))
		}
	})
}

func TestParseReplicaURLWithQuery(t *testing.T) {
	t.Run("S3WithEndpoint", func(t *testing.T) {
		url := "s3://mybucket/path/to/db?endpoint=localhost:9000&region=us-east-1&forcePathStyle=true"
		scheme, host, path, query, _, err := litestream.ParseReplicaURLWithQuery(url)
		if err != nil {
			t.Fatal(err)
		}
		if scheme != "s3" {
			t.Errorf("expected scheme 's3', got %q", scheme)
		}
		if host != "mybucket" {
			t.Errorf("expected host 'mybucket', got %q", host)
		}
		if path != "path/to/db" {
			t.Errorf("expected path 'path/to/db', got %q", path)
		}
		if query.Get("endpoint") != "localhost:9000" {
			t.Errorf("expected endpoint 'localhost:9000', got %q", query.Get("endpoint"))
		}
		if query.Get("region") != "us-east-1" {
			t.Errorf("expected region 'us-east-1', got %q", query.Get("region"))
		}
		if query.Get("forcePathStyle") != "true" {
			t.Errorf("expected forcePathStyle 'true', got %q", query.Get("forcePathStyle"))
		}
	})

	t.Run("S3WithoutQuery", func(t *testing.T) {
		url := "s3://mybucket/path/to/db"
		scheme, host, path, query, _, err := litestream.ParseReplicaURLWithQuery(url)
		if err != nil {
			t.Fatal(err)
		}
		if scheme != "s3" {
			t.Errorf("expected scheme 's3', got %q", scheme)
		}
		if host != "mybucket" {
			t.Errorf("expected host 'mybucket', got %q", host)
		}
		if path != "path/to/db" {
			t.Errorf("expected path 'path/to/db', got %q", path)
		}
		if len(query) != 0 {
			t.Errorf("expected no query parameters, got %v", query)
		}
	})

	t.Run("FileURL", func(t *testing.T) {
		url := "file:///path/to/db"
		scheme, host, path, query, _, err := litestream.ParseReplicaURLWithQuery(url)
		if err != nil {
			t.Fatal(err)
		}
		if scheme != "file" {
			t.Errorf("expected scheme 'file', got %q", scheme)
		}
		if host != "" {
			t.Errorf("expected empty host, got %q", host)
		}
		if path != "/path/to/db" {
			t.Errorf("expected path '/path/to/db', got %q", path)
		}
		if query != nil {
			t.Errorf("expected nil query for file URL, got %v", query)
		}
	})

	t.Run("BackwardCompatibility", func(t *testing.T) {
		// Test that ParseReplicaURL still works as before
		url := "s3://mybucket/path/to/db?endpoint=localhost:9000"
		scheme, host, path, err := litestream.ParseReplicaURL(url)
		if err != nil {
			t.Fatal(err)
		}
		if scheme != "s3" {
			t.Errorf("expected scheme 's3', got %q", scheme)
		}
		if host != "mybucket" {
			t.Errorf("expected host 'mybucket', got %q", host)
		}
		if path != "path/to/db" {
			t.Errorf("expected path 'path/to/db', got %q", path)
		}
	})

	t.Run("S3TigrisExample", func(t *testing.T) {
		url := "s3://mybucket/db?endpoint=fly.storage.tigris.dev&region=auto"
		scheme, host, path, query, _, err := litestream.ParseReplicaURLWithQuery(url)
		if err != nil {
			t.Fatal(err)
		}
		if scheme != "s3" {
			t.Errorf("expected scheme 's3', got %q", scheme)
		}
		if host != "mybucket" {
			t.Errorf("expected host 'mybucket', got %q", host)
		}
		if path != "db" {
			t.Errorf("expected path 'db', got %q", path)
		}
		if query.Get("endpoint") != "fly.storage.tigris.dev" {
			t.Errorf("expected endpoint 'fly.storage.tigris.dev', got %q", query.Get("endpoint"))
		}
		if query.Get("region") != "auto" {
			t.Errorf("expected region 'auto', got %q", query.Get("region"))
		}
	})

	t.Run("S3WithSkipVerify", func(t *testing.T) {
		url := "s3://mybucket/db?endpoint=self-signed.local&skipVerify=true"
		_, _, _, query, _, err := litestream.ParseReplicaURLWithQuery(url)
		if err != nil {
			t.Fatal(err)
		}
		if query.Get("skipVerify") != "true" {
			t.Errorf("expected skipVerify 'true', got %q", query.Get("skipVerify"))
		}
	})
}

func TestIsSQLiteDatabase(t *testing.T) {
	// Create temporary test files using t.TempDir() - automatically cleaned up
	tmpDir := t.TempDir()

	t.Run("valid SQLite file", func(t *testing.T) {
		path := filepath.Join(tmpDir, "valid.db")
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte("SQLite format 3\x00")); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}

		if !main.IsSQLiteDatabase(path) {
			t.Error("expected file to be identified as SQLite database")
		}
	})

	t.Run("invalid SQLite file", func(t *testing.T) {
		path := filepath.Join(tmpDir, "invalid.db")
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte("not a sqlite file")); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}

		if main.IsSQLiteDatabase(path) {
			t.Error("expected file to NOT be identified as SQLite database")
		}
	})

	t.Run("non-existent file", func(t *testing.T) {
		path := filepath.Join(tmpDir, "doesnotexist.db")
		if main.IsSQLiteDatabase(path) {
			t.Error("expected non-existent file to NOT be identified as SQLite database")
		}
	})
}

func TestDBConfigValidation(t *testing.T) {
	t.Run("both path and dir specified", func(t *testing.T) {
		config := main.Config{
			DBs: []*main.DBConfig{
				{
					Path: "/path/to/db.sqlite",
					Dir:  "/path/to/dir",
				},
			},
		}

		err := config.Validate()
		if err == nil {
			t.Error("expected validation error when both path and dir are specified")
		}
	})

	t.Run("neither path nor dir specified", func(t *testing.T) {
		config := main.Config{
			DBs: []*main.DBConfig{
				{},
			},
		}

		err := config.Validate()
		if err == nil {
			t.Error("expected validation error when neither path nor dir are specified")
		}
	})

	t.Run("dir without pattern", func(t *testing.T) {
		config := main.Config{
			DBs: []*main.DBConfig{
				{
					Dir: "/path/to/dir",
				},
			},
		}

		err := config.Validate()
		if err == nil {
			t.Error("expected validation error when dir is specified without pattern")
		}
	})

	t.Run("valid path configuration", func(t *testing.T) {
		config := main.DefaultConfig()
		config.DBs = []*main.DBConfig{
			{
				Path: "/path/to/db.sqlite",
			},
		}

		err := config.Validate()
		if err != nil {
			t.Errorf("unexpected validation error for valid path config: %v", err)
		}
	})

	t.Run("valid directory configuration", func(t *testing.T) {
		config := main.DefaultConfig()
		config.DBs = []*main.DBConfig{
			{
				Dir:       "/path/to/dir",
				Pattern:   "*.db",
				Recursive: true,
			},
		}

		err := config.Validate()
		if err != nil {
			t.Errorf("unexpected validation error for valid directory config: %v", err)
		}
	})
}

// TestNewDBsFromDirectoryConfig_UniquePaths verifies that each database discovered
// in a directory gets a unique replica path to prevent data corruption.
func TestNewDBsFromDirectoryConfig_UniquePaths(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple databases
	createSQLiteDB(t, filepath.Join(tmpDir, "db1.db"))
	createSQLiteDB(t, filepath.Join(tmpDir, "db2.db"))
	createSQLiteDB(t, filepath.Join(tmpDir, "db3.db"))

	config := &main.DBConfig{
		Dir:     tmpDir,
		Pattern: "*.db",
		Replica: &main.ReplicaConfig{
			Type: "file",
			Path: "/backup/base",
		},
	}

	dbs, err := main.NewDBsFromDirectoryConfig(config)
	if err != nil {
		t.Fatalf("NewDBsFromDirectoryConfig failed: %v", err)
	}

	if len(dbs) != 3 {
		t.Fatalf("expected 3 databases, got %d", len(dbs))
	}

	// Verify each has unique replica path
	paths := make(map[string]bool)
	for _, db := range dbs {
		if db.Replica == nil {
			t.Fatalf("database %s has no replica", db.Path())
		}
		replicaPath := db.Replica.Client.(*file.ReplicaClient).Path()
		if paths[replicaPath] {
			t.Errorf("duplicate replica path: %s", replicaPath)
		}
		paths[replicaPath] = true

		// Verify path includes database name
		dbName := filepath.Base(db.Path())
		if !strings.Contains(replicaPath, dbName) {
			t.Errorf("replica path %s does not contain database name %s", replicaPath, dbName)
		}
	}

	// Verify all paths are different
	if len(paths) != 3 {
		t.Errorf("expected 3 unique paths, got %d", len(paths))
	}
}

// TestNewDBsFromDirectoryConfig_MetaPathPerDatabase ensures that each database
// discovered via a directory config receives a unique metadata directory when a
// base meta-path is provided.
func TestNewDBsFromDirectoryConfig_MetaPathPerDatabase(t *testing.T) {
	tmpDir := t.TempDir()

	rootDB := filepath.Join(tmpDir, "primary.db")
	createSQLiteDB(t, rootDB)

	nestedDir := filepath.Join(tmpDir, "team", "nested")
	if err := os.MkdirAll(nestedDir, 0o755); err != nil {
		t.Fatalf("failed to create nested directory: %v", err)
	}
	nestedDB := filepath.Join(nestedDir, "analytics.db")
	createSQLiteDB(t, nestedDB)

	u, err := user.Current()
	if err != nil {
		t.Skipf("user.Current failed: %v", err)
	}
	if u.HomeDir == "" {
		t.Skip("no home directory available for expansion test")
	}

	metaRoot := filepath.Join("~", "meta-root")
	expandedMetaRoot := filepath.Join(u.HomeDir, "meta-root")
	replicaDir := filepath.Join(t.TempDir(), "replica")
	config := &main.DBConfig{
		Dir:       tmpDir,
		Pattern:   "*.db",
		Recursive: true,
		MetaPath:  &metaRoot,
		Replica: &main.ReplicaConfig{
			Type: "file",
			Path: replicaDir,
		},
	}

	dbs, err := main.NewDBsFromDirectoryConfig(config)
	if err != nil {
		t.Fatalf("NewDBsFromDirectoryConfig failed: %v", err)
	}
	if len(dbs) != 2 {
		t.Fatalf("expected 2 databases, got %d", len(dbs))
	}

	expectedMetaPaths := map[string]string{
		rootDB:   filepath.Join(expandedMetaRoot, ".primary.db"+litestream.MetaDirSuffix),
		nestedDB: filepath.Join(expandedMetaRoot, "team", "nested", ".analytics.db"+litestream.MetaDirSuffix),
	}

	metaSeen := make(map[string]struct{})
	for _, db := range dbs {
		metaPath := db.MetaPath()
		want, ok := expectedMetaPaths[db.Path()]
		if !ok {
			t.Fatalf("unexpected database path returned: %s", db.Path())
		}
		if metaPath != want {
			t.Fatalf("database %s meta path mismatch: got %s, want %s", db.Path(), metaPath, want)
		}
		if _, dup := metaSeen[metaPath]; dup {
			t.Fatalf("duplicate meta path detected: %s", metaPath)
		}
		metaSeen[metaPath] = struct{}{}
	}
}

// TestNewDBsFromDirectoryConfig_SubdirectoryPaths verifies that the relative
// directory structure is preserved in replica paths when using recursive scanning.
func TestNewDBsFromDirectoryConfig_SubdirectoryPaths(t *testing.T) {
	tmpDir := t.TempDir()

	// Create databases in subdirectories
	createSQLiteDB(t, filepath.Join(tmpDir, "db1.db"))
	createSQLiteDB(t, filepath.Join(tmpDir, "team-a", "db2.db"))
	createSQLiteDB(t, filepath.Join(tmpDir, "team-b", "nested", "db3.db"))

	config := &main.DBConfig{
		Dir:       tmpDir,
		Pattern:   "*.db",
		Recursive: true,
		Replica: &main.ReplicaConfig{
			Type: "file",
			Path: "/backup",
		},
	}

	dbs, err := main.NewDBsFromDirectoryConfig(config)
	if err != nil {
		t.Fatalf("NewDBsFromDirectoryConfig failed: %v", err)
	}

	if len(dbs) != 3 {
		t.Fatalf("expected 3 databases, got %d", len(dbs))
	}

	// Build expected path mappings
	expectedPaths := map[string]string{
		filepath.Join(tmpDir, "db1.db"):                     "/backup/db1.db",
		filepath.Join(tmpDir, "team-a", "db2.db"):           "/backup/team-a/db2.db",
		filepath.Join(tmpDir, "team-b", "nested", "db3.db"): "/backup/team-b/nested/db3.db",
	}

	for _, db := range dbs {
		expectedPath, ok := expectedPaths[db.Path()]
		if !ok {
			t.Errorf("unexpected database path: %s", db.Path())
			continue
		}

		replicaPath := db.Replica.Client.(*file.ReplicaClient).Path()
		if replicaPath != expectedPath {
			t.Errorf("database %s: expected replica path %s, got %s", db.Path(), expectedPath, replicaPath)
		}
	}
}

// TestNewDBsFromDirectoryConfig_DuplicateFilenames verifies that databases with
// the same filename in different subdirectories get unique replica paths.
func TestNewDBsFromDirectoryConfig_DuplicateFilenames(t *testing.T) {
	tmpDir := t.TempDir()

	// Create databases with same name in different directories
	createSQLiteDB(t, filepath.Join(tmpDir, "team-a", "db.sqlite"))
	createSQLiteDB(t, filepath.Join(tmpDir, "team-b", "db.sqlite"))

	config := &main.DBConfig{
		Dir:       tmpDir,
		Pattern:   "*.sqlite",
		Recursive: true,
		Replica: &main.ReplicaConfig{
			Type: "s3",
			Path: "backups",
			ReplicaSettings: main.ReplicaSettings{
				Bucket: "test-bucket",
				Region: "us-east-1",
			},
		},
	}

	dbs, err := main.NewDBsFromDirectoryConfig(config)
	if err != nil {
		t.Fatalf("NewDBsFromDirectoryConfig failed: %v", err)
	}

	if len(dbs) != 2 {
		t.Fatalf("expected 2 databases, got %d", len(dbs))
	}

	// Verify paths are unique despite duplicate filenames
	paths := make(map[string]bool)
	for _, db := range dbs {
		replicaPath := db.Replica.Client.(*s3.ReplicaClient).Path
		if paths[replicaPath] {
			t.Errorf("duplicate replica path found: %s", replicaPath)
		}
		paths[replicaPath] = true
	}

	if len(paths) != 2 {
		t.Errorf("expected 2 unique paths, got %d", len(paths))
	}

	// Verify paths contain subdirectory to disambiguate
	for _, db := range dbs {
		replicaPath := db.Replica.Client.(*s3.ReplicaClient).Path
		if !strings.Contains(replicaPath, "team-a") && !strings.Contains(replicaPath, "team-b") {
			t.Errorf("replica path %s does not contain team subdirectory", replicaPath)
		}
	}
}

// TestNewDBsFromDirectoryConfig_S3URL verifies that replica URLs receive a
// per-database suffix so multiple databases do not overwrite one another.
func TestNewDBsFromDirectoryConfig_S3URL(t *testing.T) {
	tmpDir := t.TempDir()

	createSQLiteDB(t, filepath.Join(tmpDir, "team-a", "db.sqlite"))
	createSQLiteDB(t, filepath.Join(tmpDir, "team-b", "nested", "db.sqlite"))

	config := &main.DBConfig{
		Dir:       tmpDir,
		Pattern:   "*.sqlite",
		Recursive: true,
		Replica: &main.ReplicaConfig{
			URL: "s3://test-bucket/backups",
		},
	}

	dbs, err := main.NewDBsFromDirectoryConfig(config)
	if err != nil {
		t.Fatalf("NewDBsFromDirectoryConfig failed: %v", err)
	}

	if len(dbs) != 2 {
		t.Fatalf("expected 2 databases, got %d", len(dbs))
	}

	expectedPaths := map[string]string{
		filepath.Join(tmpDir, "team-a", "db.sqlite"):           "backups/team-a/db.sqlite",
		filepath.Join(tmpDir, "team-b", "nested", "db.sqlite"): "backups/team-b/nested/db.sqlite",
	}

	for _, db := range dbs {
		expectedPath, ok := expectedPaths[db.Path()]
		if !ok {
			t.Errorf("unexpected database path: %s", db.Path())
			continue
		}

		client := db.Replica.Client.(*s3.ReplicaClient)
		if client.Path != expectedPath {
			t.Errorf("database %s: expected replica path %s, got %s", db.Path(), expectedPath, client.Path)
		}
	}
}

// TestNewDBsFromDirectoryConfig_ReplicasArrayURL verifies URL handling when
// using the deprecated replicas array form.
func TestNewDBsFromDirectoryConfig_ReplicasArrayURL(t *testing.T) {
	tmpDir := t.TempDir()

	createSQLiteDB(t, filepath.Join(tmpDir, "db1.sqlite"))
	createSQLiteDB(t, filepath.Join(tmpDir, "subs", "db2.sqlite"))

	config := &main.DBConfig{
		Dir:       tmpDir,
		Pattern:   "*.sqlite",
		Recursive: true,
		Replicas: []*main.ReplicaConfig{
			{
				URL: "s3://legacy-bucket/base",
			},
		},
	}

	dbs, err := main.NewDBsFromDirectoryConfig(config)
	if err != nil {
		t.Fatalf("NewDBsFromDirectoryConfig failed: %v", err)
	}

	if len(dbs) != 2 {
		t.Fatalf("expected 2 databases, got %d", len(dbs))
	}

	expectedPaths := map[string]string{
		filepath.Join(tmpDir, "db1.sqlite"):         "base/db1.sqlite",
		filepath.Join(tmpDir, "subs", "db2.sqlite"): "base/subs/db2.sqlite",
	}

	for _, db := range dbs {
		expectedPath, ok := expectedPaths[db.Path()]
		if !ok {
			t.Errorf("unexpected database path: %s", db.Path())
			continue
		}

		client := db.Replica.Client.(*s3.ReplicaClient)
		if client.Path != expectedPath {
			t.Errorf("database %s: expected replica path %s, got %s", db.Path(), expectedPath, client.Path)
		}
	}
}

// TestNewDBsFromDirectoryConfig_SpecialCharacters verifies that special characters
// in database filenames are handled correctly in replica paths.
func TestNewDBsFromDirectoryConfig_SpecialCharacters(t *testing.T) {
	tmpDir := t.TempDir()

	// Create databases with special characters
	specialNames := []string{
		"my database.db",
		"user@example.com.db",
		"tenant#1.db",
	}

	for _, name := range specialNames {
		createSQLiteDB(t, filepath.Join(tmpDir, name))
	}

	config := &main.DBConfig{
		Dir:     tmpDir,
		Pattern: "*.db",
		Replica: &main.ReplicaConfig{
			Type: "file",
			Path: "/backup",
		},
	}

	dbs, err := main.NewDBsFromDirectoryConfig(config)
	if err != nil {
		t.Fatalf("NewDBsFromDirectoryConfig failed: %v", err)
	}

	if len(dbs) != len(specialNames) {
		t.Fatalf("expected %d databases, got %d", len(specialNames), len(dbs))
	}

	// Verify each special name is in a replica path
	for _, db := range dbs {
		replicaPath := db.Replica.Client.(*file.ReplicaClient).Path()
		dbName := filepath.Base(db.Path())

		if !strings.Contains(replicaPath, dbName) {
			t.Errorf("replica path %s does not contain database name %s", replicaPath, dbName)
		}
	}
}

// TestNewDBsFromDirectoryConfig_EmptyBasePath verifies that an empty base path
// results in the database relative path being used as the entire replica path.
func TestNewDBsFromDirectoryConfig_EmptyBasePath(t *testing.T) {
	tmpDir := t.TempDir()

	createSQLiteDB(t, filepath.Join(tmpDir, "test.db"))

	config := &main.DBConfig{
		Dir:     tmpDir,
		Pattern: "*.db",
		Replica: &main.ReplicaConfig{
			Type: "file",
			Path: "", // Empty base path
		},
	}

	dbs, err := main.NewDBsFromDirectoryConfig(config)
	if err != nil {
		t.Fatalf("NewDBsFromDirectoryConfig failed: %v", err)
	}

	if len(dbs) != 1 {
		t.Fatalf("expected 1 database, got %d", len(dbs))
	}

	replicaPath := dbs[0].Replica.Client.(*file.ReplicaClient).Path()
	// When base path is empty, the relative path (just filename) is used
	// But it's still expanded to absolute path by the file backend
	if !strings.HasSuffix(replicaPath, "test.db") {
		t.Errorf("expected replica path to end with 'test.db', got %s", replicaPath)
	}
}

// TestNewDBsFromDirectoryConfig_ReplicasArray verifies that the deprecated
// 'replicas' array field is handled correctly with unique paths.
func TestNewDBsFromDirectoryConfig_ReplicasArray(t *testing.T) {
	tmpDir := t.TempDir()

	createSQLiteDB(t, filepath.Join(tmpDir, "db1.db"))
	createSQLiteDB(t, filepath.Join(tmpDir, "db2.db"))

	config := &main.DBConfig{
		Dir:     tmpDir,
		Pattern: "*.db",
		Replicas: []*main.ReplicaConfig{
			{
				Type: "file",
				Path: "/backup",
			},
		},
	}

	dbs, err := main.NewDBsFromDirectoryConfig(config)
	if err != nil {
		t.Fatalf("NewDBsFromDirectoryConfig failed: %v", err)
	}

	if len(dbs) != 2 {
		t.Fatalf("expected 2 databases, got %d", len(dbs))
	}

	// Verify each has unique replica path
	paths := make(map[string]bool)
	for _, db := range dbs {
		if db.Replica == nil {
			t.Fatalf("database %s has no replica", db.Path())
		}
		replicaPath := db.Replica.Client.(*file.ReplicaClient).Path()
		if paths[replicaPath] {
			t.Errorf("duplicate replica path: %s", replicaPath)
		}
		paths[replicaPath] = true
	}
}

func TestNewDBsFromDirectoryConfig_EmptyDirectoryRequiresDatabases(t *testing.T) {
	tmpDir := t.TempDir()
	replicaDir := filepath.Join(tmpDir, "replica")

	config := &main.DBConfig{
		Dir:     tmpDir,
		Pattern: "*.db",
		Replica: &main.ReplicaConfig{Type: "file", Path: replicaDir},
	}

	if _, err := main.NewDBsFromDirectoryConfig(config); err == nil {
		t.Fatalf("expected error for empty directory when watch disabled")
	}
}

func TestNewDBsFromDirectoryConfig_EmptyDirectoryWithWatch(t *testing.T) {
	tmpDir := t.TempDir()
	replicaDir := filepath.Join(tmpDir, "replica")

	config := &main.DBConfig{
		Dir:     tmpDir,
		Pattern: "*.db",
		Watch:   true,
		Replica: &main.ReplicaConfig{Type: "file", Path: replicaDir},
	}

	dbs, err := main.NewDBsFromDirectoryConfig(config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(dbs) != 0 {
		t.Fatalf("expected 0 databases, got %d", len(dbs))
	}
}

func TestDirectoryMonitor_DetectsDatabaseLifecycle(t *testing.T) {
	ctx := context.Background()

	rootDir := t.TempDir()
	replicaDir := filepath.Join(t.TempDir(), "replicas")

	initialPath := filepath.Join(rootDir, "initial.db")
	createSQLiteDB(t, initialPath)

	config := &main.DBConfig{
		Dir:     rootDir,
		Pattern: "*.db",
		Replica: &main.ReplicaConfig{Type: "file", Path: replicaDir},
	}

	dbs, err := main.NewDBsFromDirectoryConfig(config)
	if err != nil {
		t.Fatalf("NewDBsFromDirectoryConfig failed: %v", err)
	}

	storeConfig := main.DefaultConfig()
	store := litestream.NewStore(dbs, storeConfig.CompactionLevels())
	store.CompactionMonitorEnabled = false

	if err := store.Open(ctx); err != nil {
		t.Fatalf("unexpected error opening store: %v", err)
	}
	defer func() {
		if err := store.Close(context.Background()); err != nil {
			t.Fatalf("unexpected error closing store: %v", err)
		}
	}()

	monitor, err := main.NewDirectoryMonitor(ctx, store, config, dbs)
	if err != nil {
		t.Fatalf("failed to initialize directory monitor: %v", err)
	}
	defer monitor.Close()

	newPath := filepath.Join(rootDir, "new.db")
	createSQLiteDB(t, newPath)

	if !waitForCondition(5*time.Second, func() bool { return hasDBPath(store.DBs(), newPath) }) {
		t.Fatalf("expected new database %s to be detected", newPath)
	}

	if err := os.Remove(newPath); err != nil {
		t.Fatalf("failed to remove database: %v", err)
	}

	if !waitForCondition(5*time.Second, func() bool { return !hasDBPath(store.DBs(), newPath) }) {
		t.Fatalf("expected database %s to be removed", newPath)
	}
}

func TestDirectoryMonitor_RecursiveDetectsNestedDatabases(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	replicaDir := filepath.Join(t.TempDir(), "replicas")

	config := &main.DBConfig{
		Dir:       rootDir,
		Pattern:   "*.db",
		Recursive: true,
		Watch:     true,
		Replica:   &main.ReplicaConfig{Type: "file", Path: replicaDir},
	}

	storeConfig := main.DefaultConfig()
	store := litestream.NewStore(nil, storeConfig.CompactionLevels())
	store.CompactionMonitorEnabled = false
	if err := store.Open(ctx); err != nil {
		t.Fatalf("unexpected error opening store: %v", err)
	}
	defer func() {
		if err := store.Close(context.Background()); err != nil {
			t.Fatalf("unexpected error closing store: %v", err)
		}
	}()

	monitor, err := main.NewDirectoryMonitor(ctx, store, config, nil)
	if err != nil {
		t.Fatalf("failed to initialize directory monitor: %v", err)
	}
	defer monitor.Close()

	deepDir := filepath.Join(rootDir, "tenant", "nested", "deeper")
	if err := os.MkdirAll(deepDir, 0755); err != nil {
		t.Fatalf("failed to create nested directories: %v", err)
	}
	deepDB := filepath.Join(deepDir, "deep.db")
	createSQLiteDB(t, deepDB)

	if !waitForCondition(5*time.Second, func() bool { return hasDBPath(store.DBs(), deepDB) }) {
		t.Fatalf("expected nested database %s to be detected", deepDB)
	}
}

// createSQLiteDB creates a minimal SQLite database file for testing
func createSQLiteDB(t *testing.T, path string) {
	t.Helper()

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("failed to create directory %s: %v", dir, err)
	}

	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create file %s: %v", path, err)
	}
	defer file.Close()

	// Write SQLite header
	if _, err := file.Write([]byte("SQLite format 3\x00")); err != nil {
		t.Fatalf("failed to write SQLite header: %v", err)
	}
}

func TestNewS3ReplicaClientFromConfig(t *testing.T) {
	t.Run("URLWithEndpointQuery", func(t *testing.T) {
		config := &main.ReplicaConfig{
			URL: "s3://mybucket/path/to/db?endpoint=localhost:9000&region=us-west-2&forcePathStyle=true&skipVerify=true",
		}

		client, err := main.NewS3ReplicaClientFromConfig(config, nil)
		if err != nil {
			t.Fatal(err)
		}

		if client.Bucket != "mybucket" {
			t.Errorf("expected bucket 'mybucket', got %q", client.Bucket)
		}
		if client.Path != "path/to/db" {
			t.Errorf("expected path 'path/to/db', got %q", client.Path)
		}
		if client.Endpoint != "http://localhost:9000" {
			t.Errorf("expected endpoint 'http://localhost:9000', got %q", client.Endpoint)
		}
		if client.Region != "us-west-2" {
			t.Errorf("expected region 'us-west-2', got %q", client.Region)
		}
		if !client.ForcePathStyle {
			t.Error("expected ForcePathStyle to be true")
		}
		if !client.SkipVerify {
			t.Error("expected SkipVerify to be true")
		}
	})

	t.Run("URLWithoutQuery", func(t *testing.T) {
		config := &main.ReplicaConfig{
			URL: "s3://mybucket.s3.amazonaws.com/path/to/db",
		}

		client, err := main.NewS3ReplicaClientFromConfig(config, nil)
		if err != nil {
			t.Fatal(err)
		}

		if client.Bucket != "mybucket" {
			t.Errorf("expected bucket 'mybucket', got %q", client.Bucket)
		}
		if client.Path != "path/to/db" {
			t.Errorf("expected path 'path/to/db', got %q", client.Path)
		}
		// Should use default AWS settings
		if client.Endpoint != "" {
			t.Errorf("expected empty endpoint for AWS S3, got %q", client.Endpoint)
		}
		if client.ForcePathStyle {
			t.Error("expected ForcePathStyle to be false for AWS S3")
		}
	})

	t.Run("ConfigOverridesQuery", func(t *testing.T) {
		config := &main.ReplicaConfig{
			URL: "s3://mybucket/path?endpoint=from-query&region=us-east-1",
			ReplicaSettings: main.ReplicaSettings{
				Endpoint: "from-config",
				Region:   "us-west-1",
			},
		}

		client, err := main.NewS3ReplicaClientFromConfig(config, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Config values should take precedence over query params
		if client.Endpoint != "from-config" {
			t.Errorf("expected endpoint from config 'from-config', got %q", client.Endpoint)
		}
		if client.Region != "us-west-1" {
			t.Errorf("expected region from config 'us-west-1', got %q", client.Region)
		}
	})

	t.Run("TigrisExample", func(t *testing.T) {
		config := &main.ReplicaConfig{
			URL: "s3://my-tigris-bucket/db.sqlite?endpoint=fly.storage.tigris.dev&region=auto",
		}

		client, err := main.NewS3ReplicaClientFromConfig(config, nil)
		if err != nil {
			t.Fatal(err)
		}

		if client.Bucket != "my-tigris-bucket" {
			t.Errorf("expected bucket 'my-tigris-bucket', got %q", client.Bucket)
		}
		if client.Endpoint != "http://fly.storage.tigris.dev" {
			t.Errorf("expected Tigris endpoint, got %q", client.Endpoint)
		}
		if client.Region != "auto" {
			t.Errorf("expected region 'auto' for Tigris, got %q", client.Region)
		}
		if !client.ForcePathStyle {
			t.Error("expected ForcePathStyle to be true for custom endpoint")
		}
		if !client.SignPayload {
			t.Error("expected SignPayload to be true for Tigris")
		}
		if client.RequireContentMD5 {
			t.Error("expected RequireContentMD5 to be false for Tigris")
		}
	})

	t.Run("TigrisConfigEndpoint", func(t *testing.T) {
		config := &main.ReplicaConfig{
			Path: "path",
			ReplicaSettings: main.ReplicaSettings{
				Bucket:   "mybucket",
				Endpoint: "https://fly.storage.tigris.dev",
			},
		}

		client, err := main.NewS3ReplicaClientFromConfig(config, nil)
		if err != nil {
			t.Fatal(err)
		}

		if !client.SignPayload {
			t.Error("expected SignPayload to be true for config-based Tigris endpoint")
		}
		if client.RequireContentMD5 {
			t.Error("expected RequireContentMD5 to be false for config-based Tigris endpoint")
		}
	})

	t.Run("HTTPSEndpoint", func(t *testing.T) {
		config := &main.ReplicaConfig{
			URL: "s3://mybucket/path?endpoint=https://secure.storage.com&region=us-east-1",
		}

		client, err := main.NewS3ReplicaClientFromConfig(config, nil)
		if err != nil {
			t.Fatal(err)
		}

		if client.Endpoint != "https://secure.storage.com" {
			t.Errorf("expected endpoint 'https://secure.storage.com', got %q", client.Endpoint)
		}
		if !client.ForcePathStyle {
			t.Error("expected ForcePathStyle to be true for custom endpoint")
		}
	})

	t.Run("QuerySigningOptions", func(t *testing.T) {
		config := &main.ReplicaConfig{
			URL: "s3://bucket/db?sign-payload=true&require-content-md5=false",
		}

		client, err := main.NewS3ReplicaClientFromConfig(config, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !client.SignPayload {
			t.Error("expected SignPayload to be true when query parameter is set")
		}
		if client.RequireContentMD5 {
			t.Error("expected RequireContentMD5 to be false when disabled via query")
		}
	})

	t.Run("ConfigOverridesQuerySigning", func(t *testing.T) {
		signTrue := true
		requireFalse := false
		config := &main.ReplicaConfig{
			URL: "s3://bucket/db?sign-payload=false&require-content-md5=true",
			ReplicaSettings: main.ReplicaSettings{
				SignPayload:       &signTrue,
				RequireContentMD5: &requireFalse,
			},
		}

		client, err := main.NewS3ReplicaClientFromConfig(config, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !client.SignPayload {
			t.Error("expected config SignPayload to override query parameter")
		}
		if client.RequireContentMD5 {
			t.Error("expected config RequireContentMD5=false to override query parameter")
		}
	})

	t.Run("TigrisManualOverride", func(t *testing.T) {
		signFalse := false
		requireTrue := true
		config := &main.ReplicaConfig{
			URL: "s3://bucket/db?endpoint=fly.storage.tigris.dev&region=auto",
			ReplicaSettings: main.ReplicaSettings{
				SignPayload:       &signFalse,
				RequireContentMD5: &requireTrue,
			},
		}

		client, err := main.NewS3ReplicaClientFromConfig(config, nil)
		if err != nil {
			t.Fatal(err)
		}
		if client.SignPayload {
			t.Error("expected manual SignPayload override to take precedence")
		}
		if !client.RequireContentMD5 {
			t.Error("expected manual RequireContentMD5 override to take precedence")
		}
	})
}
func TestGlobalDefaults(t *testing.T) {
	// Test comprehensive global defaults functionality
	t.Run("GlobalReplicaDefaults", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "litestream.yml")
		syncInterval := "30s"
		validationInterval := "1h"

		if err := os.WriteFile(filename, []byte(`
# Global defaults for all replicas
access-key-id: GLOBAL_ACCESS_KEY
secret-access-key: GLOBAL_SECRET_KEY
region: us-west-2
endpoint: custom.s3.endpoint.com
sync-interval: `+syncInterval+`
validation-interval: `+validationInterval+`

dbs:
  # Database 1: Uses all global defaults
  - path: /tmp/db1.sqlite
    replica:
      type: s3
      bucket: my-bucket-1

  # Database 2: Overrides some defaults
  - path: /tmp/db2.sqlite
    replica:
      type: s3
      bucket: my-bucket-2
      region: us-east-1           # Override global region
      access-key-id: CUSTOM_KEY   # Override global access key

  # Database 3: Uses legacy replicas format
  - path: /tmp/db3.sqlite
    replicas:
      - type: s3
        bucket: my-bucket-3
        # Should inherit all other global settings
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, true)
		if err != nil {
			t.Fatal(err)
		}

		// Test global settings were parsed correctly
		if got, want := config.AccessKeyID, "GLOBAL_ACCESS_KEY"; got != want {
			t.Errorf("config.AccessKeyID=%v, want %v", got, want)
		}
		if got, want := config.SecretAccessKey, "GLOBAL_SECRET_KEY"; got != want {
			t.Errorf("config.SecretAccessKey=%v, want %v", got, want)
		}
		if got, want := config.Region, "us-west-2"; got != want {
			t.Errorf("config.Region=%v, want %v", got, want)
		}
		if got, want := config.Endpoint, "custom.s3.endpoint.com"; got != want {
			t.Errorf("config.Endpoint=%v, want %v", got, want)
		}

		// Parse expected intervals
		expectedSyncInterval, err := time.ParseDuration(syncInterval)
		if err != nil {
			t.Fatal(err)
		}
		expectedValidationInterval, err := time.ParseDuration(validationInterval)
		if err != nil {
			t.Fatal(err)
		}

		if config.SyncInterval == nil || *config.SyncInterval != expectedSyncInterval {
			t.Errorf("config.SyncInterval=%v, want %v", config.SyncInterval, expectedSyncInterval)
		}
		if config.ValidationInterval == nil || *config.ValidationInterval != expectedValidationInterval {
			t.Errorf("config.ValidationInterval=%v, want %v", config.ValidationInterval, expectedValidationInterval)
		}

		// Test Database 1: Should inherit all global defaults
		db1 := config.DBs[0]
		if db1.Replica == nil {
			t.Fatal("db1.Replica is nil")
		}
		replica1 := db1.Replica

		if got, want := replica1.AccessKeyID, "GLOBAL_ACCESS_KEY"; got != want {
			t.Errorf("replica1.AccessKeyID=%v, want %v", got, want)
		}
		if got, want := replica1.SecretAccessKey, "GLOBAL_SECRET_KEY"; got != want {
			t.Errorf("replica1.SecretAccessKey=%v, want %v", got, want)
		}
		if got, want := replica1.Region, "us-west-2"; got != want {
			t.Errorf("replica1.Region=%v, want %v", got, want)
		}
		if got, want := replica1.Endpoint, "custom.s3.endpoint.com"; got != want {
			t.Errorf("replica1.Endpoint=%v, want %v", got, want)
		}
		if got, want := replica1.Bucket, "my-bucket-1"; got != want {
			t.Errorf("replica1.Bucket=%v, want %v", got, want)
		}
		if replica1.SyncInterval == nil || *replica1.SyncInterval != expectedSyncInterval {
			t.Errorf("replica1.SyncInterval=%v, want %v", replica1.SyncInterval, expectedSyncInterval)
		}
		if replica1.ValidationInterval == nil || *replica1.ValidationInterval != expectedValidationInterval {
			t.Errorf("replica1.ValidationInterval=%v, want %v", replica1.ValidationInterval, expectedValidationInterval)
		}

		// Test Database 2: Should override some defaults
		db2 := config.DBs[1]
		if db2.Replica == nil {
			t.Fatal("db2.Replica is nil")
		}
		replica2 := db2.Replica

		if got, want := replica2.AccessKeyID, "CUSTOM_KEY"; got != want {
			t.Errorf("replica2.AccessKeyID=%v, want %v", got, want)
		}
		if got, want := replica2.SecretAccessKey, "GLOBAL_SECRET_KEY"; got != want {
			t.Errorf("replica2.SecretAccessKey=%v, want %v", got, want)
		}
		if got, want := replica2.Region, "us-east-1"; got != want {
			t.Errorf("replica2.Region=%v, want %v", got, want)
		}
		if got, want := replica2.Endpoint, "custom.s3.endpoint.com"; got != want {
			t.Errorf("replica2.Endpoint=%v, want %v", got, want)
		}
		if got, want := replica2.Bucket, "my-bucket-2"; got != want {
			t.Errorf("replica2.Bucket=%v, want %v", got, want)
		}

		// Test Database 3: Legacy replicas format should work
		db3 := config.DBs[2]
		if len(db3.Replicas) != 1 {
			t.Fatalf("db3.Replicas length=%v, want 1", len(db3.Replicas))
		}
		replica3 := db3.Replicas[0]

		if got, want := replica3.AccessKeyID, "GLOBAL_ACCESS_KEY"; got != want {
			t.Errorf("replica3.AccessKeyID=%v, want %v", got, want)
		}
		if got, want := replica3.SecretAccessKey, "GLOBAL_SECRET_KEY"; got != want {
			t.Errorf("replica3.SecretAccessKey=%v, want %v", got, want)
		}
		if got, want := replica3.Region, "us-west-2"; got != want {
			t.Errorf("replica3.Region=%v, want %v", got, want)
		}
		if got, want := replica3.Endpoint, "custom.s3.endpoint.com"; got != want {
			t.Errorf("replica3.Endpoint=%v, want %v", got, want)
		}
		if got, want := replica3.Bucket, "my-bucket-3"; got != want {
			t.Errorf("replica3.Bucket=%v, want %v", got, want)
		}
	})

	// Test different replica types inherit appropriate defaults
	t.Run("MultipleReplicaTypes", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "litestream.yml")

		if err := os.WriteFile(filename, []byte(`
# Global defaults that apply to all supported replica types
access-key-id: GLOBAL_S3_KEY
secret-access-key: GLOBAL_S3_SECRET
region: global-region
endpoint: global.endpoint.com
account-name: global-abs-account
account-key: global-abs-key
host: global.sftp.host
user: global-sftp-user
password: global-sftp-pass
sync-interval: 45s

dbs:
  - path: /tmp/s3.sqlite
    replica:
      type: s3
      bucket: s3-bucket

  - path: /tmp/abs.sqlite
    replica:
      type: abs
      bucket: abs-container

  - path: /tmp/sftp.sqlite
    replica:
      type: sftp
      path: /backup/path
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, true)
		if err != nil {
			t.Fatal(err)
		}

		expectedSyncInterval, _ := time.ParseDuration("45s")

		// Test S3 replica inherits S3-specific defaults
		s3Replica := config.DBs[0].Replica
		if got, want := s3Replica.AccessKeyID, "GLOBAL_S3_KEY"; got != want {
			t.Errorf("s3Replica.AccessKeyID=%v, want %v", got, want)
		}
		if got, want := s3Replica.SecretAccessKey, "GLOBAL_S3_SECRET"; got != want {
			t.Errorf("s3Replica.SecretAccessKey=%v, want %v", got, want)
		}
		if got, want := s3Replica.Region, "global-region"; got != want {
			t.Errorf("s3Replica.Region=%v, want %v", got, want)
		}
		if got, want := s3Replica.Endpoint, "global.endpoint.com"; got != want {
			t.Errorf("s3Replica.Endpoint=%v, want %v", got, want)
		}
		if s3Replica.SyncInterval == nil || *s3Replica.SyncInterval != expectedSyncInterval {
			t.Errorf("s3Replica.SyncInterval=%v, want %v", s3Replica.SyncInterval, expectedSyncInterval)
		}

		// Test ABS replica inherits ABS-specific defaults
		absReplica := config.DBs[1].Replica
		if got, want := absReplica.AccountName, "global-abs-account"; got != want {
			t.Errorf("absReplica.AccountName=%v, want %v", got, want)
		}
		if got, want := absReplica.AccountKey, "global-abs-key"; got != want {
			t.Errorf("absReplica.AccountKey=%v, want %v", got, want)
		}
		if absReplica.SyncInterval == nil || *absReplica.SyncInterval != expectedSyncInterval {
			t.Errorf("absReplica.SyncInterval=%v, want %v", absReplica.SyncInterval, expectedSyncInterval)
		}

		// Test SFTP replica inherits SFTP-specific defaults
		sftpReplica := config.DBs[2].Replica
		if got, want := sftpReplica.Host, "global.sftp.host"; got != want {
			t.Errorf("sftpReplica.Host=%v, want %v", got, want)
		}
		if got, want := sftpReplica.User, "global-sftp-user"; got != want {
			t.Errorf("sftpReplica.User=%v, want %v", got, want)
		}
		if got, want := sftpReplica.Password, "global-sftp-pass"; got != want {
			t.Errorf("sftpReplica.Password=%v, want %v", got, want)
		}
		if sftpReplica.SyncInterval == nil || *sftpReplica.SyncInterval != expectedSyncInterval {
			t.Errorf("sftpReplica.SyncInterval=%v, want %v", sftpReplica.SyncInterval, expectedSyncInterval)
		}
	})
}

func TestStripSQLitePrefix(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"sqlite3 prefix", "sqlite3:///path/to/db.sqlite", "/path/to/db.sqlite"},
		{"sqlite prefix", "sqlite:///path/to/db.sqlite", "/path/to/db.sqlite"},
		{"sqlite3 relative path", "sqlite3://./data/db.sqlite", "./data/db.sqlite"},
		{"sqlite relative path", "sqlite://./data/db.sqlite", "./data/db.sqlite"},
		{"sqlite3 tilde path", "sqlite3://~/db.sqlite", "~/db.sqlite"},
		{"sqlite tilde path", "sqlite://~/db.sqlite", "~/db.sqlite"},
		{"no prefix", "/path/to/db.sqlite", "/path/to/db.sqlite"},
		{"relative no prefix", "./data/db.sqlite", "./data/db.sqlite"},
		{"tilde no prefix", "~/db.sqlite", "~/db.sqlite"},
		{"empty string", "", ""},
		{"sqlite3 windows path", "sqlite3://C:/data/db.sqlite", "C:/data/db.sqlite"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := main.StripSQLitePrefix(tt.input)
			if got != tt.want {
				t.Errorf("StripSQLitePrefix(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestReadConfigFile_SQLiteConnectionString(t *testing.T) {
	t.Run("ConfigWithSQLitePrefix", func(t *testing.T) {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "test.db")

		filename := filepath.Join(dir, "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: sqlite3://`+dbPath+`
    replicas:
      - url: file://`+filepath.Join(dir, "replica")+`
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, true)
		if err != nil {
			t.Fatalf("ReadConfigFile failed: %v", err)
		}

		if len(config.DBs) != 1 {
			t.Fatalf("expected 1 database, got %d", len(config.DBs))
		}

		if got := config.DBs[0].Path; got != dbPath {
			t.Errorf("DBs[0].Path = %q, want %q", got, dbPath)
		}
	})

	t.Run("ConfigWithSQLite3Prefix", func(t *testing.T) {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "test.db")

		filename := filepath.Join(dir, "litestream.yml")
		if err := os.WriteFile(filename, []byte(`
dbs:
  - path: sqlite://`+dbPath+`
    replicas:
      - url: file://`+filepath.Join(dir, "replica")+`
`[1:]), 0666); err != nil {
			t.Fatal(err)
		}

		config, err := main.ReadConfigFile(filename, true)
		if err != nil {
			t.Fatalf("ReadConfigFile failed: %v", err)
		}

		if len(config.DBs) != 1 {
			t.Fatalf("expected 1 database, got %d", len(config.DBs))
		}

		if got := config.DBs[0].Path; got != dbPath {
			t.Errorf("DBs[0].Path = %q, want %q", got, dbPath)
		}
	})
}

func hasDBPath(dbs []*litestream.DB, path string) bool {
	for _, db := range dbs {
		if db.Path() == path {
			return true
		}
	}
	return false
}

func waitForCondition(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return true
		}
		if time.Now().After(deadline) {
			return fn()
		}
		time.Sleep(50 * time.Millisecond)
	}
}
