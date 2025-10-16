package main_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gs"
	"github.com/benbjohnson/litestream/s3"
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
