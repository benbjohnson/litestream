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
		if len(config.Levels) != 2 {
			t.Fatalf("expected 2 default compaction levels, got %d", len(config.Levels))
		}

		// Check default intervals: 5m and 1h
		if config.Levels[0].Interval != 5*time.Minute {
			t.Errorf("expected default level[0] interval of 5m, got %v", config.Levels[0].Interval)
		}
		if config.Levels[1].Interval != 1*time.Hour {
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
