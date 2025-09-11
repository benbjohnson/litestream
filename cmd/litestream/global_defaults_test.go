package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

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

		config, err := ReadConfigFile(filename, true)
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

		config, err := ReadConfigFile(filename, true)
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
