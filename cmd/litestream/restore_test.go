package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	litestream "github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestRestoreCommand_FollowIntervalFlag(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantVal time.Duration
		wantErr bool
	}{
		{
			name:    "Default",
			args:    []string{"/tmp/db"},
			wantVal: time.Second,
		},
		{
			name:    "CustomValue",
			args:    []string{"-follow-interval", "500ms", "/tmp/db"},
			wantVal: 500 * time.Millisecond,
		},
		{
			name:    "LongerInterval",
			args:    []string{"-follow-interval", "5s", "/tmp/db"},
			wantVal: 5 * time.Second,
		},
		{
			name:    "InvalidDuration",
			args:    []string{"-follow-interval", "notaduration", "/tmp/db"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := litestream.NewRestoreOptions()
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.DurationVar(&opt.FollowInterval, "follow-interval", opt.FollowInterval, "polling interval for follow mode")

			err := fs.Parse(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if opt.FollowInterval != tt.wantVal {
				t.Fatalf("FollowInterval=%v, want %v", opt.FollowInterval, tt.wantVal)
			}
		})
	}
}

func TestRestoreCommand_RunMissingOutputPathForReplicaURL(t *testing.T) {
	cmd := &RestoreCommand{}
	err := cmd.Run(context.Background(), []string{"s3://bucket/prefix"})
	if err == nil {
		t.Fatal("expected error for missing output path")
	}
	if err.Error() != "-o is required when restoring from a replica URL. Try: litestream restore -o /path/to/db s3://bucket/prefix" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewS3ReplicaClientFromConfig_SuggestedHintExample(t *testing.T) {
	client, err := NewS3ReplicaClientFromConfig(&ReplicaConfig{URL: "s3://bucket/prefix"}, nil)
	if err == nil {
		if client.Bucket != "bucket" {
			t.Fatalf("Bucket=%q, want %q", client.Bucket, "bucket")
		}
		if client.Path != "prefix" {
			t.Fatalf("Path=%q, want %q", client.Path, "prefix")
		}
		return
	}

	t.Fatalf("unexpected error: %v", err)
}

func TestRestoreCommand_RunSuggestedOutputArgs(t *testing.T) {
	cmd := &RestoreCommand{}
	err := cmd.Run(context.Background(), []string{"-o", filepath.Join(t.TempDir(), "db.sqlite"), "file://" + t.TempDir()})
	if err == nil {
		t.Fatal("expected error for empty replica")
	}
	if err.Error() != "no matching backup files available" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRestoreCommand_RunJSONOutput(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.sqlite")
	replicaPath := filepath.Join(dir, "replica")
	restorePath := filepath.Join(dir, "restored.sqlite")

	db := testingutil.NewDB(t, dbPath)
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	client := file.NewReplicaClient(replicaPath)
	replica := litestream.NewReplicaWithClient(db, client)
	replica.MonitorEnabled = false
	db.Replica = replica

	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb := testingutil.MustOpenSQLDB(t, dbPath)
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err := db.SyncAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}

	output := captureLTXCommandStdout(t, func() {
		cmd := &RestoreCommand{}
		if err := cmd.Run(ctx, []string{"-json", "-o", restorePath, "file://" + replicaPath}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var got RestoreResult
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("failed to parse output: %v\n%s", err, output)
	}
	if got.DBPath != restorePath {
		t.Fatalf("unexpected db path: %s", got.DBPath)
	}
	if got.Replica != "file" {
		t.Fatalf("unexpected replica: %s", got.Replica)
	}
	if got.TXID == "" {
		t.Fatal("expected txid")
	}
	if got.DurationMS < 0 {
		t.Fatalf("unexpected duration_ms: %d", got.DurationMS)
	}
	if got.IntegrityCheck != "none" {
		t.Fatalf("unexpected integrity check: %s", got.IntegrityCheck)
	}
	if _, err := os.Stat(restorePath); err != nil {
		t.Fatalf("expected restored database: %v", err)
	}
}
