package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"

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
	if err.Error() != "-o is required when restoring from a replica URL" {
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
	replicaPath, restorePath := createRestoreCommandTestData(t, ctx)

	output := captureLTXCommandStdout(t, func() {
		cmd := &RestoreCommand{}
		if err := cmd.Run(ctx, []string{"-json", "-o", restorePath, "file://" + replicaPath}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var got RestoreOutput
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("failed to parse output: %v\n%s", err, output)
	}
	if got.Status != RestoreStatusRestored {
		t.Fatalf("unexpected status: %s", got.Status)
	}
	if got.Reason != "" {
		t.Fatalf("unexpected reason: %s", got.Reason)
	}
	if got.RestoreResult == nil {
		t.Fatal("expected restore result")
	}
	if got.DBPath != restorePath {
		t.Fatalf("unexpected db path: %s", got.DBPath)
	}
	if got.Replica != "file" {
		t.Fatalf("unexpected replica: %s", got.Replica)
	}
	if got.TXID != "" {
		t.Fatalf("automatic legacy-capable restore reported unverified TXID %q", got.TXID)
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

func TestRestoreCommand_RunJSONSkipOutput(t *testing.T) {
	tests := []struct {
		name       string
		args       func(*testing.T) []string
		wantReason RestoreReason
	}{
		{
			name: "DatabaseExists",
			args: func(t *testing.T) []string {
				restorePath := filepath.Join(t.TempDir(), "db")
				if err := os.WriteFile(restorePath, []byte("existing"), 0o600); err != nil {
					t.Fatal(err)
				}
				return []string{"-json", "-if-db-not-exists", "-o", restorePath, "file://" + t.TempDir()}
			},
			wantReason: RestoreReasonDatabaseExists,
		},
		{
			name: "NoMatchingBackups",
			args: func(t *testing.T) []string {
				return []string{"-json", "-if-replica-exists", "-o", filepath.Join(t.TempDir(), "db"), "file://" + t.TempDir()}
			},
			wantReason: RestoreReasonNoMatchingBackups,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := captureLTXCommandStdout(t, func() {
				if err := (&RestoreCommand{}).Run(t.Context(), tt.args(t)); err != nil {
					t.Fatal(err)
				}
			})

			var got RestoreOutput
			if err := json.Unmarshal([]byte(output), &got); err != nil {
				t.Fatalf("failed to parse output: %v\n%s", err, output)
			}
			if got.Status != RestoreStatusSkipped {
				t.Fatalf("status=%q, want %q", got.Status, RestoreStatusSkipped)
			}
			if got.Reason != tt.wantReason {
				t.Fatalf("reason=%q, want %q", got.Reason, tt.wantReason)
			}
			if got.RestoreResult != nil {
				t.Fatalf("result=%v, want nil", got.RestoreResult)
			}
		})
	}
}

func TestRestoreCommand_RunDryRunJSONOutput(t *testing.T) {
	ctx := context.Background()
	replicaPath, restorePath := createRestoreCommandTestData(t, ctx)

	output := captureLTXCommandStdout(t, func() {
		cmd := &RestoreCommand{}
		if err := cmd.Run(ctx, []string{"-dry-run", "-json", "-o", restorePath, "file://" + replicaPath}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var got RestorePlan
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("failed to parse output: %v\n%s", err, output)
	}
	if got.Source != "file://"+replicaPath {
		t.Fatalf("unexpected source: %s", got.Source)
	}
	if got.TargetPath != restorePath {
		t.Fatalf("unexpected target path: %s", got.TargetPath)
	}
	if got.Replica != "file" {
		t.Fatalf("unexpected replica: %s", got.Replica)
	}
	if got.MinTXID == "" {
		t.Fatal("expected min txid")
	}
	if got.MaxTXID == "" {
		t.Fatal("expected max txid")
	}
	if len(got.Files) == 0 {
		t.Fatal("expected files")
	}
	if got.Files[0].Name == "" {
		t.Fatal("expected file name")
	}
	if got.Files[0].Timestamp == "" {
		t.Fatal("expected file timestamp")
	}
	if _, err := os.Stat(restorePath); !os.IsNotExist(err) {
		t.Fatalf("expected no restored database, stat err=%v", err)
	}
}

func TestRestoreCommand_RunRequiresForceForExistingOutput(t *testing.T) {
	ctx := context.Background()
	replicaPath, restorePath := createRestoreCommandTestData(t, ctx)
	if err := os.WriteFile(restorePath, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}

	cmd := &RestoreCommand{}
	err := cmd.Run(ctx, []string{"-o", restorePath, "file://" + replicaPath})
	if err == nil {
		t.Fatal("expected error")
	}
	expected := "cannot restore, output path already exists and is not empty: " + restorePath + ". Use -force to overwrite"
	if err.Error() != expected {
		t.Fatalf("unexpected error: %v", err)
	}

	buf, err := os.ReadFile(restorePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != "existing" {
		t.Fatalf("existing output was modified: %q", string(buf))
	}
}

func TestRestoreCommand_RunForceOverwritesExistingOutput(t *testing.T) {
	ctx := context.Background()
	replicaPath, restorePath := createRestoreCommandTestData(t, ctx)
	for _, path := range []string{restorePath, restorePath + "-wal", restorePath + "-shm", restorePath + "-journal"} {
		if err := os.WriteFile(path, []byte("existing"), 0600); err != nil {
			t.Fatal(err)
		}
	}

	cmd := &RestoreCommand{}
	if err := cmd.Run(ctx, []string{"-force", "-o", restorePath, "file://" + replicaPath}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertRestoreCommandDB(t, restorePath)
}

func TestRestoreCommand_RunRequiresForceForExistingJournal(t *testing.T) {
	ctx := context.Background()
	replicaPath, restorePath := createRestoreCommandTestData(t, ctx)
	// Empty main file passes the size check; journal sidecar should still trip the guard.
	if err := os.WriteFile(restorePath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	journalPath := restorePath + "-journal"
	if err := os.WriteFile(journalPath, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}

	cmd := &RestoreCommand{}
	err := cmd.Run(ctx, []string{"-o", restorePath, "file://" + replicaPath})
	if err == nil {
		t.Fatal("expected error")
	}
	expected := "cannot restore, SQLite sidecar path already exists: " + journalPath + ". Use -force to overwrite"
	if err.Error() != expected {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRestoreCommand_RunAllowsEmptyOutput(t *testing.T) {
	ctx := context.Background()
	replicaPath, restorePath := createRestoreCommandTestData(t, ctx)
	if err := os.WriteFile(restorePath, nil, 0600); err != nil {
		t.Fatal(err)
	}

	cmd := &RestoreCommand{}
	if err := cmd.Run(ctx, []string{"-o", restorePath, "file://" + replicaPath}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertRestoreCommandDB(t, restorePath)
}

func createRestoreCommandTestData(t *testing.T, ctx context.Context) (string, string) {
	t.Helper()

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

	return replicaPath, restorePath
}

func assertRestoreCommandDB(t *testing.T, path string) {
	t.Helper()

	sqldb := testingutil.MustOpenSQLDB(t, path)
	defer func() {
		if err := sqldb.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	var count int
	if err := sqldb.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM t`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("count=%d, want 1", count)
	}
}

func TestRestoreCommandOutputAccessError(t *testing.T) {
	for _, fromConfig := range []bool{false, true} {
		t.Run(map[bool]string{false: "url", true: "config"}[fromConfig], func(t *testing.T) {
			dir := t.TempDir()
			output := filepath.Join(dir, strings.Repeat("x", 300))
			args := []string{"-json", "-if-db-not-exists", "-o", output}
			if fromConfig {
				config := filepath.Join(dir, "litestream.yml")
				dbPath := filepath.Join(dir, "db")
				content := "dbs:\n  - path: " + dbPath + "\n    replica:\n      url: file://" + dir + "/replica\n"
				if err := os.WriteFile(config, []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "-config", config, dbPath)
			} else {
				args = append(args, "file://"+dir+"/replica")
			}
			err := (&RestoreCommand{}).Run(t.Context(), args)
			if err == nil || !strings.Contains(err.Error(), "cannot access output path") {
				t.Fatalf("error=%v, want output access error", err)
			}
		})
	}
}

func TestRestoreCommandLegacySelection(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy only", true: "newer legacy"}[mixed], func(t *testing.T) {
			dir := t.TempDir()
			replicaPath, restorePath := filepath.Join(dir, "replica"), filepath.Join(dir, "restored.db")
			if mixed {
				replicaPath, restorePath = createRestoreCommandTestData(t, t.Context())
			}
			source := filepath.Join(dir, "legacy.db")
			db := testingutil.MustOpenSQLDB(t, source)
			if _, err := db.ExecContext(t.Context(), "CREATE TABLE t (id INT); INSERT INTO t VALUES (2)"); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(source)
			if err != nil {
				t.Fatal(err)
			}
			var compressed bytes.Buffer
			writer := lz4.NewWriter(&compressed)
			if _, err := writer.Write(data); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			snapshotDir := filepath.Join(replicaPath, "generations", "0123456789abcdef", "snapshots")
			if err := os.MkdirAll(snapshotDir, 0755); err != nil {
				t.Fatal(err)
			}
			snapshot := filepath.Join(snapshotDir, "00000000.snapshot.lz4")
			if err := os.WriteFile(snapshot, compressed.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			newer := time.Now().Add(time.Hour)
			if err := os.Chtimes(snapshot, newer, newer); err != nil {
				t.Fatal(err)
			}
			output := captureLTXCommandStdout(t, func() {
				if err := (&RestoreCommand{}).Run(t.Context(), []string{"-json", "-o", restorePath, "file://" + replicaPath}); err != nil {
					t.Fatal(err)
				}
			})
			var result RestoreOutput
			if err := json.Unmarshal([]byte(output), &result); err != nil {
				t.Fatal(err)
			}
			if result.RestoreResult == nil || result.TXID != "" {
				t.Fatalf("result=%+v, want unknown legacy TXID", result)
			}
			restored := testingutil.MustOpenSQLDB(t, restorePath)
			defer func() {
				if err := restored.Close(); err != nil {
					t.Error(err)
				}
			}()
			var id int
			if err := restored.QueryRowContext(t.Context(), "SELECT id FROM t").Scan(&id); err != nil {
				t.Fatal(err)
			}
			if id != 2 {
				t.Fatalf("restored id=%d, want legacy value 2", id)
			}
		})
	}
}

func TestRestoreCommandExplicitTXIDOutput(t *testing.T) {
	replicaPath, restorePath := createRestoreCommandTestData(t, t.Context())
	output := captureLTXCommandStdout(t, func() {
		if err := (&RestoreCommand{}).Run(t.Context(), []string{"-json", "-txid", "0000000000000001", "-o", restorePath, "file://" + replicaPath}); err != nil {
			t.Fatal(err)
		}
	})
	var result RestoreOutput
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatal(err)
	}
	if result.RestoreResult == nil || result.TXID != "0000000000000001" {
		t.Fatalf("result=%+v", result)
	}
}
