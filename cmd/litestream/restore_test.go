package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"

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
	if !strings.HasPrefix(err.Error(), "no matching backup files available") {
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

func TestRestoreCommand_RunUnavailableTarget(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		t.Run(fmt.Sprint(dryRun), func(t *testing.T) {
			ctx := context.Background()
			path := t.TempDir()
			client := file.NewReplicaClient(path)
			var buf bytes.Buffer
			enc, err := ltx.NewEncoder(&buf)
			if err != nil {
				t.Fatal(err)
			}
			if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: 4096, Commit: 1, MinTXID: 1, MaxTXID: 3}); err != nil {
				t.Fatal(err)
			}
			if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, make([]byte, 4096)); err != nil {
				t.Fatal(err)
			}
			if err := enc.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := client.WriteLTXFile(ctx, litestream.SnapshotLevel, 1, 3, &buf); err != nil {
				t.Fatal(err)
			}
			args := []string{"-txid", "0000000000000002", "-o", filepath.Join(t.TempDir(), "restore.db")}
			if dryRun {
				args = append(args, "-dry-run")
			}
			args = append(args, "file://"+path)
			err = (&RestoreCommand{}).Run(ctx, args)
			if !errors.Is(err, litestream.ErrTxNotAvailable) {
				t.Fatalf("error=%v, want unavailable target", err)
			}
			for _, want := range []string{"txid 0000000000000002 unavailable", "before=none", "after=0000000000000003"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error=%v, want %q", err, want)
				}
			}
		})
	}
}

func TestRestoreCommand_RunIfReplicaExists(t *testing.T) {
	err := (&RestoreCommand{}).Run(context.Background(), []string{"-if-replica-exists", "-o", filepath.Join(t.TempDir(), "restore.db"), "file://" + t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRestoreCommand_RestoreReportsAdvancedTXID(t *testing.T) {
	ctx := context.Background()
	client := &advancingRestoreCommandClient{ReplicaClient: file.NewReplicaClient(t.TempDir())}
	for _, txid := range []ltx.TXID{1, 2} {
		client.writeLTX(t, ctx, 1, txid)
	}
	client.advance = func() {
		client.writeLTX(t, ctx, 1, 3)
		if err := client.DeleteLTXFiles(ctx, []*ltx.FileInfo{{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 2}}); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "restore.db")
	result, err := (&RestoreCommand{}).restore(ctx, litestream.NewReplicaWithClient(nil, client), litestream.RestoreOptions{OutputPath: path}, "none")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, bytes.Repeat([]byte{3}, 4096)) {
		t.Fatal("restored database does not contain TXID 3 data")
	}
	output, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var got RestoreResult
	if err := json.Unmarshal(output, &got); err != nil {
		t.Fatal(err)
	}
	if got.TXID != "0000000000000003" {
		t.Fatalf("reported TXID=%s, want 0000000000000003", got.TXID)
	}
}

type advancingRestoreCommandClient struct {
	*file.ReplicaClient
	advance func()
}

func (c *advancingRestoreCommandClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if maxTXID == 2 && c.advance != nil {
		advance := c.advance
		c.advance = nil
		advance()
		return nil, os.ErrNotExist
	}
	return c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
}

func (c *advancingRestoreCommandClient) writeLTX(t *testing.T, ctx context.Context, minTXID, maxTXID ltx.TXID) {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: 4096, Commit: 1, MinTXID: minTXID, MaxTXID: maxTXID}); err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, bytes.Repeat([]byte{byte(maxTXID)}, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteLTXFile(ctx, litestream.SnapshotLevel, minTXID, maxTXID, &buf); err != nil {
		t.Fatal(err)
	}
}
