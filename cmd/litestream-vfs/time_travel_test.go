//go:build vfs
// +build vfs

package main_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestVFS_TimeTravelFunctions(t *testing.T) {
	ctx := context.Background()
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	vfs.PollInterval = 50 * time.Millisecond
	if err := sqlite3vfs.RegisterVFS("litestream-time", vfs); err != nil {
		t.Fatalf("failed to register litestream vfs: %v", err)
	}

	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 50 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.SyncInterval = 50 * time.Millisecond
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close(ctx) }()

	sqldb0 := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb0)

	if _, err := sqldb0.Exec("CREATE TABLE t (x INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb0.Exec("INSERT INTO t (x) VALUES (100)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(6 * db.MonitorInterval)

	firstCreatedAt := fetchLTXCreatedAt(t, ctx, client)

	time.Sleep(20 * time.Millisecond) // Ensure a different timestamp for the next file.
	if _, err := sqldb0.Exec("UPDATE t SET x = 200"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * db.MonitorInterval)

	sqldb1, err := sql.Open("sqlite3", "file:/tmp/time-travel.db?vfs=litestream-time")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()
	sqldb1.SetMaxOpenConns(1)
	time.Sleep(2 * vfs.PollInterval)

	var value int
	if err := sqldb1.QueryRow("SELECT x FROM t").Scan(&value); err != nil {
		t.Fatalf("query latest value: %v", err)
	} else if got, want := value, 200; got != want {
		t.Fatalf("latest value: got %d, want %d", got, want)
	}

	target := firstCreatedAt.Add(1 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	if _, err := sqldb1.Exec(fmt.Sprintf("PRAGMA LITESTREAM_TIME = '%s'", target)); err != nil {
		t.Fatalf("set target time: %v", err)
	}

	if err := sqldb1.QueryRow("SELECT x FROM t").Scan(&value); err != nil {
		t.Fatalf("query historical value: %v", err)
	} else if got, want := value, 100; got != want {
		t.Fatalf("historical value: got %d, want %d", got, want)
	}

	var currentTime string
	if err := sqldb1.QueryRow("PRAGMA litestream_time").Scan(&currentTime); err != nil {
		t.Fatalf("current time: %v", err)
	} else if currentTime != target {
		t.Fatalf("current time mismatch: got %s, want %s", currentTime, target)
	}

	if _, err := sqldb1.Exec("PRAGMA LITESTREAM_TIME = LATEST"); err != nil {
		t.Fatalf("reset time: %v", err)
	}

	if err := sqldb1.QueryRow("SELECT x FROM t").Scan(&value); err != nil {
		t.Fatalf("query reset value: %v", err)
	} else if got, want := value, 200; got != want {
		t.Fatalf("reset value: got %d, want %d", got, want)
	}

	if err := sqldb1.QueryRow("PRAGMA litestream_time").Scan(&currentTime); err != nil {
		t.Fatalf("current time after reset: %v", err)
	} else if currentTime != "latest" {
		t.Fatalf("current time after reset mismatch: got %s, want latest", currentTime)
	}
}

func fetchLTXCreatedAt(tb testing.TB, ctx context.Context, client litestream.ReplicaClient) time.Time {
	tb.Helper()

	itr, err := client.LTXFiles(ctx, 0, 0, true)
	if err != nil {
		tb.Fatalf("ltx files: %v", err)
	}
	defer itr.Close()

	var ts time.Time
	for itr.Next() {
		ts = itr.Item().CreatedAt
	}
	if err := itr.Close(); err != nil {
		tb.Fatalf("close iterator: %v", err)
	}
	if ts.IsZero() {
		tb.Fatalf("no ltx files found")
	}
	return ts.UTC()
}
