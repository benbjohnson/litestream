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
	}
	// After reset, should return actual LTX timestamp (not "latest" anymore per #853)
	if _, err := time.Parse(time.RFC3339Nano, currentTime); err != nil {
		t.Fatalf("current time after reset should be valid RFC3339Nano timestamp, got %s: %v", currentTime, err)
	}
}

func TestVFS_PragmaLitestreamTxid(t *testing.T) {
	ctx := context.Background()
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	vfs.PollInterval = 50 * time.Millisecond
	if err := sqlite3vfs.RegisterVFS("litestream-txid", vfs); err != nil {
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

	sqldb1, err := sql.Open("sqlite3", "file:/tmp/txid-test.db?vfs=litestream-txid")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()
	sqldb1.SetMaxOpenConns(1)
	time.Sleep(2 * vfs.PollInterval)

	var txid int64
	if err := sqldb1.QueryRow("PRAGMA litestream_txid").Scan(&txid); err != nil {
		t.Fatalf("query txid: %v", err)
	}
	if txid <= 0 {
		t.Fatalf("expected positive TXID, got %d", txid)
	}

	// Test that setting litestream_txid fails (read-only)
	if _, err := sqldb1.Exec("PRAGMA litestream_txid = 123"); err == nil {
		t.Fatal("expected error setting litestream_txid (read-only)")
	}
}

func TestVFS_PragmaLitestreamLag(t *testing.T) {
	ctx := context.Background()
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	vfs.PollInterval = 50 * time.Millisecond
	if err := sqlite3vfs.RegisterVFS("litestream-lag", vfs); err != nil {
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

	sqldb1, err := sql.Open("sqlite3", "file:/tmp/lag-test.db?vfs=litestream-lag")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()
	sqldb1.SetMaxOpenConns(1)

	// Wait for at least one successful poll (poll runs on ticker, not immediately)
	time.Sleep(5 * vfs.PollInterval)

	var lag int64
	if err := sqldb1.QueryRow("PRAGMA litestream_lag").Scan(&lag); err != nil {
		t.Fatalf("query lag: %v", err)
	}
	// Lag should be -1 (never polled) or a small non-negative value after polling
	// -1 is valid if no poll has succeeded yet
	if lag < -1 || lag > 5 {
		t.Fatalf("expected lag between -1 and 5 seconds, got %d", lag)
	}

	// Test that setting litestream_lag fails (read-only)
	if _, err := sqldb1.Exec("PRAGMA litestream_lag = 123"); err == nil {
		t.Fatal("expected error setting litestream_lag (read-only)")
	}
}

func TestVFS_PragmaRelativeTime(t *testing.T) {
	ctx := context.Background()
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	vfs.PollInterval = 50 * time.Millisecond
	if err := sqlite3vfs.RegisterVFS("litestream-relative", vfs); err != nil {
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

	sqldb1, err := sql.Open("sqlite3", "file:/tmp/relative-test.db?vfs=litestream-relative")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()
	sqldb1.SetMaxOpenConns(1)
	time.Sleep(2 * vfs.PollInterval)

	// Test that relative time parsing works (even if no data exists at that time)
	// The parse should succeed, but may return "no backup files available" if too far in past
	now := time.Now()
	_, err = sqldb1.Exec("PRAGMA litestream_time = '1 second ago'")
	// This might fail if no LTX files exist at that time, which is expected
	// The important thing is that the parsing worked (not a "parse timestamp" error)
	if err != nil && err.Error() != "no backup files available" {
		// Check if it's a parse error vs a "no files" error
		if err.Error() == "invalid timestamp (expected RFC3339 or relative time like '5 minutes ago'): 1 second ago" {
			t.Fatalf("relative time parsing failed: %v", err)
		}
	}

	// Reset to latest
	if _, err := sqldb1.Exec("PRAGMA litestream_time = LATEST"); err != nil {
		t.Fatalf("reset to latest: %v", err)
	}

	// Verify the current time is recent (within last minute)
	var currentTime string
	if err := sqldb1.QueryRow("PRAGMA litestream_time").Scan(&currentTime); err != nil {
		t.Fatalf("query current time: %v", err)
	}
	ts, err := time.Parse(time.RFC3339Nano, currentTime)
	if err != nil {
		t.Fatalf("parse current time: %v", err)
	}
	if now.Sub(ts) > time.Minute {
		t.Fatalf("current time too old: %v (now: %v)", ts, now)
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
