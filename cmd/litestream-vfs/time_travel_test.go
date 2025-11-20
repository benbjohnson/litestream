//go:build vfs
// +build vfs

package main_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"

	"github.com/benbjohnson/litestream"
	vfsbin "github.com/benbjohnson/litestream/cmd/litestream-vfs"
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
	time.Sleep(2 * db.MonitorInterval)

	firstCreatedAt := fetchLTXCreatedAt(t, ctx, client)

	time.Sleep(20 * time.Millisecond) // Ensure a different timestamp for the next file.
	if _, err := sqldb0.Exec("UPDATE t SET x = 200"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * db.MonitorInterval)

	driverName := fmt.Sprintf("litestream-time-%d", time.Now().UnixNano())
	sql.Register(driverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return registerTimeFunctions(t, conn)
		},
	})

	sqldb1, err := sql.Open(driverName, "file:/tmp/time-travel.db?vfs=litestream-time")
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
	if _, err := sqldb1.Exec("SELECT litestream_set_time(?)", target); err != nil {
		t.Fatalf("set target time: %v", err)
	}

	if err := sqldb1.QueryRow("SELECT x FROM t").Scan(&value); err != nil {
		t.Fatalf("query historical value: %v", err)
	} else if got, want := value, 100; got != want {
		t.Fatalf("historical value: got %d, want %d", got, want)
	}

	var currentTime string
	if err := sqldb1.QueryRow("SELECT litestream_current_time()").Scan(&currentTime); err != nil {
		t.Fatalf("current time: %v", err)
	} else if currentTime != target {
		t.Fatalf("current time mismatch: got %s, want %s", currentTime, target)
	}

	if _, err := sqldb1.Exec("SELECT litestream_reset_time()"); err != nil {
		t.Fatalf("reset time: %v", err)
	}

	if err := sqldb1.QueryRow("SELECT x FROM t").Scan(&value); err != nil {
		t.Fatalf("query reset value: %v", err)
	} else if got, want := value, 200; got != want {
		t.Fatalf("reset value: got %d, want %d", got, want)
	}

	if err := sqldb1.QueryRow("SELECT litestream_current_time()").Scan(&currentTime); err != nil {
		t.Fatalf("current time after reset: %v", err)
	} else if currentTime != "latest" {
		t.Fatalf("current time after reset mismatch: got %s, want latest", currentTime)
	}
}

func registerTimeFunctions(tb testing.TB, conn *sqlite3.SQLiteConn) error {
	tb.Helper()

	handle := sqliteHandle(conn)
	if handle == nil {
		return fmt.Errorf("nil sqlite handle")
	}

	fileID, err := vfsbin.LitestreamFileID(handle)
	if err != nil {
		return err
	}

	dbPtr := uintptr(handle)
	if err := litestream.RegisterVFSConnection(dbPtr, fileID); err != nil {
		return err
	}
	tb.Cleanup(func() {
		litestream.UnregisterVFSConnection(dbPtr)
	})

	if err := conn.RegisterFunc("litestream_set_time", func(ts string) error {
		return litestream.SetVFSConnectionTime(dbPtr, ts)
	}, true); err != nil {
		return err
	}
	if err := conn.RegisterFunc("litestream_reset_time", func() error {
		return litestream.ResetVFSConnectionTime(dbPtr)
	}, true); err != nil {
		return err
	}
	if err := conn.RegisterFunc("litestream_current_time", func() (string, error) {
		return litestream.CurrentVFSConnectionTime(dbPtr)
	}, true); err != nil {
		return err
	}

	return nil
}

func sqliteHandle(conn *sqlite3.SQLiteConn) unsafe.Pointer {
	v := reflect.ValueOf(conn).Elem().FieldByName("db")
	if !v.IsValid() || v.IsNil() {
		return nil
	}
	return unsafe.Pointer(v.UnsafePointer())
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
