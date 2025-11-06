//go:build vfs
// +build vfs

package main_test

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestVFS_Simple(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	if err := sqlite3vfs.RegisterVFS("litestream", vfs); err != nil {
		t.Fatalf("failed to register litestream vfs: %v", err)
	}

	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb0 := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb0)

	if _, err := sqldb0.Exec("CREATE TABLE t (x)"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb0.Exec("INSERT INTO t (x) VALUES (100)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * db.MonitorInterval)

	sqldb1, err := sql.Open("sqlite3", "file:/tmp/test.db?vfs=litestream")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()

	// Execute query
	var x int
	if err := sqldb1.QueryRow("SELECT * FROM t").Scan(&x); err != nil {
		t.Fatalf("failed to query database: %v", err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}

func TestVFS_Updating(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	if err := sqlite3vfs.RegisterVFS("litestream", vfs); err != nil {
		t.Fatalf("failed to register litestream vfs: %v", err)
	}

	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.SyncInterval = 100 * time.Millisecond
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb0 := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb0)

	t.Log("creating table")
	if _, err := sqldb0.Exec("CREATE TABLE t (x)"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb0.Exec("INSERT INTO t (x) VALUES (100)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * db.MonitorInterval)

	t.Log("opening vfs")
	sqldb1, err := sql.Open("sqlite3", "file:/tmp/test.db?vfs=litestream")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()

	// Execute query
	var x int
	if err := sqldb1.QueryRow("SELECT * FROM t").Scan(&x); err != nil {
		t.Fatalf("failed to query database: %v", err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}

	t.Log("updating source database")
	// Update the value from the source database.
	if _, err := sqldb0.Exec("UPDATE t SET x = 200"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * db.MonitorInterval)

	// Ensure replica has updated itself.
	t.Log("ensuring replica has updated")
	if err := sqldb1.QueryRow("SELECT * FROM t").Scan(&x); err != nil {
		t.Fatalf("failed to query database: %v", err)
	} else if got, want := x, 200; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}

func TestVFS_ActiveReadTransaction(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	if err := sqlite3vfs.RegisterVFS("litestream-txn", vfs); err != nil {
		t.Fatalf("failed to register litestream vfs: %v", err)
	}

	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.SyncInterval = 100 * time.Millisecond
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb0 := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb0)

	// Create a table with many rows to ensure we span multiple pages
	// With 4KB page size, we want to ensure we're using hundreds of pages
	t.Log("creating table with many rows")
	if _, err := sqldb0.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)"); err != nil {
		t.Fatal(err)
	}

	// Insert ~10000 rows, each with substantial data to span many pages
	// This should occupy at least 200+ pages (assuming ~200 bytes per row, ~20 rows per 4KB page)
	if _, err := sqldb0.Exec("BEGIN"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		data := fmt.Sprintf("initial_data_%d_padding_%s", i, string(make([]byte, 100)))
		if _, err := sqldb0.Exec("INSERT INTO t (id, data) VALUES (?, ?)", i, data); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sqldb0.Exec("COMMIT"); err != nil {
		t.Fatal(err)
	}

	// Wait for replication to sync
	time.Sleep(3 * db.MonitorInterval)

	t.Log("opening vfs replica")
	sqldb1, err := sql.Open("sqlite3", "file:/tmp/test-txn.db?vfs=litestream-txn")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()

	// Start a read transaction on the replica
	t.Log("starting read transaction on replica")
	tx, err := sqldb1.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Verify we can read initial data from within the transaction
	var initialData string
	if err := tx.QueryRow("SELECT data FROM t WHERE id = 5000").Scan(&initialData); err != nil {
		t.Fatalf("failed to query initial data: %v", err)
	}
	if !strings.HasPrefix(initialData, "initial_data_5000") {
		t.Fatalf("unexpected initial data: %s", initialData)
	}

	t.Log("updating source database with many affected pages")
	// Update many rows in the source database to affect many pages
	if _, err := sqldb0.Exec("BEGIN"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		data := fmt.Sprintf("updated_data_%d_padding_%s", i, string(make([]byte, 100)))
		if _, err := sqldb0.Exec("UPDATE t SET data = ? WHERE id = ?", data, i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sqldb0.Exec("COMMIT"); err != nil {
		t.Fatal(err)
	}

	// Wait for replication to sync the updates
	t.Log("waiting for replication sync")
	time.Sleep(5 * db.MonitorInterval)

	// The active read transaction should still see old data (snapshot isolation)
	t.Log("verifying read transaction still sees old data")
	var txData string
	if err := tx.QueryRow("SELECT data FROM t WHERE id = 5000").Scan(&txData); err != nil {
		t.Fatalf("failed to query within transaction: %v", err)
	}
	if !strings.HasPrefix(txData, "initial_data_5000") {
		t.Fatalf("transaction should see old data, got: %s", txData)
	}

	// Commit the read transaction
	t.Log("committing read transaction")
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	// Now a new query should see the updated data
	t.Log("verifying new query sees updated data")
	var updatedData string
	if err := sqldb1.QueryRow("SELECT data FROM t WHERE id = 5000").Scan(&updatedData); err != nil {
		t.Fatalf("failed to query updated data: %v", err)
	}
	if !strings.HasPrefix(updatedData, "updated_data_5000") {
		t.Fatalf("expected updated data, got: %s", updatedData)
	}

	// Verify multiple rows across different pages
	t.Log("verifying multiple rows across pages")
	for _, id := range []int{0, 2500, 5000, 7500, 9999} {
		var data string
		if err := sqldb1.QueryRow("SELECT data FROM t WHERE id = ?", id).Scan(&data); err != nil {
			t.Fatalf("failed to query id %d: %v", id, err)
		}
		expected := fmt.Sprintf("updated_data_%d", id)
		if !strings.HasPrefix(data, expected) {
			t.Fatalf("id %d: expected prefix %s, got: %s", id, expected, data)
		}
	}
}

func newVFS(tb testing.TB, client litestream.ReplicaClient) *litestream.VFS {
	tb.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	vfs := litestream.NewVFS(client, logger)
	vfs.PollInterval = 100 * time.Millisecond
	return vfs
}
