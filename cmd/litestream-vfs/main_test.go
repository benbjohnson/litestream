//go:build vfs
// +build vfs

package main_test

import (
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

const sharedReplicaDir = "/tmp/litestream-vfs-test-shared"

// TestMain sets up a shared VFS once for all tests using Ben's approach:
// - Single shared temp directory
// - VFS registered once at package level
// - Tests clean up the directory between runs
func TestMain(m *testing.M) {
	// Create shared replica directory
	if err := os.RemoveAll(sharedReplicaDir); err != nil && !os.IsNotExist(err) {
		panic(err)
	}
	if err := os.MkdirAll(sharedReplicaDir, 0755); err != nil {
		panic(err)
	}

	// Set up VFS once for all tests
	client := file.NewReplicaClient(sharedReplicaDir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	vfs := litestream.NewVFS(client, logger)
	vfs.PollInterval = 100 * time.Millisecond

	if err := sqlite3vfs.RegisterVFS("litestream", vfs); err != nil {
		panic(err)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	os.RemoveAll(sharedReplicaDir)

	os.Exit(code)
}

func TestVFS_Integration(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		// Clean shared replica directory for this test
		if err := os.RemoveAll(sharedReplicaDir); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if err := os.MkdirAll(sharedReplicaDir, 0755); err != nil {
			t.Fatal(err)
		}

		// Use shared replica directory (Ben's approach)
		client := file.NewReplicaClient(sharedReplicaDir)

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

		// Use the VFS registered in TestMain - no complex extension loading needed!
		sqldb1, err := sql.Open("sqlite3", "file:/tmp/test.db?vfs=litestream&mode=ro")
		if err != nil {
			t.Fatalf("failed to open database with VFS: %v", err)
		}
		defer sqldb1.Close()

		// Execute query
		var x int
		if err := sqldb1.QueryRow("SELECT * FROM t").Scan(&x); err != nil {
			t.Fatalf("failed to query database: %v", err)
		} else if got, want := x, 100; got != want {
			t.Fatalf("got %d, want %d", got, want)
		}
	})

	t.Run("Updating", func(t *testing.T) {
		// Clean shared replica directory for this test
		if err := os.RemoveAll(sharedReplicaDir); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if err := os.MkdirAll(sharedReplicaDir, 0755); err != nil {
			t.Fatal(err)
		}

		// Use shared replica directory (Ben's approach)
		// VFS already registered in TestMain!
		client := file.NewReplicaClient(sharedReplicaDir)

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
	})
}

// Note: With Ben's approach of using a shared temp directory and setting up VFS once
// in TestMain, we can use simple static linking. No complex extension loading needed.
//
// See VFS_EXTENSION_FINDINGS.md for full analysis and test results.
