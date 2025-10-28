//go:build vfs
// +build vfs

package main_test

import (
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestVFS_Integration(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())

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

		sqldb1 := openWithVFS(t, client.Path())
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
	})
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

var driverOnce sync.Once

// openWithVFS attempts to open a database using the litestream VFS loaded as an extension.
//
// CURRENT STATE: This currently fails with a segfault because github.com/psanford/sqlite3vfs
// cannot be used from within a dynamically loaded SQLite extension. The sqlite3vfs package
// is designed for static linking only.
//
// The LazyReplicaClient pattern implemented in litestream-vfs.go successfully solves the
// environment variable propagation issue, but we hit the sqlite3vfs limitation before
// we can test it fully.
//
// See VFS_EXTENSION_FINDINGS.md for full analysis and recommendations.
func openWithVFS(tb testing.TB, path string) *sql.DB {
	tb.Helper()

	// Set environment variables BEFORE registering the driver.
	// The LazyReplicaClient will read these when the VFS is first accessed.
	os.Setenv("LITESTREAM_REPLICA_TYPE", "file")
	os.Setenv("LITESTREAM_REPLICA_PATH", path)
	os.Setenv("LITESTREAM_LOG_LEVEL", "DEBUG")

	// Register the driver once with the extension.
	// The extension will load on first connection and attempt to register a lazy VFS.
	driverOnce.Do(func() {
		sql.Register("sqlite3_ext",
			&sqlite3.SQLiteDriver{
				Extensions: []string{
					`/Users/corylanou/projects/benbjohnson/litestream/dist/litestream-vfs.so`,
				},
			})
	})

	// Open connection - this will load the extension and attempt VFS registration.
	// Currently segfaults during sqlite3vfs.RegisterVFS() call.
	db, err := sql.Open("sqlite3_ext", ":memory:")
	if err != nil {
		tb.Fatalf("failed to open database: %v", err)
	}

	// Ping to actually establish the connection and load the extension.
	if err := db.Ping(); err != nil {
		tb.Fatalf("failed to ping database: %v", err)
	}

	// Close the initial connection.
	if err := db.Close(); err != nil {
		tb.Fatalf("failed to close initial database: %v", err)
	}

	// Now open with the litestream VFS that was registered by the extension.
	db, err = sql.Open("sqlite3_ext", ":memory:?vfs=litestream")
	if err != nil {
		tb.Fatalf("failed to open database with VFS: %v", err)
	}

	return db
}
