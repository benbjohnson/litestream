package litestream_test

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

func TestDB_Path(t *testing.T) {
	db := newDB(t, "/tmp/db")
	if got, want := db.Path(), `/tmp/db`; got != want {
		t.Fatalf("Path()=%v, want %v", got, want)
	}
}

func TestDB_WALPath(t *testing.T) {
	db := newDB(t, "/tmp/db")
	if got, want := db.WALPath(), `/tmp/db-wal`; got != want {
		t.Fatalf("WALPath()=%v, want %v", got, want)
	}
}

func TestDB_MetaPath(t *testing.T) {
	t.Run("Absolute", func(t *testing.T) {
		db := newDB(t, "/tmp/db")
		if got, want := db.MetaPath(), `/tmp/.db-litestream`; got != want {
			t.Fatalf("MetaPath()=%v, want %v", got, want)
		}
	})
	t.Run("Relative", func(t *testing.T) {
		db := newDB(t, "db")
		if got, want := db.MetaPath(), `.db-litestream`; got != want {
			t.Fatalf("MetaPath()=%v, want %v", got, want)
		}
	})
}

// Ensure we can compute a checksum on the real database.
func TestDB_CRC64(t *testing.T) {
	t.Run("ErrNotExist", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)
		if _, _, err := db.CRC64(context.Background()); !os.IsNotExist(err) {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("DB", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		t.Log("sync database")

		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Log("compute crc64")

		chksum0, _, err := db.CRC64(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		t.Log("issue change")

		// Issue change that is applied to the WAL. Checksum should not change.
		if _, err := sqldb.Exec(`CREATE TABLE t (id INT);`); err != nil {
			t.Fatal(err)
		} else if chksum1, _, err := db.CRC64(context.Background()); err != nil {
			t.Fatal(err)
		} else if chksum0 == chksum1 {
			t.Fatal("expected different checksum event after WAL change")
		}

		t.Log("checkpointing database")

		// Checkpoint change into database. Checksum should change.
		if err := db.Checkpoint(context.Background(), litestream.CheckpointModeTruncate); err != nil {
			t.Fatal(err)
		}

		t.Log("compute crc64 again")

		if chksum2, _, err := db.CRC64(context.Background()); err != nil {
			t.Fatal(err)
		} else if chksum0 == chksum2 {
			t.Fatal("expected different checksums after checkpoint")
		}
	})
}

// Ensure we can sync the real WAL to the shadow WAL.
func TestDB_Sync(t *testing.T) {
	// Ensure sync is skipped if no database exists.
	t.Run("NoDB", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure sync can successfully run on the initial sync.
	t.Run("Initial", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Verify page size if now available.
		if db.PageSize() == 0 {
			t.Fatal("expected page size after initial sync")
		}

		// Obtain real WAL size.
		fi, err := os.Stat(db.WALPath())
		if err != nil {
			t.Fatal(err)
		} else if fi.Size() == 0 {
			t.Fatal("expected wal")
		}

		// Ensure position now available.
		if pos, err := db.Pos(); err != nil {
			t.Fatal(err)
		} else if got, want := pos.TXID, ltx.TXID(1); got != want {
			t.Fatalf("pos.Index=%v, want %v", got, want)
		}
	})

	// Ensure DB can keep in sync across multiple Sync() invocations.
	t.Run("MultiSync", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		// Execute a query to force a write to the WAL.
		if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
			t.Fatal(err)
		}

		// Perform initial sync & grab initial position.
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		pos0, err := db.Pos()
		if err != nil {
			t.Fatal(err)
		}

		// Insert into table.
		if _, err := sqldb.Exec(`INSERT INTO foo (bar) VALUES ('baz');`); err != nil {
			t.Fatal(err)
		}

		// Sync to ensure position moves forward one page.
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		} else if pos1, err := db.Pos(); err != nil {
			t.Fatal(err)
		} else if got, want := pos1.TXID, pos0.TXID+1; got != want {
			t.Fatalf("TXID=%v, want %v", got, want)
		}
	})

	// Ensure a WAL file is created if one does not already exist.
	t.Run("NoWAL", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		// Issue initial sync and truncate WAL.
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Obtain initial position.
		if _, err := db.Pos(); err != nil {
			t.Fatal(err)
		}

		// Checkpoint & fully close which should close WAL file.
		if err := db.Checkpoint(context.Background(), litestream.CheckpointModeTruncate); err != nil {
			t.Fatal(err)
		}

		if err := db.Close(context.Background()); err != nil {
			t.Fatal(err)
		} else if err := sqldb.Close(); err != nil {
			t.Fatal(err)
		}

		// Remove WAL file.
		if err := os.Remove(db.WALPath()); err != nil {
			t.Fatal(err)
		}

		// Reopen the managed database.
		db = MustOpenDBAt(t, db.Path())
		defer MustCloseDB(t, db)

		// Re-sync and ensure new generation has been created.
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Obtain initial position.
		if _, err := db.Pos(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure DB can start new generation if it detects it cannot verify last position.
	t.Run("OverwritePrevPosition", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		// Execute a query to force a write to the WAL.
		if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
			t.Fatal(err)
		}

		// Issue initial sync and truncate WAL.
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Obtain initial position.
		if _, err := db.Pos(); err != nil {
			t.Fatal(err)
		}

		// Fully close which should close WAL file.
		if err := db.Close(context.Background()); err != nil {
			t.Fatal(err)
		} else if err := sqldb.Close(); err != nil {
			t.Fatal(err)
		}

		// Verify WAL does not exist.
		if _, err := os.Stat(db.WALPath()); !os.IsNotExist(err) {
			t.Fatal(err)
		}

		// Insert into table multiple times to move past old offset
		sqldb = MustOpenSQLDB(t, db.Path())
		defer MustCloseSQLDB(t, sqldb)
		for i := 0; i < 100; i++ {
			if _, err := sqldb.Exec(`INSERT INTO foo (bar) VALUES ('baz');`); err != nil {
				t.Fatal(err)
			}
		}

		// Reopen the managed database.
		db = MustOpenDBAt(t, db.Path())
		defer MustCloseDB(t, db)

		// Re-sync and ensure new generation has been created.
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Obtain initial position.
		if _, err := db.Pos(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure DB checkpoints after minimum number of pages.
	t.Run("MinCheckpointPageN", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		// Execute a query to force a write to the WAL and then sync.
		if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
			t.Fatal(err)
		} else if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Write at least minimum number of pages to trigger rollover.
		for i := 0; i < db.MinCheckpointPageN; i++ {
			if _, err := sqldb.Exec(`INSERT INTO foo (bar) VALUES ('baz');`); err != nil {
				t.Fatal(err)
			}
		}

		// Sync to shadow WAL.
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Ensure position is now on the second index.
		if pos, err := db.Pos(); err != nil {
			t.Fatal(err)
		} else if got, want := pos.TXID, ltx.TXID(2); got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		}
	})

	// Ensure DB checkpoints after interval.
	t.Run("CheckpointInterval", func(t *testing.T) {
		t.Skip("TODO(ltx)")

		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		// Execute a query to force a write to the WAL and then sync.
		if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
			t.Fatal(err)
		} else if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Reduce checkpoint interval to ensure a rollover is triggered.
		db.CheckpointInterval = 1 * time.Nanosecond

		// Write to WAL & sync.
		if _, err := sqldb.Exec(`INSERT INTO foo (bar) VALUES ('baz');`); err != nil {
			t.Fatal(err)
		} else if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Ensure position is now on the second index.
		if pos, err := db.Pos(); err != nil {
			t.Fatal(err)
		} else if got, want := pos.TXID, ltx.TXID(1); got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		}
	})
}

func newDB(tb testing.TB, path string) *litestream.DB {
	tb.Helper()
	tb.Logf("db=%s", path)
	db := litestream.NewDB(path)
	db.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	return db
}

// MustOpenDBs returns a new instance of a DB & associated SQL DB.
func MustOpenDBs(tb testing.TB) (*litestream.DB, *sql.DB) {
	tb.Helper()
	db := MustOpenDB(tb)
	return db, MustOpenSQLDB(tb, db.Path())
}

// MustCloseDBs closes db & sqldb and removes the parent directory.
func MustCloseDBs(tb testing.TB, db *litestream.DB, sqldb *sql.DB) {
	tb.Helper()
	MustCloseDB(tb, db)
	MustCloseSQLDB(tb, sqldb)
}

// MustOpenDB returns a new instance of a DB.
func MustOpenDB(tb testing.TB) *litestream.DB {
	dir := tb.TempDir()
	return MustOpenDBAt(tb, filepath.Join(dir, "db"))
}

// MustOpenDBAt returns a new instance of a DB for a given path.
func MustOpenDBAt(tb testing.TB, path string) *litestream.DB {
	tb.Helper()
	db := newDB(tb, path)
	db.MonitorInterval = 0 // disable background goroutine
	if err := db.Open(); err != nil {
		tb.Fatal(err)
	}
	return db
}

// MustCloseDB closes db and removes its parent directory.
func MustCloseDB(tb testing.TB, db *litestream.DB) {
	tb.Helper()
	if err := db.Close(context.Background()); err != nil && !strings.Contains(err.Error(), `database is closed`) {
		tb.Fatal(err)
	} else if err := os.RemoveAll(filepath.Dir(db.Path())); err != nil {
		tb.Fatal(err)
	}
}

// MustOpenSQLDB returns a database/sql DB.
func MustOpenSQLDB(tb testing.TB, path string) *sql.DB {
	tb.Helper()
	d, err := sql.Open("sqlite3", path)
	if err != nil {
		tb.Fatal(err)
	} else if _, err := d.Exec(`PRAGMA journal_mode = wal;`); err != nil {
		tb.Fatal(err)
	}
	return d
}

// MustCloseSQLDB closes a database/sql DB.
func MustCloseSQLDB(tb testing.TB, d *sql.DB) {
	tb.Helper()
	if err := d.Close(); err != nil {
		tb.Fatal(err)
	}
}
