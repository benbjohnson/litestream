package litestream_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
)

func TestDB_Path(t *testing.T) {
	db := litestream.NewDB("/tmp/db")
	if got, want := db.Path(), `/tmp/db`; got != want {
		t.Fatalf("Path()=%v, want %v", got, want)
	}
}

func TestDB_WALPath(t *testing.T) {
	db := litestream.NewDB("/tmp/db")
	if got, want := db.WALPath(), `/tmp/db-wal`; got != want {
		t.Fatalf("WALPath()=%v, want %v", got, want)
	}
}

func TestDB_MetaPath(t *testing.T) {
	t.Run("Absolute", func(t *testing.T) {
		db := litestream.NewDB("/tmp/db")
		if got, want := db.MetaPath(), `/tmp/.db-litestream`; got != want {
			t.Fatalf("MetaPath()=%v, want %v", got, want)
		}
	})
	t.Run("Relative", func(t *testing.T) {
		db := litestream.NewDB("db")
		if got, want := db.MetaPath(), `.db-litestream`; got != want {
			t.Fatalf("MetaPath()=%v, want %v", got, want)
		}
	})
}

func TestDB_GenerationNamePath(t *testing.T) {
	db := litestream.NewDB("/tmp/db")
	if got, want := db.GenerationNamePath(), `/tmp/.db-litestream/generation`; got != want {
		t.Fatalf("GenerationNamePath()=%v, want %v", got, want)
	}
}

func TestDB_GenerationPath(t *testing.T) {
	db := litestream.NewDB("/tmp/db")
	if got, want := db.GenerationPath("xxxx"), `/tmp/.db-litestream/generations/xxxx`; got != want {
		t.Fatalf("GenerationPath()=%v, want %v", got, want)
	}
}

func TestDB_ShadowWALDir(t *testing.T) {
	db := litestream.NewDB("/tmp/db")
	if got, want := db.ShadowWALDir("xxxx"), `/tmp/.db-litestream/generations/xxxx/wal`; got != want {
		t.Fatalf("ShadowWALDir()=%v, want %v", got, want)
	}
}

func TestDB_ShadowWALPath(t *testing.T) {
	db := litestream.NewDB("/tmp/db")
	if got, want := db.ShadowWALPath("xxxx", 1000), `/tmp/.db-litestream/generations/xxxx/wal/00000000000003e8.wal`; got != want {
		t.Fatalf("ShadowWALPath()=%v, want %v", got, want)
	}
}

// Ensure we can check the last modified time of the real database and its WAL.
func TestDB_UpdatedAt(t *testing.T) {
	t.Run("ErrNotExist", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)
		if _, err := db.UpdatedAt(); !os.IsNotExist(err) {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("DB", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		if t0, err := db.UpdatedAt(); err != nil {
			t.Fatal(err)
		} else if time.Since(t0) > 10*time.Second {
			t.Fatalf("unexpected updated at time: %s", t0)
		}
	})

	t.Run("WAL", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		t0, err := db.UpdatedAt()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := sqldb.Exec(`CREATE TABLE t (id INT);`); err != nil {
			t.Fatal(err)
		}

		if t1, err := db.UpdatedAt(); err != nil {
			t.Fatal(err)
		} else if !t1.After(t0) {
			t.Fatalf("expected newer updated at time: %s > %s", t1, t0)
		}
	})
}

// Ensure we can compute a checksum on the real database.
func TestDB_CRC64(t *testing.T) {
	t.Run("ErrNotExist", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)
		if _, _, err := db.CRC64(); !os.IsNotExist(err) {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("DB", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		chksum0, _, err := db.CRC64()
		if err != nil {
			t.Fatal(err)
		}

		// Issue change that is applied to the WAL. Checksum should not change.
		if _, err := sqldb.Exec(`CREATE TABLE t (id INT);`); err != nil {
			t.Fatal(err)
		} else if chksum1, _, err := db.CRC64(); err != nil {
			t.Fatal(err)
		} else if chksum0 != chksum1 {
			t.Fatal("expected equal checksum after WAL change")
		}

		// Checkpoint change into database. Checksum should change.
		if _, err := sqldb.Exec(`PRAGMA wal_checkpoint(TRUNCATE);`); err != nil {
			t.Fatal(err)
		}

		if chksum2, _, err := db.CRC64(); err != nil {
			t.Fatal(err)
		} else if chksum0 == chksum2 {
			t.Fatal("expected different checksums after checkpoint")
		}
	})
}

// MustOpenDBs returns a new instance of a DB & associated SQL DB.
func MustOpenDBs(tb testing.TB) (*litestream.DB, *sql.DB) {
	db := MustOpenDB(tb)
	return db, MustOpenSQLDB(tb, db.Path())
}

// MustCloseDBs closes db & sqldb and removes the parent directory.
func MustCloseDBs(tb testing.TB, db *litestream.DB, sqldb *sql.DB) {
	MustCloseDB(tb, db)
	MustCloseSQLDB(tb, sqldb)
}

// MustOpenDB returns a new instance of a DB.
func MustOpenDB(tb testing.TB) *litestream.DB {
	tb.Helper()

	dir := tb.TempDir()
	db := litestream.NewDB(filepath.Join(dir, "db"))
	db.MonitorInterval = 0 // disable background goroutine
	if err := db.Open(); err != nil {
		tb.Fatal(err)
	}
	return db
}

// MustCloseDB closes db and removes its parent directory.
func MustCloseDB(tb testing.TB, db *litestream.DB) {
	tb.Helper()
	if err := db.Close(); err != nil {
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
