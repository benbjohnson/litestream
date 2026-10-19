package litestream_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// TestDB_WriterKilledBeforeWALIndexUpdate simulates a writer killed with
// SIGKILL after it wrote and synced a commit's WAL frames but before it
// updated the wal-index header in the -shm file. SQLite treats those frames
// as uncommitted: Litestream's open connection keeps the wal-index valid, so
// no WAL recovery runs and the next writer reuses those frame slots.
// Putting back the wal-index header from before the commit leaves the same
// state. Litestream must not replicate the killed writer's transaction.
func TestDB_WriterKilledBeforeWALIndexUpdate(t *testing.T) {
	testDBWriterKilledBeforeWALIndexUpdate(t, "")
}

func TestDB_WriterKilledBeforeWALIndexUpdate_UnusableHeader(t *testing.T) {
	for _, name := range []string{"copies differ", "uninitialized"} {
		t.Run(name, func(t *testing.T) {
			testDBWriterKilledBeforeWALIndexUpdate(t, name)
		})
	}
}

func testDBWriterKilledBeforeWALIndexUpdate(t *testing.T, unusableHeader string) {
	t.Helper()
	ctx := t.Context()
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	} else if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (v) VALUES ('committed')`); err != nil {
		t.Fatal(err)
	} else if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Close it only after the connections: closing a descriptor drops this
	// process's non-OFD locks on the -shm file, which they rely on.
	shm, err := os.OpenFile(db.Path()+"-shm", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shm.Close() })
	hdr := make([]byte, 96) // both copies of the wal-index header
	if _, err := shm.ReadAt(hdr, 0); err != nil {
		t.Fatal(err)
	} else if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (v) VALUES ('killed')`); err != nil {
		t.Fatal(err)
	} else if _, err := shm.WriteAt(hdr, 0); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := sqldb.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("database has %d rows after the simulated kill, want 1", n)
	}

	if unusableHeader != "" {
		pos, err := db.Pos()
		if err != nil {
			t.Fatal(err)
		}
		unstable := append([]byte(nil), hdr...)
		switch unusableHeader {
		case "copies differ":
			unstable[56] ^= 1
		case "uninitialized":
			unstable[12], unstable[60] = 0, 0
		}
		if _, err := shm.WriteAt(unstable, 0); err != nil {
			t.Fatal(err)
		}
		syncErr := db.Sync(ctx)
		if _, err := shm.WriteAt(hdr, 0); err != nil {
			t.Fatal(err)
		}
		if syncErr == nil {
			t.Error("expected sync to reject unusable wal-index header")
		}
		if got, err := db.Pos(); err != nil {
			t.Fatal(err)
		} else if got != pos {
			t.Errorf("position advanced with unusable wal-index header: got %v, want %v", got, pos)
		}
	}

	// Litestream's next checkpoint copies the WAL, checkpoints and restarts it.
	if err := db.Checkpoint(ctx, litestream.CheckpointModePassive); err != nil {
		t.Fatal(err)
	} else if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	restorePath := filepath.Join(t.TempDir(), "restored.db")
	if err := db.Replica.Restore(ctx, litestream.RestoreOptions{OutputPath: restorePath}); err != nil {
		t.Fatal(err)
	}
	restored := testingutil.MustOpenSQLDB(t, restorePath)
	defer testingutil.MustCloseSQLDB(t, restored)
	if err := restored.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("restored %d rows, want 1: the killed writer's uncommitted transaction was replicated", n)
	}
}
