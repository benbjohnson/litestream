package litestream_test

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// After a power loss under synchronous=NORMAL, the WAL frames Litestream
// already shipped may not exist in SQLite's recovered WAL. verify() checks
// continuity with the single frame before its offset, so these rewinds go
// undetected and the replica diverges from the database with no error.
func TestDB_WALContinuityAfterPowerLoss(t *testing.T) {
	// A clean restart preserves the old incremental path when the saved WAL
	// checksum still matches the frames on disk.
	t.Run("CleanRestart", func(t *testing.T) {
		c := setupPowerLoss(t)
		db := openContinuityDB(t, c)
		sqldb := testingutil.MustOpenSQLDB(t, c.path)
		defer testingutil.MustCloseSQLDB(t, sqldb)

		before, err := db.Pos()
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		after, err := db.Pos()
		if err != nil {
			t.Fatal(err)
		}
		if after.TXID != before.TXID {
			t.Fatalf("unexpected sync after clean restart: txid advanced from %s to %s", before.TXID, after.TXID)
		}
		assertReplicaMatches(t, db, sqldb)
	})

	// Databases created before continuity checks have no saved checksum. They
	// must establish a fresh snapshot instead of trusting the old single-frame
	// check.
	t.Run("MissingChecksumResnapshots", func(t *testing.T) {
		c := setupPowerLoss(t)
		checksumPath := filepath.Join(litestream.NewDB(c.path).LTXDir(), "wal-checksum.json")
		if err := os.Remove(checksumPath); err != nil {
			t.Fatal(err)
		}

		db := openContinuityDB(t, c)
		sqldb := testingutil.MustOpenSQLDB(t, c.path)
		defer testingutil.MustCloseSQLDB(t, sqldb)
		before, err := db.Pos()
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		after, err := db.Pos()
		if err != nil {
			t.Fatal(err)
		}
		if after.TXID != before.TXID+1 {
			t.Fatalf("expected one resnapshot transaction after missing checksum, txid advanced from %s to %s", before.TXID, after.TXID)
		}
		assertReplicaMatches(t, db, sqldb)
	})

	t.Run("InvalidChecksumResnapshots", func(t *testing.T) {
		c := setupPowerLoss(t)
		checksumPath := filepath.Join(litestream.NewDB(c.path).LTXDir(), "wal-checksum.json")
		if err := os.WriteFile(checksumPath, []byte("{"), 0o600); err != nil {
			t.Fatal(err)
		}

		db := openContinuityDB(t, c)
		sqldb := testingutil.MustOpenSQLDB(t, c.path)
		defer testingutil.MustCloseSQLDB(t, sqldb)
		before, err := db.Pos()
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		after, err := db.Pos()
		if err != nil {
			t.Fatal(err)
		}
		if after.TXID != before.TXID+1 {
			t.Fatalf("expected one resnapshot transaction after invalid checksum, txid advanced from %s to %s", before.TXID, after.TXID)
		}
		assertReplicaMatches(t, db, sqldb)
	})

	// The WAL tail is lost and the app rewrites it before Litestream syncs.
	// The new last frame carries the same page image; only its cumulative
	// checksum differs, and verify() doesn't compare checksums.
	t.Run("IdenticalLastPage", func(t *testing.T) {
		c := setupPowerLoss(t)
		lastFrame := readLastFrame(t, c)
		if err := os.Truncate(c.path+"-wal", c.durableWALSize); err != nil {
			t.Fatal(err)
		}

		sqldb := testingutil.MustOpenSQLDB(t, c.path)
		defer testingutil.MustCloseSQLDB(t, sqldb)
		execContinuity(t, sqldb, `UPDATE t SET v = ? WHERE id = 100`, "kept")
		execContinuity(t, sqldb, `UPDATE t SET v = ? WHERE id = 10`, "same")
		if got := readLastFrame(t, c); !bytes.Equal(got[:8], lastFrame[:8]) ||
			!bytes.Equal(got[litestream.WALFrameHeaderSize:], lastFrame[litestream.WALFrameHeaderSize:]) {
			t.Fatal("rewritten last frame has a different page; the case was not reproduced")
		}

		db := openContinuityDB(t, c)
		assertReplicaMatches(t, db, sqldb)
	})

	// Writeback reached disk out of order: an earlier lost frame is garbage
	// but the frame before Litestream's offset survived, so SQLite's recovery
	// stops at the bad frame. Litestream syncs before any app write and
	// still sees nothing wrong.
	t.Run("OutOfOrderFlush", func(t *testing.T) {
		c := setupPowerLoss(t)
		f, err := os.OpenFile(c.path+"-wal", os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(bytes.Repeat([]byte{0xff}, 512), c.durableWALSize+litestream.WALFrameHeaderSize); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		db := openContinuityDB(t, c)
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		sqldb := testingutil.MustOpenSQLDB(t, c.path)
		defer testingutil.MustCloseSQLDB(t, sqldb)
		execContinuity(t, sqldb, `UPDATE t SET v = ? WHERE id = 50`, "kept")
		assertReplicaMatches(t, db, sqldb)
	})
}

type powerLoss struct {
	path, replicaDir            string
	durableWALSize, staleOffset int64
}

// setupPowerLoss ships two commits (rows 100 and 10) past the durable WAL
// length, then returns a copy of the data dir as a crashed host would leave
// it, before the caller models what reached disk of the WAL tail.
func setupPowerLoss(t *testing.T) powerLoss {
	t.Helper()
	ctx := t.Context()
	dir := t.TempDir()
	c := powerLoss{path: filepath.Join(t.TempDir(), "db"), replicaDir: t.TempDir()}

	sqldb := testingutil.MustOpenSQLDB(t, filepath.Join(dir, "db"))
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		execContinuity(t, sqldb, `INSERT INTO t (v, id) VALUES (?, ?)`, "v0", i)
	}
	db := openContinuityDB(t, powerLoss{path: filepath.Join(dir, "db"), replicaDir: c.replicaDir})
	sync := func() {
		t.Helper()
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	sync()
	if err := db.Checkpoint(ctx, litestream.CheckpointModeTruncate); err != nil {
		t.Fatal(err)
	}
	execContinuity(t, sqldb, `UPDATE t SET v = ? WHERE id = 0`, "durable")
	sync()
	c.durableWALSize = continuityFileSize(t, db.WALPath())

	execContinuity(t, sqldb, `UPDATE t SET v = ? WHERE id = 100`, "lost")
	execContinuity(t, sqldb, `UPDATE t SET v = ? WHERE id = 10`, "same")
	sync()
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	c.staleOffset = continuityFileSize(t, db.WALPath())

	// Copy before closing: the last close checkpoints and removes the WAL.
	if err := os.CopyFS(filepath.Dir(c.path), os.DirFS(dir)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(c.path + "-shm"); err != nil {
		t.Fatal(err)
	}
	testingutil.MustCloseSQLDB(t, sqldb)
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return c
}

func assertReplicaMatches(t *testing.T, db *litestream.DB, sqldb *sql.DB) {
	t.Helper()
	ctx := t.Context()
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	restorePath := filepath.Join(t.TempDir(), "restored.db")
	if err := db.Replica.Restore(ctx, litestream.RestoreOptions{OutputPath: restorePath}); err != nil {
		t.Fatal(err)
	}
	restored := testingutil.MustOpenSQLDB(t, restorePath)
	defer testingutil.MustCloseSQLDB(t, restored)

	const query = `SELECT substr(v, 1, instr(v, '|') - 1) FROM t WHERE id = ?`
	for _, id := range []int{0, 10, 50, 100} {
		var want, got string
		if err := sqldb.QueryRowContext(ctx, query, id).Scan(&want); err != nil {
			t.Fatal(err)
		}
		if err := restored.QueryRowContext(ctx, query, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("row %d: restored %q, database has %q", id, got, want)
		}
	}
}

func openContinuityDB(t *testing.T, c powerLoss) *litestream.DB {
	t.Helper()
	db := testingutil.NewDB(t, c.path)
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = file.NewReplicaClient(c.replicaDir)
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(t.Context()) })
	return db
}

// execContinuity binds label, padded to a third of a page, as the first
// parameter, so an update to one row rewrites only that row's page.
func execContinuity(t *testing.T, sqldb *sql.DB, query, label string, args ...any) {
	t.Helper()
	args = append([]any{label + "|" + strings.Repeat("x", 1200)}, args...)
	if _, err := sqldb.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatal(err)
	}
}

// readLastFrame returns the frame that verify() checks: the one just before
// the stale offset.
func readLastFrame(t *testing.T, c powerLoss) []byte {
	t.Helper()
	b, err := os.ReadFile(c.path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	frameSize := litestream.WALFrameHeaderSize + int64(binary.BigEndian.Uint32(b[8:12]))
	return b[c.staleOffset-frameSize : c.staleOffset]
}

func continuityFileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}
