package litestream_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// TestDB_SnapshotAfterWALRewind simulates a power loss under
// synchronous=NORMAL: Litestream has already shipped WAL frames that never
// reached disk, SQLite recovers the shorter WAL, and the app appends new
// commits over the lost frames with the same salt before Litestream's first
// sync. verify() then fails its last-page check and requests a snapshot,
// which must hold every committed frame in the WAL, not only those past the
// last synced offset.
func TestDB_SnapshotAfterWALRewind(t *testing.T) {
	for _, tt := range []struct {
		name string
		// Grows the database before the offset, so an unfixed snapshot's
		// commit size exceeds the database file and every sync fails with
		// "read database page N: EOF" rather than silently losing frames.
		durableGrowth bool
	}{
		{name: "SilentLoss"},
		{name: "GrowthBeforeOffset", durableGrowth: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			exec := func(sqldb *sql.DB, query string, args ...any) {
				t.Helper()
				if _, err := sqldb.ExecContext(ctx, query, args...); err != nil {
					t.Fatal(err)
				}
			}
			sync := func(db *litestream.DB) {
				t.Helper()
				if err := db.Sync(ctx); err != nil {
					t.Fatal(err)
				}
			}
			value := func(prefix string, i int) string {
				return fmt.Sprintf("%s-%d-%s", prefix, i, strings.Repeat("x", 1200))
			}

			dir, replicaDir := t.TempDir(), t.TempDir()
			path := filepath.Join(dir, "db")
			sqldb := testingutil.MustOpenSQLDB(t, path)
			exec(sqldb, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
			for i := range 200 {
				exec(sqldb, `INSERT INTO t (id, v) VALUES (?, ?)`, i, value("v0", i))
			}

			// Checkpoint after the first sync, which creates Litestream's own
			// tables, so every page so far is in the database file.
			db := openDBWithReplicaDir(t, path, replicaDir)
			sync(db)
			if err := db.Checkpoint(ctx, litestream.CheckpointModeTruncate); err != nil {
				t.Fatal(err)
			}
			for i := range 5 {
				exec(sqldb, `UPDATE t SET v = ? WHERE id = ?`, value("v1", i), i)
			}
			if tt.durableGrowth {
				for i := range 20 {
					exec(sqldb, `INSERT INTO t (id, v) VALUES (?, ?)`, 1000+i, value("v1", i))
				}
			}
			sync(db)
			durableWALSize := fileSize(t, db.WALPath())

			// Shipped by Litestream, then lost in the power cut.
			for i := range 10 {
				exec(sqldb, `UPDATE t SET v = ? WHERE id = ?`, value("v2", i), 100+i)
			}
			sync(db)
			if err := db.Replica.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			staleOffset := fileSize(t, db.WALPath())
			salt := walSalt(t, db.WALPath())

			// Copy before closing: the last close checkpoints and removes the WAL.
			crashDir := t.TempDir()
			if err := os.CopyFS(crashDir, os.DirFS(dir)); err != nil {
				t.Fatal(err)
			}
			crashPath := filepath.Join(crashDir, "db")
			if err := os.Truncate(crashPath+"-wal", durableWALSize); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(crashPath + "-shm"); err != nil {
				t.Fatal(err)
			}
			testingutil.MustCloseSQLDB(t, sqldb)
			if err := db.Close(ctx); err != nil {
				t.Fatal(err)
			}

			// Restart: the app writes over the lost frames before Litestream syncs.
			sqldb = testingutil.MustOpenSQLDB(t, crashPath)
			defer testingutil.MustCloseSQLDB(t, sqldb)
			for i := 0; fileSize(t, crashPath+"-wal") < staleOffset; i++ {
				exec(sqldb, `UPDATE t SET v = ? WHERE id = ?`, value("v3", i), 150+i%20)
			}
			if size := fileSize(t, crashPath+"-wal"); size != staleOffset {
				t.Fatalf("post-crash wal size=%d, want stale offset %d", size, staleOffset)
			}
			if !bytes.Equal(walSalt(t, crashPath+"-wal"), salt) {
				t.Fatal("wal salt changed after recovery; the rewind was not reproduced")
			}
			for i := range 3 {
				exec(sqldb, `UPDATE t SET v = ? WHERE id = ?`, value("v4", i), 180+i)
			}

			db = openDBWithReplicaDir(t, crashPath, replicaDir)
			defer func() {
				if err := db.Close(ctx); err != nil {
					t.Error(err)
				}
			}()
			if err := db.Sync(ctx); err != nil {
				t.Fatalf("sync after restart: %v", err)
			}

			// An idle sync must accept the snapshot's offset rather than
			// snapshot again, and a later write must chain onto it.
			pos, err := db.Pos()
			if err != nil {
				t.Fatal(err)
			}
			sync(db)
			if got, err := db.Pos(); err != nil {
				t.Fatal(err)
			} else if got != pos {
				t.Fatalf("idle sync moved position %s -> %s", pos, got)
			}
			exec(sqldb, `UPDATE t SET v = ? WHERE id = ?`, value("v5", 0), 50)
			sync(db)
			if err := db.Replica.Sync(ctx); err != nil {
				t.Fatal(err)
			}

			restorePath := filepath.Join(t.TempDir(), "restored.db")
			if err := db.Replica.Restore(ctx, litestream.RestoreOptions{OutputPath: restorePath}); err != nil {
				t.Fatal(err)
			}
			restored := testingutil.MustOpenSQLDB(t, restorePath)
			defer testingutil.MustCloseSQLDB(t, restored)
			var integrity string
			if err := restored.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&integrity); err != nil {
				t.Fatal(err)
			} else if integrity != "ok" {
				t.Fatalf("integrity_check: %s", integrity)
			}
			const query = `SELECT id, substr(v, 1, instr(v, '-x') - 1) FROM t ORDER BY id`
			want, got := queryRows(t, sqldb, query), queryRows(t, restored, query)
			for i := range min(len(got), len(want)) {
				if got[i] != want[i] {
					t.Fatalf("restored row %q, want %q", got[i], want[i])
				}
			}
			if len(got) != len(want) {
				t.Fatalf("restored %d rows, want %d", len(got), len(want))
			}
		})
	}
}

func openDBWithReplicaDir(tb testing.TB, path, replicaDir string) *litestream.DB {
	tb.Helper()
	db := testingutil.NewDB(tb, path)
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = file.NewReplicaClient(replicaDir)
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		tb.Fatal(err)
	}
	return db
}

func queryRows(tb testing.TB, sqldb *sql.DB, query string) []string {
	tb.Helper()
	rows, err := sqldb.QueryContext(tb.Context(), query)
	if err != nil {
		tb.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id int
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			tb.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%d:%s", id, v))
	}
	if err := rows.Err(); err != nil {
		tb.Fatal(err)
	}
	return out
}

func fileSize(tb testing.TB, path string) int64 {
	tb.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		tb.Fatal(err)
	}
	return fi.Size()
}

func walSalt(tb testing.TB, path string) []byte {
	tb.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	} else if len(b) < litestream.WALHeaderSize {
		tb.Fatalf("wal too short: %d bytes", len(b))
	}
	return b[16:24]
}
