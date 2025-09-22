package litestream_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

// openTestDB initializes a Litestream DB with a file replica rooted at replicaPath.
// A corresponding *sql.DB (configured for WAL) is returned for issuing SQL writes.
func openTestDB(tb testing.TB, dbPath, replicaPath string) (*litestream.DB, *sql.DB) {
	tb.Helper()

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		tb.Fatalf("mkdir %s: %v", filepath.Dir(dbPath), err)
	}

	db := litestream.NewDB(dbPath)
	db.Logger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	db.MonitorInterval = 0
	db.Replica = litestream.NewReplica(db)
	client := file.NewReplicaClient(replicaPath)
	db.Replica.Client = client
	db.Replica.MonitorEnabled = false

	if err := db.Open(); err != nil {
		tb.Fatalf("open litestream db: %v", err)
	}

	client.Replica = db.Replica

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		tb.Fatalf("open sql db: %v", err)
	}
	if _, err := sqldb.ExecContext(context.Background(), `PRAGMA journal_mode = wal;`); err != nil {
		tb.Fatalf("set journal_mode=wal: %v", err)
	}

	return db, sqldb
}

// TestRestoreFailsAfterFullCheckpointWhileDown codifies bug #752 by asserting
// that the current restore path fails once the real WAL is truncated while
// Litestream is offline. The test is skipped so it can serve as a focused
// regression harness until the bug is fixed.
func TestRestoreFailsAfterFullCheckpointWhileDown(t *testing.T) {

	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.sqlite")
	replicaPath := filepath.Join(dir, "replica")

	db, sqldb := openTestDB(t, dbPath, replicaPath)
	defer func() { _ = db.Close(ctx) }()
	defer func() { _ = sqldb.Close() }()

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB);`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 128; i++ {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO t(data) VALUES (randomblob(1024));`); err != nil {
			t.Fatalf("prime insert %d: %v", i, err)
		}
	}

	if err := db.Sync(ctx); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatalf("initial replica sync: %v", err)
	}

	if err := db.Close(ctx); err != nil {
		t.Fatalf("close before downtime: %v", err)
	}

	for i := 0; i < 64; i++ {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO t(data) VALUES (randomblob(1024));`); err != nil {
			t.Fatalf("post-downtime insert %d: %v", i, err)
		}
	}

	var wantRows int
	if err := sqldb.QueryRowContext(ctx, `SELECT COUNT(*) FROM t;`).Scan(&wantRows); err != nil {
		t.Fatalf("count source rows: %v", err)
	}

	if _, err := sqldb.ExecContext(ctx, `PRAGMA wal_checkpoint(FULL);`); err != nil {
		t.Fatalf("checkpoint FULL: %v", err)
	}

	if err := sqldb.Close(); err != nil {
		t.Fatalf("close sql db before restart: %v", err)
	}

	db2, sqldb2 := openTestDB(t, dbPath, replicaPath)
	defer func() { _ = db2.Close(ctx) }()
	defer func() { _ = sqldb2.Close() }()

	if err := db2.Sync(ctx); err != nil {
		t.Fatalf("sync after restart: %v", err)
	}
	if err := db2.Replica.Sync(ctx); err != nil {
		t.Fatalf("replica sync after restart: %v", err)
	}

	if plan, err := litestream.CalcRestorePlan(ctx, db2.Replica.Client, 0, time.Time{}, db2.Logger); err == nil {
		for i, info := range plan {
			t.Logf("restore plan[%d]: level=%d min=%s max=%s size=%d", i, info.Level, info.MinTXID, info.MaxTXID, info.Size)
		}
	}

	restorePath := filepath.Join(dir, "restore.sqlite")
	if err := db2.Replica.Restore(ctx, litestream.RestoreOptions{OutputPath: restorePath}); err != nil {
		t.Fatalf("restore returned error: %v", err)
	}

	restoredDB, err := sql.Open("sqlite", restorePath)
	if err != nil {
		t.Fatalf("open restored db: %v", err)
	}
	defer restoredDB.Close()

	var gotRows int
	if err := restoredDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM t;`).Scan(&gotRows); err != nil {
		t.Fatalf("count restored rows: %v", err)
	}

	if gotRows != wantRows {
		t.Fatalf("restored row count mismatch: got %d want %d", gotRows, wantRows)
	}
}

// TestRestoreLosesRowsAfterAutoCheckpointWhileDown captures the row-loss mode
// where SQLite's automatic checkpoint runs while Litestream is offline. The
// restored database currently comes back with fewer rows even though restore
// reports success. Skipped until bug #752 is addressed.
func TestRestoreLosesRowsAfterAutoCheckpointWhileDown(t *testing.T) {

	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.sqlite")
	replicaPath := filepath.Join(dir, "replica")

	db, sqldb := openTestDB(t, dbPath, replicaPath)
	defer func() { _ = db.Close(ctx) }()
	defer func() { _ = sqldb.Close() }()

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB);`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	for i := 0; i < 64; i++ {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO t(data) VALUES (randomblob(2048));`); err != nil {
			t.Fatalf("prime insert %d: %v", i, err)
		}
	}

	if err := db.Sync(ctx); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatalf("initial replica sync: %v", err)
	}

	if _, err := sqldb.ExecContext(ctx, `PRAGMA wal_autocheckpoint = 1;`); err != nil {
		t.Fatalf("set wal_autocheckpoint: %v", err)
	}

	if err := db.Close(ctx); err != nil {
		t.Fatalf("close before downtime: %v", err)
	}

	for i := 0; i < 512; i++ {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO t(data) VALUES (randomblob(4096));`); err != nil {
			t.Fatalf("downtime insert %d: %v", i, err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 128; i++ {
		if _, err := sqldb.ExecContext(ctx, `INSERT INTO t(data) VALUES (randomblob(512));`); err != nil {
			t.Fatalf("post-checkpoint insert %d: %v", i, err)
		}
	}

	var wantRows int
	if err := sqldb.QueryRowContext(ctx, `SELECT COUNT(*) FROM t;`).Scan(&wantRows); err != nil {
		t.Fatalf("count source rows: %v", err)
	}

	if err := sqldb.Close(); err != nil {
		t.Fatalf("close sql db before restart: %v", err)
	}

	db2, sqldb2 := openTestDB(t, dbPath, replicaPath)
	defer func() { _ = db2.Close(ctx) }()
	defer func() { _ = sqldb2.Close() }()

	if err := db2.Sync(ctx); err != nil {
		t.Fatalf("sync after restart: %v", err)
	}
	if err := db2.Replica.Sync(ctx); err != nil {
		t.Fatalf("replica sync after restart: %v", err)
	}

	if plan, err := litestream.CalcRestorePlan(ctx, db2.Replica.Client, 0, time.Time{}, db2.Logger); err == nil {
		for i, info := range plan {
			t.Logf("restore plan[%d]: level=%d min=%s max=%s size=%d", i, info.Level, info.MinTXID, info.MaxTXID, info.Size)
		}
	}

	restorePath := filepath.Join(dir, "restore.sqlite")
	if err := db2.Replica.Restore(ctx, litestream.RestoreOptions{OutputPath: restorePath}); err != nil {
		t.Fatalf("restore returned error: %v", err)
	}

	restoredDB, err := sql.Open("sqlite", restorePath)
	if err != nil {
		t.Fatalf("open restored db: %v", err)
	}
	defer restoredDB.Close()

	var gotRows int
	if err := restoredDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM t;`).Scan(&gotRows); err != nil {
		t.Fatalf("count restored rows: %v", err)
	}

	if gotRows != wantRows {
		t.Fatalf("restored row count mismatch: got %d want %d", gotRows, wantRows)
	}
}
