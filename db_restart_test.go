package litestream_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/benbjohnson/litestream"
)

func TestDB_CloseResetsStateForRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db")

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sql db: %v", err)
	}
	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal;`); err != nil {
		_ = sqldb.Close()
		t.Fatalf("set wal: %v", err)
	}
	if _, err := sqldb.Exec(`CREATE TABLE t (id INTEGER);`); err != nil {
		_ = sqldb.Close()
		t.Fatalf("create table: %v", err)
	}
	if err := sqldb.Close(); err != nil {
		t.Fatalf("close sql db: %v", err)
	}

	db := litestream.NewDB(dbPath)
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0

	if err := db.Open(); err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Sync(context.Background()); err != nil {
		t.Fatalf("sync db: %v", err)
	}
	if !db.IsOpen() {
		t.Fatal("expected db to be open after sync")
	}

	if err := db.Close(context.Background()); err != nil {
		t.Fatalf("close db: %v", err)
	}
	if db.IsOpen() {
		t.Fatal("expected db to be closed after close")
	}

	if err := db.Open(); err != nil {
		t.Fatalf("reopen db: %v", err)
	}
}
