package library_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

func TestLibraryExampleFileBackend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rootDir := t.TempDir()
	dbPath := filepath.Join(rootDir, "example.db")
	replicaPath := filepath.Join(rootDir, "replica")

	db := litestream.NewDB(dbPath)
	client := file.NewReplicaClient(replicaPath)
	replica := litestream.NewReplicaWithClient(db, client)
	db.Replica = replica
	client.Replica = replica

	levels := litestream.CompactionLevels{
		{Level: 0},
		{Level: 1, Interval: 10 * time.Second},
	}
	store := litestream.NewStore([]*litestream.DB{db}, levels)

	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = store.Close(context.Background())
		}
	})

	if err := store.Open(ctx); err != nil {
		t.Fatalf("open store: %v", err)
	}

	sqlDB, err := openAppDB(ctx, dbPath)
	if err != nil {
		t.Fatalf("open app db: %v", err)
	}
	defer sqlDB.Close()

	if _, err := sqlDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			message TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if _, err := sqlDB.ExecContext(ctx, `INSERT INTO events (message) VALUES ('hello');`); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	if err := waitForLTXFiles(replicaPath, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close app db: %v", err)
	}

	if err := store.Close(ctx); err != nil && !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("close store: %v", err)
	}
	closed = true

	restoreClient := file.NewReplicaClient(replicaPath)
	restoreReplica := litestream.NewReplicaWithClient(nil, restoreClient)

	restorePath := filepath.Join(rootDir, "restored.db")
	opt := litestream.NewRestoreOptions()
	opt.OutputPath = restorePath
	if err := restoreReplica.Restore(ctx, opt); err != nil {
		t.Fatalf("restore: %v", err)
	}
}

func openAppDB(ctx context.Context, path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode = wal;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `PRAGMA busy_timeout = 5000;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func waitForLTXFiles(replicaPath string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		matches, err := filepath.Glob(filepath.Join(replicaPath, "ltx", "0", "*.ltx"))
		if err != nil {
			return fmt.Errorf("glob ltx files: %w", err)
		}
		if len(matches) > 0 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for ltx files in %s", replicaPath)
}
