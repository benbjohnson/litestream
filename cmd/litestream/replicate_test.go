package main_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	main "github.com/benbjohnson/litestream/cmd/litestream"
	"golang.org/x/sync/errgroup"
)

func TestReplicateCommand(t *testing.T) {
	if testing.Short() {
		t.Skip("long running test, skipping")
	} else if runtime.GOOS != "linux" {
		t.Skip("must run system tests on Linux, skipping")
	}

	const writeTime = 10 * time.Second

	dir := t.TempDir()
	configPath := filepath.Join(dir, "litestream.yml")
	dbPath := filepath.Join(dir, "db")
	restorePath := filepath.Join(dir, "restored")
	replicaPath := filepath.Join(dir, "replica")

	if err := os.WriteFile(configPath, []byte(`
dbs:
  - path: `+dbPath+`
    replicas:
      - path: `+replicaPath+`
`), 0666); err != nil {
		t.Fatal(err)
	}

	// Generate data into SQLite database from separate goroutine.
	g, ctx := errgroup.WithContext(context.Background())
	mainctx, cancel := context.WithCancel(ctx)
	g.Go(func() error {
		defer cancel()

		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		if _, err := db.ExecContext(ctx, `PRAGMA journal_mode = WAL`); err != nil {
			return fmt.Errorf("cannot enable wal: %w", err)
		} else if _, err := db.ExecContext(ctx, `PRAGMA synchronous = NORMAL`); err != nil {
			return fmt.Errorf("cannot enable wal: %w", err)
		} else if _, err := db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
			return fmt.Errorf("cannot create table: %w", err)
		}

		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()
		timer := time.NewTimer(writeTime)
		defer timer.Stop()

		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			case <-ticker.C:
				if _, err := db.ExecContext(ctx, `INSERT INTO t (id) VALUES (?);`, i); err != nil {
					return fmt.Errorf("cannot insert: i=%d err=%w", i, err)
				}
			}
		}
	})

	// Replicate database unless the context is canceled.
	g.Go(func() error {
		return main.NewMain().Run(mainctx, []string{"replicate", "-config", configPath})
	})

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}

	// Checkpoint database.
	mustCheckpoint(t, dbPath)
	chksum0 := mustChecksum(t, dbPath)

	// Restore to another path.
	if err := main.NewMain().Run(context.Background(), []string{"restore", "-config", configPath, "-o", restorePath, dbPath}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// Verify contents match.
	if chksum1 := mustChecksum(t, restorePath); chksum0 != chksum1 {
		t.Fatal("restore mismatch")
	}
}

func mustCheckpoint(tb testing.TB, path string) {
	tb.Helper()

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		tb.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		tb.Fatal(err)
	}
}

func mustChecksum(tb testing.TB, path string) uint64 {
	tb.Helper()

	f, err := os.Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()

	h := crc64.New(crc64.MakeTable(crc64.ISO))
	if _, err := io.Copy(h, f); err != nil {
		tb.Fatal(err)
	}
	return h.Sum64()
}
