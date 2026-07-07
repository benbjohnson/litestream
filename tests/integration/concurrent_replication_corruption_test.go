//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestConcurrentWriteReplicationCorruption reproduces a silent data-loss bug in
// the capture path.
//
// Under a concurrent writer and frequent checkpoints, a checkpoint can flush WAL
// frames into the database file before litestream captures them into an LTX file.
// Those page writes never reach the replica, so the replica's LTX chain claims a
// database state it cannot actually reconstruct. The backup becomes permanently
// unrestorable even though the live database passes integrity_check.
//
// No crash, restart, or restore/replicate loop is required - the corruption is
// baked into the replica during ordinary continuous replication. This asserts the
// core backup invariant: a replica of a healthy database must always restore to a
// consistent database.
//
// It FAILS on the buggy code (reproduces within a few seconds) and should PASS
// once the capture/checkpoint boundary is fixed.
func TestConcurrentWriteReplicationCorruption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	RequireBinaries(t)

	db := SetupTestDB(t, "replication-corruption")
	defer db.Cleanup()

	if err := db.Create(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// TEXT primary key: SQLite builds a unique auto-index whose btree overflows and
	// rebalances as rows grow. That rebalance is the multi-page transaction whose
	// pages get dropped at the checkpoint/capture boundary.
	if _, err := execSQL(db.Path,
		`CREATE TABLE editor_objects (id TEXT PRIMARY KEY NOT NULL, payload TEXT NOT NULL)`,
		`INSERT INTO editor_objects VALUES ('seed','x')`,
	); err != nil {
		t.Fatalf("Failed to seed schema: %v", err)
	}

	// Aggressive checkpointing so the WAL is recycled constantly, maximizing the
	// number of checkpoint/capture boundaries the race can land on.
	configPath := filepath.Join(db.TempDir, "litestream.yml")
	cfg := fmt.Sprintf(`dbs:
  - path: %s
    monitor-interval: 20ms
    checkpoint-interval: 40ms
    min-checkpoint-page-count: 8
    replicas:
      - type: file
        path: %s
`, filepath.ToSlash(db.Path), filepath.ToSlash(db.ReplicaPath))
	if err := os.WriteFile(configPath, []byte(cfg), 0o644); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	t.Log("[1] Starting Litestream...")
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("Failed to start Litestream: %v", err)
	}

	t.Log("[2] Writing concurrently while the replica is periodically restored...")
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); writeCorruptionRows(ctx, db.Path) }()

	// A healthy database's replica must always restore to a consistent database.
	// Restore it repeatedly during replication; the first restore that fails
	// integrity_check (or fails to decode) is the reproduced corruption.
	var corruption error
	deadline := time.Now().Add(30 * time.Second)
	for i := 0; corruption == nil && time.Now().Before(deadline); i++ {
		time.Sleep(400 * time.Millisecond)

		restoredPath := filepath.Join(db.TempDir, fmt.Sprintf("restored-%d.db", i))
		if err := db.Restore(restoredPath); err != nil {
			// A structural decode error is real corruption. Other restore errors
			// can happen transiently while litestream compacts the replica (a file
			// it listed is removed mid-read), so retry those.
			if strings.Contains(err.Error(), "nonsequential page numbers") ||
				strings.Contains(err.Error(), "decode page") {
				corruption = err
			}
			_ = os.Remove(restoredPath)
			continue
		}

		restored := &TestDB{Path: restoredPath, t: t}
		if err := restored.IntegrityCheck(); err != nil {
			corruption = err
		}
		_ = os.Remove(restoredPath)
	}

	cancel()
	wg.Wait()
	db.StopLitestream()

	if corruption != nil {
		t.Fatalf("CORRUPTION: replica of a healthy database did not restore to a consistent database: %v", corruption)
	}
	t.Log("TEST PASSED: replica stayed restorable throughout replication")
}

// writeCorruptionRows inserts unique small rows until ctx is cancelled. Errors are
// ignored - litestream holds locks and the writer is expected to contend with it.
//
// It follows Litestream's documented tips for high write loads
// (https://litestream.io/tips/): busy_timeout=5000 and, crucially,
// wal_autocheckpoint=0 so the application never checkpoints and Litestream is the
// sole checkpointer. This proves the corruption is NOT the documented
// "application checkpoints between Litestream's checkpoints" footgun - it happens
// even when the app does everything the docs recommend.
//
// The connection is pinned (SetMaxOpenConns(1) + a single *sql.Conn) because
// database/sql pools connections: a PRAGMA run on the pool only affects one
// connection, so pooled inserts could still run with the default autocheckpoint.
func writeCorruptionRows(ctx context.Context, path string) {
	dsn := fmt.Sprintf("file:%s?_busy_timeout=5000&_journal_mode=WAL", filepath.ToSlash(path))
	pool, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return
	}
	defer pool.Close()
	pool.SetMaxOpenConns(1)

	conn, err := pool.Conn(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA wal_autocheckpoint=0`); err != nil {
		return
	}

	payload := strings.Repeat("x", 48)
	for i := 0; ctx.Err() == nil; i++ {
		_, _ = conn.ExecContext(ctx,
			`INSERT INTO editor_objects (id,payload) VALUES (?,?)`,
			fmt.Sprintf("obj-%08d", i), payload)
		time.Sleep(2 * time.Millisecond)
	}
}

// execSQL opens path, runs the given statements, and closes it.
func execSQL(path string, stmts ...string) (sql.Result, error) {
	sqldb, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	defer sqldb.Close()

	var res sql.Result
	for _, stmt := range stmts {
		if res, err = sqldb.Exec(stmt); err != nil {
			return nil, fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return res, nil
}
