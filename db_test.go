package litestream_test

import (
	"context"
	"hash/crc64"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestDB_Path(t *testing.T) {
	db := testingutil.NewDB(t, "/tmp/db")
	if got, want := db.Path(), `/tmp/db`; got != want {
		t.Fatalf("Path()=%v, want %v", got, want)
	}
}

func TestDB_WALPath(t *testing.T) {
	db := testingutil.NewDB(t, "/tmp/db")
	if got, want := db.WALPath(), `/tmp/db-wal`; got != want {
		t.Fatalf("WALPath()=%v, want %v", got, want)
	}
}

func TestDB_MetaPath(t *testing.T) {
	t.Run("Absolute", func(t *testing.T) {
		db := testingutil.NewDB(t, "/tmp/db")
		if got, want := db.MetaPath(), `/tmp/.db-litestream`; got != want {
			t.Fatalf("MetaPath()=%v, want %v", got, want)
		}
	})
	t.Run("Relative", func(t *testing.T) {
		db := testingutil.NewDB(t, "db")
		if got, want := db.MetaPath(), `.db-litestream`; got != want {
			t.Fatalf("MetaPath()=%v, want %v", got, want)
		}
	})
}

// Ensure we can compute a checksum on the real database.
func TestDB_CRC64(t *testing.T) {
	t.Run("ErrNotExist", func(t *testing.T) {
		db := testingutil.MustOpenDB(t)
		defer testingutil.MustCloseDB(t, db)
		if _, _, err := db.CRC64(context.Background()); !os.IsNotExist(err) {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("DB", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

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
		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT);`); err != nil {
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
		db := testingutil.MustOpenDB(t)
		defer testingutil.MustCloseDB(t, db)
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure sync can successfully run on the initial sync.
	t.Run("Initial", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

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
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		// Execute a query to force a write to the WAL.
		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE foo (bar TEXT);`); err != nil {
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
		if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO foo (bar) VALUES ('baz');`); err != nil {
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
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

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
		if err := os.Remove(db.WALPath()); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}

		// Reopen the managed database.
		db = testingutil.MustOpenDBAt(t, db.Path())
		defer testingutil.MustCloseDB(t, db)

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
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		// Execute a query to force a write to the WAL.
		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE foo (bar TEXT);`); err != nil {
			t.Fatal(err)
		}

		// Issue initial sync and truncate WAL.
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Obtain initial position.
		if _, err := db.Pos(); err != nil {
			t.Fatal(err)
		}

		// Fully close which should close WAL file.
		if err := db.Close(t.Context()); err != nil {
			t.Fatal(err)
		} else if err := sqldb.Close(); err != nil {
			t.Fatal(err)
		}

		// Verify WAL does not exist.
		if _, err := os.Stat(db.WALPath()); !os.IsNotExist(err) {
			t.Fatal(err)
		}

		// Insert into table multiple times to move past old offset
		sqldb = testingutil.MustOpenSQLDB(t, db.Path())
		defer testingutil.MustCloseSQLDB(t, sqldb)
		for i := 0; i < 100; i++ {
			if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO foo (bar) VALUES ('baz');`); err != nil {
				t.Fatal(err)
			}
		}

		// Reopen the managed database.
		db = testingutil.MustOpenDBAt(t, db.Path())
		defer testingutil.MustCloseDB(t, db)

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
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		// Execute a query to force a write to the WAL and then sync.
		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE foo (bar TEXT);`); err != nil {
			t.Fatal(err)
		} else if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Write at least minimum number of pages to trigger rollover.
		for i := 0; i < db.MinCheckpointPageN; i++ {
			if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO foo (bar) VALUES ('baz');`); err != nil {
				t.Fatal(err)
			}
		}

		// Sync to shadow WAL.
		if err := db.Sync(t.Context()); err != nil {
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

		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		// Execute a query to force a write to the WAL and then sync.
		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE foo (bar TEXT);`); err != nil {
			t.Fatal(err)
		} else if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Reduce checkpoint interval to ensure a rollover is triggered.
		db.CheckpointInterval = 1 * time.Nanosecond

		// Write to WAL & sync.
		if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO foo (bar) VALUES ('baz');`); err != nil {
			t.Fatal(err)
		} else if err := db.Sync(t.Context()); err != nil {
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

func TestDB_Compact(t *testing.T) {
	// Ensure that raw L0 transactions can be compacted into the first level.
	t.Run("L1", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)
		db.Replica = litestream.NewReplica(db)
		db.Replica.Client = testingutil.NewFileReplicaClient(t)

		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT);`); err != nil {
			t.Fatal(err)
		}
		if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (100)`); err != nil {
			t.Fatal(err)
		}

		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		if err := db.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		info, err := db.Compact(t.Context(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := info.Level, 1; got != want {
			t.Fatalf("Level=%v, want %v", got, want)
		}
		if got, want := info.MinTXID, ltx.TXID(1); got != want {
			t.Fatalf("MinTXID=%s, want %s", got, want)
		}
		if got, want := info.MaxTXID, ltx.TXID(2); got != want {
			t.Fatalf("MaxTXID=%s, want %s", got, want)
		}
		if info.Size == 0 {
			t.Fatalf("expected non-zero size")
		}
	})

	// Ensure that higher level compactions pull from the correct levels.
	t.Run("L2+", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT);`); err != nil {
			t.Fatal(err)
		}

		// TXID 2
		if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (100)`); err != nil {
			t.Fatal(err)
		} else if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		if err := db.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Compact to L1:1-2
		if info, err := db.Compact(t.Context(), 1); err != nil {
			t.Fatal(err)
		} else if got, want := ltx.FormatFilename(info.MinTXID, info.MaxTXID), `0000000000000001-0000000000000002.ltx`; got != want {
			t.Fatalf("Filename=%s, want %s", got, want)
		}

		// TXID 3
		if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (100)`); err != nil {
			t.Fatal(err)
		} else if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		if err := db.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Compact to L1:3-3
		if info, err := db.Compact(t.Context(), 1); err != nil {
			t.Fatal(err)
		} else if got, want := ltx.FormatFilename(info.MinTXID, info.MaxTXID), `0000000000000003-0000000000000003.ltx`; got != want {
			t.Fatalf("Filename=%s, want %s", got, want)
		}

		// Compact to L2:1-3
		if info, err := db.Compact(t.Context(), 2); err != nil {
			t.Fatal(err)
		} else if got, want := info.Level, 2; got != want {
			t.Fatalf("Level=%v, want %v", got, want)
		} else if got, want := ltx.FormatFilename(info.MinTXID, info.MaxTXID), `0000000000000001-0000000000000003.ltx`; got != want {
			t.Fatalf("Filename=%s, want %s", got, want)
		}
	})
}

func TestDB_Snapshot(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = testingutil.NewFileReplicaClient(t)

	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT);`); err != nil {
		t.Fatal(err)
	} else if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (100)`); err != nil {
		t.Fatal(err)
	} else if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	info, err := db.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ltx.FormatFilename(info.MinTXID, info.MaxTXID), `0000000000000001-0000000000000002.ltx`; got != want {
		t.Fatalf("Filename=%s, want %s", got, want)
	}

	// Calculate local checksum
	chksum0, _, err := db.CRC64(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// Fetch remote LTX snapshot file and ensure it matches the checksum of the local database.
	rc, err := db.Replica.Client.OpenLTXFile(t.Context(), litestream.SnapshotLevel, 1, 2, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	h := crc64.New(crc64.MakeTable(crc64.ISO))
	if err := ltx.NewDecoder(rc).DecodeDatabaseTo(h); err != nil {
		t.Fatal(err)
	} else if got, want := h.Sum64(), chksum0; got != want {
		t.Fatal("snapshot checksum mismatch")
	}
}

func TestDB_EnforceRetention(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = testingutil.NewFileReplicaClient(t)

	// Create table and sync initial state
	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT);`); err != nil {
		t.Fatal(err)
	} else if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	// Create multiple snapshots with delays to test retention
	for i := 0; i < 3; i++ {
		if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (?)`, i); err != nil {
			t.Fatal(err)
		} else if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Snapshot(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Sleep between snapshots to create time differences
		time.Sleep(100 * time.Millisecond)
	}

	// Get list of snapshots before retention
	itr, err := db.Replica.Client.LTXFiles(t.Context(), litestream.SnapshotLevel, 0)
	if err != nil {
		t.Fatal(err)
	}
	var beforeCount int
	for itr.Next() {
		beforeCount++
	}
	itr.Close()

	if beforeCount != 3 {
		t.Fatalf("expected 3 snapshots before retention, got %d", beforeCount)
	}

	// Enforce retention to remove older snapshots
	retentionTime := time.Now().Add(-150 * time.Millisecond)
	if minSnapshotTXID, err := db.EnforceSnapshotRetention(t.Context(), retentionTime); err != nil {
		t.Fatal(err)
	} else if got, want := minSnapshotTXID, ltx.TXID(4); got != want {
		t.Fatalf("MinSnapshotTXID=%s, want %s", got, want)
	}

	// Verify snapshots after retention
	itr, err = db.Replica.Client.LTXFiles(t.Context(), litestream.SnapshotLevel, 0)
	if err != nil {
		t.Fatal(err)
	}
	var afterCount int
	for itr.Next() {
		afterCount++
	}
	itr.Close()

	// Should have at least one snapshot remaining
	if afterCount < 1 {
		t.Fatal("expected at least 1 snapshot after retention")
	}

	// Should have fewer snapshots than before
	if afterCount >= beforeCount {
		t.Fatalf("expected fewer snapshots after retention, before=%d after=%d", beforeCount, afterCount)
	}
}

// TestDB_ConcurrentMapWrite tests for race conditions in maxLTXFileInfos map access.
// This test specifically targets the concurrent map write issue found in db.go
// where sync() method writes to the map without proper locking.
// Run with: go test -race -run TestDB_ConcurrentMapWrite
func TestDB_ConcurrentMapWrite(t *testing.T) {
	// Use the standard test helpers
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	// Enable monitoring to trigger background operations
	db.MonitorInterval = 10 * time.Millisecond

	// Create a table
	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)`); err != nil {
		t.Fatal(err)
	}

	// Start multiple goroutines to trigger concurrent map access
	var wg sync.WaitGroup

	// Number of concurrent operations
	const numGoroutines = 10

	// Channel to signal start
	start := make(chan struct{})

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Wait for signal to start all goroutines simultaneously
			<-start

			// Perform operations that trigger map access
			for j := 0; j < 5; j++ {
				// This triggers sync() which had unprotected map access
				if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO t (value) VALUES (?)`, "test"); err != nil {
					t.Logf("Goroutine %d: insert error: %v", id, err)
				}

				// Trigger Sync manually which accesses the map
				if err := db.Sync(t.Context()); err != nil {
					t.Logf("Goroutine %d: sync error: %v", id, err)
				}

				// Small delay to allow race to manifest
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Additional goroutine for snapshot operations
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start

		for i := 0; i < 3; i++ {
			// This triggers Snapshot() which has protected map access
			if _, err := db.Snapshot(t.Context()); err != nil {
				t.Logf("Snapshot error: %v", err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Start all goroutines
	close(start)

	// Wait for completion
	wg.Wait()

	t.Log("Test completed without race condition")
}
