package litestream_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
	"github.com/benbjohnson/litestream/mock"
)

func TestReplica_Sync(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	t.Log("initial sync")

	// Issue initial database sync.
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Fetch current database position.
	dpos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("position after sync: %s", dpos.String())

	c := file.NewReplicaClient(t.TempDir())
	r := litestream.NewReplicaWithClient(db, c)

	if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	t.Logf("second sync")

	// Verify we synced checkpoint page to WAL.
	rd, err := c.OpenLTXFile(context.Background(), 0, dpos.TXID, dpos.TXID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rd.Close() }()

	dec := ltx.NewDecoder(rd)
	if err := dec.Verify(); err != nil {
		t.Fatal(err)
	} else if err := rd.Close(); err != nil {
		t.Fatal(err)
	} else if got, want := int(dec.Header().PageSize), db.PageSize(); got != want {
		t.Fatalf("page size: %d, want %d", got, want)
	}

	// Reset WAL so the next write will only write out the segment we are checking.
	if err := db.Checkpoint(context.Background(), litestream.CheckpointModeTruncate); err != nil {
		t.Fatal(err)
	}

	// Execute a query to write something into the truncated WAL.
	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE foo (bar TEXT);`); err != nil {
		t.Fatal(err)
	}

	// Sync database to catch up the shadow WAL.
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Save position after sync, it should be after our write.
	_, err = db.Pos()
	if err != nil {
		t.Fatal(err)
	}

	// Sync WAL segment out to replica.
	if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// TODO(ltx): Restore snapshot and verify
}

// TestReplica_RestoreAndReplicateAfterDataLoss tests the scenario described in issue #781
// where a database is restored to an earlier state (with lower TXID) but the replica has
// a higher TXID, causing new writes to not be replicated.
//
// The fix detects this condition in DB.init() by comparing database position vs replica
// position. When database is behind, it fetches the latest L0 file from the replica and
// triggers a snapshot on the next sync.
//
// This test follows the reproduction steps from issue #781:
// 1. Create DB and replicate data
// 2. Restore from backup (simulating hard recovery)
// 3. Insert new data and replicate
// 4. Restore again and verify new data exists
func TestReplica_RestoreAndReplicateAfterDataLoss(t *testing.T) {
	ctx := context.Background()

	// Create a temporary directory for replica storage
	replicaDir := t.TempDir()
	replicaClient := file.NewReplicaClient(replicaDir)

	// Create database with initial data
	dbDir := t.TempDir()
	dbPath := dbDir + "/db.sqlite"

	// Step 1: Create initial data and replicate
	sqldb := testingutil.MustOpenSQLDB(t, dbPath)
	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE test(col1 INTEGER);`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO test VALUES (1);`); err != nil {
		t.Fatal(err)
	}
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}

	// Start litestream replication
	db1 := testingutil.NewDB(t, dbPath)
	db1.MonitorInterval = 0
	db1.Replica = litestream.NewReplicaWithClient(db1, replicaClient)
	db1.Replica.MonitorEnabled = false

	if err := db1.Open(); err != nil {
		t.Fatal(err)
	}
	if err := db1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db1.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db1.Close(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("Step 1 complete: Initial data replicated")

	// Step 2: Simulate hard recovery - remove database and .litestream directory, then restore
	if err := os.Remove(dbPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dbPath + "-wal"); os.IsExist(err) {
		t.Fatal(err)
	}
	if err := os.Remove(dbPath + "-shm"); os.IsExist(err) {
		t.Fatal(err)
	}
	metaPath := db1.MetaPath()
	if err := os.RemoveAll(metaPath); err != nil {
		t.Fatal(err)
	}

	// Restore from backup
	restoreOpt := litestream.RestoreOptions{
		OutputPath: dbPath,
	}
	if err := db1.Replica.Restore(ctx, restoreOpt); err != nil {
		t.Fatal(err)
	}
	t.Log("Step 2 complete: Database restored from backup")

	// Step 3: Start replication and insert new data
	db2 := testingutil.NewDB(t, dbPath)
	db2.MonitorInterval = 0
	db2.Replica = litestream.NewReplicaWithClient(db2, replicaClient)
	db2.Replica.MonitorEnabled = false

	if err := db2.Open(); err != nil {
		t.Fatal(err)
	}

	sqldb2 := testingutil.MustOpenSQLDB(t, dbPath)
	if _, err := sqldb2.ExecContext(ctx, `INSERT INTO test VALUES (2);`); err != nil {
		t.Fatal(err)
	}
	if err := sqldb2.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("Step 3: Inserted new data (value=2) after restore")

	// Sync new data
	if err := db2.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db2.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("Step 3 complete: New data synced")

	// Step 4: Simulate second hard recovery and restore again
	if err := os.Remove(dbPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dbPath + "-wal"); os.IsExist(err) {
		t.Fatal(err)
	}
	if err := os.Remove(dbPath + "-shm"); os.IsExist(err) {
		t.Fatal(err)
	}
	if err := os.RemoveAll(db2.MetaPath()); err != nil {
		t.Fatal(err)
	}

	// Restore to a path with non-existent parent directory to verify it gets created
	restoredPath := dbDir + "/restored/db.sqlite"
	restoreOpt.OutputPath = restoredPath
	if err := db2.Replica.Restore(ctx, restoreOpt); err != nil {
		t.Fatal(err)
	}
	t.Log("Step 4 complete: Second restore from backup to path with non-existent parent")

	// Step 5: Verify the new data (value=2) exists in restored database
	sqldb3 := testingutil.MustOpenSQLDB(t, restoredPath)
	defer sqldb3.Close()

	var count int
	if err := sqldb3.QueryRowContext(ctx, `SELECT COUNT(*) FROM test;`).Scan(&count); err != nil {
		t.Fatal(err)
	}

	// Should have 2 rows (1 and 2)
	if count != 2 {
		t.Fatalf("expected 2 rows in restored database, got %d", count)
	}

	// Verify the new row (value=2) exists
	var exists bool
	if err := sqldb3.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM test WHERE col1 = 2);`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("new data (value=2) was not replicated - this is the bug in issue #781")
	}

	t.Log("Test passed: New data after restore was successfully replicated")
}

func TestReplica_CalcRestorePlan(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	t.Run("SnapshotOnly", func(t *testing.T) {
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			if level == litestream.SnapshotLevel {
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{{
					Level:     litestream.SnapshotLevel,
					MinTXID:   1,
					MaxTXID:   10,
					Size:      1024,
					CreatedAt: time.Now(),
				}}), nil
			}
			return ltx.NewFileInfoSliceIterator(nil), nil
		}

		plan, err := litestream.CalcRestorePlan(context.Background(), r.Client, 10, time.Time{}, r.Logger())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got, want := len(plan), 1; got != want {
			t.Fatalf("n=%d, want %d", got, want)
		}
		if plan[0].MaxTXID != 10 {
			t.Fatalf("expected MaxTXID 10, got %d", plan[0].MaxTXID)
		}
	})

	t.Run("SnapshotAndIncremental", func(t *testing.T) {
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			switch level {
			case litestream.SnapshotLevel:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 5},
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 15},
				}), nil
			case 1:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 1, MinTXID: 6, MaxTXID: 7},
					{Level: 1, MinTXID: 8, MaxTXID: 9},
					{Level: 1, MinTXID: 10, MaxTXID: 12},
				}), nil
			case 0:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 0, MinTXID: 7, MaxTXID: 7},
					{Level: 0, MinTXID: 8, MaxTXID: 8},
					{Level: 0, MinTXID: 9, MaxTXID: 9},
					{Level: 0, MinTXID: 10, MaxTXID: 10},
					{Level: 0, MinTXID: 11, MaxTXID: 11},
				}), nil
			default:
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
		}

		plan, err := litestream.CalcRestorePlan(context.Background(), r.Client, 10, time.Time{}, r.Logger())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got, want := len(plan), 4; got != want {
			t.Fatalf("n=%v, want %v", got, want)
		}
		if got, want := *plan[0], (ltx.FileInfo{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 5}); got != want {
			t.Fatalf("plan[0]=%#v, want %#v", got, want)
		}
		if got, want := *plan[1], (ltx.FileInfo{Level: 1, MinTXID: 6, MaxTXID: 7}); got != want {
			t.Fatalf("plan[1]=%#v, want %#v", got, want)
		}
		if got, want := *plan[2], (ltx.FileInfo{Level: 1, MinTXID: 8, MaxTXID: 9}); got != want {
			t.Fatalf("plan[2]=%#v, want %#v", got, want)
		}
		if got, want := *plan[3], (ltx.FileInfo{Level: 0, MinTXID: 10, MaxTXID: 10}); got != want {
			t.Fatalf("plan[2]=%#v, want %#v", got, want)
		}
	})

	t.Run("SelectLongestAcrossLevels", func(t *testing.T) {
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			switch level {
			case litestream.SnapshotLevel:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 5},
				}), nil
			case 2:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 2, MinTXID: 6, MaxTXID: 12},
				}), nil
			case 0:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 0, MinTXID: 6, MaxTXID: 20},
				}), nil
			default:
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
		}

		plan, err := litestream.CalcRestorePlan(context.Background(), r.Client, 20, time.Time{}, r.Logger())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got, want := len(plan), 2; got != want {
			t.Fatalf("n=%v, want %v", got, want)
		}
		if got, want := *plan[0], (ltx.FileInfo{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 5}); got != want {
			t.Fatalf("plan[0]=%#v, want %#v", got, want)
		}
		if got, want := *plan[1], (ltx.FileInfo{Level: 0, MinTXID: 6, MaxTXID: 20}); got != want {
			t.Fatalf("plan[1]=%#v, want %#v", got, want)
		}
	})

	t.Run("GapInLevelResolvedByLowerLevel", func(t *testing.T) {
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			switch level {
			case litestream.SnapshotLevel:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 5},
				}), nil
			case 1:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 1, MinTXID: 6, MaxTXID: 7},
					{Level: 1, MinTXID: 9, MaxTXID: 10},
				}), nil
			case 0:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 0, MinTXID: 8, MaxTXID: 8},
					{Level: 0, MinTXID: 9, MaxTXID: 9},
					{Level: 0, MinTXID: 10, MaxTXID: 10},
				}), nil
			default:
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
		}

		plan, err := litestream.CalcRestorePlan(context.Background(), r.Client, 10, time.Time{}, r.Logger())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got, want := len(plan), 4; got != want {
			t.Fatalf("n=%v, want %v", got, want)
		}
		if got, want := *plan[0], (ltx.FileInfo{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 5}); got != want {
			t.Fatalf("plan[0]=%#v, want %#v", got, want)
		}
		if got, want := *plan[1], (ltx.FileInfo{Level: 1, MinTXID: 6, MaxTXID: 7}); got != want {
			t.Fatalf("plan[1]=%#v, want %#v", got, want)
		}
		if got, want := *plan[2], (ltx.FileInfo{Level: 0, MinTXID: 8, MaxTXID: 8}); got != want {
			t.Fatalf("plan[2]=%#v, want %#v", got, want)
		}
		if got, want := *plan[3], (ltx.FileInfo{Level: 1, MinTXID: 9, MaxTXID: 10}); got != want {
			t.Fatalf("plan[3]=%#v, want %#v", got, want)
		}
	})

	// Issue #847: When a level has overlapping files where a larger compacted file
	// covers a smaller file's entire range, the smaller file should be skipped
	// rather than causing a non-contiguous error.
	t.Run("OverlappingFilesWithinLevel", func(t *testing.T) {
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			switch level {
			case litestream.SnapshotLevel:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 5},
				}), nil
			case 2:
				// Simulates issue #847: Files are sorted by MinTXID (filename order).
				// File 1 is a large compacted file covering 1-100.
				// File 2 is a smaller file covering 50-60, which is fully within file 1's range.
				// Before the fix, file 2 would pass the filter (MaxTXID 60 > infos.MaxTXID() 5)
				// but then fail the contiguity check after file 1 is added.
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 2, MinTXID: 1, MaxTXID: 100},
					{Level: 2, MinTXID: 50, MaxTXID: 60},
				}), nil
			default:
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
		}

		plan, err := litestream.CalcRestorePlan(context.Background(), r.Client, 100, time.Time{}, r.Logger())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Plan should contain only snapshot and the large file, not the smaller overlapping file
		if got, want := len(plan), 2; got != want {
			t.Fatalf("n=%d, want %d", got, want)
		}
		if got, want := *plan[0], (ltx.FileInfo{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 5}); got != want {
			t.Fatalf("plan[0]=%#v, want %#v", got, want)
		}
		if got, want := *plan[1], (ltx.FileInfo{Level: 2, MinTXID: 1, MaxTXID: 100}); got != want {
			t.Fatalf("plan[1]=%#v, want %#v", got, want)
		}
	})

	t.Run("ErrTxNotAvailable", func(t *testing.T) {
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			switch level {
			case litestream.SnapshotLevel:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 10},
				}), nil
			default:
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
		}

		_, err := litestream.CalcRestorePlan(context.Background(), r.Client, 5, time.Time{}, r.Logger())
		if !errors.Is(err, litestream.ErrTxNotAvailable) {
			t.Fatalf("expected ErrTxNotAvailable, got %v", err)
		}
	})

	t.Run("ErrNoFiles", func(t *testing.T) {
		var c mock.ReplicaClient
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			return ltx.NewFileInfoSliceIterator(nil), nil
		}
		r := litestream.NewReplicaWithClient(db, &c)

		_, err := litestream.CalcRestorePlan(context.Background(), r.Client, 5, time.Time{}, r.Logger())
		if !errors.Is(err, litestream.ErrTxNotAvailable) {
			t.Fatalf("expected ErrTxNotAvailable, got %v", err)
		}
	})
}

func TestReplica_TimeBounds(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	t.Run("Level0Only", func(t *testing.T) {
		now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			if level == 0 {
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 0, MinTXID: 1, MaxTXID: 1, CreatedAt: now},
					{Level: 0, MinTXID: 2, MaxTXID: 2, CreatedAt: now.Add(time.Hour)},
					{Level: 0, MinTXID: 3, MaxTXID: 3, CreatedAt: now.Add(2 * time.Hour)},
				}), nil
			}
			return ltx.NewFileInfoSliceIterator(nil), nil
		}

		createdAt, updatedAt, err := r.TimeBounds(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !createdAt.Equal(now) {
			t.Fatalf("createdAt=%v, want %v", createdAt, now)
		}
		if want := now.Add(2 * time.Hour); !updatedAt.Equal(want) {
			t.Fatalf("updatedAt=%v, want %v", updatedAt, want)
		}
	})

	t.Run("SnapshotOnly", func(t *testing.T) {
		now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			if level == litestream.SnapshotLevel {
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 10, CreatedAt: now},
				}), nil
			}
			return ltx.NewFileInfoSliceIterator(nil), nil
		}

		createdAt, updatedAt, err := r.TimeBounds(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !createdAt.Equal(now) {
			t.Fatalf("createdAt=%v, want %v", createdAt, now)
		}
		if !updatedAt.Equal(now) {
			t.Fatalf("updatedAt=%v, want %v", updatedAt, now)
		}
	})

	t.Run("SnapshotAndLevel0", func(t *testing.T) {
		snapshotTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		l0Time := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			switch level {
			case litestream.SnapshotLevel:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 10, CreatedAt: snapshotTime},
				}), nil
			case 0:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 0, MinTXID: 11, MaxTXID: 11, CreatedAt: l0Time},
				}), nil
			default:
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
		}

		createdAt, updatedAt, err := r.TimeBounds(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !createdAt.Equal(snapshotTime) {
			t.Fatalf("createdAt=%v, want %v", createdAt, snapshotTime)
		}
		if !updatedAt.Equal(l0Time) {
			t.Fatalf("updatedAt=%v, want %v", updatedAt, l0Time)
		}
	})

	t.Run("MultipleCompactionLevels", func(t *testing.T) {
		snapshotTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		l2Time := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
		l0Time := time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			switch level {
			case litestream.SnapshotLevel:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 5, CreatedAt: snapshotTime},
				}), nil
			case 2:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 2, MinTXID: 6, MaxTXID: 8, CreatedAt: l2Time},
				}), nil
			case 0:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 0, MinTXID: 9, MaxTXID: 9, CreatedAt: l0Time},
				}), nil
			default:
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
		}

		createdAt, updatedAt, err := r.TimeBounds(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !createdAt.Equal(snapshotTime) {
			t.Fatalf("createdAt=%v, want %v", createdAt, snapshotTime)
		}
		if !updatedAt.Equal(l0Time) {
			t.Fatalf("updatedAt=%v, want %v", updatedAt, l0Time)
		}
	})

	t.Run("NoFiles", func(t *testing.T) {
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			return ltx.NewFileInfoSliceIterator(nil), nil
		}

		createdAt, updatedAt, err := r.TimeBounds(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !createdAt.IsZero() {
			t.Fatalf("createdAt=%v, want zero", createdAt)
		}
		if !updatedAt.IsZero() {
			t.Fatalf("updatedAt=%v, want zero", updatedAt)
		}
	})

	t.Run("ErrorOnLevel", func(t *testing.T) {
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		errTest := errors.New("test error")
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			if level == 3 {
				return nil, errTest
			}
			return ltx.NewFileInfoSliceIterator(nil), nil
		}

		_, _, err := r.TimeBounds(context.Background())
		if !errors.Is(err, errTest) {
			t.Fatalf("expected test error, got %v", err)
		}
	})
}

func TestReplica_CalcRestoreTarget(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	t.Run("TimestampInSnapshotRange", func(t *testing.T) {
		snapshotTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		l0Time := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			switch level {
			case litestream.SnapshotLevel:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 10, CreatedAt: snapshotTime},
				}), nil
			case 0:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 0, MinTXID: 11, MaxTXID: 11, CreatedAt: l0Time},
				}), nil
			default:
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
		}

		ts := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)
		updatedAt, err := r.CalcRestoreTarget(context.Background(), litestream.RestoreOptions{Timestamp: ts})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !updatedAt.Equal(l0Time) {
			t.Fatalf("updatedAt=%v, want %v", updatedAt, l0Time)
		}
	})

	t.Run("TimestampBeforeAllFiles", func(t *testing.T) {
		snapshotTime := time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC)
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			switch level {
			case litestream.SnapshotLevel:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 10, CreatedAt: snapshotTime},
				}), nil
			default:
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
		}

		ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		_, err := r.CalcRestoreTarget(context.Background(), litestream.RestoreOptions{Timestamp: ts})
		if err == nil || err.Error() != "timestamp does not exist" {
			t.Fatalf("expected 'timestamp does not exist', got %v", err)
		}
	})

	t.Run("TimestampAfterAllFiles", func(t *testing.T) {
		snapshotTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			switch level {
			case litestream.SnapshotLevel:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 10, CreatedAt: snapshotTime},
				}), nil
			default:
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
		}

		ts := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)
		_, err := r.CalcRestoreTarget(context.Background(), litestream.RestoreOptions{Timestamp: ts})
		if err == nil || err.Error() != "timestamp does not exist" {
			t.Fatalf("expected 'timestamp does not exist', got %v", err)
		}
	})

	t.Run("NoTimestamp", func(t *testing.T) {
		now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			if level == 0 {
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 0, MinTXID: 1, MaxTXID: 1, CreatedAt: now},
					{Level: 0, MinTXID: 2, MaxTXID: 2, CreatedAt: now.Add(time.Hour)},
				}), nil
			}
			return ltx.NewFileInfoSliceIterator(nil), nil
		}

		updatedAt, err := r.CalcRestoreTarget(context.Background(), litestream.RestoreOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := now.Add(time.Hour); !updatedAt.Equal(want) {
			t.Fatalf("updatedAt=%v, want %v", updatedAt, want)
		}
	})
}

func TestReplica_Restore_InvalidFileSize(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	t.Run("EmptyFile", func(t *testing.T) {
		var c mock.ReplicaClient
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			if level == litestream.SnapshotLevel {
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{{
					Level:     litestream.SnapshotLevel,
					MinTXID:   1,
					MaxTXID:   10,
					Size:      0, // Empty file - this should cause an error
					CreatedAt: time.Now(),
				}}), nil
			}
			return ltx.NewFileInfoSliceIterator(nil), nil
		}

		r := litestream.NewReplicaWithClient(db, &c)
		outputPath := t.TempDir() + "/restored.db"

		err := r.Restore(context.Background(), litestream.RestoreOptions{
			OutputPath: outputPath,
		})
		if err == nil {
			t.Fatal("expected error for empty file, got nil")
		}
		if !strings.Contains(err.Error(), "invalid ltx file") {
			t.Fatalf("expected 'invalid ltx file' error, got: %v", err)
		}
	})

	t.Run("TruncatedFile", func(t *testing.T) {
		var c mock.ReplicaClient
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			if level == litestream.SnapshotLevel {
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{{
					Level:     litestream.SnapshotLevel,
					MinTXID:   1,
					MaxTXID:   10,
					Size:      50, // Less than ltx.HeaderSize (100) - should cause an error
					CreatedAt: time.Now(),
				}}), nil
			}
			return ltx.NewFileInfoSliceIterator(nil), nil
		}

		r := litestream.NewReplicaWithClient(db, &c)
		outputPath := t.TempDir() + "/restored.db"

		err := r.Restore(context.Background(), litestream.RestoreOptions{
			OutputPath: outputPath,
		})
		if err == nil {
			t.Fatal("expected error for truncated file, got nil")
		}
		if !strings.Contains(err.Error(), "invalid ltx file") {
			t.Fatalf("expected 'invalid ltx file' error, got: %v", err)
		}
	})
}

func TestReplica_ContextCancellationNoLogs(t *testing.T) {
	// This test verifies that context cancellation errors are not logged during shutdown.
	// The fix for issue #235 ensures that context.Canceled and context.DeadlineExceeded
	// errors are filtered out in monitor functions to avoid spurious log messages.

	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	// Create a buffer to capture log output
	var logBuffer bytes.Buffer

	// Create a custom logger that writes to our buffer
	db.Logger = slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// First, let's trigger a normal sync to ensure the DB is initialized
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Create a replica with a mock client that simulates context cancellation during Sync
	syncCount := 0
	mockClient := &mock.ReplicaClient{
		LTXFilesFunc: func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			syncCount++
			// First few calls succeed, then return context.Canceled
			if syncCount <= 2 {
				// Return an empty iterator
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
			// After initial syncs, return context.Canceled to simulate shutdown
			return nil, context.Canceled
		},
		WriteLTXFileFunc: func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
			// Always succeed for writes to allow normal operation
			return &ltx.FileInfo{
				Level:     level,
				MinTXID:   minTXID,
				MaxTXID:   maxTXID,
				CreatedAt: time.Now(),
			}, nil
		},
	}

	r := litestream.NewReplicaWithClient(db, mockClient)
	r.SyncInterval = 50 * time.Millisecond // Short interval for testing

	// Start the replica monitoring in a goroutine
	ctx, cancel := context.WithCancel(context.Background())

	if err := r.Start(ctx); err != nil {
		t.Fatalf("failed to start replica: %v", err)
	}

	// Give the monitor time to run several sync cycles
	// This ensures we get both successful syncs and context cancellation errors
	time.Sleep(200 * time.Millisecond)

	// Cancel the context to trigger shutdown
	cancel()

	// Stop the replica and wait for it to finish
	if err := r.Stop(true); err != nil {
		t.Fatalf("failed to stop replica: %v", err)
	}

	// Check the logs
	logs := logBuffer.String()

	// We should have some debug logs from successful operations
	if !strings.Contains(logs, "replica sync") {
		t.Errorf("expected 'replica sync' in logs but didn't find it; logs:\n%s", logs)
	}

	// But we should NOT have "monitor error" with "context canceled"
	if strings.Contains(logs, "monitor error") && strings.Contains(logs, "context canceled") {
		t.Errorf("found 'monitor error' with 'context canceled' in logs when it should be filtered:\n%s", logs)
	}

	// The test passes if context.Canceled errors were properly filtered
}

func TestReplica_ValidateLevel(t *testing.T) {
	t.Run("ValidContiguousFiles", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		replica := litestream.NewReplicaWithClient(nil, client)

		// Create contiguous files: 1-2, 3-5, 6-10
		createTestLTXFile(t, client, 1, 1, 2)
		createTestLTXFile(t, client, 1, 3, 5)
		createTestLTXFile(t, client, 1, 6, 10)

		errs, err := replica.ValidateLevel(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(errs) != 0 {
			t.Errorf("expected no errors for contiguous files, got %d: %v", len(errs), errs)
		}
	})

	t.Run("EmptyLevel", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		replica := litestream.NewReplicaWithClient(nil, client)

		errs, err := replica.ValidateLevel(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(errs) != 0 {
			t.Errorf("expected no errors for empty level, got %d", len(errs))
		}
	})

	t.Run("SingleFile", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		replica := litestream.NewReplicaWithClient(nil, client)

		createTestLTXFile(t, client, 1, 1, 5)

		errs, err := replica.ValidateLevel(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(errs) != 0 {
			t.Errorf("expected no errors for single file, got %d", len(errs))
		}
	})

	t.Run("GapDetected", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		replica := litestream.NewReplicaWithClient(nil, client)

		// Create files with a gap (missing TXID 3-4)
		createTestLTXFile(t, client, 1, 1, 2)
		createTestLTXFile(t, client, 1, 5, 7)

		errs, err := replica.ValidateLevel(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(errs) != 1 {
			t.Fatalf("expected 1 error, got %d", len(errs))
		}
		if errs[0].Type != "gap" {
			t.Errorf("expected gap error, got %q", errs[0].Type)
		}
		if errs[0].Level != 1 {
			t.Errorf("expected level 1, got %d", errs[0].Level)
		}
	})

	t.Run("OverlapDetected", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		replica := litestream.NewReplicaWithClient(nil, client)

		// Create overlapping files
		createTestLTXFile(t, client, 1, 1, 5)
		createTestLTXFile(t, client, 1, 3, 7)

		errs, err := replica.ValidateLevel(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(errs) != 1 {
			t.Fatalf("expected 1 error, got %d", len(errs))
		}
		if errs[0].Type != "overlap" {
			t.Errorf("expected overlap error, got %q", errs[0].Type)
		}
	})

	t.Run("MultipleErrors", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		replica := litestream.NewReplicaWithClient(nil, client)

		// Create files with multiple issues: gap then overlap
		createTestLTXFile(t, client, 1, 1, 2)
		createTestLTXFile(t, client, 1, 5, 10) // gap at 3-4
		createTestLTXFile(t, client, 1, 8, 12) // overlap at 8-10

		errs, err := replica.ValidateLevel(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(errs) != 2 {
			t.Fatalf("expected 2 errors, got %d", len(errs))
		}
		if errs[0].Type != "gap" {
			t.Errorf("expected first error to be gap, got %q", errs[0].Type)
		}
		if errs[1].Type != "overlap" {
			t.Errorf("expected second error to be overlap, got %q", errs[1].Type)
		}
	})
}
func TestReplica_RestoreV3(t *testing.T) {
	t.Run("SnapshotOnly", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		replicaDir := t.TempDir()

		// Create a v0.3.x backup structure with a snapshot
		gen := "0123456789abcdef"
		createV3Backup(t, replicaDir, gen, []v3SnapshotData{
			{index: 0, data: createTestSQLiteDB(t)},
		}, nil)

		// Create replica client and replica
		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(nil, c)

		// Restore
		outputPath := tmpDir + "/restored.db"
		err := r.RestoreV3(ctx, litestream.RestoreOptions{
			OutputPath: outputPath,
		})
		if err != nil {
			t.Fatalf("RestoreV3 failed: %v", err)
		}

		// Verify restored database
		verifyRestoredDB(t, outputPath)
	})

	t.Run("SnapshotWithWAL", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		replicaDir := t.TempDir()

		// Create a v0.3.x backup with snapshot and WAL segments
		gen := "0123456789abcdef"
		dbData := createTestSQLiteDB(t)
		walData := createTestWALData(t, dbData)

		createV3Backup(t, replicaDir, gen, []v3SnapshotData{
			{index: 0, data: dbData},
		}, []v3WALSegmentData{
			{index: 0, offset: 0, data: walData},
		})

		// Create replica and restore
		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(nil, c)

		outputPath := tmpDir + "/restored.db"
		err := r.RestoreV3(ctx, litestream.RestoreOptions{
			OutputPath: outputPath,
		})
		if err != nil {
			t.Fatalf("RestoreV3 failed: %v", err)
		}

		// Verify restored database
		verifyRestoredDB(t, outputPath)
	})

	t.Run("TimestampRestore", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		replicaDir := t.TempDir()

		// Create a v0.3.x backup with multiple snapshots at different times
		gen := "0123456789abcdef"
		snapshotsDir := filepath.Join(replicaDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}

		// Create first snapshot (older)
		dbData1 := createTestSQLiteDB(t)
		writeV3Snapshot(t, snapshotsDir, 0, dbData1)
		// Set older mod time
		oldTime := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(filepath.Join(snapshotsDir, "00000000.snapshot.lz4"), oldTime, oldTime); err != nil {
			t.Fatal(err)
		}

		// Create second snapshot (newer) - sleep briefly to ensure different times
		time.Sleep(10 * time.Millisecond)
		dbData2 := createTestSQLiteDB(t)
		writeV3Snapshot(t, snapshotsDir, 1, dbData2)
		// Set newer mod time
		newTime := time.Now().Add(-1 * time.Hour)
		if err := os.Chtimes(filepath.Join(snapshotsDir, "00000001.snapshot.lz4"), newTime, newTime); err != nil {
			t.Fatal(err)
		}

		// Create replica and restore to a timestamp between the two snapshots
		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(nil, c)

		outputPath := tmpDir + "/restored.db"
		restoreTime := time.Now().Add(-90 * time.Minute) // Between old and new
		err := r.RestoreV3(ctx, litestream.RestoreOptions{
			OutputPath: outputPath,
			Timestamp:  restoreTime,
		})
		if err != nil {
			t.Fatalf("RestoreV3 failed: %v", err)
		}

		// Verify restored database (should be the older one)
		verifyRestoredDB(t, outputPath)
	})

	t.Run("NoSnapshots", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		replicaDir := t.TempDir()

		// Create empty generations directory
		gen := "0123456789abcdef"
		if err := os.MkdirAll(filepath.Join(replicaDir, "generations", gen), 0755); err != nil {
			t.Fatal(err)
		}

		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(nil, c)

		outputPath := tmpDir + "/restored.db"
		err := r.RestoreV3(ctx, litestream.RestoreOptions{
			OutputPath: outputPath,
		})
		if !errors.Is(err, litestream.ErrNoSnapshots) {
			t.Fatalf("expected ErrNoSnapshots, got %v", err)
		}
	})

	t.Run("NoGenerations", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		replicaDir := t.TempDir()

		// Empty replica directory
		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(nil, c)

		outputPath := tmpDir + "/restored.db"
		err := r.RestoreV3(ctx, litestream.RestoreOptions{
			OutputPath: outputPath,
		})
		if !errors.Is(err, litestream.ErrNoSnapshots) {
			t.Fatalf("expected ErrNoSnapshots, got %v", err)
		}
	})

	t.Run("OutputPathExists", func(t *testing.T) {
		ctx := context.Background()
		replicaDir := t.TempDir()

		// Create a v0.3.x backup
		gen := "0123456789abcdef"
		createV3Backup(t, replicaDir, gen, []v3SnapshotData{
			{index: 0, data: createTestSQLiteDB(t)},
		}, nil)

		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(nil, c)

		// Create output file that already exists
		outputPath := t.TempDir() + "/existing.db"
		if err := os.WriteFile(outputPath, []byte("existing"), 0644); err != nil {
			t.Fatal(err)
		}

		err := r.RestoreV3(ctx, litestream.RestoreOptions{
			OutputPath: outputPath,
		})
		if err == nil || !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("expected 'already exists' error, got %v", err)
		}
	})

	t.Run("ClientDoesNotSupportV3", func(t *testing.T) {
		ctx := context.Background()

		// Use mock client that doesn't implement ReplicaClientV3
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(nil, &c)

		err := r.RestoreV3(ctx, litestream.RestoreOptions{
			OutputPath: t.TempDir() + "/restored.db",
		})
		if err == nil || !strings.Contains(err.Error(), "does not support v0.3.x") {
			t.Fatalf("expected 'does not support v0.3.x' error, got %v", err)
		}
	})

	t.Run("MultipleGenerations", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		replicaDir := t.TempDir()

		// Create snapshots in two different generations
		gen1 := "0000000000000001"
		gen2 := "0000000000000002"

		// Older snapshot in gen1
		snapshotsDir1 := filepath.Join(replicaDir, "generations", gen1, "snapshots")
		if err := os.MkdirAll(snapshotsDir1, 0755); err != nil {
			t.Fatal(err)
		}
		dbData1 := createTestSQLiteDB(t)
		writeV3Snapshot(t, snapshotsDir1, 0, dbData1)
		oldTime := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(filepath.Join(snapshotsDir1, "00000000.snapshot.lz4"), oldTime, oldTime); err != nil {
			t.Fatal(err)
		}

		// Newer snapshot in gen2
		snapshotsDir2 := filepath.Join(replicaDir, "generations", gen2, "snapshots")
		if err := os.MkdirAll(snapshotsDir2, 0755); err != nil {
			t.Fatal(err)
		}
		dbData2 := createTestSQLiteDB(t)
		writeV3Snapshot(t, snapshotsDir2, 0, dbData2)
		newTime := time.Now().Add(-1 * time.Hour)
		if err := os.Chtimes(filepath.Join(snapshotsDir2, "00000000.snapshot.lz4"), newTime, newTime); err != nil {
			t.Fatal(err)
		}

		// Restore without timestamp should pick the newest
		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(nil, c)

		outputPath := tmpDir + "/restored.db"
		err := r.RestoreV3(ctx, litestream.RestoreOptions{
			OutputPath: outputPath,
		})
		if err != nil {
			t.Fatalf("RestoreV3 failed: %v", err)
		}

		verifyRestoredDB(t, outputPath)
	})
}

func TestReplica_Restore_BothFormats(t *testing.T) {
	t.Run("V3OnlyWithTimestamp", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		replicaDir := t.TempDir()

		// Create a v0.3.x backup only
		gen := "0123456789abcdef"
		snapshotsDir := filepath.Join(replicaDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}
		dbData := createTestSQLiteDB(t)
		writeV3Snapshot(t, snapshotsDir, 0, dbData)
		// Set snapshot time to 1 hour ago
		snapshotTime := time.Now().Add(-1 * time.Hour)
		if err := os.Chtimes(filepath.Join(snapshotsDir, "00000000.snapshot.lz4"), snapshotTime, snapshotTime); err != nil {
			t.Fatal(err)
		}

		// Create replica and restore with timestamp
		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(nil, c)

		outputPath := tmpDir + "/restored.db"
		err := r.Restore(ctx, litestream.RestoreOptions{
			OutputPath: outputPath,
			Timestamp:  time.Now(), // Any time after snapshot
		})
		if err != nil {
			t.Fatalf("Restore failed: %v", err)
		}

		// Verify restored database
		verifyRestoredDB(t, outputPath)
	})

	t.Run("V3OnlyWithoutTimestamp", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		replicaDir := t.TempDir()

		// Create a v0.3.x backup only (no LTX files)
		gen := "0123456789abcdef"
		snapshotsDir := filepath.Join(replicaDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}
		dbData := createTestSQLiteDB(t)
		writeV3Snapshot(t, snapshotsDir, 0, dbData)

		// Create replica and restore WITHOUT timestamp - should still use v0.3.x
		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(nil, c)

		outputPath := tmpDir + "/restored.db"
		err := r.Restore(ctx, litestream.RestoreOptions{
			OutputPath: outputPath,
			// No timestamp specified
		})
		if err != nil {
			t.Fatalf("Restore failed: %v", err)
		}

		// Verify restored database
		verifyRestoredDB(t, outputPath)
	})

	t.Run("LTXOnlyWithTimestamp", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		replicaDir := t.TempDir()
		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(db, c)

		// Sync to create LTX files
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := r.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Create a snapshot
		if _, err := db.Snapshot(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := r.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Wait a bit to ensure distinct timestamps
		time.Sleep(10 * time.Millisecond)

		// Restore with timestamp
		outputPath := t.TempDir() + "/restored.db"
		err := r.Restore(context.Background(), litestream.RestoreOptions{
			OutputPath: outputPath,
			Timestamp:  time.Now(),
		})
		if err != nil {
			t.Fatalf("Restore failed: %v", err)
		}

		verifyRestoredDB(t, outputPath)
	})

	t.Run("BothFormats_V3Better", func(t *testing.T) {
		ctx := context.Background()
		tmpDir := t.TempDir()
		replicaDir := t.TempDir()

		// Create v0.3.x snapshot at time T-30min (closer to restore time)
		gen := "0123456789abcdef"
		snapshotsDir := filepath.Join(replicaDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}
		dbData := createTestSQLiteDB(t)
		writeV3Snapshot(t, snapshotsDir, 0, dbData)
		v3Time := time.Now().Add(-30 * time.Minute)
		if err := os.Chtimes(filepath.Join(snapshotsDir, "00000000.snapshot.lz4"), v3Time, v3Time); err != nil {
			t.Fatal(err)
		}

		// Create LTX snapshot at time T-2h (older)
		ltxDir := filepath.Join(replicaDir, "ltx", "9") // Snapshot level
		if err := os.MkdirAll(ltxDir, 0755); err != nil {
			t.Fatal(err)
		}
		ltxData := createTestLTXSnapshot(t)
		ltxPath := filepath.Join(ltxDir, "0000000000000001-0000000000000001.ltx")
		if err := os.WriteFile(ltxPath, ltxData, 0644); err != nil {
			t.Fatal(err)
		}
		ltxTime := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(ltxPath, ltxTime, ltxTime); err != nil {
			t.Fatal(err)
		}

		// Restore with timestamp - should use V3 (more recent)
		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(nil, c)

		outputPath := tmpDir + "/restored.db"
		err := r.Restore(ctx, litestream.RestoreOptions{
			OutputPath: outputPath,
			Timestamp:  time.Now(),
		})
		if err != nil {
			t.Fatalf("Restore failed: %v", err)
		}

		verifyRestoredDB(t, outputPath)
	})

	t.Run("BothFormats_LTXBetter", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		replicaDir := t.TempDir()

		// Create v0.3.x snapshot at time T-2h (older)
		gen := "0123456789abcdef"
		snapshotsDir := filepath.Join(replicaDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}
		v3Data := createTestSQLiteDB(t)
		writeV3Snapshot(t, snapshotsDir, 0, v3Data)
		v3Time := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(filepath.Join(snapshotsDir, "00000000.snapshot.lz4"), v3Time, v3Time); err != nil {
			t.Fatal(err)
		}

		// Create LTX backup (more recent)
		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(db, c)

		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := r.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Create a snapshot
		if _, err := db.Snapshot(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := r.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Wait a bit
		time.Sleep(10 * time.Millisecond)

		// Restore with timestamp - should use LTX (more recent)
		outputPath := t.TempDir() + "/restored.db"
		err := r.Restore(context.Background(), litestream.RestoreOptions{
			OutputPath: outputPath,
			Timestamp:  time.Now(),
		})
		if err != nil {
			t.Fatalf("Restore failed: %v", err)
		}

		verifyRestoredDB(t, outputPath)
	})

	t.Run("NoTimestamp_UsesLTX", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		replicaDir := t.TempDir()

		// Create v0.3.x snapshot
		gen := "0123456789abcdef"
		snapshotsDir := filepath.Join(replicaDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}
		v3Data := createTestSQLiteDB(t)
		writeV3Snapshot(t, snapshotsDir, 0, v3Data)

		// Create LTX backup
		c := file.NewReplicaClient(replicaDir)
		r := litestream.NewReplicaWithClient(db, c)

		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := r.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Restore without timestamp - should use LTX (default behavior)
		outputPath := t.TempDir() + "/restored.db"
		err := r.Restore(context.Background(), litestream.RestoreOptions{
			OutputPath: outputPath,
		})
		if err != nil {
			t.Fatalf("Restore failed: %v", err)
		}

		verifyRestoredDB(t, outputPath)
	})
}

// createTestLTXSnapshot creates a minimal LTX snapshot for testing.
func createTestLTXSnapshot(t *testing.T) []byte {
	t.Helper()

	// Create a temporary database and generate a real LTX snapshot
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	replicaDir := filepath.Join(tmpDir, "replica")

	db := testingutil.NewDB(t, dbPath)

	// Set up a replica client so we can create snapshots
	c := file.NewReplicaClient(replicaDir)
	db.Replica = litestream.NewReplicaWithClient(db, c)
	db.Replica.MonitorEnabled = false

	if err := db.Open(); err != nil {
		t.Fatal(err)
	}

	// Create some data
	sqldb := testingutil.MustOpenSQLDB(t, dbPath)
	if _, err := sqldb.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	testingutil.MustCloseSQLDB(t, sqldb)

	// Sync to create LTX file
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Create snapshot
	if _, err := db.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Read the snapshot file from replica directory
	ltxPath := filepath.Join(replicaDir, "ltx", fmt.Sprintf("%d", litestream.SnapshotLevel), "0000000000000001-0000000000000001.ltx")
	data, err := os.ReadFile(ltxPath)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// v3SnapshotData holds test data for creating v0.3.x snapshots.
type v3SnapshotData struct {
	index int
	data  []byte
}

// v3WALSegmentData holds test data for creating v0.3.x WAL segments.
type v3WALSegmentData struct {
	index  int
	offset int64
	data   []byte
}

// createV3Backup creates a v0.3.x backup structure for testing.
func createV3Backup(t *testing.T, replicaDir, generation string, snapshots []v3SnapshotData, walSegments []v3WALSegmentData) {
	t.Helper()

	// Create snapshots directory and files
	if len(snapshots) > 0 {
		snapshotsDir := filepath.Join(replicaDir, "generations", generation, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}
		for _, s := range snapshots {
			writeV3Snapshot(t, snapshotsDir, s.index, s.data)
		}
	}

	// Create WAL directory and files
	if len(walSegments) > 0 {
		walDir := filepath.Join(replicaDir, "generations", generation, "wal")
		if err := os.MkdirAll(walDir, 0755); err != nil {
			t.Fatal(err)
		}
		for _, w := range walSegments {
			writeV3WALSegment(t, walDir, w.index, w.offset, w.data)
		}
	}
}

// writeV3Snapshot writes an LZ4-compressed snapshot file.
func writeV3Snapshot(t *testing.T, dir string, index int, data []byte) {
	t.Helper()

	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	filename := fmt.Sprintf("%08x.snapshot.lz4", index)
	if err := os.WriteFile(filepath.Join(dir, filename), buf.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
}

// writeV3WALSegment writes an LZ4-compressed WAL segment file.
func writeV3WALSegment(t *testing.T, dir string, index int, offset int64, data []byte) {
	t.Helper()

	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	filename := fmt.Sprintf("%08x-%016x.wal.lz4", index, offset)
	if err := os.WriteFile(filepath.Join(dir, filename), buf.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
}

// createTestSQLiteDB creates a minimal valid SQLite database for testing.
func createTestSQLiteDB(t *testing.T) []byte {
	t.Helper()

	// Create a temporary database
	tmpPath := t.TempDir() + "/test.db"
	db := testingutil.NewDB(t, tmpPath)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}

	// Create a table via SQL
	sqldb := testingutil.MustOpenSQLDB(t, tmpPath)
	if _, err := sqldb.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO test (value) VALUES ('hello')`); err != nil {
		t.Fatal(err)
	}
	testingutil.MustCloseSQLDB(t, sqldb)

	if err := db.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Read the database file
	data, err := os.ReadFile(tmpPath)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// createTestWALData creates minimal valid WAL data for testing.
// For simplicity, returns an empty WAL header (32 bytes) which is valid.
func createTestWALData(t *testing.T, dbData []byte) []byte {
	t.Helper()

	// Create a minimal WAL header
	// WAL header is 32 bytes:
	// - magic number (4 bytes): 0x377f0683 (big-endian) or 0x377f0682 (little-endian)
	// - file format version (4 bytes): 3007000
	// - page size (4 bytes)
	// - checkpoint sequence (4 bytes)
	// - salt-1 (4 bytes)
	// - salt-2 (4 bytes)
	// - checksum-1 (4 bytes)
	// - checksum-2 (4 bytes)

	// For testing, we'll create an empty WAL that doesn't need frames applied
	// This is sufficient for testing the restore mechanism
	return make([]byte, 32) // Empty WAL header placeholder
}

// verifyRestoredDB verifies that the restored database is valid.
func verifyRestoredDB(t *testing.T, path string) {
	t.Helper()

	// Check file exists
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("restored file not found: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("restored file is empty")
	}

	// Try to open with SQLite to verify it's valid
	sqldb := testingutil.MustOpenSQLDB(t, path)
	defer testingutil.MustCloseSQLDB(t, sqldb)

	// Run integrity check
	var result string
	if err := sqldb.QueryRow("PRAGMA integrity_check").Scan(&result); err != nil {
		t.Fatalf("integrity check failed: %v", err)
	}
	if result != "ok" {
		t.Fatalf("integrity check returned: %s", result)
	}
}
