package litestream_test

import (
	"context"
	"errors"
	"testing"
	"time"

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

func TestReplica_CalcRestorePlan(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	t.Run("SnapshotOnly", func(t *testing.T) {
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID) (ltx.FileIterator, error) {
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
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID) (ltx.FileIterator, error) {
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

	t.Run("ErrNonContiguousFiles", func(t *testing.T) {
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID) (ltx.FileIterator, error) {
			switch level {
			case litestream.SnapshotLevel:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: litestream.SnapshotLevel, MinTXID: 1, MaxTXID: 5},
				}), nil
			case 1:
				return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{
					{Level: 1, MinTXID: 8, MaxTXID: 9},
				}), nil
			default:
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
		}

		_, err := litestream.CalcRestorePlan(context.Background(), r.Client, 10, time.Time{}, r.Logger())
		if err == nil || err.Error() != `non-contiguous transaction files: prev=0000000000000005 filename=0000000000000008-0000000000000009.ltx` {
			t.Fatalf("unexpected error: %q", err)
		}
	})

	t.Run("ErrTxNotAvailable", func(t *testing.T) {
		var c mock.ReplicaClient
		r := litestream.NewReplicaWithClient(db, &c)
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID) (ltx.FileIterator, error) {
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
		c.LTXFilesFunc = func(ctx context.Context, level int, seek ltx.TXID) (ltx.FileIterator, error) {
			return ltx.NewFileInfoSliceIterator(nil), nil
		}
		r := litestream.NewReplicaWithClient(db, &c)

		_, err := litestream.CalcRestorePlan(context.Background(), r.Client, 5, time.Time{}, r.Logger())
		if !errors.Is(err, litestream.ErrTxNotAvailable) {
			t.Fatalf("expected ErrTxNotAvailable, got %v", err)
		}
	})
}
