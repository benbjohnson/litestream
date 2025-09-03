package litestream_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
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
		LTXFilesFunc: func(ctx context.Context, level int, seek ltx.TXID) (ltx.FileIterator, error) {
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
