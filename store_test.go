package litestream_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestStore_CompactDB(t *testing.T) {
	t.Run("L1", func(t *testing.T) {
		db0, sqldb0 := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db0, sqldb0)

		db1, sqldb1 := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db1, sqldb1)

		levels := litestream.CompactionLevels{
			{Level: 0},
			{Level: 1, Interval: 1 * time.Second},
			{Level: 2, Interval: 500 * time.Millisecond},
		}
		s := litestream.NewStore([]*litestream.DB{db0, db1}, levels)
		s.CompactionMonitorEnabled = false
		if err := s.Open(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer s.Close(t.Context())

		if _, err := sqldb0.ExecContext(t.Context(), `CREATE TABLE t (id INT);`); err != nil {
			t.Fatal(err)
		}
		if _, err := sqldb0.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (100)`); err != nil {
			t.Fatal(err)
		} else if err := db0.Sync(t.Context()); err != nil {
			t.Fatal(err)
		} else if err := db0.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		_, err := s.CompactDB(t.Context(), db0, levels[1])
		require.NoError(t, err)

		// Re-compacting immediately should return an error indicating compaction
		// cannot proceed. This may be ErrCompactionTooEarly (detected timing conflict)
		// or ErrNoCompaction (no new files to compact). Both are valid outcomes
		// depending on whether we crossed a second boundary during the first compaction
		// (PrevCompactionAt truncates to seconds, causing edge cases at boundaries).
		_, err = s.CompactDB(t.Context(), db0, levels[1])
		require.True(t,
			errors.Is(err, litestream.ErrCompactionTooEarly) || errors.Is(err, litestream.ErrNoCompaction),
			"expected ErrCompactionTooEarly or ErrNoCompaction, got: %v", err)

		// Re-compacting after the interval should show that there is nothing to compact.
		time.Sleep(levels[1].Interval)
		_, err = s.CompactDB(t.Context(), db0, levels[1])
		require.ErrorIs(t, err, litestream.ErrNoCompaction)
	})

	t.Run("Snapshot", func(t *testing.T) {
		db0, sqldb0 := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db0, sqldb0)

		levels := litestream.CompactionLevels{
			{Level: 0},
			{Level: 1, Interval: 100 * time.Millisecond},
			{Level: 2, Interval: 500 * time.Millisecond},
		}
		s := litestream.NewStore([]*litestream.DB{db0}, levels)
		s.CompactionMonitorEnabled = false
		if err := s.Open(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer s.Close(t.Context())

		if _, err := sqldb0.ExecContext(t.Context(), `CREATE TABLE t (id INT);`); err != nil {
			t.Fatal(err)
		}
		if _, err := sqldb0.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (100)`); err != nil {
			t.Fatal(err)
		} else if err := db0.Sync(t.Context()); err != nil {
			t.Fatal(err)
		} else if err := db0.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		if _, err := s.CompactDB(t.Context(), db0, s.SnapshotLevel()); err != nil {
			t.Fatal(err)
		}

		// Re-compacting immediately should return an error that there's nothing to compact.
		if _, err := s.CompactDB(t.Context(), db0, s.SnapshotLevel()); !errors.Is(err, litestream.ErrCompactionTooEarly) {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	// Regression test for GitHub issue #877: level 9 compaction fails with
	// "page size not initialized yet" error when attempted before DB initialization.
	t.Run("DBNotReady", func(t *testing.T) {
		db0 := testingutil.MustOpenDB(t)
		defer testingutil.MustCloseDB(t, db0)

		levels := litestream.CompactionLevels{
			{Level: 0},
			{Level: 1, Interval: 100 * time.Millisecond},
		}
		s := litestream.NewStore([]*litestream.DB{db0}, levels)
		s.CompactionMonitorEnabled = false
		if err := s.Open(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer s.Close(t.Context())

		// Attempt snapshot before DB is initialized (page size not set).
		// This reproduces the timing issue where level 9 compaction fires
		// immediately at startup before db.Sync() has been called.
		if _, err := s.CompactDB(t.Context(), db0, s.SnapshotLevel()); !errors.Is(err, litestream.ErrDBNotReady) {
			t.Fatalf("expected ErrDBNotReady, got: %v", err)
		}
	})
}

func TestStore_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const factor = 1

	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = factor * 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = file.NewReplicaClient(t.TempDir())
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb)

	store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{
		{Level: 0},
		{Level: 1, Interval: factor * 200 * time.Millisecond},
		{Level: 2, Interval: factor * 500 * time.Millisecond},
	})
	store.SnapshotInterval = factor * 1 * time.Second
	if err := store.Open(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer store.Close(t.Context())

	// Create initial table
	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);`); err != nil {
		t.Fatal(err)
	}

	// Run test for a fixed duration.
	done := make(chan struct{})
	time.AfterFunc(10*time.Second, func() { close(done) })

	// Channel for insert errors
	insertErr := make(chan error, 1)

	// WaitGroup to ensure insert goroutine completes before cleanup
	var wg sync.WaitGroup

	// Wait for insert goroutine to finish before cleanup & surface any errors.
	defer func() {
		wg.Wait()

		select {
		case err := <-insertErr:
			t.Fatalf("insert error during test: %v", err)
		default:
			// No insert errors
		}
	}()

	// Start goroutine to continuously insert records
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(factor * 10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-t.Context().Done():
				return
			case <-done:
				return
			case <-ticker.C:
				if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO t (val) VALUES (?);`, time.Now().String()); err != nil {
					// Check if we're shutting down
					select {
					case <-done:
						// Expected during shutdown, just exit
						return
					default:
						// Real error, send it
						select {
						case insertErr <- err:
						default:
						}
						return
					}
				}
			}
		}
	}()

	// Periodically snapshot, restore and validate
	ticker := time.NewTicker(factor * 500 * time.Millisecond)
	defer ticker.Stop()

	for i := 0; ; i++ {
		select {
		case <-t.Context().Done():
			return
		case <-done:
			return
		case <-ticker.C:
			// Restore database to a temporary location.
			outputPath := filepath.Join(t.TempDir(), fmt.Sprintf("restore-%d.db", i))
			if err := db.Replica.Restore(t.Context(), litestream.RestoreOptions{
				OutputPath: outputPath,
			}); err != nil {
				t.Fatal(err)
			}

			func() {
				restoreDB := testingutil.MustOpenSQLDB(t, outputPath)
				defer testingutil.MustCloseSQLDB(t, restoreDB)

				var result string
				if err := restoreDB.QueryRowContext(t.Context(), `PRAGMA integrity_check;`).Scan(&result); err != nil {
					t.Fatal(err)
				} else if result != "ok" {
					t.Fatalf("integrity check failed: %s", result)
				}

				var count int
				if err := restoreDB.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM t`).Scan(&count); err != nil {
					t.Fatal(err)
				} else if count == 0 {
					t.Fatal("no records found in restored database")
				}
				t.Logf("restored database: %d records", count)
			}()
		}
	}
}

// TestStore_SnapshotInterval_Default ensures that the default snapshot interval
// is preserved when not explicitly set (regression test for issue #689).
func TestStore_SnapshotInterval_Default(t *testing.T) {
	// Create a store with no databases and no levels
	store := litestream.NewStore(nil, nil)

	// Verify default snapshot interval is set
	if store.SnapshotInterval != litestream.DefaultSnapshotInterval {
		t.Errorf("expected default snapshot interval of %v, got %v",
			litestream.DefaultSnapshotInterval, store.SnapshotInterval)
	}

	// Verify default is 24 hours
	if store.SnapshotInterval != 24*time.Hour {
		t.Errorf("expected default snapshot interval of 24h, got %v",
			store.SnapshotInterval)
	}
}
