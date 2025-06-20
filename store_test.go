package litestream_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
)

func TestStore_CompactDB(t *testing.T) {
	t.Run("L1", func(t *testing.T) {
		db0, sqldb0 := MustOpenDBs(t)
		defer MustCloseDBs(t, db0, sqldb0)

		db1, sqldb1 := MustOpenDBs(t)
		defer MustCloseDBs(t, db1, sqldb1)

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
		defer s.Close()

		if _, err := sqldb0.Exec(`CREATE TABLE t (id INT);`); err != nil {
			t.Fatal(err)
		}
		if _, err := sqldb0.Exec(`INSERT INTO t (id) VALUES (100)`); err != nil {
			t.Fatal(err)
		} else if err := db0.Sync(context.Background()); err != nil {
			t.Fatal(err)
		} else if err := db0.Replica.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		if _, err := s.CompactDB(t.Context(), db0, levels[1]); err != nil {
			t.Fatal(err)
		}

		// Re-compacting immediately should return an error that it's too soon.
		if _, err := s.CompactDB(t.Context(), db0, levels[1]); err != litestream.ErrCompactionTooEarly {
			t.Fatalf("unexpected error: %s", err)
		}

		// Re-compacting after the interval should show that there is nothing to compact.
		time.Sleep(levels[1].Interval)
		if _, err := s.CompactDB(t.Context(), db0, levels[1]); err != litestream.ErrNoCompaction {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("Snapshot", func(t *testing.T) {
		db0, sqldb0 := MustOpenDBs(t)
		defer MustCloseDBs(t, db0, sqldb0)

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
		defer s.Close()

		if _, err := sqldb0.Exec(`CREATE TABLE t (id INT);`); err != nil {
			t.Fatal(err)
		}
		if _, err := sqldb0.Exec(`INSERT INTO t (id) VALUES (100)`); err != nil {
			t.Fatal(err)
		} else if err := db0.Sync(context.Background()); err != nil {
			t.Fatal(err)
		} else if err := db0.Replica.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		if _, err := s.CompactDB(t.Context(), db0, s.SnapshotLevel()); err != nil {
			t.Fatal(err)
		}

		// Re-compacting immediately should return an error that there's nothing to compact.
		if _, err := s.CompactDB(t.Context(), db0, s.SnapshotLevel()); err != litestream.ErrCompactionTooEarly {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}
