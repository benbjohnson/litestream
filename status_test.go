package litestream_test

import (
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
	"github.com/superfly/ltx"
)

func TestStatusMonitor_Subscribe(t *testing.T) {
	t.Run("SubscribeAndUnsubscribe", func(t *testing.T) {
		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore(nil, levels)
		monitor := litestream.NewStatusMonitor(store)

		ch := monitor.Subscribe()
		if ch == nil {
			t.Fatal("expected non-nil channel")
		}

		// Unsubscribe should close the channel
		monitor.Unsubscribe(ch)

		// Reading from closed channel should return ok=false
		_, ok := <-ch
		if ok {
			t.Error("expected channel to be closed after Unsubscribe")
		}
	})

	t.Run("MultipleSubscribers", func(t *testing.T) {
		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore(nil, levels)
		monitor := litestream.NewStatusMonitor(store)

		ch1 := monitor.Subscribe()
		ch2 := monitor.Subscribe()
		ch3 := monitor.Subscribe()

		if ch1 == ch2 || ch2 == ch3 || ch1 == ch3 {
			t.Error("expected different channels for different subscribers")
		}

		monitor.Unsubscribe(ch1)
		monitor.Unsubscribe(ch2)
		monitor.Unsubscribe(ch3)
	})
}

func TestStatusMonitor_NotifySync(t *testing.T) {
	t.Run("BroadcastsToSubscribers", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore([]*litestream.DB{db}, levels)
		store.CompactionMonitorEnabled = false

		if err := store.Open(t.Context()); err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer store.Close(t.Context())

		monitor := litestream.NewStatusMonitor(store)

		ch1 := monitor.Subscribe()
		ch2 := monitor.Subscribe()
		defer monitor.Unsubscribe(ch1)
		defer monitor.Unsubscribe(ch2)

		// Notify sync
		pos := ltx.Pos{TXID: 42}
		monitor.NotifySync(db, pos)

		// Both subscribers should receive the event
		select {
		case event := <-ch1:
			if event.Type != "sync" {
				t.Errorf("expected type 'sync', got %q", event.Type)
			}
			if event.Database == nil {
				t.Fatal("expected non-nil database in event")
			}
			if event.Database.ReplicaTXID != "000000000000002a" {
				t.Errorf("expected replica_txid '000000000000002a', got %q", event.Database.ReplicaTXID)
			}
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for event on ch1")
		}

		select {
		case event := <-ch2:
			if event.Type != "sync" {
				t.Errorf("expected type 'sync', got %q", event.Type)
			}
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for event on ch2")
		}
	})

	t.Run("DropsEventsForSlowSubscribers", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore([]*litestream.DB{db}, levels)
		store.CompactionMonitorEnabled = false

		if err := store.Open(t.Context()); err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer store.Close(t.Context())

		monitor := litestream.NewStatusMonitor(store)

		// Subscribe but don't read
		ch := monitor.Subscribe()
		defer monitor.Unsubscribe(ch)

		// Send more events than the buffer size (64)
		for i := 0; i < 100; i++ {
			monitor.NotifySync(db, ltx.Pos{TXID: ltx.TXID(i)})
		}

		// Should not block - events should be dropped for slow subscriber
		// This test passes if it doesn't hang
	})
}

func TestStatusMonitor_GetFullStatus(t *testing.T) {
	t.Run("EmptyStore", func(t *testing.T) {
		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore(nil, levels)
		monitor := litestream.NewStatusMonitor(store)

		statuses := monitor.GetFullStatus()
		if len(statuses) != 0 {
			t.Errorf("expected empty statuses, got %d", len(statuses))
		}
	})

	t.Run("WithDatabases", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore([]*litestream.DB{db}, levels)
		store.CompactionMonitorEnabled = false

		if err := store.Open(t.Context()); err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer store.Close(t.Context())

		monitor := litestream.NewStatusMonitor(store)

		statuses := monitor.GetFullStatus()
		if len(statuses) != 1 {
			t.Fatalf("expected 1 status, got %d", len(statuses))
		}

		status := statuses[0]
		if status.Path != db.Path() {
			t.Errorf("expected path %q, got %q", db.Path(), status.Path)
		}
		if !status.Enabled {
			t.Error("expected database to be enabled")
		}
	})

	t.Run("WithSyncedDatabase", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore([]*litestream.DB{db}, levels)
		store.CompactionMonitorEnabled = false

		if err := store.Open(t.Context()); err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer store.Close(t.Context())

		// Create table and sync
		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := db.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		monitor := litestream.NewStatusMonitor(store)
		statuses := monitor.GetFullStatus()

		if len(statuses) != 1 {
			t.Fatalf("expected 1 status, got %d", len(statuses))
		}

		status := statuses[0]
		if status.Status != "ok" {
			t.Errorf("expected status 'ok', got %q", status.Status)
		}
		if status.LocalTXID == "" {
			t.Error("expected non-empty local_txid")
		}
		if status.ReplicaTXID == "" {
			t.Error("expected non-empty replica_txid")
		}
		if status.ReplicaType != "file" {
			t.Errorf("expected replica_type 'file', got %q", status.ReplicaType)
		}
	})
}

func TestStatusMonitor_ConcurrentAccess(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	levels := litestream.CompactionLevels{{Level: 0}}
	store := litestream.NewStore([]*litestream.DB{db}, levels)
	store.CompactionMonitorEnabled = false

	if err := store.Open(t.Context()); err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close(t.Context())

	monitor := litestream.NewStatusMonitor(store)

	var wg sync.WaitGroup

	// Concurrent subscribers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := monitor.Subscribe()
			time.Sleep(10 * time.Millisecond)
			monitor.Unsubscribe(ch)
		}()
	}

	// Concurrent notifications
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(txid int) {
			defer wg.Done()
			monitor.NotifySync(db, ltx.Pos{TXID: ltx.TXID(txid)})
		}(i)
	}

	// Concurrent status reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = monitor.GetFullStatus()
		}()
	}

	wg.Wait()
}
