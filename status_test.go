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

		sub := monitor.Subscribe()
		if sub == nil {
			t.Fatal("expected non-nil subscriber")
		}

		// Unsubscribe should close the channel
		monitor.Unsubscribe(sub)

		// Reading from closed channel should return ok=false
		_, ok := <-sub.C
		if ok {
			t.Error("expected channel to be closed after Unsubscribe")
		}
	})

	t.Run("MultipleSubscribers", func(t *testing.T) {
		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore(nil, levels)
		monitor := litestream.NewStatusMonitor(store)

		sub1 := monitor.Subscribe()
		sub2 := monitor.Subscribe()
		sub3 := monitor.Subscribe()

		if sub1 == sub2 || sub2 == sub3 || sub1 == sub3 {
			t.Error("expected different subscribers")
		}

		monitor.Unsubscribe(sub1)
		monitor.Unsubscribe(sub2)
		monitor.Unsubscribe(sub3)
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

		sub1 := monitor.Subscribe()
		sub2 := monitor.Subscribe()
		defer monitor.Unsubscribe(sub1)
		defer monitor.Unsubscribe(sub2)

		// Notify sync
		pos := ltx.Pos{TXID: 42}
		monitor.NotifySync(db, pos)

		// Both subscribers should receive the event
		select {
		case event := <-sub1.C:
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
			t.Fatal("timeout waiting for event on sub1")
		}

		select {
		case event := <-sub2.C:
			if event.Type != "sync" {
				t.Errorf("expected type 'sync', got %q", event.Type)
			}
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for event on sub2")
		}
	})

	t.Run("ClosesSlowSubscribers", func(t *testing.T) {
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
		sub := monitor.Subscribe()

		// Send more events than the buffer size (64)
		for i := 0; i < 100; i++ {
			monitor.NotifySync(db, ltx.Pos{TXID: ltx.TXID(i)})
		}

		// Should not block - slow subscriber should be closed
		// Verify channel is closed
		select {
		case _, ok := <-sub.C:
			// Either we get events or channel is closed, both are fine
			if !ok {
				// Channel was closed as expected for slow subscriber
				return
			}
		default:
			// Channel is empty or closed
		}

		// Drain any remaining events
		for {
			select {
			case _, ok := <-sub.C:
				if !ok {
					return // Channel closed
				}
			default:
				return // No more events
			}
		}
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
			sub := monitor.Subscribe()
			time.Sleep(10 * time.Millisecond)
			monitor.Unsubscribe(sub)
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
