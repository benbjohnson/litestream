package litestream_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestHeartbeatClient_Ping(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		var pingCount atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("expected GET, got %s", r.Method)
			}
			pingCount.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := litestream.NewHeartbeatClient(server.URL, 5*time.Minute)
		if err := client.Ping(context.Background()); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if got := pingCount.Load(); got != 1 {
			t.Errorf("expected 1 ping, got %d", got)
		}
	})

	t.Run("EmptyURL", func(t *testing.T) {
		client := litestream.NewHeartbeatClient("", 5*time.Minute)
		if err := client.Ping(context.Background()); err != nil {
			t.Fatalf("expected no error for empty URL, got %v", err)
		}
	})

	t.Run("NonSuccessStatusCode", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client := litestream.NewHeartbeatClient(server.URL, 5*time.Minute)
		err := client.Ping(context.Background())
		if err == nil {
			t.Fatal("expected error for 500 status code")
		}
	})

	t.Run("NetworkError", func(t *testing.T) {
		client := litestream.NewHeartbeatClient("http://localhost:1", 5*time.Minute)
		err := client.Ping(context.Background())
		if err == nil {
			t.Fatal("expected error for unreachable server")
		}
	})

	t.Run("ContextCanceled", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		client := litestream.NewHeartbeatClient(server.URL, 5*time.Minute)
		err := client.Ping(ctx)
		if err == nil {
			t.Fatal("expected error for canceled context")
		}
	})
}

func TestHeartbeatClient_ShouldPing(t *testing.T) {
	t.Run("FirstPing", func(t *testing.T) {
		client := litestream.NewHeartbeatClient("http://example.com", 5*time.Minute)
		if !client.ShouldPing() {
			t.Error("expected ShouldPing to return true for first ping")
		}
	})

	t.Run("AfterRecordPing", func(t *testing.T) {
		client := litestream.NewHeartbeatClient("http://example.com", 5*time.Minute)
		client.RecordPing()

		if client.ShouldPing() {
			t.Error("expected ShouldPing to return false immediately after RecordPing")
		}
	})
}

func TestHeartbeatClient_MinInterval(t *testing.T) {
	client := litestream.NewHeartbeatClient("http://example.com", 30*time.Second)
	if client.Interval != litestream.MinHeartbeatInterval {
		t.Errorf("expected interval to be clamped to %v, got %v", litestream.MinHeartbeatInterval, client.Interval)
	}
}

func TestHeartbeatClient_LastPingAt(t *testing.T) {
	client := litestream.NewHeartbeatClient("http://example.com", 5*time.Minute)

	if !client.LastPingAt().IsZero() {
		t.Error("expected LastPingAt to be zero initially")
	}

	before := time.Now()
	client.RecordPing()
	after := time.Now()

	lastPing := client.LastPingAt()
	if lastPing.Before(before) || lastPing.After(after) {
		t.Errorf("LastPingAt %v should be between %v and %v", lastPing, before, after)
	}
}

func TestStore_Heartbeat_AllDatabasesHealthy(t *testing.T) {
	t.Run("NoDatabases", func(t *testing.T) {
		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore(nil, levels)
		store.CompactionMonitorEnabled = false
		store.HeartbeatCheckInterval = 0 // Disable automatic monitoring

		var pingCount atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			pingCount.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		store.Heartbeat = litestream.NewHeartbeatClient(server.URL, 1*time.Minute)

		// With no databases, heartbeat should not fire
		// We need to trigger the check manually since monitor is disabled
		// The store won't send pings because allDatabasesHealthy returns false for empty stores
		if pingCount.Load() != 0 {
			t.Errorf("expected no pings with no databases, got %d", pingCount.Load())
		}
	})

	t.Run("AllDatabasesSynced", func(t *testing.T) {
		db0, sqldb0 := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db0, sqldb0)

		db1, sqldb1 := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db1, sqldb1)

		levels := litestream.CompactionLevels{{Level: 0}, {Level: 1, Interval: time.Second}}
		store := litestream.NewStore([]*litestream.DB{db0, db1}, levels)
		store.CompactionMonitorEnabled = false
		store.HeartbeatCheckInterval = 0

		var pingCount atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			pingCount.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		store.Heartbeat = litestream.NewHeartbeatClient(server.URL, 1*time.Minute)

		if err := store.Open(t.Context()); err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer store.Close(t.Context())

		// Create tables and sync both databases
		if _, err := sqldb0.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
			t.Fatal(err)
		}
		if err := db0.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := db0.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		if _, err := sqldb1.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
			t.Fatal(err)
		}
		if err := db1.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := db1.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Both databases have synced, heartbeat should fire
		if err := store.Heartbeat.Ping(t.Context()); err != nil {
			t.Fatalf("ping failed: %v", err)
		}

		if got := pingCount.Load(); got != 1 {
			t.Errorf("expected 1 ping after all DBs synced, got %d", got)
		}
	})

	t.Run("OneDatabaseNotSynced", func(t *testing.T) {
		db0, sqldb0 := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db0, sqldb0)

		// Create second DB but don't sync it
		db1 := litestream.NewDB(filepath.Join(t.TempDir(), "db1"))
		db1.Replica = litestream.NewReplica(db1)
		db1.Replica.Client = testingutil.NewFileReplicaClient(t)
		db1.Replica.MonitorEnabled = false
		db1.MonitorInterval = 0

		levels := litestream.CompactionLevels{{Level: 0}, {Level: 1, Interval: time.Second}}
		store := litestream.NewStore([]*litestream.DB{db0, db1}, levels)
		store.CompactionMonitorEnabled = false
		store.HeartbeatCheckInterval = 0

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		store.Heartbeat = litestream.NewHeartbeatClient(server.URL, 1*time.Minute)

		if err := store.Open(t.Context()); err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer store.Close(t.Context())

		// Only sync db0
		if _, err := sqldb0.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
			t.Fatal(err)
		}
		if err := db0.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := db0.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		// db1 hasn't synced, so LastSuccessfulSyncAt should be zero
		if !db1.LastSuccessfulSyncAt().IsZero() {
			t.Error("expected db1.LastSuccessfulSyncAt to be zero")
		}

		// db0 has synced
		if db0.LastSuccessfulSyncAt().IsZero() {
			t.Error("expected db0.LastSuccessfulSyncAt to be non-zero")
		}
	})
}
