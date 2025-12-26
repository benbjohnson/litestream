package litestream_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
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
