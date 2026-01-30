package s3

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
)

func TestLeaser_AcquireLease_NewLease(t *testing.T) {
	var putCalled atomic.Bool
	var receivedIfNoneMatch string
	currentETag := `"etag-1"`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			putCalled.Store(true)
			receivedIfNoneMatch = r.Header.Get("If-None-Match")

			body, _ := io.ReadAll(r.Body)
			r.Body.Close()

			var lease litestream.Lease
			if err := json.Unmarshal(body, &lease); err != nil {
				t.Errorf("failed to unmarshal lock file: %v", err)
			}

			if lease.Generation != 1 {
				t.Errorf("expected generation=1, got %d", lease.Generation)
			}

			w.Header().Set("ETag", currentETag)
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected method: %s", r.Method)
		}
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	leaser := NewLeaser(client)
	leaser.TTL = 10 * time.Second
	leaser.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	lease, err := leaser.AcquireLease(ctx)
	if err != nil {
		t.Fatalf("AcquireLease() error: %v", err)
	}

	if !putCalled.Load() {
		t.Error("expected PUT to be called")
	}
	if receivedIfNoneMatch != "*" {
		t.Errorf("expected If-None-Match: *, got %q", receivedIfNoneMatch)
	}
	if lease.Generation != 1 {
		t.Errorf("expected generation=1, got %d", lease.Generation)
	}
	if lease.ETag != currentETag {
		t.Errorf("expected ETag=%q, got %q", currentETag, lease.ETag)
	}
	if lease.TTL() < 9*time.Second || lease.TTL() > 10*time.Second {
		t.Errorf("unexpected TTL: %v", lease.TTL())
	}
}

func TestLeaser_AcquireLease_ExpiredLease(t *testing.T) {
	oldETag := `"old-etag"`
	newETag := `"new-etag"`
	var receivedIfMatch string

	expiredLease := litestream.Lease{
		Generation: 5,
		ExpiresAt:  time.Now().Add(-1 * time.Hour),
		Owner:      "previous-owner",
	}
	expiredData, _ := json.Marshal(expiredLease)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("ETag", oldETag)
			w.WriteHeader(http.StatusOK)
			w.Write(expiredData)
		case http.MethodPut:
			receivedIfMatch = r.Header.Get("If-Match")
			w.Header().Set("ETag", newETag)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	leaser := NewLeaser(client)
	leaser.TTL = 30 * time.Second
	leaser.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	lease, err := leaser.AcquireLease(ctx)
	if err != nil {
		t.Fatalf("AcquireLease() error: %v", err)
	}

	if receivedIfMatch != oldETag {
		t.Errorf("expected If-Match=%q, got %q", oldETag, receivedIfMatch)
	}
	if lease.Generation != 6 {
		t.Errorf("expected generation=6 (previous+1), got %d", lease.Generation)
	}
	if lease.ETag != newETag {
		t.Errorf("expected ETag=%q, got %q", newETag, lease.ETag)
	}
}

func TestLeaser_AcquireLease_ActiveLease(t *testing.T) {
	activeLease := litestream.Lease{
		Generation: 3,
		ExpiresAt:  time.Now().Add(5 * time.Minute),
		Owner:      "active-owner",
	}
	activeData, _ := json.Marshal(activeLease)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("ETag", `"active-etag"`)
			w.WriteHeader(http.StatusOK)
			w.Write(activeData)
		case http.MethodPut:
			t.Error("PUT should not be called when lease is active")
		}
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	leaser := NewLeaser(client)
	leaser.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	_, err := leaser.AcquireLease(ctx)
	if err == nil {
		t.Fatal("expected error for active lease")
	}

	var leaseErr *litestream.LeaseExistsError
	if !errors.As(err, &leaseErr) {
		t.Fatalf("expected *LeaseExistsError, got %T: %v", err, err)
	}
	if leaseErr.Owner != "active-owner" {
		t.Errorf("expected Owner=%q, got %q", "active-owner", leaseErr.Owner)
	}
	if leaseErr.ExpiresAt.IsZero() {
		t.Error("expected non-zero ExpiresAt")
	}
	if leaseErr.ExpiresAt.Before(time.Now()) {
		t.Errorf("expected future ExpiresAt, got %v", leaseErr.ExpiresAt)
	}
}

func TestLeaser_AcquireLease_RaceCondition412(t *testing.T) {
	var getCalls atomic.Int32
	winnerLease := litestream.Lease{
		Generation: 1,
		ExpiresAt:  time.Now().Add(30 * time.Second),
		Owner:      "race-winner",
	}
	winnerData, _ := json.Marshal(winnerLease)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			calls := getCalls.Add(1)
			if calls == 1 {
				w.WriteHeader(http.StatusNotFound)
			} else {
				w.Header().Set("ETag", `"winner-etag"`)
				w.WriteHeader(http.StatusOK)
				w.Write(winnerData)
			}
		case http.MethodPut:
			w.WriteHeader(http.StatusPreconditionFailed)
		}
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	leaser := NewLeaser(client)
	leaser.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	_, err := leaser.AcquireLease(ctx)
	if err == nil {
		t.Fatal("expected error for 412 response")
	}

	var leaseErr *litestream.LeaseExistsError
	if !errors.As(err, &leaseErr) {
		t.Fatalf("expected *LeaseExistsError, got %T: %v", err, err)
	}
	if leaseErr.Owner != "race-winner" {
		t.Errorf("expected Owner=%q, got %q", "race-winner", leaseErr.Owner)
	}
	if leaseErr.ExpiresAt.IsZero() {
		t.Error("expected non-zero ExpiresAt after re-read")
	}
}

func TestLeaser_RenewLease(t *testing.T) {
	oldETag := `"old-etag"`
	newETag := `"new-etag"`
	var receivedIfMatch string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			receivedIfMatch = r.Header.Get("If-Match")

			body, _ := io.ReadAll(r.Body)
			r.Body.Close()

			var lease litestream.Lease
			if err := json.Unmarshal(body, &lease); err != nil {
				t.Errorf("failed to unmarshal: %v", err)
			}

			if lease.Generation != 5 {
				t.Errorf("expected generation=5, got %d", lease.Generation)
			}

			w.Header().Set("ETag", newETag)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	leaser := NewLeaser(client)
	leaser.TTL = 30 * time.Second
	leaser.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	oldLease := &litestream.Lease{
		Generation: 5,
		ExpiresAt:  time.Now().Add(5 * time.Second),
		Owner:      "me",
		ETag:       oldETag,
	}

	newLease, err := leaser.RenewLease(ctx, oldLease)
	if err != nil {
		t.Fatalf("RenewLease() error: %v", err)
	}

	if receivedIfMatch != oldETag {
		t.Errorf("expected If-Match=%q, got %q", oldETag, receivedIfMatch)
	}
	if newLease.Generation != 5 {
		t.Errorf("expected generation=5, got %d", newLease.Generation)
	}
	if newLease.ETag != newETag {
		t.Errorf("expected ETag=%q, got %q", newETag, newLease.ETag)
	}
	if newLease.TTL() < 29*time.Second {
		t.Errorf("expected TTL ~30s, got %v", newLease.TTL())
	}
}

func TestLeaser_RenewLease_LostLease(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusPreconditionFailed)
		}
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	leaser := NewLeaser(client)
	leaser.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	oldLease := &litestream.Lease{
		Generation: 5,
		ExpiresAt:  time.Now().Add(5 * time.Second),
		Owner:      "me",
		ETag:       `"stale-etag"`,
	}

	_, err := leaser.RenewLease(ctx, oldLease)
	if err != litestream.ErrLeaseNotHeld {
		t.Errorf("expected ErrLeaseNotHeld, got %v", err)
	}
}

func TestLeaser_RenewLease_NilLease(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("no request should be made for nil lease")
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	leaser := NewLeaser(client)
	leaser.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	_, err := leaser.RenewLease(ctx, nil)
	if err != litestream.ErrLeaseNotHeld {
		t.Errorf("expected ErrLeaseNotHeld for nil lease, got %v", err)
	}
}

func TestLeaser_ReleaseLease(t *testing.T) {
	var deleteCalled atomic.Bool
	var receivedIfMatch string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleteCalled.Store(true)
			receivedIfMatch = r.Header.Get("If-Match")
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	leaser := NewLeaser(client)
	leaser.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	lease := &litestream.Lease{
		Generation: 5,
		ExpiresAt:  time.Now().Add(5 * time.Minute),
		Owner:      "me",
		ETag:       `"my-etag"`,
	}

	err := leaser.ReleaseLease(ctx, lease)
	if err != nil {
		t.Fatalf("ReleaseLease() error: %v", err)
	}

	if !deleteCalled.Load() {
		t.Error("expected DELETE to be called")
	}
	if receivedIfMatch != `"my-etag"` {
		t.Errorf("expected If-Match=%q, got %q", `"my-etag"`, receivedIfMatch)
	}
}

func TestLeaser_ReleaseLease_StaleETag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusPreconditionFailed)
		}
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	leaser := NewLeaser(client)
	leaser.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	lease := &litestream.Lease{
		Generation: 5,
		ExpiresAt:  time.Now().Add(5 * time.Minute),
		Owner:      "me",
		ETag:       `"stale-etag"`,
	}

	err := leaser.ReleaseLease(ctx, lease)
	if err != litestream.ErrLeaseNotHeld {
		t.Errorf("expected ErrLeaseNotHeld for stale ETag, got %v", err)
	}
}

func TestLeaser_ReleaseLease_AlreadyDeleted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	leaser := NewLeaser(client)
	leaser.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	lease := &litestream.Lease{
		Generation: 5,
		ExpiresAt:  time.Now().Add(5 * time.Minute),
		Owner:      "me",
		ETag:       `"my-etag"`,
	}

	err := leaser.ReleaseLease(ctx, lease)
	if err != nil {
		t.Fatalf("ReleaseLease() should not error for already-deleted lock: %v", err)
	}
}

func TestLeaser_ReleaseLease_NilLease(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("no request should be made for nil lease")
	}))
	defer server.Close()

	client := newTestClient(server.URL)
	leaser := NewLeaser(client)
	leaser.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	err := leaser.ReleaseLease(ctx, nil)
	if err != litestream.ErrLeaseNotHeld {
		t.Errorf("expected ErrLeaseNotHeld for nil lease, got %v", err)
	}
}

func TestLeaser_ConcurrentAcquisition(t *testing.T) {
	var mu sync.Mutex
	var leaseHolder string
	currentETag := ""
	etagCounter := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch r.Method {
		case http.MethodGet:
			if leaseHolder == "" {
				w.WriteHeader(http.StatusNotFound)
			} else {
				lease := litestream.Lease{
					Generation: 1,
					ExpiresAt:  time.Now().Add(30 * time.Second),
					Owner:      leaseHolder,
				}
				data, _ := json.Marshal(lease)
				w.Header().Set("ETag", currentETag)
				w.WriteHeader(http.StatusOK)
				w.Write(data)
			}
		case http.MethodPut:
			ifNoneMatch := r.Header.Get("If-None-Match")
			ifMatch := r.Header.Get("If-Match")

			if ifNoneMatch == "*" && leaseHolder != "" {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			if ifMatch != "" && ifMatch != currentETag {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}

			body, _ := io.ReadAll(r.Body)
			r.Body.Close()
			var lease litestream.Lease
			json.Unmarshal(body, &lease)

			leaseHolder = lease.Owner
			etagCounter++
			currentETag = `"etag-` + string(rune('0'+etagCounter)) + `"`

			w.Header().Set("ETag", currentETag)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	const numClients = 10
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var failCount atomic.Int32

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			client := newTestClient(server.URL)
			leaser := NewLeaser(client)
			leaser.Owner = string(rune('A' + id))
			leaser.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

			ctx := context.Background()
			if err := client.Init(ctx); err != nil {
				t.Errorf("client %d Init() error: %v", id, err)
				return
			}

			_, err := leaser.AcquireLease(ctx)
			if err == nil {
				successCount.Add(1)
			} else {
				failCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	if successCount.Load() != 1 {
		t.Errorf("expected exactly 1 successful acquisition, got %d", successCount.Load())
	}
	if failCount.Load() != numClients-1 {
		t.Errorf("expected %d failures, got %d", numClients-1, failCount.Load())
	}
}

func TestLeaser_LockKey(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantKey string
	}{
		{
			name:    "EmptyPath",
			path:    "",
			wantKey: "lock.json",
		},
		{
			name:    "SimplePath",
			path:    "replica",
			wantKey: "replica/lock.json",
		},
		{
			name:    "NestedPath",
			path:    "my/db/replica",
			wantKey: "my/db/replica/lock.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewReplicaClient()
			client.Path = tt.path

			leaser := NewLeaser(client)
			if got := leaser.lockKey(); got != tt.wantKey {
				t.Errorf("lockKey() = %q, want %q", got, tt.wantKey)
			}
		})
	}
}

func TestLeaser_Type(t *testing.T) {
	client := NewReplicaClient()
	leaser := NewLeaser(client)

	if got := leaser.Type(); got != "s3" {
		t.Errorf("Type() = %q, want %q", got, "s3")
	}
}

func newTestClient(serverURL string) *ReplicaClient {
	client := NewReplicaClient()
	client.Bucket = "test-bucket"
	client.Path = "test-path"
	client.Region = "us-east-1"
	client.Endpoint = serverURL
	client.ForcePathStyle = true
	client.AccessKeyID = "test-access-key"
	client.SecretAccessKey = "test-secret-key"
	return client
}
