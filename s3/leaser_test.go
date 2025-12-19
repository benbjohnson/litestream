package s3

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
)

func TestLeaser_Type(t *testing.T) {
	l := NewLeaser()
	if got, want := l.Type(), "s3"; got != want {
		t.Errorf("Type() = %q, want %q", got, want)
	}
}

func TestLeaser_Init_BucketRequired(t *testing.T) {
	l := NewLeaser()
	l.Bucket = ""
	l.Region = "us-east-1"

	err := l.Init(context.Background())
	if err == nil {
		t.Fatal("expected error for empty bucket")
	}
	if !strings.Contains(err.Error(), "bucket name is required") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestLeaser_AcquireLease_Fresh(t *testing.T) {
	// Mock S3 server that returns empty list initially and accepts PUT
	var putReceived atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.RawQuery, "list-type=2"):
			// ListObjectsV2 - return empty list
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<Name>test-bucket</Name>
					<Prefix>leases/</Prefix>
					<IsTruncated>false</IsTruncated>
				</ListBucketResult>`))

		case r.Method == http.MethodPut:
			// PutObject with conditional write
			if r.Header.Get("If-None-Match") != "*" {
				t.Error("expected If-None-Match: * header")
			}
			putReceived.Store(true)
			w.Header().Set("ETag", `"abc123"`)
			w.WriteHeader(http.StatusOK)

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	l := NewLeaser()
	l.Bucket = "test-bucket"
	l.Path = "leases"
	l.Region = "us-east-1"
	l.Endpoint = server.URL
	l.ForcePathStyle = true
	l.AccessKeyID = "test-key"
	l.SecretAccessKey = "test-secret"
	l.Owner = "test-owner"

	ctx := context.Background()
	lease, err := l.AcquireLease(ctx)
	if err != nil {
		t.Fatalf("AcquireLease() error: %v", err)
	}

	if !putReceived.Load() {
		t.Error("expected PUT request to be received")
	}
	if lease.Epoch != 1 {
		t.Errorf("Epoch = %d, want 1", lease.Epoch)
	}
	if lease.Owner != "test-owner" {
		t.Errorf("Owner = %q, want %q", lease.Owner, "test-owner")
	}
	if lease.Timeout != litestream.DefaultLeaseTimeout {
		t.Errorf("Timeout = %v, want %v", lease.Timeout, litestream.DefaultLeaseTimeout)
	}
}

func TestLeaser_AcquireLease_ExistingActive(t *testing.T) {
	existingLease := &litestream.Lease{
		Epoch:   5,
		Timeout: 30 * time.Second,
		Owner:   "other-instance",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.RawQuery, "list-type=2"):
			// Return one existing lease file
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<Name>test-bucket</Name>
					<Prefix>leases/</Prefix>
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>leases/0000000000000005.lock</Key>
						<LastModified>` + time.Now().Format(time.RFC3339) + `</LastModified>
					</Contents>
				</ListBucketResult>`))

		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, ".lock"):
			// GetObject - return existing lease with recent Last-Modified
			body, _ := json.Marshal(existingLease)
			w.Header().Set("Content-Type", "application/json")
			// Use current time as Last-Modified so lease is NOT expired
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)

		case r.Method == http.MethodPut:
			// Should NOT reach here - if we do, the test will fail
			t.Error("unexpected PUT request - lease should have been detected as active")
			w.WriteHeader(http.StatusBadRequest)

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	l := NewLeaser()
	l.Bucket = "test-bucket"
	l.Path = "leases"
	l.Region = "us-east-1"
	l.Endpoint = server.URL
	l.ForcePathStyle = true
	l.AccessKeyID = "test-key"
	l.SecretAccessKey = "test-secret"

	ctx := context.Background()
	_, err := l.AcquireLease(ctx)

	var leaseExistsErr *litestream.LeaseExistsError
	if !errors.As(err, &leaseExistsErr) {
		t.Fatalf("expected LeaseExistsError, got: %v", err)
	}
	if leaseExistsErr.Lease.Epoch != 5 {
		t.Errorf("existing lease epoch = %d, want 5", leaseExistsErr.Lease.Epoch)
	}
	if leaseExistsErr.Lease.Owner != "other-instance" {
		t.Errorf("existing lease owner = %q, want %q", leaseExistsErr.Lease.Owner, "other-instance")
	}
}

func TestLeaser_AcquireLease_ExpiredLease(t *testing.T) {
	// Lease with timeout=0 (expired)
	expiredLease := &litestream.Lease{
		Epoch:   3,
		Timeout: 0,
		Owner:   "old-instance",
	}

	var putReceived atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.RawQuery, "list-type=2"):
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<Name>test-bucket</Name>
					<Prefix>leases/</Prefix>
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>leases/0000000000000003.lock</Key>
						<LastModified>` + time.Now().Add(-time.Hour).Format(time.RFC3339) + `</LastModified>
					</Contents>
				</ListBucketResult>`))

		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "0000000000000003.lock"):
			body, _ := json.Marshal(expiredLease)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Last-Modified", time.Now().Add(-time.Hour).Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)

		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "0000000000000004.lock"):
			putReceived.Store(true)
			w.Header().Set("ETag", `"abc123"`)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodDelete:
			// Background reaping of old lease
			w.WriteHeader(http.StatusNoContent)

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	l := NewLeaser()
	l.Bucket = "test-bucket"
	l.Path = "leases"
	l.Region = "us-east-1"
	l.Endpoint = server.URL
	l.ForcePathStyle = true
	l.AccessKeyID = "test-key"
	l.SecretAccessKey = "test-secret"

	ctx := context.Background()
	lease, err := l.AcquireLease(ctx)
	if err != nil {
		t.Fatalf("AcquireLease() error: %v", err)
	}

	if !putReceived.Load() {
		t.Error("expected PUT request for new lease")
	}
	if lease.Epoch != 4 {
		t.Errorf("Epoch = %d, want 4 (previous was 3)", lease.Epoch)
	}
}

func TestLeaser_AcquireLease_RaceCondition(t *testing.T) {
	// Simulates race condition where another instance wins the lease
	var putCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.RawQuery, "list-type=2"):
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<Name>test-bucket</Name>
					<Prefix>leases/</Prefix>
					<IsTruncated>false</IsTruncated>
				</ListBucketResult>`))

		case r.Method == http.MethodPut:
			putCount.Add(1)
			// Return 412 Precondition Failed - another instance won
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
				<Error>
					<Code>PreconditionFailed</Code>
					<Message>At least one of the preconditions you specified did not hold.</Message>
				</Error>`))

		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, ".lock"):
			// Return the winning lease
			winningLease := &litestream.Lease{
				Epoch:   1,
				Timeout: 30 * time.Second,
				Owner:   "winner-instance",
			}
			body, _ := json.Marshal(winningLease)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Last-Modified", time.Now().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	l := NewLeaser()
	l.Bucket = "test-bucket"
	l.Path = "leases"
	l.Region = "us-east-1"
	l.Endpoint = server.URL
	l.ForcePathStyle = true
	l.AccessKeyID = "test-key"
	l.SecretAccessKey = "test-secret"

	ctx := context.Background()
	_, err := l.AcquireLease(ctx)

	var leaseExistsErr *litestream.LeaseExistsError
	if !errors.As(err, &leaseExistsErr) {
		t.Fatalf("expected LeaseExistsError, got: %v", err)
	}
	if leaseExistsErr.Lease.Owner != "winner-instance" {
		t.Errorf("winning lease owner = %q, want %q", leaseExistsErr.Lease.Owner, "winner-instance")
	}
}

func TestLeaser_RenewLease(t *testing.T) {
	var putPath string
	var deleteReceived atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.RawQuery, "list-type=2"):
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<Name>test-bucket</Name>
					<Prefix>leases/</Prefix>
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>leases/0000000000000005.lock</Key>
						<LastModified>` + time.Now().Format(time.RFC3339) + `</LastModified>
					</Contents>
				</ListBucketResult>`))

		case r.Method == http.MethodPut:
			putPath = r.URL.Path
			w.Header().Set("ETag", `"abc123"`)
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodDelete:
			deleteReceived.Store(true)
			w.WriteHeader(http.StatusNoContent)

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	l := NewLeaser()
	l.Bucket = "test-bucket"
	l.Path = "leases"
	l.Region = "us-east-1"
	l.Endpoint = server.URL
	l.ForcePathStyle = true
	l.AccessKeyID = "test-key"
	l.SecretAccessKey = "test-secret"

	existingLease := &litestream.Lease{
		Epoch:   5,
		Timeout: 30 * time.Second,
		Owner:   "my-instance",
	}

	ctx := context.Background()
	newLease, err := l.RenewLease(ctx, existingLease)
	if err != nil {
		t.Fatalf("RenewLease() error: %v", err)
	}

	// Should create epoch 6
	if newLease.Epoch != 6 {
		t.Errorf("new lease Epoch = %d, want 6", newLease.Epoch)
	}
	if !strings.Contains(putPath, "0000000000000006.lock") {
		t.Errorf("PUT path = %q, expected epoch 6 lock file", putPath)
	}

	// Wait a bit for background reaping
	time.Sleep(100 * time.Millisecond)
	if !deleteReceived.Load() {
		t.Log("background reaping may not have completed yet")
	}
}

func TestLeaser_ReleaseLease(t *testing.T) {
	var putBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, ".lock"):
			activeLease := &litestream.Lease{
				Epoch:   3,
				Timeout: 30 * time.Second,
				Owner:   "my-instance",
			}
			body, _ := json.Marshal(activeLease)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Last-Modified", time.Now().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)

		case r.Method == http.MethodPut:
			putBody, _ = io.ReadAll(r.Body)
			w.Header().Set("ETag", `"abc123"`)
			w.WriteHeader(http.StatusOK)

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	l := NewLeaser()
	l.Bucket = "test-bucket"
	l.Path = "leases"
	l.Region = "us-east-1"
	l.Endpoint = server.URL
	l.ForcePathStyle = true
	l.AccessKeyID = "test-key"
	l.SecretAccessKey = "test-secret"

	ctx := context.Background()
	err := l.ReleaseLease(ctx, 3)
	if err != nil {
		t.Fatalf("ReleaseLease() error: %v", err)
	}

	// Verify the released lease has timeout=0
	var releasedLease litestream.Lease
	if err := json.Unmarshal(putBody, &releasedLease); err != nil {
		t.Fatalf("unmarshal released lease: %v", err)
	}
	if releasedLease.Timeout != 0 {
		t.Errorf("released lease Timeout = %v, want 0", releasedLease.Timeout)
	}
}

func TestLeaser_ReleaseLease_AlreadyReleased(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, ".lock"):
			// Lease already has timeout=0
			releasedLease := &litestream.Lease{
				Epoch:   3,
				Timeout: 0,
				Owner:   "my-instance",
			}
			body, _ := json.Marshal(releasedLease)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Last-Modified", time.Now().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)

		case r.Method == http.MethodPut:
			t.Error("should not PUT when lease already released")
			w.WriteHeader(http.StatusOK)

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	l := NewLeaser()
	l.Bucket = "test-bucket"
	l.Path = "leases"
	l.Region = "us-east-1"
	l.Endpoint = server.URL
	l.ForcePathStyle = true
	l.AccessKeyID = "test-key"
	l.SecretAccessKey = "test-secret"

	ctx := context.Background()
	err := l.ReleaseLease(ctx, 3)
	if err != nil {
		t.Fatalf("ReleaseLease() error: %v", err)
	}
}

func TestLeaser_ReleaseLease_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, ".lock") {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
				<Error>
					<Code>NoSuchKey</Code>
					<Message>The specified key does not exist.</Message>
				</Error>`))
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	l := NewLeaser()
	l.Bucket = "test-bucket"
	l.Path = "leases"
	l.Region = "us-east-1"
	l.Endpoint = server.URL
	l.ForcePathStyle = true
	l.AccessKeyID = "test-key"
	l.SecretAccessKey = "test-secret"

	ctx := context.Background()
	err := l.ReleaseLease(ctx, 99)
	if err != nil {
		t.Fatalf("ReleaseLease() should not error for missing lease: %v", err)
	}
}

func TestLeaser_DeleteLease(t *testing.T) {
	var deleteReceived atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleteReceived.Store(true)
			if !strings.Contains(r.URL.Path, "0000000000000007.lock") {
				t.Errorf("unexpected delete path: %s", r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	l := NewLeaser()
	l.Bucket = "test-bucket"
	l.Path = "leases"
	l.Region = "us-east-1"
	l.Endpoint = server.URL
	l.ForcePathStyle = true
	l.AccessKeyID = "test-key"
	l.SecretAccessKey = "test-secret"

	ctx := context.Background()
	err := l.DeleteLease(ctx, 7)
	if err != nil {
		t.Fatalf("DeleteLease() error: %v", err)
	}

	if !deleteReceived.Load() {
		t.Error("expected DELETE request")
	}
}

func TestLeaser_Epochs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.RawQuery, "list-type=2") {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<Name>test-bucket</Name>
					<Prefix>leases/</Prefix>
					<IsTruncated>false</IsTruncated>
					<Contents>
						<Key>leases/0000000000000003.lock</Key>
					</Contents>
					<Contents>
						<Key>leases/0000000000000001.lock</Key>
					</Contents>
					<Contents>
						<Key>leases/0000000000000005.lock</Key>
					</Contents>
					<Contents>
						<Key>leases/other-file.txt</Key>
					</Contents>
				</ListBucketResult>`))
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	l := NewLeaser()
	l.Bucket = "test-bucket"
	l.Path = "leases"
	l.Region = "us-east-1"
	l.Endpoint = server.URL
	l.ForcePathStyle = true
	l.AccessKeyID = "test-key"
	l.SecretAccessKey = "test-secret"

	ctx := context.Background()
	epochs, err := l.Epochs(ctx)
	if err != nil {
		t.Fatalf("Epochs() error: %v", err)
	}

	// Should be sorted and only include valid lock files
	want := []int64{1, 3, 5}
	if len(epochs) != len(want) {
		t.Fatalf("Epochs() = %v, want %v", epochs, want)
	}
	for i, e := range epochs {
		if e != want[i] {
			t.Errorf("epochs[%d] = %d, want %d", i, e, want[i])
		}
	}
}

func TestLeaser_ConcurrentAcquire(t *testing.T) {
	// Test that concurrent acquires result in exactly one winner
	var mu sync.Mutex
	var acquireCount int
	var winnerSet bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.RawQuery, "list-type=2"):
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
				<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
					<Name>test-bucket</Name>
					<Prefix>leases/</Prefix>
					<IsTruncated>false</IsTruncated>
				</ListBucketResult>`))

		case r.Method == http.MethodPut:
			mu.Lock()
			acquireCount++
			isWinner := !winnerSet
			if !winnerSet {
				winnerSet = true
			}
			mu.Unlock()

			if isWinner {
				w.Header().Set("ETag", `"abc123"`)
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
					<Error><Code>PreconditionFailed</Code></Error>`))
			}

		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, ".lock"):
			winnerLease := &litestream.Lease{Epoch: 1, Timeout: 30 * time.Second, Owner: "winner"}
			body, _ := json.Marshal(winnerLease)
			w.Header().Set("Last-Modified", time.Now().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)

		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	const numClients = 5
	var wg sync.WaitGroup
	results := make(chan error, numClients)
	winners := make(chan *litestream.Lease, numClients)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			l := NewLeaser()
			l.Bucket = "test-bucket"
			l.Path = "leases"
			l.Region = "us-east-1"
			l.Endpoint = server.URL
			l.ForcePathStyle = true
			l.AccessKeyID = "test-key"
			l.SecretAccessKey = "test-secret"
			l.Owner = "client-" + string(rune('A'+id))

			ctx := context.Background()
			lease, err := l.AcquireLease(ctx)
			if err != nil {
				results <- err
			} else {
				winners <- lease
			}
		}(i)
	}

	wg.Wait()
	close(results)
	close(winners)

	winnerCount := 0
	for range winners {
		winnerCount++
	}

	errorCount := 0
	for err := range results {
		var leaseExistsErr *litestream.LeaseExistsError
		if errors.As(err, &leaseExistsErr) {
			errorCount++
		} else {
			t.Errorf("unexpected error: %v", err)
		}
	}

	if winnerCount != 1 {
		t.Errorf("expected exactly 1 winner, got %d", winnerCount)
	}
	if errorCount != numClients-1 {
		t.Errorf("expected %d LeaseExistsErrors, got %d", numClients-1, errorCount)
	}
}

func TestLease_Expired(t *testing.T) {
	tests := []struct {
		name    string
		lease   litestream.Lease
		expired bool
	}{
		{
			name: "active lease",
			lease: litestream.Lease{
				Epoch:   1,
				ModTime: time.Now(),
				Timeout: 30 * time.Second,
			},
			expired: false,
		},
		{
			name: "zero timeout",
			lease: litestream.Lease{
				Epoch:   1,
				ModTime: time.Now(),
				Timeout: 0,
			},
			expired: true,
		},
		{
			name: "negative timeout",
			lease: litestream.Lease{
				Epoch:   1,
				ModTime: time.Now(),
				Timeout: -1 * time.Second,
			},
			expired: true,
		},
		{
			name: "past deadline",
			lease: litestream.Lease{
				Epoch:   1,
				ModTime: time.Now().Add(-1 * time.Minute),
				Timeout: 30 * time.Second,
			},
			expired: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.lease.Expired(); got != tt.expired {
				t.Errorf("Expired() = %v, want %v", got, tt.expired)
			}
		})
	}
}

func TestLease_Deadline(t *testing.T) {
	modTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	lease := litestream.Lease{
		Epoch:   1,
		ModTime: modTime,
		Timeout: 30 * time.Second,
	}

	want := modTime.Add(30 * time.Second)
	if got := lease.Deadline(); !got.Equal(want) {
		t.Errorf("Deadline() = %v, want %v", got, want)
	}
}

func TestLeaseExistsError(t *testing.T) {
	lease := &litestream.Lease{
		Epoch:   5,
		ModTime: time.Now(),
		Timeout: 30 * time.Second,
		Owner:   "test-owner",
	}

	err := litestream.NewLeaseExistsError(lease)
	if err.Lease != lease {
		t.Error("LeaseExistsError.Lease should reference the provided lease")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "epoch 5") {
		t.Errorf("error message should contain epoch: %s", errMsg)
	}
	if !strings.Contains(errMsg, "test-owner") {
		t.Errorf("error message should contain owner: %s", errMsg)
	}
}

func TestIsPreconditionFailed(t *testing.T) {
	tests := []struct {
		code string
		want bool
	}{
		{"PreconditionFailed", true},
		{"412", true},
		{"NoSuchKey", false},
		{"AccessDenied", false},
	}

	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			err := &mockAPIError{code: tt.code}
			if got := isPreconditionFailed(err); got != tt.want {
				t.Errorf("isPreconditionFailed(%q) = %v, want %v", tt.code, got, tt.want)
			}
		})
	}
}
