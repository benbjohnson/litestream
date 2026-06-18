package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	litestreams3 "github.com/benbjohnson/litestream/s3"
)

func TestLeaseManager_OpenClose(t *testing.T) {
	leaser := newTestCommandLeaser()
	manager := newLeaseManager([]leaseEntry{{
		path:             "/tmp/test.db",
		leaser:           leaser,
		ttl:              30 * time.Second,
		heartbeat:        time.Hour,
		acquireTimeout:   time.Second,
		acquireRetryWait: time.Millisecond,
	}}, nil)

	if err := manager.Open(context.Background()); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	if got, want := leaser.acquireN.Load(), int32(1); got != want {
		t.Fatalf("AcquireLease() calls=%d, want %d", got, want)
	}
	if got, want := leaser.releaseN.Load(), int32(0); got != want {
		t.Fatalf("ReleaseLease() calls before Close()=%d, want %d", got, want)
	}

	if err := manager.Close(context.Background()); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if got, want := leaser.releaseN.Load(), int32(1); got != want {
		t.Fatalf("ReleaseLease() calls=%d, want %d", got, want)
	}
}

func TestLeaseManager_RenewalLossNotifies(t *testing.T) {
	leaser := newTestCommandLeaser()
	leaser.renewErr = litestream.ErrLeaseNotHeld
	notifyCh := make(chan error, 1)
	manager := newLeaseManager([]leaseEntry{{
		path:             "/tmp/test.db",
		leaser:           leaser,
		ttl:              time.Second,
		heartbeat:        time.Millisecond,
		acquireTimeout:   time.Second,
		acquireRetryWait: time.Millisecond,
	}}, func(err error) {
		notifyCh <- err
	})

	if err := manager.Open(context.Background()); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	defer func() {
		if err := manager.Close(context.Background()); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
	}()

	select {
	case err := <-notifyCh:
		if !errors.Is(err, litestream.ErrLeaseNotHeld) {
			t.Fatalf("notify error=%v, want %v", err, litestream.ErrLeaseNotHeld)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for renewal loss notification")
	}
}

func TestLeaseManager_AcquireTimeout(t *testing.T) {
	leaser := newTestCommandLeaser()
	leaser.acquireErr = &litestream.LeaseExistsError{
		Owner:     "other",
		ExpiresAt: time.Now().Add(time.Hour),
	}
	manager := newLeaseManager([]leaseEntry{{
		path:             "/tmp/test.db",
		leaser:           leaser,
		ttl:              time.Second,
		heartbeat:        time.Millisecond,
		acquireTimeout:   time.Millisecond,
		acquireRetryWait: time.Hour,
	}}, nil)

	err := manager.Open(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open() error=%v, want %v", err, context.DeadlineExceeded)
	}
}

func TestNewLeaseEntry_S3(t *testing.T) {
	client := litestreams3.NewReplicaClient()
	client.Bucket = "test-bucket"
	client.Path = "test-path"
	client.Region = "us-east-1"
	client.Endpoint = "http://127.0.0.1:1"
	client.ForcePathStyle = true
	client.AccessKeyID = "test-access-key"
	client.SecretAccessKey = "test-secret-key"

	db := litestream.NewDB("/tmp/test.db")
	db.Replica = litestream.NewReplicaWithClient(db, client)
	ttl := 45 * time.Second
	heartbeat := 10 * time.Second
	acquireTimeout := time.Minute

	entry, err := newLeaseEntry(context.Background(), db, LeaseConfig{
		Required:       true,
		TTL:            &ttl,
		Heartbeat:      &heartbeat,
		AcquireTimeout: &acquireTimeout,
	})
	if err != nil {
		t.Fatalf("newLeaseEntry() error: %v", err)
	}
	if entry.path != db.Path() {
		t.Fatalf("entry.path=%q, want %q", entry.path, db.Path())
	}
	if entry.leaser.Type() != "s3" {
		t.Fatalf("entry.leaser.Type()=%q, want s3", entry.leaser.Type())
	}
	if entry.ttl != ttl {
		t.Fatalf("entry.ttl=%v, want %v", entry.ttl, ttl)
	}
	if entry.heartbeat != heartbeat {
		t.Fatalf("entry.heartbeat=%v, want %v", entry.heartbeat, heartbeat)
	}
	if entry.acquireTimeout != acquireTimeout {
		t.Fatalf("entry.acquireTimeout=%v, want %v", entry.acquireTimeout, acquireTimeout)
	}

	leaser := entry.leaser.(*litestreams3.Leaser)
	if leaser.TTL != ttl {
		t.Fatalf("leaser.TTL=%v, want %v", leaser.TTL, ttl)
	}
}

func TestNewLeaseEntry_UnsupportedReplica(t *testing.T) {
	db := litestream.NewDB("/tmp/test.db")
	db.Replica = litestream.NewReplicaWithClient(db, file.NewReplicaClient(t.TempDir()))

	_, err := newLeaseEntry(context.Background(), db, LeaseConfig{Required: true})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), `replica type "file" does not support distributed leasing`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

type testCommandLeaser struct {
	acquireN atomic.Int32
	renewN   atomic.Int32
	releaseN atomic.Int32

	acquireErr error
	renewErr   error
	releaseErr error
}

func newTestCommandLeaser() *testCommandLeaser {
	return &testCommandLeaser{}
}

func (l *testCommandLeaser) Type() string {
	return "test"
}

func (l *testCommandLeaser) AcquireLease(context.Context) (*litestream.Lease, error) {
	l.acquireN.Add(1)
	if l.acquireErr != nil {
		return nil, l.acquireErr
	}
	return &litestream.Lease{
		Generation: 1,
		ExpiresAt:  time.Now().Add(time.Minute),
		Owner:      "test",
		ETag:       "etag",
	}, nil
}

func (l *testCommandLeaser) RenewLease(context.Context, *litestream.Lease) (*litestream.Lease, error) {
	l.renewN.Add(1)
	if l.renewErr != nil {
		return nil, l.renewErr
	}
	return &litestream.Lease{
		Generation: 1,
		ExpiresAt:  time.Now().Add(time.Minute),
		Owner:      "test",
		ETag:       "etag",
	}, nil
}

func (l *testCommandLeaser) ReleaseLease(context.Context, *litestream.Lease) error {
	l.releaseN.Add(1)
	return l.releaseErr
}
