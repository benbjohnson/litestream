package litestream_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
	"github.com/benbjohnson/litestream/mock"
)

func TestDB_Close_SyncRetry(t *testing.T) {
	t.Run("SucceedsAfterTransientFailure", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)

		// Write some data to create LTX files
		if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := sqldb.Close(); err != nil {
			t.Fatal(err)
		}

		// Create mock client that fails first 2 times, succeeds on 3rd
		var attempts int32
		client := &mock.ReplicaClient{
			LTXFilesFunc: func(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
				return ltx.NewFileInfoSliceIterator(nil), nil
			},
			WriteLTXFileFunc: func(_ context.Context, _ int, _, _ ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
				n := atomic.AddInt32(&attempts, 1)
				if n < 3 {
					return nil, errors.New("rate limited (429)")
				}
				// Drain the reader
				_, _ = io.Copy(io.Discard, r)
				return &ltx.FileInfo{}, nil
			},
		}

		db.Replica = litestream.NewReplicaWithClient(db, client)
		db.ShutdownSyncTimeout = 5 * time.Second
		db.ShutdownSyncInterval = 50 * time.Millisecond

		// Close should succeed after retries
		if err := db.Close(context.Background()); err != nil {
			t.Fatalf("expected success after retries, got: %v", err)
		}
		if got := atomic.LoadInt32(&attempts); got < 3 {
			t.Fatalf("expected at least 3 attempts, got %d", got)
		}
	})

	t.Run("FailsAfterTimeout", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)

		// Write some data
		if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := sqldb.Close(); err != nil {
			t.Fatal(err)
		}

		// Create mock client that always fails
		var attempts int32
		client := &mock.ReplicaClient{
			LTXFilesFunc: func(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
				return ltx.NewFileInfoSliceIterator(nil), nil
			},
			WriteLTXFileFunc: func(_ context.Context, _ int, _, _ ltx.TXID, _ io.Reader) (*ltx.FileInfo, error) {
				atomic.AddInt32(&attempts, 1)
				return nil, errors.New("persistent error")
			},
		}

		db.Replica = litestream.NewReplicaWithClient(db, client)
		db.ShutdownSyncTimeout = 300 * time.Millisecond
		db.ShutdownSyncInterval = 50 * time.Millisecond

		// Close should fail with timeout error
		err := db.Close(context.Background())
		if err == nil {
			t.Fatal("expected error after timeout")
		}
		if !strings.Contains(err.Error(), "timeout") {
			t.Fatalf("expected timeout error, got: %v", err)
		}
		// Should have made multiple attempts
		if got := atomic.LoadInt32(&attempts); got < 2 {
			t.Fatalf("expected multiple retry attempts, got %d", got)
		}
	})

	t.Run("RespectsContextCancellation", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)

		// Write some data
		if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := sqldb.Close(); err != nil {
			t.Fatal(err)
		}

		// Create mock client that always fails
		client := &mock.ReplicaClient{
			LTXFilesFunc: func(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
				return ltx.NewFileInfoSliceIterator(nil), nil
			},
			WriteLTXFileFunc: func(_ context.Context, _ int, _, _ ltx.TXID, _ io.Reader) (*ltx.FileInfo, error) {
				return nil, errors.New("error")
			},
		}

		db.Replica = litestream.NewReplicaWithClient(db, client)
		db.ShutdownSyncTimeout = 10 * time.Second
		db.ShutdownSyncInterval = 50 * time.Millisecond

		// Cancel context after short delay
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()

		start := time.Now()
		_ = db.Close(ctx)
		elapsed := time.Since(start)

		// Should exit within reasonable time of context cancellation
		if elapsed > 500*time.Millisecond {
			t.Fatalf("took too long to respect cancellation: %v", elapsed)
		}
	})

	t.Run("ZeroTimeoutNoRetry", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)

		// Write some data
		if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := sqldb.Close(); err != nil {
			t.Fatal(err)
		}

		// Create mock client that always fails
		var attempts int32
		client := &mock.ReplicaClient{
			LTXFilesFunc: func(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
				return ltx.NewFileInfoSliceIterator(nil), nil
			},
			WriteLTXFileFunc: func(_ context.Context, _ int, _, _ ltx.TXID, _ io.Reader) (*ltx.FileInfo, error) {
				atomic.AddInt32(&attempts, 1)
				return nil, errors.New("error")
			},
		}

		db.Replica = litestream.NewReplicaWithClient(db, client)
		db.ShutdownSyncTimeout = 0 // Disable retries
		db.ShutdownSyncInterval = 50 * time.Millisecond

		// Close should fail after single attempt
		start := time.Now()
		err := db.Close(context.Background())
		elapsed := time.Since(start)

		if err == nil {
			t.Fatal("expected error")
		}
		// Should have only made 1 attempt
		if got := atomic.LoadInt32(&attempts); got != 1 {
			t.Fatalf("expected exactly 1 attempt with zero timeout, got %d", got)
		}
		// Should be fast (no retry delay)
		if elapsed > 100*time.Millisecond {
			t.Fatalf("took too long for single attempt: %v", elapsed)
		}
	})

	t.Run("SuccessFirstAttempt", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)

		// Write some data
		if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := sqldb.Close(); err != nil {
			t.Fatal(err)
		}

		// Create mock client that succeeds immediately
		var attempts int32
		client := &mock.ReplicaClient{
			LTXFilesFunc: func(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
				return ltx.NewFileInfoSliceIterator(nil), nil
			},
			WriteLTXFileFunc: func(_ context.Context, _ int, _, _ ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
				atomic.AddInt32(&attempts, 1)
				// Drain the reader
				_, _ = io.Copy(io.Discard, r)
				return &ltx.FileInfo{}, nil
			},
		}

		db.Replica = litestream.NewReplicaWithClient(db, client)
		db.ShutdownSyncTimeout = 5 * time.Second
		db.ShutdownSyncInterval = 50 * time.Millisecond

		// Close should succeed immediately
		if err := db.Close(context.Background()); err != nil {
			t.Fatalf("expected success, got: %v", err)
		}
		// Should have made exactly 1 attempt
		if got := atomic.LoadInt32(&attempts); got != 1 {
			t.Fatalf("expected exactly 1 attempt, got %d", got)
		}
	})

	t.Run("DoneChannelInterruptsRetryLoop", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)

		// Write some data
		if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := sqldb.Close(); err != nil {
			t.Fatal(err)
		}

		// Create mock client that always fails
		var attempts int32
		client := &mock.ReplicaClient{
			LTXFilesFunc: func(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
				return ltx.NewFileInfoSliceIterator(nil), nil
			},
			WriteLTXFileFunc: func(_ context.Context, _ int, _, _ ltx.TXID, _ io.Reader) (*ltx.FileInfo, error) {
				atomic.AddInt32(&attempts, 1)
				return nil, errors.New("persistent error")
			},
		}

		db.Replica = litestream.NewReplicaWithClient(db, client)
		db.ShutdownSyncTimeout = 10 * time.Second
		db.ShutdownSyncInterval = 50 * time.Millisecond

		// Create done channel and close it after short delay
		done := make(chan struct{})
		db.Done = done
		go func() {
			time.Sleep(200 * time.Millisecond)
			close(done)
		}()

		start := time.Now()
		err := db.Close(context.Background())
		elapsed := time.Since(start)

		// Should exit quickly (well before 10 second timeout)
		if elapsed > 2*time.Second {
			t.Fatalf("took too long to respond to done signal: %v", elapsed)
		}

		// Should have error mentioning interrupt
		if err == nil {
			t.Fatal("expected error after done signal")
		}
		if !strings.Contains(err.Error(), "interrupted") {
			t.Fatalf("expected interrupted error, got: %v", err)
		}

		// Should have made at least 1 attempt before being interrupted
		if got := atomic.LoadInt32(&attempts); got < 1 {
			t.Fatalf("expected at least 1 attempt, got %d", got)
		}
	})

	t.Run("AlreadyClosedDoneSkipsSync", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)

		// Write some data
		if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := sqldb.Close(); err != nil {
			t.Fatal(err)
		}

		// Create mock client that always fails
		var attempts int32
		client := &mock.ReplicaClient{
			LTXFilesFunc: func(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
				return ltx.NewFileInfoSliceIterator(nil), nil
			},
			WriteLTXFileFunc: func(_ context.Context, _ int, _, _ ltx.TXID, _ io.Reader) (*ltx.FileInfo, error) {
				atomic.AddInt32(&attempts, 1)
				return nil, errors.New("persistent error")
			},
		}

		db.Replica = litestream.NewReplicaWithClient(db, client)
		db.ShutdownSyncTimeout = 10 * time.Second
		db.ShutdownSyncInterval = 50 * time.Millisecond

		// Close done before calling Close
		done := make(chan struct{})
		close(done)
		db.Done = done

		start := time.Now()
		err := db.Close(context.Background())
		elapsed := time.Since(start)

		// Should exit immediately
		if elapsed > 500*time.Millisecond {
			t.Fatalf("took too long with pre-closed done channel: %v", elapsed)
		}

		// Should have error mentioning interrupt
		if err == nil {
			t.Fatal("expected error with pre-closed done channel")
		}
		if !strings.Contains(err.Error(), "interrupted") {
			t.Fatalf("expected interrupted error, got: %v", err)
		}

		// Should not have made any sync attempts
		if got := atomic.LoadInt32(&attempts); got != 0 {
			t.Fatalf("expected 0 sync attempts with pre-closed done, got %d", got)
		}
	})

	t.Run("NilDoneBehavesLikeClose", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)

		// Write some data
		if _, err := sqldb.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := sqldb.Close(); err != nil {
			t.Fatal(err)
		}

		// Create mock client that succeeds immediately
		var attempts int32
		client := &mock.ReplicaClient{
			LTXFilesFunc: func(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
				return ltx.NewFileInfoSliceIterator(nil), nil
			},
			WriteLTXFileFunc: func(_ context.Context, _ int, _, _ ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
				atomic.AddInt32(&attempts, 1)
				_, _ = io.Copy(io.Discard, r)
				return &ltx.FileInfo{}, nil
			},
		}

		db.Replica = litestream.NewReplicaWithClient(db, client)
		db.ShutdownSyncTimeout = 5 * time.Second
		db.ShutdownSyncInterval = 50 * time.Millisecond

		// Done is nil by default, Close should work normally
		if err := db.Close(context.Background()); err != nil {
			t.Fatalf("expected success, got: %v", err)
		}
		if got := atomic.LoadInt32(&attempts); got != 1 {
			t.Fatalf("expected exactly 1 attempt, got %d", got)
		}
	})
}
