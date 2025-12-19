package litestream_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
	"github.com/benbjohnson/litestream/s3"
)

func TestLeaser_AcquireLease(t *testing.T) {
	RunWithLeaser(t, "Fresh", func(t *testing.T, l litestream.Leaser) {
		t.Parallel()

		ctx := context.Background()
		lease, err := l.AcquireLease(ctx)
		if err != nil {
			t.Fatalf("AcquireLease() error: %v", err)
		}

		if lease.Epoch != 1 {
			t.Errorf("Epoch = %d, want 1", lease.Epoch)
		}
		if lease.Timeout != litestream.DefaultLeaseTimeout {
			t.Errorf("Timeout = %v, want %v", lease.Timeout, litestream.DefaultLeaseTimeout)
		}
		if lease.ModTime.IsZero() {
			t.Error("ModTime should not be zero")
		}

		epochs, err := l.Epochs(ctx)
		if err != nil {
			t.Fatalf("Epochs() error: %v", err)
		}
		if !slices.Contains(epochs, int64(1)) {
			t.Errorf("Epochs() = %v, expected to contain 1", epochs)
		}
	})

	RunWithLeaser(t, "AfterRelease", func(t *testing.T, l litestream.Leaser) {
		t.Parallel()

		ctx := context.Background()

		// Acquire first lease
		lease1, err := l.AcquireLease(ctx)
		if err != nil {
			t.Fatalf("first AcquireLease() error: %v", err)
		}

		// Release it
		if err := l.ReleaseLease(ctx, lease1.Epoch); err != nil {
			t.Fatalf("ReleaseLease() error: %v", err)
		}

		// Acquire second lease
		lease2, err := l.AcquireLease(ctx)
		if err != nil {
			t.Fatalf("second AcquireLease() error: %v", err)
		}

		if lease2.Epoch != 2 {
			t.Errorf("second lease Epoch = %d, want 2", lease2.Epoch)
		}
	})

	RunWithLeaser(t, "ExistingActive", func(t *testing.T, l litestream.Leaser) {
		t.Parallel()

		ctx := context.Background()

		// Acquire first lease
		_, err := l.AcquireLease(ctx)
		if err != nil {
			t.Fatalf("first AcquireLease() error: %v", err)
		}

		// Try to acquire again without releasing
		_, err = l.AcquireLease(ctx)
		var leaseExistsErr *litestream.LeaseExistsError
		if !errors.As(err, &leaseExistsErr) {
			t.Fatalf("expected LeaseExistsError, got: %v", err)
		}
		if leaseExistsErr.Lease.Epoch != 1 {
			t.Errorf("existing lease Epoch = %d, want 1", leaseExistsErr.Lease.Epoch)
		}
	})
}

func TestLeaser_RenewLease(t *testing.T) {
	RunWithLeaser(t, "OK", func(t *testing.T, l litestream.Leaser) {
		t.Parallel()

		ctx := context.Background()

		// Acquire initial lease
		lease1, err := l.AcquireLease(ctx)
		if err != nil {
			t.Fatalf("AcquireLease() error: %v", err)
		}

		// Renew it
		lease2, err := l.RenewLease(ctx, lease1)
		if err != nil {
			t.Fatalf("RenewLease() error: %v", err)
		}

		if lease2.Epoch != 2 {
			t.Errorf("renewed lease Epoch = %d, want 2", lease2.Epoch)
		}

		// Wait for background reaping
		time.Sleep(500 * time.Millisecond)

		// Old lease should be reaped
		epochs, err := l.Epochs(ctx)
		if err != nil {
			t.Fatalf("Epochs() error: %v", err)
		}
		if slices.Contains(epochs, int64(1)) {
			t.Logf("epochs = %v (old epoch may not be reaped yet)", epochs)
		}
	})

	RunWithLeaser(t, "Reaped", func(t *testing.T, l litestream.Leaser) {
		t.Parallel()

		ctx := context.Background()

		// Try to renew a lease when all epochs have been deleted
		lease, err := l.RenewLease(ctx, &litestream.Lease{Epoch: 999})
		if err != nil {
			t.Fatalf("RenewLease() with reaped lease error: %v", err)
		}

		// Should start from epoch 1
		if lease.Epoch != 1 {
			t.Errorf("renewed lease Epoch = %d, want 1", lease.Epoch)
		}
	})
}

func TestLeaser_ReleaseLease(t *testing.T) {
	RunWithLeaser(t, "OK", func(t *testing.T, l litestream.Leaser) {
		t.Parallel()

		ctx := context.Background()

		// Acquire lease
		lease, err := l.AcquireLease(ctx)
		if err != nil {
			t.Fatalf("AcquireLease() error: %v", err)
		}

		// Release it
		if err := l.ReleaseLease(ctx, lease.Epoch); err != nil {
			t.Fatalf("ReleaseLease() error: %v", err)
		}

		// Should be able to acquire again immediately
		lease2, err := l.AcquireLease(ctx)
		if err != nil {
			t.Fatalf("second AcquireLease() error: %v", err)
		}
		if lease2.Epoch != 2 {
			t.Errorf("new lease Epoch = %d, want 2", lease2.Epoch)
		}
	})

	RunWithLeaser(t, "NotFound", func(t *testing.T, l litestream.Leaser) {
		t.Parallel()

		ctx := context.Background()

		// Release a non-existent lease should not error
		err := l.ReleaseLease(ctx, 999)
		if err != nil {
			t.Fatalf("ReleaseLease() for non-existent lease error: %v", err)
		}
	})
}

func TestLeaser_DeleteLease(t *testing.T) {
	RunWithLeaser(t, "OK", func(t *testing.T, l litestream.Leaser) {
		t.Parallel()

		ctx := context.Background()

		// Acquire lease
		lease, err := l.AcquireLease(ctx)
		if err != nil {
			t.Fatalf("AcquireLease() error: %v", err)
		}

		// Delete it
		if err := l.DeleteLease(ctx, lease.Epoch); err != nil {
			t.Fatalf("DeleteLease() error: %v", err)
		}

		// Should no longer be in epochs list
		epochs, err := l.Epochs(ctx)
		if err != nil {
			t.Fatalf("Epochs() error: %v", err)
		}
		if slices.Contains(epochs, lease.Epoch) {
			t.Errorf("deleted epoch %d still in list: %v", lease.Epoch, epochs)
		}
	})
}

func TestLeaser_Epochs(t *testing.T) {
	RunWithLeaser(t, "Empty", func(t *testing.T, l litestream.Leaser) {
		t.Parallel()

		ctx := context.Background()
		epochs, err := l.Epochs(ctx)
		if err != nil {
			t.Fatalf("Epochs() error: %v", err)
		}
		if len(epochs) != 0 {
			t.Errorf("Epochs() = %v, want empty", epochs)
		}
	})

	RunWithLeaser(t, "Multiple", func(t *testing.T, l litestream.Leaser) {
		t.Parallel()

		ctx := context.Background()

		// Create multiple leases
		for i := 0; i < 3; i++ {
			lease, err := l.AcquireLease(ctx)
			if err != nil {
				t.Fatalf("AcquireLease() %d error: %v", i, err)
			}
			if err := l.ReleaseLease(ctx, lease.Epoch); err != nil {
				t.Fatalf("ReleaseLease() %d error: %v", i, err)
			}
		}

		// Final acquire
		_, err := l.AcquireLease(ctx)
		if err != nil {
			t.Fatalf("final AcquireLease() error: %v", err)
		}

		epochs, err := l.Epochs(ctx)
		if err != nil {
			t.Fatalf("Epochs() error: %v", err)
		}

		// Should have at least epoch 4 (some older ones may be reaped)
		if !slices.Contains(epochs, int64(4)) {
			t.Errorf("Epochs() = %v, expected to contain 4", epochs)
		}
	})
}

func TestLeaser_ConcurrentAcquire(t *testing.T) {
	if !testingutil.Integration() {
		t.Skip("skipping integration test, use -integration flag to run")
	}
	if !slices.Contains(testingutil.ReplicaClientTypes(), "s3") {
		t.Skip("skipping S3 integration test")
	}

	t.Run("S3", func(t *testing.T) {
		const numClients = 3

		// Create separate leasers with the same path
		basePath := testingutil.NewS3Leaser(t).Path
		leasers := make([]*s3.Leaser, numClients)
		for i := 0; i < numClients; i++ {
			l := testingutil.NewS3Leaser(t)
			l.Path = basePath // Same path for all leasers
			l.Owner = string(rune('A' + i))
			leasers[i] = l
		}
		defer testingutil.MustDeleteAllLeases(t, leasers[0])

		ctx := context.Background()
		var wg sync.WaitGroup
		winners := make(chan *litestream.Lease, numClients)
		losers := make(chan *litestream.LeaseExistsError, numClients)

		for _, l := range leasers {
			wg.Add(1)
			go func(leaser *s3.Leaser) {
				defer wg.Done()
				lease, err := leaser.AcquireLease(ctx)
				if err != nil {
					var leaseExistsErr *litestream.LeaseExistsError
					if errors.As(err, &leaseExistsErr) {
						losers <- leaseExistsErr
					} else {
						t.Errorf("unexpected error: %v", err)
					}
				} else {
					winners <- lease
				}
			}(l)
		}

		wg.Wait()
		close(winners)
		close(losers)

		winnerCount := 0
		for range winners {
			winnerCount++
		}

		loserCount := 0
		for range losers {
			loserCount++
		}

		if winnerCount != 1 {
			t.Errorf("expected exactly 1 winner, got %d", winnerCount)
		}
		if loserCount != numClients-1 {
			t.Errorf("expected %d losers, got %d", numClients-1, loserCount)
		}
	})
}

func TestLeaser_CustomTimeout(t *testing.T) {
	if !testingutil.Integration() {
		t.Skip("skipping integration test, use -integration flag to run")
	}
	if !slices.Contains(testingutil.ReplicaClientTypes(), "s3") {
		t.Skip("skipping S3 integration test")
	}

	t.Run("S3", func(t *testing.T) {
		l := testingutil.NewS3Leaser(t)
		l.LeaseTimeout = 5 * time.Second
		l.Owner = "custom-timeout-test"
		defer testingutil.MustDeleteAllLeases(t, l)

		ctx := context.Background()
		lease, err := l.AcquireLease(ctx)
		if err != nil {
			t.Fatalf("AcquireLease() error: %v", err)
		}

		if lease.Timeout != 5*time.Second {
			t.Errorf("Timeout = %v, want 5s", lease.Timeout)
		}
		if lease.Owner != "custom-timeout-test" {
			t.Errorf("Owner = %q, want %q", lease.Owner, "custom-timeout-test")
		}
	})
}

func TestLeaser_GracefulHandoff(t *testing.T) {
	if !testingutil.Integration() {
		t.Skip("skipping integration test, use -integration flag to run")
	}
	if !slices.Contains(testingutil.ReplicaClientTypes(), "s3") {
		t.Skip("skipping S3 integration test")
	}

	t.Run("S3", func(t *testing.T) {
		// Create two leasers with the same path
		basePath := testingutil.NewS3Leaser(t).Path
		l1 := testingutil.NewS3Leaser(t)
		l1.Path = basePath
		l1.Owner = "instance-1"

		l2 := testingutil.NewS3Leaser(t)
		l2.Path = basePath
		l2.Owner = "instance-2"

		defer testingutil.MustDeleteAllLeases(t, l1)

		ctx := context.Background()

		// Instance 1 acquires lease
		lease1, err := l1.AcquireLease(ctx)
		if err != nil {
			t.Fatalf("l1.AcquireLease() error: %v", err)
		}
		t.Logf("instance-1 acquired epoch %d", lease1.Epoch)

		// Instance 2 tries to acquire - should fail
		_, err = l2.AcquireLease(ctx)
		var leaseExistsErr *litestream.LeaseExistsError
		if !errors.As(err, &leaseExistsErr) {
			t.Fatalf("l2 expected LeaseExistsError, got: %v", err)
		}
		t.Logf("instance-2 blocked by epoch %d (owner: %s)", leaseExistsErr.Lease.Epoch, leaseExistsErr.Lease.Owner)

		// Instance 1 releases lease
		if err := l1.ReleaseLease(ctx, lease1.Epoch); err != nil {
			t.Fatalf("l1.ReleaseLease() error: %v", err)
		}
		t.Logf("instance-1 released epoch %d", lease1.Epoch)

		// Instance 2 should now be able to acquire
		lease2, err := l2.AcquireLease(ctx)
		if err != nil {
			t.Fatalf("l2.AcquireLease() after release error: %v", err)
		}
		t.Logf("instance-2 acquired epoch %d", lease2.Epoch)

		if lease2.Epoch <= lease1.Epoch {
			t.Errorf("new epoch %d should be > old epoch %d", lease2.Epoch, lease1.Epoch)
		}
	})
}

// RunWithLeaser executes fn with each leaser specified by the -integration flag
func RunWithLeaser(t *testing.T, name string, fn func(*testing.T, litestream.Leaser)) {
	t.Run(name, func(t *testing.T) {
		for _, typ := range testingutil.ReplicaClientTypes() {
			// Only S3 supports leasing currently
			if typ != "s3" {
				continue
			}

			t.Run(typ, func(t *testing.T) {
				if !testingutil.Integration() {
					t.Skip("skipping integration test, use -integration flag to run")
				}

				l := testingutil.NewS3Leaser(t)
				defer testingutil.MustDeleteAllLeases(t, l)

				fn(t, l)
			})
		}
	})
}
