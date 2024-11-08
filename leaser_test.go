package litestream_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/s3"
)

const (
	testOwner = "TESTOWNER"
)

func TestLeaser_AcquireLease(t *testing.T) {
	runWithLeaser(t, "OK", func(t *testing.T, leaser litestream.Leaser) {
		// Create the initial lease.
		lease, err := leaser.AcquireLease(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if got, want := lease.Epoch, int64(1); got != want {
			t.Fatalf("Epoch=%v, want %v", got, want)
		} else if got, want := lease.Owner, testOwner; got != want {
			t.Fatalf("Owner=%v, want %v", got, want)
		} else if got, want := lease.Timeout, litestream.DefaultLeaseTimeout; got != want {
			t.Fatalf("Timeout=%v, want %v", got, want)
		} else if lease.ModTime.IsZero() {
			t.Fatalf("expected ModTime")
		}

		// Fetch associated epoch.
		epochs, err := leaser.Epochs(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if got, want := epochs, []int64{1}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Epochs()=%v, want %v", got, want)
		}
	})

	runWithLeaser(t, "Reacquire", func(t *testing.T, leaser litestream.Leaser) {
		// Create the initial lease.
		lease1, err := leaser.AcquireLease(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		// Release the lease.
		if err := leaser.ReleaseLease(context.Background(), lease1.Epoch); err != nil {
			t.Fatal(err)
		}

		// Acquire a new lease.
		lease2, err := leaser.AcquireLease(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if got, want := lease2.Epoch, int64(2); got != want {
			t.Fatalf("Epoch=%v, want %v", got, want)
		}
	})

	// Ensure that acquiring a lease before the previous one is released returns a LeaseExistsError.
	runWithLeaser(t, "ErrLeaseExists", func(t *testing.T, leaser litestream.Leaser) {
		// Acquire an initial lease.
		if _, err := leaser.AcquireLease(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Attempt to acquire a new lease before it has been released.
		var leaseExistsErr *litestream.LeaseExistsError
		if _, err := leaser.AcquireLease(context.Background()); !errors.As(err, &leaseExistsErr) {
			t.Fatalf("unexpected error: %#v", err)
		} else if leaseExistsErr.Lease == nil {
			t.Fatalf("expected lease")
		} else if got, want := leaseExistsErr.Lease.Epoch, int64(1); got != want {
			t.Fatalf("err.Lease.Epoch=%v, want %v", got, want)
		}
	})
}

func TestLeaser_RenewLease(t *testing.T) {
	runWithLeaser(t, "OK", func(t *testing.T, leaser litestream.Leaser) {
		// Create the initial lease.
		lease1, err := leaser.AcquireLease(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		// Renew lease.
		lease2, err := leaser.RenewLease(context.Background(), lease1)
		if err != nil {
			t.Fatal(err)
		} else if got, want := lease2.Epoch, int64(2); got != want {
			t.Fatalf("Epoch=%v, want %v", got, want)
		}

		// Pause momentarily so original lease is reaped.
		time.Sleep(1 * time.Second)

		// Fetch associated epochs.
		epochs, err := leaser.Epochs(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if got, want := epochs, []int64{2}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Epochs()=%v, want %v", got, want)
		}
	})

	runWithLeaser(t, "Reaped", func(t *testing.T, leaser litestream.Leaser) {
		// Try to renew a lease when all the lock files have been reaped.
		lease, err := leaser.RenewLease(context.Background(), &litestream.Lease{Epoch: 100})
		if err != nil {
			t.Fatal(err)
		} else if got, want := lease.Epoch, int64(1); got != want {
			t.Fatalf("Epoch=%v, want %v", got, want)
		}

		// Fetch associated epochs.
		epochs, err := leaser.Epochs(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if got, want := epochs, []int64{1}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Epochs()=%v, want %v", got, want)
		}
	})

	runWithLeaser(t, "Released", func(t *testing.T, leaser litestream.Leaser) {
		// Create & release the initial lease.
		lease1, err := leaser.AcquireLease(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if err := leaser.ReleaseLease(context.Background(), lease1.Epoch); err != nil {
			t.Fatal(err)
		}

		// Create & release a lease from a different client.
		lease2, err := leaser.AcquireLease(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if err := leaser.ReleaseLease(context.Background(), lease2.Epoch); err != nil {
			t.Fatal(err)
		}

		// Try to renew the original lease
		lease, err := leaser.RenewLease(context.Background(), lease1)
		if err != nil {
			t.Fatal(err)
		} else if got, want := lease.Epoch, int64(3); got != want {
			t.Fatalf("Epoch=%v, want %v", got, want)
		}

		time.Sleep(1 * time.Second)

		// Fetch associated epochs.
		epochs, err := leaser.Epochs(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if got, want := epochs, []int64{3}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Epochs()=%v, want %v", got, want)
		}
	})

	runWithLeaser(t, "ErrLeaseExists", func(t *testing.T, leaser litestream.Leaser) {
		// Create & release the initial lease.
		lease1, err := leaser.AcquireLease(context.Background())
		if err != nil {
			t.Fatal(err)
		} else if err := leaser.ReleaseLease(context.Background(), lease1.Epoch); err != nil {
			t.Fatal(err)
		}

		// Create lease from different client.
		if _, err := leaser.AcquireLease(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Try to renew the original lease.
		var leaseExistsError *litestream.LeaseExistsError
		if _, err := leaser.RenewLease(context.Background(), lease1); !errors.As(err, &leaseExistsError) {
			t.Fatalf("unexpected error: %#v", err)
		} else if got, want := leaseExistsError.Lease.Epoch, int64(2); got != want {
			t.Fatalf("err.Lease.Epoch=%v, want %v", got, want)
		}
	})
}

// runWithLeaser executes fn with each leaser specified by the -integration flag
func runWithLeaser(t *testing.T, name string, fn func(*testing.T, litestream.Leaser)) {
	t.Run(name, func(t *testing.T) {
		for _, typ := range strings.Split("s3", ",") {
			t.Run(typ, func(t *testing.T) {
				c := newOpenLeaser(t, typ)
				defer mustDeleteAllLeases(t, c)
				fn(t, c)
			})
		}
	})
}

// newOpenLeaser returns a new, open leaser for integration testing by type name.
func newOpenLeaser(tb testing.TB, typ string) litestream.Leaser {
	tb.Helper()

	switch typ {
	case "s3":
		leaser := newS3Leaser(tb)
		leaser.Owner = testOwner
		if err := leaser.Open(); err != nil {
			tb.Fatal(err)
		}
		return leaser
	default:
		tb.Fatalf("invalid leaser type: %q", typ)
		return nil
	}
}

func newS3Leaser(tb testing.TB) *s3.Leaser {
	tb.Helper()

	l := s3.NewLeaser()
	l.AccessKeyID = *s3AccessKeyID
	l.SecretAccessKey = *s3SecretAccessKey
	l.Region = *s3Region
	l.Bucket = *s3Bucket
	l.Path = path.Join(*s3Path, fmt.Sprintf("%016x", rand.Uint64()))
	l.Endpoint = *s3Endpoint
	l.ForcePathStyle = *s3ForcePathStyle
	l.SkipVerify = *s3SkipVerify
	return l
}

// mustDeleteAllLeases deletes all lease objects.
func mustDeleteAllLeases(tb testing.TB, l litestream.Leaser) {
	tb.Helper()

	epochs, err := l.Epochs(context.Background())
	if err != nil {
		tb.Fatalf("cannot list lease epochs for deletion: %s", err)
	}

	for _, epoch := range epochs {
		if err := l.DeleteLease(context.Background(), epoch); err != nil {
			tb.Fatalf("cannot delete lease (epoch=%d): %s", epoch, err)
		}
	}
}
