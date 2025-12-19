package litestream

import (
	"context"
	"fmt"
	"time"
)

// DefaultLeaseTimeout is the default duration a lease is held before expiring.
const DefaultLeaseTimeout = 30 * time.Second

// Leaser represents a client for a distributed leasing service.
// It uses epoch-based leader election where each leadership change increments
// the epoch, providing a fencing token to prevent split-brain scenarios.
//
// Leases are not zero-downtime: a graceful shutdown should ReleaseLease for
// immediate takeover, otherwise a standby must wait for the lease to expire.
// Leases are intended to be scoped globally across all databases managed by a
// single process.
type Leaser interface {
	// Type returns the name of the implementation (e.g. "s3").
	Type() string

	// Epochs returns a sorted list of existing lease epochs.
	// Typically there should only be one or two as old leases are reaped.
	Epochs(ctx context.Context) ([]int64, error)

	// AcquireLease attempts to acquire a new lease. If an unexpired lease
	// exists, it returns a LeaseExistsError containing the current lease.
	AcquireLease(ctx context.Context) (*Lease, error)

	// RenewLease renews an existing lease by creating a new lease with an
	// incremented epoch. The old lease file is reaped in the background.
	RenewLease(ctx context.Context, lease *Lease) (*Lease, error)

	// ReleaseLease releases a previously acquired lease by setting its timeout
	// to zero, allowing other instances to acquire a new lease immediately.
	ReleaseLease(ctx context.Context, epoch int64) error

	// DeleteLease removes the lease file. This is typically used for reaping
	// expired leases or for test cleanup.
	DeleteLease(ctx context.Context, epoch int64) error
}

// Lease represents a distributed lease that ensures only a single Litestream
// instance replicates to a replica at a time.
type Lease struct {
	// Epoch is incremented on each leader change and serves as a fencing token.
	Epoch int64 `json:"epoch"`

	// ModTime is the timestamp when the lease was last modified.
	// Used with Timeout to determine expiration.
	ModTime time.Time `json:"-"`

	// Timeout is the duration after ModTime that the lease remains valid.
	// A zero or negative value indicates the lease is immediately expired.
	Timeout time.Duration `json:"timeout"`

	// Owner identifies the process holding the lease (e.g. hostname).
	// This is for debugging and observability only.
	Owner string `json:"owner,omitempty"`
}

// Expired returns true if the lease has expired.
func (l *Lease) Expired() bool {
	return l.Timeout <= 0 || l.Deadline().Before(time.Now())
}

// Deadline returns the time when the lease will expire if not renewed.
func (l *Lease) Deadline() time.Time {
	return l.ModTime.Add(l.Timeout)
}

// LeaseExistsError is returned when attempting to acquire a lease while
// another unexpired lease exists.
type LeaseExistsError struct {
	Lease *Lease
}

// NewLeaseExistsError returns a new LeaseExistsError with the given lease.
func NewLeaseExistsError(lease *Lease) *LeaseExistsError {
	return &LeaseExistsError{Lease: lease}
}

// Error implements the error interface.
func (e *LeaseExistsError) Error() string {
	return fmt.Sprintf("lease exists (epoch %d, owner %q, expires %s)",
		e.Lease.Epoch, e.Lease.Owner, e.Lease.Deadline().Format(time.RFC3339))
}
