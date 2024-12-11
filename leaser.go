package litestream

import (
	"context"
	"fmt"
	"time"
)

// DefaultLeaseTimeout is the default amount of time to hold a lease object.
const DefaultLeaseTimeout = 30 * time.Second

// LeaseRetryInterval is the interval to retry lease acquisition when there
// is an already existing lease. This ensures that the process will pick up
// the lease quickly after it has expired from another process.
const LeaseRetryInterval = 1 * time.Second

// Leaser represents a client for a distributed leasing service.
type Leaser interface {
	// The name of the implementation (e.g. "s3").
	Type() string

	// Returns a sorted list of existing lease epochs.
	Epochs(ctx context.Context) ([]int64, error)

	// Attempts to acquire a new lease.
	AcquireLease(ctx context.Context) (*Lease, error)

	// Renews an existing lease.
	RenewLease(ctx context.Context, lease *Lease) (*Lease, error)

	// Releases an previously acquired lease via expiration.
	ReleaseLease(ctx context.Context, epoch int64) error

	// Removes the lease by deleting its underlying file.
	// This is typically used for reaping expired leases or for test cleanup.
	DeleteLease(ctx context.Context, epoch int64) error
}

// Lease represents a distributed lease to ensure that only a single Litestream
// instance replicates to a replica at a time. This prevents duplicate streams
// from overwriting each other and causing data loss.
type Lease struct {
	// Required. Incremented on each leader change.
	Epoch int64 `json:"epoch"`

	// Timestamp of when the lease was last modified.
	ModTime time.Time `json:"-"`

	// Required. Duration after last modified time that lease is valid.
	// If set to zero, lease is immediately expired.
	Timeout time.Duration `json:"timeout"`

	// Optional. Specifies a description of the process that acquired the lease.
	// For example, a hostname or machine ID.
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

// LeaseExistsError represents an error returned when trying to acquire a lease
// when another lease already exists and has not expired yet.
type LeaseExistsError struct {
	Lease *Lease
}

// NewLeaseExistsError returns a new instance of LeaseExistsError.
func NewLeaseExistsError(lease *Lease) *LeaseExistsError {
	return &LeaseExistsError{Lease: lease}
}

// Error implements the error interface.
func (e *LeaseExistsError) Error() string {
	return fmt.Sprintf("lease exists (epoch %d)", e.Lease.Epoch)
}
