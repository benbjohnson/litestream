package litestream

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrLeaseNotHeld = errors.New("lease not held")

type LeaseExistsError struct {
	Owner     string
	ExpiresAt time.Time
}

func (e *LeaseExistsError) Error() string {
	if e.Owner != "" {
		return fmt.Sprintf("lease already held by %s until %s", e.Owner, e.ExpiresAt.Format(time.RFC3339))
	}
	return fmt.Sprintf("lease already held until %s", e.ExpiresAt.Format(time.RFC3339))
}

type Leaser interface {
	Type() string
	AcquireLease(ctx context.Context) (*Lease, error)
	RenewLease(ctx context.Context, lease *Lease) (*Lease, error)
	ReleaseLease(ctx context.Context, lease *Lease) error
}

type Lease struct {
	Token     int64     `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
	Owner     string    `json:"owner,omitempty"`
	ETag      string    `json:"-"`
}

func (l *Lease) IsExpired() bool {
	return time.Now().After(l.ExpiresAt)
}

func (l *Lease) TTL() time.Duration {
	return time.Until(l.ExpiresAt)
}
