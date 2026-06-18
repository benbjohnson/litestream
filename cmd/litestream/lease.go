package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	litestreams3 "github.com/benbjohnson/litestream/s3"
)

const defaultLeaseAcquireRetryWait = time.Second

type leaseRequest struct {
	db     *litestream.DB
	config LeaseConfig
}

type leaseEntry struct {
	path             string
	leaser           litestream.Leaser
	ttl              time.Duration
	heartbeat        time.Duration
	acquireTimeout   time.Duration
	acquireRetryWait time.Duration
}

type leaseManager struct {
	mu     sync.Mutex
	once   sync.Once
	wg     sync.WaitGroup
	cancel context.CancelFunc

	entries []leaseEntry
	leases  []*litestream.Lease
	notify  func(error)
}

func newLeaseManager(entries []leaseEntry, notify func(error)) *leaseManager {
	return &leaseManager{entries: entries, notify: notify}
}

func newLeaseManagerFromRequests(ctx context.Context, requests []leaseRequest, notify func(error)) (*leaseManager, error) {
	entries := make([]leaseEntry, 0, len(requests))
	for _, req := range requests {
		if !req.config.Required {
			continue
		}

		entry, err := newLeaseEntry(ctx, req.db, req.config)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return newLeaseManager(entries, notify), nil
}

func newLeaseEntry(ctx context.Context, db *litestream.DB, config LeaseConfig) (leaseEntry, error) {
	if db == nil || db.Replica == nil || db.Replica.Client == nil {
		return leaseEntry{}, fmt.Errorf("lease requires database replica")
	}

	client, ok := db.Replica.Client.(*litestreams3.ReplicaClient)
	if !ok {
		return leaseEntry{}, fmt.Errorf("lease required for %s but replica type %q does not support distributed leasing", db.Path(), db.Replica.Client.Type())
	}

	leaser, err := client.NewLeaser(ctx)
	if err != nil {
		return leaseEntry{}, fmt.Errorf("create s3 leaser for %s: %w", db.Path(), err)
	}

	ttl := leaser.TTL
	if config.TTL != nil {
		ttl = *config.TTL
		leaser.TTL = ttl
	}

	heartbeat := ttl / 2
	if config.Heartbeat != nil {
		heartbeat = *config.Heartbeat
	}
	if heartbeat >= ttl {
		return leaseEntry{}, &ConfigValidationError{
			Err:   ErrInvalidLeaseHeartbeat,
			Field: fmt.Sprintf("dbs[%s].lease.heartbeat", db.Path()),
			Value: heartbeat,
		}
	}

	var acquireTimeout time.Duration
	if config.AcquireTimeout != nil {
		acquireTimeout = *config.AcquireTimeout
	}

	return leaseEntry{
		path:             db.Path(),
		leaser:           leaser,
		ttl:              ttl,
		heartbeat:        heartbeat,
		acquireTimeout:   acquireTimeout,
		acquireRetryWait: defaultLeaseAcquireRetryWait,
	}, nil
}

func (m *leaseManager) Open(ctx context.Context) error {
	if m == nil || len(m.entries) == 0 {
		return nil
	}

	monitorCtx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancel = cancel
	m.leases = make([]*litestream.Lease, len(m.entries))
	m.mu.Unlock()

	for i := range m.entries {
		lease, err := m.acquireLease(ctx, &m.entries[i])
		if err != nil {
			cancel()
			if closeErr := m.releaseLeases(context.Background()); closeErr != nil {
				return fmt.Errorf("%w; release leases: %v", err, closeErr)
			}
			return err
		}

		m.mu.Lock()
		m.leases[i] = lease
		m.mu.Unlock()
	}

	for i := range m.entries {
		i := i
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.monitorLease(monitorCtx, i)
		}()
	}

	return nil
}

func (m *leaseManager) Close(ctx context.Context) error {
	if m == nil {
		return nil
	}

	m.mu.Lock()
	cancel := m.cancel
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
	return m.releaseLeases(ctx)
}

func (m *leaseManager) acquireLease(ctx context.Context, entry *leaseEntry) (*litestream.Lease, error) {
	acquireCtx := ctx
	cancel := func() {}
	if entry.acquireTimeout > 0 {
		acquireCtx, cancel = context.WithTimeout(ctx, entry.acquireTimeout)
	}
	defer cancel()

	for {
		if err := acquireCtx.Err(); err != nil {
			return nil, fmt.Errorf("acquire lease for %s: %w", entry.path, err)
		}

		lease, err := entry.leaser.AcquireLease(acquireCtx)
		if err == nil {
			return lease, nil
		}

		var existsErr *litestream.LeaseExistsError
		if !errors.As(err, &existsErr) {
			return nil, fmt.Errorf("acquire lease for %s: %w", entry.path, err)
		}

		wait := entry.acquireRetryWait
		if wait <= 0 {
			wait = defaultLeaseAcquireRetryWait
		}
		if !existsErr.ExpiresAt.IsZero() {
			if until := time.Until(existsErr.ExpiresAt); until > 0 && until < wait {
				wait = until
			}
		}

		timer := time.NewTimer(wait)
		select {
		case <-acquireCtx.Done():
			timer.Stop()
			return nil, fmt.Errorf("acquire lease for %s: %w", entry.path, acquireCtx.Err())
		case <-timer.C:
		}
	}
}

func (m *leaseManager) monitorLease(ctx context.Context, index int) {
	entry := m.entries[index]
	heartbeat := entry.heartbeat
	if heartbeat <= 0 {
		heartbeat = entry.ttl / 2
	}
	if heartbeat <= 0 {
		heartbeat = time.Second
	}

	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		m.mu.Lock()
		lease := m.leases[index]
		m.mu.Unlock()
		if lease == nil {
			return
		}

		nextLease, err := entry.leaser.RenewLease(ctx, lease)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.fail(fmt.Errorf("renew lease for %s: %w", entry.path, err))
			return
		}

		m.mu.Lock()
		m.leases[index] = nextLease
		m.mu.Unlock()
	}
}

func (m *leaseManager) fail(err error) {
	m.once.Do(func() {
		m.mu.Lock()
		cancel := m.cancel
		m.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if m.notify != nil {
			m.notify(err)
		}
	})
}

func (m *leaseManager) releaseLeases(ctx context.Context) error {
	type releaseEntry struct {
		entry leaseEntry
		lease *litestream.Lease
	}

	m.mu.Lock()
	releases := make([]releaseEntry, 0, len(m.leases))
	for i, lease := range m.leases {
		if lease == nil {
			continue
		}
		releases = append(releases, releaseEntry{entry: m.entries[i], lease: lease})
		m.leases[i] = nil
	}
	m.mu.Unlock()

	var err error
	for _, release := range releases {
		if e := release.entry.leaser.ReleaseLease(ctx, release.lease); e != nil && err == nil {
			err = fmt.Errorf("release lease for %s: %w", release.entry.path, e)
		}
	}
	return err
}
