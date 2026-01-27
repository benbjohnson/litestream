package litestream

import (
	"os"
	"sync"
	"time"

	"github.com/superfly/ltx"
)

// StatusMonitor manages replication status monitoring and event streaming.
type StatusMonitor struct {
	store *Store

	mu          sync.RWMutex
	subscribers map[chan *StatusEvent]struct{}
}

// NewStatusMonitor creates a new status monitor.
func NewStatusMonitor(store *Store) *StatusMonitor {
	return &StatusMonitor{
		store:       store,
		subscribers: make(map[chan *StatusEvent]struct{}),
	}
}

// Subscribe returns a channel for receiving status events.
// The caller must call Unsubscribe when done to avoid resource leaks.
func (m *StatusMonitor) Subscribe() chan *StatusEvent {
	ch := make(chan *StatusEvent, 64)
	m.mu.Lock()
	m.subscribers[ch] = struct{}{}
	m.mu.Unlock()
	return ch
}

// Unsubscribe removes a subscriber channel and closes it.
func (m *StatusMonitor) Unsubscribe(ch chan *StatusEvent) {
	m.mu.Lock()
	delete(m.subscribers, ch)
	m.mu.Unlock()
	close(ch)
}

// GetFullStatus returns a snapshot of all database statuses.
func (m *StatusMonitor) GetFullStatus() []DatabaseStatus {
	dbs := m.store.DBs()
	statuses := make([]DatabaseStatus, 0, len(dbs))
	for _, db := range dbs {
		statuses = append(statuses, getDatabaseStatus(db))
	}
	return statuses
}

// NotifySync broadcasts a sync event to all subscribers.
// Called by Replica.OnSync when LTX files are uploaded.
func (m *StatusMonitor) NotifySync(db *DB, pos ltx.Pos) {
	event := &StatusEvent{
		Type:      "sync",
		Timestamp: time.Now(),
		Database: &DatabaseStatus{
			Path:        db.Path(),
			ReplicaTXID: pos.TXID.String(),
			Status:      "ok",
		},
	}
	m.broadcast(event)
}

// broadcast sends an event to all subscribers.
// Events are dropped for slow subscribers to avoid blocking.
func (m *StatusMonitor) broadcast(event *StatusEvent) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for ch := range m.subscribers {
		select {
		case ch <- event:
		default:
			// Drop if subscriber is slow
		}
	}
}

// getDatabaseStatus gathers status information for a single database.
func getDatabaseStatus(db *DB) DatabaseStatus {
	status := DatabaseStatus{
		Path:   db.Path(),
		Status: "unknown",
	}

	status.Enabled = db.IsOpen()
	if !status.Enabled {
		status.Status = "disabled"
		return status
	}

	// Get local TXID from L0 directory
	_, maxTXID, err := db.MaxLTX()
	if err == nil && maxTXID > 0 {
		status.LocalTXID = maxTXID.String()
		status.Status = "ok"
	} else if err == nil {
		status.Status = "initializing"
	} else {
		status.Status = "error"
	}

	// Get replica info
	if db.Replica != nil {
		replicaPos := db.Replica.Pos()
		if !replicaPos.IsZero() {
			status.ReplicaTXID = replicaPos.TXID.String()
		}
		status.ReplicaType = db.Replica.Client.Type()

		// Calculate sync lag (local - replica)
		if maxTXID > 0 && !replicaPos.IsZero() {
			status.SyncLag = int64(maxTXID) - int64(replicaPos.TXID)
		}
	}

	// Get last sync time
	status.LastSyncAt = db.LastSuccessfulSyncAt()

	// Get file sizes
	if fi, err := os.Stat(db.Path()); err == nil {
		status.DBSize = fi.Size()
	}
	if fi, err := os.Stat(db.WALPath()); err == nil {
		status.WALSize = fi.Size()
	}

	return status
}

// StatusEvent represents a single NDJSON event sent to monitoring clients.
type StatusEvent struct {
	Type      string           `json:"type"` // "full" or "sync"
	Timestamp time.Time        `json:"timestamp"`
	Database  *DatabaseStatus  `json:"database,omitempty"`  // for "sync" events
	Databases []DatabaseStatus `json:"databases,omitempty"` // for "full" events
}

// DatabaseStatus represents the replication status of a single database.
type DatabaseStatus struct {
	Path        string    `json:"path"`
	Enabled     bool      `json:"enabled"`
	LocalTXID   string    `json:"local_txid,omitempty"`
	ReplicaTXID string    `json:"replica_txid,omitempty"`
	LastSyncAt  time.Time `json:"last_sync_at,omitempty"`
	Status      string    `json:"status"` // "ok", "syncing", "disabled", "error", "initializing"
	SyncLag     int64     `json:"sync_lag,omitempty"`
	ReplicaType string    `json:"replica_type,omitempty"`
	DBSize      int64     `json:"db_size_bytes,omitempty"`
	WALSize     int64     `json:"wal_size_bytes,omitempty"`
}
