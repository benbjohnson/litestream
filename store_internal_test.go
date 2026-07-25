package litestream

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStore_CloseStopsMonitorsBeforeDBClose(t *testing.T) {
	handler := newBlockingCompactionMonitorHandler()
	db := NewDB(filepath.Join(t.TempDir(), "db"))
	db.Replica = NewReplicaWithClient(db, &testReplicaClient{dir: t.TempDir()})
	store := NewStore([]*DB{db}, CompactionLevels{{Level: 0}})
	store.L0Retention = 0
	store.Logger = slog.New(handler)

	if err := store.Open(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		handler.unblock()
		if db.IsOpen() {
			_ = store.Close(context.Background())
		}
	})

	select {
	case <-handler.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for compaction monitor")
	}

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- store.Close(context.Background())
	}()

	var dbOpenAtCancel bool
	select {
	case <-store.ctx.Done():
		dbOpenAtCancel = db.IsOpen()
	case <-time.After(5 * time.Second):
		handler.unblock()
		<-closeResult
		t.Fatal("timed out waiting for Store cancellation")
	}

	handler.unblock()
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
	if !dbOpenAtCancel {
		t.Fatal("database closed before Store monitors stopped")
	}
}

type blockingCompactionMonitorHandler struct {
	started     chan struct{}
	unblockCh   chan struct{}
	startedOnce sync.Once
	unblockOnce sync.Once
}

func newBlockingCompactionMonitorHandler() *blockingCompactionMonitorHandler {
	return &blockingCompactionMonitorHandler{
		started:   make(chan struct{}),
		unblockCh: make(chan struct{}),
	}
}

func (h *blockingCompactionMonitorHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *blockingCompactionMonitorHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Message == "starting compaction monitor" {
		h.startedOnce.Do(func() { close(h.started) })
		<-h.unblockCh
	}
	return nil
}

func (h *blockingCompactionMonitorHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *blockingCompactionMonitorHandler) WithGroup(string) slog.Handler {
	return h
}

func (h *blockingCompactionMonitorHandler) unblock() {
	h.unblockOnce.Do(func() { close(h.unblockCh) })
}
