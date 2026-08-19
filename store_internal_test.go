package litestream

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

func TestStore_CloseFlushesBeforeInterruptibleMonitorWait(t *testing.T) {
	client := newBlockingSnapshotClient(t.TempDir())
	dbPath := filepath.Join(t.TempDir(), "db")
	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.Replica = NewReplicaWithClient(db, client)
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()
	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, beforeTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}

	store := NewStore([]*DB{db}, CompactionLevels{{Level: 0}})
	store.L0Retention = 0
	store.SnapshotInterval = time.Hour
	done := make(chan struct{})
	store.SetDone(done)

	if err := store.Open(t.Context()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-client.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for snapshot upload")
	}

	if _, err := sqldb.Exec(`INSERT INTO t DEFAULT VALUES`); err != nil {
		t.Fatal(err)
	}

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- store.Close(context.Background())
	}()

	cleaned := false
	t.Cleanup(func() {
		if cleaned {
			return
		}
		client.unblock()
		select {
		case <-done:
		default:
			close(done)
		}
		select {
		case <-closeResult:
		case <-time.After(5 * time.Second):
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for db.IsOpen() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if db.IsOpen() {
		t.Fatal("database did not reach final sync while snapshot upload was blocked")
	}

	_, afterTXID, err := db.MaxLTX()
	if err != nil {
		t.Fatal(err)
	}
	if afterTXID <= beforeTXID {
		t.Fatalf("final sync txid=%s, want greater than %s", afterTXID, beforeTXID)
	}
	if got := db.Replica.Pos().TXID; got != afterTXID {
		t.Fatalf("replica txid=%s, want %s", got, afterTXID)
	}

	select {
	case err := <-closeResult:
		t.Fatalf("Store.Close returned before the second signal: %v", err)
	default:
	}

	close(done)
	select {
	case err := <-closeResult:
		if !errors.Is(err, ErrShutdownInterrupted) {
			t.Fatalf("Store.Close error=%v, want ErrShutdownInterrupted", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Store.Close did not honor the second signal")
	}

	client.unblock()
	monitorsDone := make(chan struct{})
	go func() {
		store.wg.Wait()
		close(monitorsDone)
	}()
	select {
	case <-monitorsDone:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot monitor did not stop after upload unblocked")
	}
	cleaned = true
}

func TestStore_WaitForMonitorsHonorsContext(t *testing.T) {
	store := NewStore(nil, nil)
	store.wg.Add(1)
	defer store.wg.Done()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.waitForMonitors(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want context canceled", err)
	}
}

type blockingSnapshotClient struct {
	*testReplicaClient
	started     chan struct{}
	unblockCh   chan struct{}
	startedOnce sync.Once
	unblockOnce sync.Once
}

func newBlockingSnapshotClient(dir string) *blockingSnapshotClient {
	return &blockingSnapshotClient{
		testReplicaClient: &testReplicaClient{dir: dir},
		started:           make(chan struct{}),
		unblockCh:         make(chan struct{}),
	}
}

func (c *blockingSnapshotClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if level != SnapshotLevel {
		return c.testReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
	}
	c.startedOnce.Do(func() { close(c.started) })
	<-c.unblockCh
	return nil, context.Canceled
}

func (c *blockingSnapshotClient) unblock() {
	c.unblockOnce.Do(func() { close(c.unblockCh) })
}
