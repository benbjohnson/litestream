//go:build vfs && soak
// +build vfs,soak

package main_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// TestVFS_LongRunningSoak exercises the VFS under sustained read/write load.
// The default duration is 5 minutes but can be overridden with the
// LITESTREAM_VFS_SOAK_DURATION environment variable (e.g. "10m").
func TestVFS_LongRunningSoak(t *testing.T) {
	duration := 5 * time.Minute
	if v := os.Getenv("LITESTREAM_VFS_SOAK_DURATION"); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil {
			duration = parsed
		}
	}
	if testing.Short() && duration > time.Minute {
		duration = time.Minute
	}

	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	vfs.PollInterval = 100 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)

	db, primary := openReplicatedPrimary(t, client, 75*time.Millisecond, 75*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec(`CREATE TABLE t (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		value TEXT,
		updated_at INTEGER
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	seedLargeTable(t, primary, 1000)
	forceReplicaSync(t, db)

	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()

	waitForReplicaRowCount(t, primary, replica, time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var writeOps atomic.Int64
	var readOps atomic.Int64
	errCh := make(chan error, 8)
	var wg sync.WaitGroup

	// Writers continuously mutate the primary database.
	startWriter := func(name string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rnd := time.NewTicker(7 * time.Millisecond)
			defer rnd.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-rnd.C:
					if _, err := primary.Exec("INSERT INTO t (value, updated_at) VALUES (?, strftime('%s','now'))", fmt.Sprintf("%s-%d", name, time.Now().UnixNano())); err != nil {
						errCh <- fmt.Errorf("writer %s insert: %w", name, err)
						return
					}
					if _, err := primary.Exec("UPDATE t SET value = value || '-w' WHERE id IN (SELECT id FROM t ORDER BY RANDOM() LIMIT 1)"); err != nil {
						errCh <- fmt.Errorf("writer %s update: %w", name, err)
						return
					}
					writeOps.Add(2)
				}
			}
		}()
	}

	startReader := func(name string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				var minID, maxID, count int
				if err := replica.QueryRow("SELECT IFNULL(MIN(id),0), IFNULL(MAX(id),0), COUNT(*) FROM t").Scan(&minID, &maxID, &count); err != nil {
					errCh <- fmt.Errorf("reader %s query: %w", name, err)
					return
				}
				if minID > maxID && count > 0 {
					errCh <- fmt.Errorf("reader %s saw invalid range", name)
					return
				}
				readOps.Add(1)
			}
		}()
	}

	for i := 0; i < 2; i++ {
		startWriter(fmt.Sprintf("writer-%d", i))
	}
	for i := 0; i < 4; i++ {
		startReader(fmt.Sprintf("reader-%d", i))
	}

	<-ctx.Done()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("soak error: %v", err)
		}
	}

	if writeOps.Load() < int64(duration/time.Millisecond) {
		t.Fatalf("expected sustained writes, got %d ops", writeOps.Load())
	}
	if readOps.Load() == 0 {
		t.Fatalf("expected replica reads during soak")
	}

	waitForReplicaRowCount(t, primary, replica, time.Minute)
}
