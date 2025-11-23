//go:build vfs && stress
// +build vfs,stress

package main_test

import (
	"context"
	"math/rand"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestVFS_RaceStressHarness(t *testing.T) {
	if os.Getenv("LITESTREAM_ALLOW_RACE") != "1" {
		t.Skip("set LITESTREAM_ALLOW_RACE=1 to run unstable race harness; modernc.org/sqlite checkptr panics are still unresolved")
	}
	if !runtime.RaceEnabled() {
		t.Skip("requires go test -race")
	}

	client := file.NewReplicaClient(t.TempDir())
	db, primary := openReplicatedPrimary(t, client, 20*time.Millisecond, 20*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE stress (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	seedLargeTable(t, primary, 100)

	vfs := newVFS(t, client)
	vfs.PollInterval = 5 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)
	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()
	waitForReplicaRowCount(t, primary, replica, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var writes atomic.Int64
	go func() {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if _, err := primary.Exec("INSERT INTO stress (value) VALUES (?)", randomPayload(rnd, 64)); err != nil && !isBusyError(err) {
				t.Errorf("writer error: %v", err)
				return
			}
			writes.Add(1)
		}
	}()

	const readers = 64
	errCh := make(chan error, readers)
	for i := 0; i < readers; i++ {
		go func() {
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				select {
				case <-ctx.Done():
					errCh <- nil
					return
				default:
				}
				var count int
				if err := replica.QueryRow("SELECT COUNT(*) FROM stress WHERE id >= ?", rnd.Intn(50)).Scan(&count); err != nil {
					if isBusyError(err) {
						continue
					}
					errCh <- err
					return
				}
			}
		}()
	}

	for i := 0; i < readers; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("reader error: %v", err)
		}
	}

	if writes.Load() == 0 {
		t.Fatalf("writer never made progress")
	}
}
