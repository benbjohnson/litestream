//go:build vfs

package litestream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/psanford/sqlite3vfs"
	"github.com/superfly/ltx"
)

func TestVFSFile_LockStateMachine(t *testing.T) {
	f := &VFSFile{logger: slog.Default()}

	if err := f.Lock(sqlite3vfs.LockShared); err != nil {
		t.Fatalf("lock shared: %v", err)
	}
	if reserved, _ := f.CheckReservedLock(); reserved {
		t.Fatalf("shared lock should not report reserved")
	}

	if err := f.Lock(sqlite3vfs.LockReserved); err != nil {
		t.Fatalf("lock reserved: %v", err)
	}
	if reserved, _ := f.CheckReservedLock(); !reserved {
		t.Fatalf("reserved lock should report reserved")
	}

	if err := f.Lock(sqlite3vfs.LockShared); err == nil {
		t.Fatalf("expected downgrade via Lock to fail")
	}

	if err := f.Unlock(sqlite3vfs.LockShared); err != nil {
		t.Fatalf("unlock to shared: %v", err)
	}
	if reserved, _ := f.CheckReservedLock(); reserved {
		t.Fatalf("unlock to shared should clear reserved state")
	}

	if err := f.Unlock(sqlite3vfs.LockPending); err == nil {
		t.Fatalf("expected unlock to pending to fail")
	}

	if err := f.Lock(sqlite3vfs.LockExclusive); err != nil {
		t.Fatalf("lock exclusive: %v", err)
	}

	if err := f.Unlock(sqlite3vfs.LockNone); err != nil {
		t.Fatalf("unlock to none: %v", err)
	}
}

func TestVFSFile_PendingIndexIsolation(t *testing.T) {
	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))

	f := NewVFSFile(client, "test.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}

	if err := f.Lock(sqlite3vfs.LockShared); err != nil {
		t.Fatalf("lock shared: %v", err)
	}

	client.addFixture(t, buildLTXFixture(t, 2, 'b'))
	if err := f.pollReplicaClient(context.Background()); err != nil {
		t.Fatalf("poll replica: %v", err)
	}

	f.mu.Lock()
	pendingLen := len(f.pending)
	current := f.index[1]
	f.mu.Unlock()

	if pendingLen == 0 {
		t.Fatalf("expected pending index entries while shared lock held")
	}
	if current.MinTXID != 1 {
		t.Fatalf("main index should still reference first txid, got %s", current.MinTXID)
	}

	buf := make([]byte, 4096)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("read during lock: %v", err)
	}
	if buf[0] != 'a' {
		t.Fatalf("expected old data during lock, got %q", buf[0])
	}

	if err := f.Unlock(sqlite3vfs.LockNone); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("read after unlock: %v", err)
	}
	if buf[0] != 'b' {
		t.Fatalf("expected updated data after unlock, got %q", buf[0])
	}
}

func TestVFSFile_PendingIndexRace(t *testing.T) {
	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))

	f := NewVFSFile(client, "race.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}

	if err := f.Lock(sqlite3vfs.LockShared); err != nil {
		t.Fatalf("lock shared: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	// continuously stream new fixtures
	go func() {
		txid := ltx.TXID(2)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			client.addFixture(t, buildLTXFixture(t, txid, byte('a'+int(txid%26))))
			if err := f.pollReplicaClient(context.Background()); err != nil {
				t.Errorf("poll replica: %v", err)
				return
			}
			txid++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	var wg sync.WaitGroup
	buf := make([]byte, 4096)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if _, err := f.ReadAt(buf, 0); err != nil {
					t.Errorf("reader %d: %v", id, err)
					return
				}
			}
		}(i)
	}

	<-ctx.Done()
	f.Unlock(sqlite3vfs.LockNone)
	wg.Wait()
}

func TestVFSFileMonitorStopsOnCancel(t *testing.T) {
	client := newCountingReplicaClient()
	f := &VFSFile{client: client, logger: slog.Default(), PollInterval: 5 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); f.monitorReplicaClient(ctx) }()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if client.calls.Load() > 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	if client.calls.Load() == 0 {
		t.Fatalf("monitor never invoked LTXFiles")
	}

	cancel()
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("monitor goroutine did not exit after cancel")
	}
}

func TestVFSFile_NonContiguousTXIDError(t *testing.T) {
	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))

	f := NewVFSFile(client, "gap.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}

	client.addFixture(t, buildLTXFixture(t, 3, 'c'))
	if err := f.pollReplicaClient(context.Background()); err != nil {
		t.Fatalf("poll replica: %v", err)
	}
	if pos := f.Pos(); pos.TXID != 1 {
		t.Fatalf("unexpected txid advance after gap: got %s", pos.TXID.String())
	}
}

func TestVFSFile_IndexMemoryDoesNotGrowUnbounded(t *testing.T) {
	const pageLimit = 16
	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))

	f := NewVFSFile(client, "mem.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}

	for i := 0; i < 100; i++ {
		pgno := uint32(i%pageLimit) + 2
		client.addFixture(t, buildLTXFixtureWithPages(t, ltx.TXID(i+2), 4096, []uint32{pgno}, byte('b'+byte(i%26))))
		if err := f.pollReplicaClient(context.Background()); err != nil {
			t.Fatalf("poll replica: %v", err)
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if l := len(f.index); l > pageLimit+1 { // +1 for initial page 1
		t.Fatalf("index grew unexpectedly: got %d want <= %d", l, pageLimit+1)
	}
}

func TestVFSFile_AutoVacuumShrinksCommit(t *testing.T) {
	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixtureWithPages(t, 1, 4096, []uint32{1, 2, 3, 4}, 'a'))

	f := NewVFSFile(client, "autovac.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}

	client.addFixture(t, buildLTXFixtureWithPages(t, 2, 4096, []uint32{1, 2}, 'b'))
	if err := f.pollReplicaClient(context.Background()); err != nil {
		t.Fatalf("poll replica: %v", err)
	}

	size, err := f.FileSize()
	if err != nil {
		t.Fatalf("file size: %v", err)
	}
	if size != int64(2*4096) {
		t.Fatalf("unexpected file size after vacuum: got %d want %d", size, 2*4096)
	}

	buf := make([]byte, 4096)
	lockOffset := int64(3-1) * 4096
	if _, err := f.ReadAt(buf, lockOffset); err == nil || !strings.Contains(err.Error(), "page not found") {
		t.Fatalf("expected missing page after vacuum, got %v", err)
	}
}

func TestVFSFile_PendingIndexReplacementRemovesStalePages(t *testing.T) {
	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixtureWithPages(t, 1, 4096, []uint32{1, 2, 3, 4}, 'a'))

	f := NewVFSFile(client, "pending-replace.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}

	if err := f.Lock(sqlite3vfs.LockShared); err != nil {
		t.Fatalf("lock shared: %v", err)
	}

	client.addFixture(t, buildLTXFixtureWithPages(t, 2, 4096, []uint32{1, 2}, 'b'))
	if err := f.pollReplicaClient(context.Background()); err != nil {
		t.Fatalf("poll replica: %v", err)
	}

	f.mu.Lock()
	if _, ok := f.index[4]; !ok {
		t.Fatalf("expected stale page to remain in main index while lock is held")
	}
	if !f.pendingReplace {
		t.Fatalf("expected pending replacement flag set")
	}
	f.mu.Unlock()

	if err := f.Unlock(sqlite3vfs.LockNone); err != nil {
		t.Fatalf("unlock: %v", err)
	}

	size, err := f.FileSize()
	if err != nil {
		t.Fatalf("file size: %v", err)
	}
	if size != int64(2*4096) {
		t.Fatalf("unexpected file size after pending replacement applied: got %d want %d", size, 2*4096)
	}

	buf := make([]byte, 4096)
	lockOffset := int64(3-1) * 4096
	if _, err := f.ReadAt(buf, lockOffset); err == nil || !strings.Contains(err.Error(), "page not found") {
		t.Fatalf("expected missing page after pending replacement applied, got %v", err)
	}
}

func TestVFSFile_CorruptedPageIndexRecovery(t *testing.T) {
	client := newMockReplicaClient()
	client.addFixture(t, &ltxFixture{info: &ltx.FileInfo{Level: 0, MinTXID: 1, MaxTXID: 1, Size: 0}, data: []byte("bad-index")})

	f := NewVFSFile(client, "corrupt.db", slog.Default())
	if err := f.Open(); err == nil {
		t.Fatalf("expected open to fail on corrupted index")
	}
}

func TestVFSFile_OpenSeedsLevel1Position(t *testing.T) {
	client := newMockReplicaClient()
	snapshot := buildLTXFixture(t, 1, 's')
	snapshot.info.Level = SnapshotLevel
	client.addFixture(t, snapshot)
	l1 := buildLTXFixture(t, 2, 'l')
	l1.info.Level = 1
	client.addFixture(t, l1)
	l0 := buildLTXFixture(t, 3, 'z')
	l0.info.Level = 0
	client.addFixture(t, l0)

	f := NewVFSFile(client, "seed-level1.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}
	defer f.Close()

	if got, want := f.maxTXID1, l1.info.MaxTXID; got != want {
		t.Fatalf("unexpected maxTXID1: got %s want %s", got, want)
	}
	if got, want := f.Pos().TXID, l0.info.MaxTXID; got != want {
		t.Fatalf("unexpected pos after open: got %s want %s", got, want)
	}
}

func TestVFSFile_OpenSeedsLevel1PositionFromPos(t *testing.T) {
	client := newMockReplicaClient()
	snapshot := buildLTXFixture(t, 1, 's')
	snapshot.info.Level = SnapshotLevel
	client.addFixture(t, snapshot)
	l0 := buildLTXFixture(t, 2, '0')
	l0.info.Level = 0
	client.addFixture(t, l0)

	f := NewVFSFile(client, "seed-default.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}
	defer f.Close()

	pos := f.Pos().TXID
	if pos == 0 {
		t.Fatalf("expected non-zero position")
	}
	if got := f.maxTXID1; got != pos {
		t.Fatalf("expected maxTXID1 to equal pos when no L1 files, got %s want %s", got, pos)
	}
}

func TestVFSFile_HeaderForcesDeleteJournal(t *testing.T) {
	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'h'))

	f := NewVFSFile(client, "header.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 32)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("read header: %v", err)
	}
	if buf[18] != 0x01 || buf[19] != 0x01 {
		t.Fatalf("journal mode bytes not forced to DELETE, got %x %x", buf[18], buf[19])
	}
}

func TestVFSFile_ReadAtLockPageBoundary(t *testing.T) {
	pageSizes := []uint32{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}
	for _, pageSize := range pageSizes {
		pageSize := pageSize
		t.Run(fmt.Sprintf("page_%d", pageSize), func(t *testing.T) {
			client := newMockReplicaClient()
			lockPgno := ltx.LockPgno(pageSize)
			before := lockPgno - 1
			after := lockPgno + 1

			client.addFixture(t, buildLTXFixtureWithPage(t, 1, pageSize, 1, 'z'))
			client.addFixture(t, buildLTXFixtureWithPage(t, 2, pageSize, before, 'b'))
			client.addFixture(t, buildLTXFixtureWithPage(t, 3, pageSize, after, 'a'))

			f := NewVFSFile(client, fmt.Sprintf("lock-boundary-%d.db", pageSize), slog.Default())
			if err := f.Open(); err != nil {
				t.Fatalf("open vfs file: %v", err)
			}
			defer f.Close()

			buf := make([]byte, int(pageSize))
			off := int64(before-1) * int64(pageSize)
			if _, err := f.ReadAt(buf, off); err != nil {
				t.Fatalf("read before lock page: %v", err)
			}
			if buf[0] != 'b' {
				t.Fatalf("unexpected data before lock page: got %q", buf[0])
			}

			buf = make([]byte, int(pageSize))
			off = int64(after-1) * int64(pageSize)
			if _, err := f.ReadAt(buf, off); err != nil {
				t.Fatalf("read after lock page: %v", err)
			}
			if buf[0] != 'a' {
				t.Fatalf("unexpected data after lock page: got %q", buf[0])
			}

			buf = make([]byte, int(pageSize))
			lockOffset := int64(lockPgno-1) * int64(pageSize)
			if _, err := f.ReadAt(buf, lockOffset); err == nil || !strings.Contains(err.Error(), "page not found") {
				t.Fatalf("expected missing lock page error, got %v", err)
			}
		})
	}
}

func TestVFS_TempFileLifecycleStress(t *testing.T) {
	vfs := NewVFS(nil, slog.Default())
	const (
		workers    = 8
		iterations = 50
	)

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				name := fmt.Sprintf("temp-%02d-%02d.db", w, i)
				flags := sqlite3vfs.OpenTempDB | sqlite3vfs.OpenReadWrite | sqlite3vfs.OpenCreate
				deleteOnClose := (w+i)%2 == 0
				if deleteOnClose {
					flags |= sqlite3vfs.OpenDeleteOnClose
				}

				file, _, err := vfs.openTempFile(name, flags)
				if err != nil {
					errCh <- fmt.Errorf("open temp file: %w", err)
					return
				}
				tf := file.(*localTempFile)
				if _, err := tf.WriteAt([]byte("hot-data"), 0); err != nil {
					errCh <- fmt.Errorf("write temp file: %w", err)
					return
				}

				path, tracked := vfs.loadTempFilePath(name)
				if !tracked && name != "" {
					errCh <- fmt.Errorf("temp file %s was not tracked", name)
					return
				}

				if err := tf.Close(); err != nil {
					errCh <- fmt.Errorf("close temp file: %w", err)
					return
				}

				if deleteOnClose {
					if path != "" {
						if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
							errCh <- fmt.Errorf("delete-on-close leaked temp file %s", path)
							return
						}
					}
				} else {
					if path == "" {
						errCh <- fmt.Errorf("missing tracked path for %s", name)
						return
					}
					if _, err := os.Stat(path); err != nil {
						errCh <- fmt.Errorf("expected temp file on disk: %v", err)
						return
					}
					if err := os.Remove(path); err != nil {
						errCh <- fmt.Errorf("cleanup temp file: %v", err)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("temp file stress: %v", err)
		}
	}

	leak := false
	vfs.tempFiles.Range(func(key, value any) bool {
		leak = true
		return false
	})
	if leak {
		t.Fatalf("temp files still tracked after stress run")
	}

	if dir := vfs.tempDir; dir != "" {
		entries, err := os.ReadDir(dir)
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("read temp dir: %v", err)
		}
		if err == nil && len(entries) > 0 {
			names := make([]string, 0, len(entries))
			for _, entry := range entries {
				names = append(names, entry.Name())
			}
			t.Fatalf("temp dir not cleaned: %v", names)
		}
	}
}

func TestVFS_TempFileNameCollision(t *testing.T) {
	vfs := NewVFS(nil, slog.Default())
	name := "collision.db"
	flags := sqlite3vfs.OpenTempDB | sqlite3vfs.OpenReadWrite | sqlite3vfs.OpenCreate

	file1, _, err := vfs.openTempFile(name, flags)
	if err != nil {
		t.Fatalf("open temp file1: %v", err)
	}
	tf1 := file1.(*localTempFile)
	path1, ok := vfs.loadTempFilePath(name)
	if !ok {
		t.Fatalf("first temp file not tracked")
	}

	file2, _, err := vfs.openTempFile(name, flags|sqlite3vfs.OpenDeleteOnClose)
	if err != nil {
		t.Fatalf("open temp file2: %v", err)
	}
	tf2 := file2.(*localTempFile)
	path2, ok := vfs.loadTempFilePath(name)
	if !ok {
		t.Fatalf("second temp file not tracked")
	}
	if path1 != path2 {
		t.Fatalf("expected same canonical path, got %s vs %s", path1, path2)
	}

	if err := tf2.Close(); err != nil {
		t.Fatalf("close second file: %v", err)
	}
	if _, err := os.Stat(path2); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected file removed after delete-on-close")
	}
	if _, ok := vfs.loadTempFilePath(name); ok {
		t.Fatalf("canonical entry should be cleared after delete-on-close")
	}
	if err := tf1.Close(); err != nil {
		t.Fatalf("close first file: %v", err)
	}
}

func TestVFS_TempFileSameBasenameDifferentDirs(t *testing.T) {
	vfs := NewVFS(nil, slog.Default())
	flags := sqlite3vfs.OpenTempDB | sqlite3vfs.OpenReadWrite | sqlite3vfs.OpenCreate

	name1 := filepath.Join("foo", "shared.db")
	name2 := filepath.Join("bar", "shared.db")

	file1, _, err := vfs.openTempFile(name1, flags)
	if err != nil {
		t.Fatalf("open first temp file: %v", err)
	}
	tf1 := file1.(*localTempFile)
	path1, ok := vfs.loadTempFilePath(name1)
	if !ok {
		t.Fatalf("first temp file not tracked")
	}

	file2, _, err := vfs.openTempFile(name2, flags|sqlite3vfs.OpenDeleteOnClose)
	if err != nil {
		t.Fatalf("open second temp file: %v", err)
	}
	tf2 := file2.(*localTempFile)
	path2, ok := vfs.loadTempFilePath(name2)
	if !ok {
		t.Fatalf("second temp file not tracked")
	}

	if path1 == path2 {
		t.Fatalf("expected unique paths for %s and %s", name1, name2)
	}

	if err := tf1.Close(); err != nil {
		t.Fatalf("close first file: %v", err)
	}

	if _, ok := vfs.loadTempFilePath(name2); !ok {
		t.Fatalf("closing first file should not unregister second")
	}

	if path1 != "" {
		if err := os.Remove(path1); err != nil && !os.IsNotExist(err) {
			t.Fatalf("cleanup first temp file: %v", err)
		}
	}

	if err := tf2.Close(); err != nil {
		t.Fatalf("close second file: %v", err)
	}
	if _, ok := vfs.loadTempFilePath(name2); ok {
		t.Fatalf("delete-on-close should clear second temp file")
	}
}

func TestVFS_TempFileDeleteOnClose(t *testing.T) {
	vfs := NewVFS(nil, slog.Default())
	name := "delete-on-close.db"
	flags := sqlite3vfs.OpenTempDB | sqlite3vfs.OpenReadWrite | sqlite3vfs.OpenCreate | sqlite3vfs.OpenDeleteOnClose

	file, _, err := vfs.openTempFile(name, flags)
	if err != nil {
		t.Fatalf("open temp file: %v", err)
	}
	tf := file.(*localTempFile)
	path, ok := vfs.loadTempFilePath(name)
	if !ok {
		t.Fatalf("temp file not tracked")
	}

	if _, err := tf.WriteAt([]byte("x"), 0); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := tf.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected delete-on-close to remove temp file")
	}
	if _, ok := vfs.loadTempFilePath(name); ok {
		t.Fatalf("temp file tracking entry should be cleared")
	}
	if err := vfs.Delete(name, false); err != nil {
		t.Fatalf("delete should ignore missing temp files: %v", err)
	}
	if err := vfs.Delete(name, false); err != nil {
		t.Fatalf("delete should ignore repeated temp deletes: %v", err)
	}
}

func TestLocalTempFileLocking(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "local-temp-*")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	tf := newLocalTempFile(f, false, nil)
	defer tf.Close()

	assertReserved := func(want bool) {
		t.Helper()
		got, err := tf.CheckReservedLock()
		if err != nil {
			t.Fatalf("check reserved: %v", err)
		}
		if got != want {
			t.Fatalf("reserved lock state mismatch: got %v want %v", got, want)
		}
	}

	assertReserved(false)

	if err := tf.Lock(sqlite3vfs.LockShared); err != nil {
		t.Fatalf("lock shared: %v", err)
	}
	assertReserved(false)

	if err := tf.Lock(sqlite3vfs.LockReserved); err != nil {
		t.Fatalf("lock reserved: %v", err)
	}
	assertReserved(true)

	if err := tf.Unlock(sqlite3vfs.LockShared); err != nil {
		t.Fatalf("unlock shared: %v", err)
	}
	assertReserved(false)

	if err := tf.Lock(sqlite3vfs.LockExclusive); err != nil {
		t.Fatalf("lock exclusive: %v", err)
	}
	assertReserved(true)

	if err := tf.Unlock(sqlite3vfs.LockNone); err != nil {
		t.Fatalf("unlock none: %v", err)
	}
	assertReserved(false)
}

func TestVFS_DeleteIgnoresMissingTempFiles(t *testing.T) {
	vfs := NewVFS(nil, slog.Default())

	t.Run("AlreadyRemovedEntry", func(t *testing.T) {
		name := "already-removed.db"
		flags := sqlite3vfs.OpenTempDB | sqlite3vfs.OpenReadWrite | sqlite3vfs.OpenCreate | sqlite3vfs.OpenDeleteOnClose

		file, _, err := vfs.openTempFile(name, flags)
		if err != nil {
			t.Fatalf("open temp file: %v", err)
		}
		tf := file.(*localTempFile)
		if err := tf.Close(); err != nil {
			t.Fatalf("close temp file: %v", err)
		}
		if err := vfs.Delete(name, false); err != nil {
			t.Fatalf("delete should ignore missing tracked entry: %v", err)
		}
	})

	t.Run("MissingOnDisk", func(t *testing.T) {
		name := "missing-on-disk.db"
		flags := sqlite3vfs.OpenTempDB | sqlite3vfs.OpenReadWrite | sqlite3vfs.OpenCreate

		file, _, err := vfs.openTempFile(name, flags)
		if err != nil {
			t.Fatalf("open temp file: %v", err)
		}
		tf := file.(*localTempFile)

		path, ok := vfs.loadTempFilePath(name)
		if !ok {
			t.Fatalf("temp file not tracked")
		}
		if err := os.Remove(path); err != nil {
			t.Fatalf("remove backing file: %v", err)
		}
		if err := vfs.Delete(name, false); err != nil {
			t.Fatalf("delete should ignore missing file: %v", err)
		}
		if _, ok := vfs.loadTempFilePath(name); ok {
			t.Fatalf("temp file tracking entry should be cleared")
		}
		if err := tf.Close(); err != nil {
			t.Fatalf("close temp file: %v", err)
		}
	})
}

func TestVFS_TempDirExhaustion(t *testing.T) {
	vfs := NewVFS(nil, slog.Default())
	injected := fmt.Errorf("temp dir exhausted")
	vfs.tempDirOnce.Do(func() { vfs.tempDirErr = injected })

	if _, err := vfs.ensureTempDir(); !errors.Is(err, injected) {
		t.Fatalf("expected ensureTempDir error, got %v", err)
	}

	if _, _, err := vfs.openTempFile("exhausted.db", sqlite3vfs.OpenTempDB); !errors.Is(err, injected) {
		t.Fatalf("openTempFile should surface exhaustion error, got %v", err)
	}
}

func TestVFSFile_PollingCancelsBlockedLTXFiles(t *testing.T) {
	client := newBlockingReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))

	f := NewVFSFile(client, "blocking.db", slog.Default())
	f.PollInterval = 5 * time.Millisecond
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}

	client.blockNext.Store(true)
	deadline := time.After(200 * time.Millisecond)
	select {
	case <-client.blocked:
	case <-deadline:
		t.Fatalf("expected monitor to block on LTXFiles")
	}

	done := make(chan struct{})
	go func() {
		_ = f.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("close did not unblock blocked LTXFiles call")
	}

	if !client.cancelled.Load() {
		t.Fatalf("blocking client did not observe context cancellation")
	}
}

// mockReplicaClient implements ReplicaClient for deterministic LTX fixtures.
type mockReplicaClient struct {
	mu    sync.Mutex
	files []*ltx.FileInfo
	data  map[string][]byte
}

type blockingReplicaClient struct {
	*mockReplicaClient
	blockNext atomic.Bool
	blocked   chan struct{}
	cancelled atomic.Bool
	once      sync.Once
}

type countingReplicaClient struct {
	calls atomic.Uint64
}

func newCountingReplicaClient() *countingReplicaClient { return &countingReplicaClient{} }

func (c *countingReplicaClient) Type() string { return "count" }

func (c *countingReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	c.calls.Add(1)
	return ltx.NewFileInfoSliceIterator(nil), nil
}

func (c *countingReplicaClient) OpenLTXFile(context.Context, int, ltx.TXID, ltx.TXID, int64, int64) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(nil)), nil
}

func (c *countingReplicaClient) WriteLTXFile(context.Context, int, ltx.TXID, ltx.TXID, io.Reader) (*ltx.FileInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *countingReplicaClient) DeleteLTXFiles(context.Context, []*ltx.FileInfo) error { return nil }

func (c *countingReplicaClient) DeleteAll(context.Context) error { return nil }

func newMockReplicaClient() *mockReplicaClient {
	return &mockReplicaClient{data: make(map[string][]byte)}
}

func newBlockingReplicaClient() *blockingReplicaClient {
	return &blockingReplicaClient{
		mockReplicaClient: newMockReplicaClient(),
		blocked:           make(chan struct{}),
	}
}

func (c *mockReplicaClient) Type() string { return "mock" }

func (c *mockReplicaClient) addFixture(tb testing.TB, fx *ltxFixture) {
	tb.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.files = append(c.files, fx.info)
	c.data[c.key(fx.info)] = fx.data
}

func (c *mockReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*ltx.FileInfo
	for _, info := range c.files {
		if info.Level == level && info.MinTXID >= seek {
			out = append(out, info)
		}
	}
	return ltx.NewFileInfoSliceIterator(out), nil
}

func (c *mockReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.makeKey(level, minTXID, maxTXID)
	data, ok := c.data[key]
	if !ok {
		return nil, fmt.Errorf("ltx file not found")
	}
	if offset > int64(len(data)) {
		return nil, fmt.Errorf("offset beyond data")
	}
	slice := data[offset:]
	if size > 0 && size < int64(len(slice)) {
		slice = slice[:size]
	}
	return io.NopCloser(bytes.NewReader(slice)), nil
}

func (c *mockReplicaClient) WriteLTXFile(context.Context, int, ltx.TXID, ltx.TXID, io.Reader) (*ltx.FileInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *mockReplicaClient) DeleteLTXFiles(context.Context, []*ltx.FileInfo) error {
	return fmt.Errorf("not implemented")
}

func (c *mockReplicaClient) DeleteAll(context.Context) error {
	return fmt.Errorf("not implemented")
}

func (c *blockingReplicaClient) Type() string { return "blocking" }

func (c *blockingReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	if seek > 1 && c.blockNext.Load() {
		if c.blockNext.CompareAndSwap(true, false) {
			c.once.Do(func() { close(c.blocked) })
			<-ctx.Done()
			c.cancelled.Store(true)
			return nil, ctx.Err()
		}
	}
	return c.mockReplicaClient.LTXFiles(ctx, level, seek, useMetadata)
}

func (c *blockingReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	return c.mockReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
}

func (c *blockingReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	return c.mockReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

func (c *blockingReplicaClient) DeleteLTXFiles(ctx context.Context, files []*ltx.FileInfo) error {
	return c.mockReplicaClient.DeleteLTXFiles(ctx, files)
}

func (c *blockingReplicaClient) DeleteAll(ctx context.Context) error {
	return c.mockReplicaClient.DeleteAll(ctx)
}

func (c *mockReplicaClient) key(info *ltx.FileInfo) string {
	return c.makeKey(info.Level, info.MinTXID, info.MaxTXID)
}

func (c *mockReplicaClient) makeKey(level int, minTXID, maxTXID ltx.TXID) string {
	return fmt.Sprintf("%d:%s:%s", level, minTXID.String(), maxTXID.String())
}

type ltxFixture struct {
	info *ltx.FileInfo
	data []byte
}

func buildLTXFixture(tb testing.TB, txid ltx.TXID, fill byte) *ltxFixture {
	return buildLTXFixtureWithPage(tb, txid, 4096, 1, fill)
}

func buildLTXFixtureWithPage(tb testing.TB, txid ltx.TXID, pageSize, pgno uint32, fill byte) *ltxFixture {
	return buildLTXFixtureWithPages(tb, txid, pageSize, []uint32{pgno}, fill)
}

func buildLTXFixtureWithPages(tb testing.TB, txid ltx.TXID, pageSize uint32, pgnos []uint32, fill byte) *ltxFixture {
	tb.Helper()
	if len(pgnos) == 0 {
		tb.Fatalf("pgnos required")
	}
	if txid == 1 {
		if len(pgnos) == 0 || pgnos[0] != 1 {
			tb.Fatalf("snapshot fixture must start at page 1")
		}
	}

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		tb.Fatalf("new encoder: %v", err)
	}
	maxPg := uint32(0)
	for _, pg := range pgnos {
		if pg > maxPg {
			maxPg = pg
		}
	}
	if maxPg == 0 {
		maxPg = 1
	}
	hdr := ltx.Header{
		Version:   ltx.Version,
		PageSize:  pageSize,
		Commit:    maxPg,
		MinTXID:   txid,
		MaxTXID:   txid,
		Timestamp: time.Now().UnixMilli(),
		Flags:     ltx.HeaderFlagNoChecksum,
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		tb.Fatalf("encode header: %v", err)
	}
	for _, pg := range pgnos {
		if pg == 0 {
			pg = 1
		}
		page := bytes.Repeat([]byte{fill}, int(pageSize))
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pg}, page); err != nil {
			tb.Fatalf("encode page %d: %v", pg, err)
		}
	}
	if err := enc.Close(); err != nil {
		tb.Fatalf("close encoder: %v", err)
	}

	info := &ltx.FileInfo{
		Level:     0,
		MinTXID:   txid,
		MaxTXID:   txid,
		Size:      int64(buf.Len()),
		CreatedAt: time.Now().UTC(),
	}

	return &ltxFixture{info: info, data: buf.Bytes()}
}
