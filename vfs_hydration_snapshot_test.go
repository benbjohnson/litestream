//go:build vfs

package litestream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/psanford/sqlite3vfs"
	"github.com/superfly/ltx"
)

func TestVFSFile_Hydration_PersistsAppliedPosition(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(fmt.Sprintf("pinned=%t", pinned), func(t *testing.T) {
			client := newMockReplicaClient()
			initial := buildLTXFixtureWithPages(t, 1, 4096, []uint32{1, 2}, 'a')
			client.addFixture(t, initial)
			f := NewVFSFile(client, "position.db", slog.Default())
			f.PollInterval = time.Hour
			if err := f.Open(); err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = f.Close()
				}
			})
			path := filepath.Join(t.TempDir(), "hydrated.db")
			f.hydrator = NewHydrator(path, true, 4096, client, slog.Default())
			if err := f.hydrator.Init(); err != nil {
				t.Fatal(err)
			}
			if err := f.hydrator.Restore(context.Background(), []*ltx.FileInfo{initial.info}); err != nil {
				t.Fatal(err)
			}
			f.hydrator.SetComplete()
			if pinned {
				if err := f.Lock(sqlite3vfs.LockShared); err != nil {
					t.Fatal(err)
				}
			}
			client.addFixture(t, buildLTXFixtureWithPages(t, 2, 4096, []uint32{1, 2}, 'b'))
			if err := f.pollReplicaClient(context.Background()); err != nil {
				t.Fatal(err)
			}
			if pinned {
				if got := f.hydrator.TXID(); got != 1 {
					t.Fatalf("pending file position = %s, want 1", got)
				}
				if err := f.Unlock(sqlite3vfs.LockNone); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true
			meta, err := os.ReadFile(path + ".meta")
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.TrimSpace(string(meta)); got != "2" {
				t.Fatalf("saved applied position = %s, want 2", got)
			}
			resumed := NewHydrator(path, true, 4096, client, slog.Default())
			if err := resumed.Init(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := resumed.Close(); err != nil {
					t.Error(err)
				}
			})
			if got := resumed.TXID(); got != 2 {
				t.Fatalf("reopened position = %s, want 2", got)
			}
			for _, offset := range []int64{0, 4096} {
				page := make([]byte, 4096)
				if _, err := resumed.ReadAt(page, offset); err != nil {
					t.Fatal(err)
				}
				if page[128] != 'b' {
					t.Fatalf("reopened page at %d = %q, want b", offset, page[128])
				}
			}
		})
	}
}

func TestVFSFile_Hydration_PreservesSharedRead(t *testing.T) {
	client := newMockReplicaClient()
	initial := buildLTXFixtureWithPages(t, 1, 4096, []uint32{1, 2}, 'a')
	client.addFixture(t, initial)
	f := NewVFSFile(client, "read.db", slog.Default())
	f.PollInterval = time.Hour
	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	// Restore synchronously through the real hydrator API. No wall-clock wait or
	// artificial background readiness is involved in this lock/apply test.
	f.hydrator = NewHydrator(filepath.Join(t.TempDir(), "hydrated.db"), false, 4096, client, slog.Default())
	if err := f.hydrator.Init(); err != nil {
		t.Fatal(err)
	}
	if err := f.hydrator.Restore(context.Background(), []*ltx.FileInfo{initial.info}); err != nil {
		t.Fatal(err)
	}
	f.hydrator.SetComplete()
	if err := f.Lock(sqlite3vfs.LockShared); err != nil {
		t.Fatal(err)
	}
	read := func(off int64, want byte) {
		t.Helper()
		page := make([]byte, 4096)
		if _, err := f.ReadAt(page, off); err != nil {
			t.Fatal(err)
		}
		if page[128] != want {
			t.Fatalf("page at %d = %q, want %q", off, page[128], want)
		}
	}
	read(0, 'a')
	client.addFixture(t, buildLTXFixtureWithPages(t, 2, 4096, []uint32{1, 2}, 'b'))
	if err := f.pollReplicaClient(context.Background()); err != nil {
		t.Fatal(err)
	}
	read(4096, 'a') // A page not read before the update must retain the same view.
	if err := f.Unlock(sqlite3vfs.LockShared); err != nil {
		t.Fatal(err)
	}
	read(4096, 'a') // A downgrade that retains SHARED must not publish pending pages.
	if err := f.Unlock(sqlite3vfs.LockNone); err != nil {
		t.Fatal(err)
	}
	read(0, 'b')
	read(4096, 'b')
}

func assertHydrationState(t *testing.T, f *VFSFile, want string) {
	t.Helper()
	value, err := f.FileControl(14, "litestream_hydration_state", nil)
	if err != nil {
		t.Fatal(err)
	}
	if value == nil || *value != want {
		t.Fatalf("hydration state = %v, want %s", value, want)
	}
}

func TestVFSFile_Hydration_CompletionPreservesActiveRead(t *testing.T) {
	client := newMockReplicaClient()
	initial := buildLTXFixtureWithPages(t, 1, 4096, []uint32{1, 2}, 'a')
	client.addFixture(t, initial)
	f := NewVFSFile(client, "initial-read.db", slog.Default())
	f.PollInterval = time.Hour
	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	f.hydrator = NewHydrator(filepath.Join(t.TempDir(), "hydrated.db"), false, 4096, client, slog.Default())
	if err := f.hydrator.Init(); err != nil {
		t.Fatal(err)
	}
	if err := f.Lock(sqlite3vfs.LockShared); err != nil {
		t.Fatal(err)
	}
	page := make([]byte, 4096)
	if _, err := f.ReadAt(page, 0); err != nil {
		t.Fatal(err)
	}
	if page[128] != 'a' {
		t.Fatal("initial read is not the expected fixture")
	}
	client.addFixture(t, buildLTXFixtureWithPages(t, 2, 4096, []uint32{1, 2}, 'b'))
	if err := f.pollReplicaClient(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Complete the real restore/catch-up path synchronously while the reader is
	// pinned. Switching to a newer local file must wait for the shared lock too.
	f.wg.Add(1)
	f.runHydration([]*ltx.FileInfo{initial.info})
	assertHydrationState(t, f, "hydrating")
	if _, err := f.ReadAt(page, 4096); err != nil {
		t.Fatal(err)
	}
	if page[128] != 'a' {
		t.Fatalf("hydration completion changed active snapshot to %q", page[128])
	}
	if err := f.Unlock(sqlite3vfs.LockNone); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ReadAt(page, 4096); err != nil {
		t.Fatal(err)
	}
	if page[128] != 'b' {
		t.Fatalf("released snapshot did not advance: %q", page[128])
	}
	assertHydrationState(t, f, "ready")
	denied := "ready"
	if _, err := f.FileControl(14, "litestream_hydration_state", &denied); err == nil {
		t.Fatal("hydration state accepted a write")
	}
}

type failingHydrationClient struct {
	ReplicaClient
	enabled bool
	reads   int
}

func (c *failingHydrationClient) OpenLTXFile(ctx context.Context, level int, min, max ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if c.enabled && offset > 0 {
		c.reads++
		if c.reads == 2 {
			return nil, errors.New("synthetic page read failure")
		}
	}
	return c.ReplicaClient.OpenLTXFile(ctx, level, min, max, offset, size)
}

func TestVFSFile_Hydration_PartialApplyRefusesReads(t *testing.T) {
	client := newMockReplicaClient()
	initial := buildLTXFixtureWithPages(t, 1, 4096, []uint32{1, 2}, 'a')
	client.addFixture(t, initial)
	f := NewVFSFile(client, "failed-read.db", slog.Default())
	f.PollInterval = time.Hour
	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			if err := f.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	faulty := &failingHydrationClient{ReplicaClient: client}
	path := filepath.Join(t.TempDir(), "hydrated.db")
	f.hydrator = NewHydrator(path, true, 4096, faulty, slog.Default())
	if err := f.hydrator.Init(); err != nil {
		t.Fatal(err)
	}
	if err := f.hydrator.Restore(context.Background(), []*ltx.FileInfo{initial.info}); err != nil {
		t.Fatal(err)
	}
	f.hydrator.SetComplete()
	faulty.enabled = true
	client.addFixture(t, buildLTXFixtureWithPages(t, 2, 4096, []uint32{1, 2}, 'b'))
	applyErr := f.pollReplicaClient(context.Background())
	if _, err := f.ReadAt(make([]byte, 4096), 0); err == nil {
		t.Fatal("a partially applied hydrated file was still readable")
	}
	if applyErr == nil {
		t.Fatal("partial apply failure was not reported")
	}
	assertHydrationState(t, f, "failed")
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	resumed := NewHydrator(path, true, 4096, client, slog.Default())
	if err := resumed.Init(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := resumed.Close(); err != nil {
			t.Error(err)
		}
	})
	if got := resumed.TXID(); got != 1 {
		t.Fatalf("failed apply saved position %s, want 1", got)
	}
	if err := resumed.CatchUp(context.Background(), 1, 2); err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{0, 4096} {
		page := make([]byte, 4096)
		if _, err := resumed.ReadAt(page, offset); err != nil {
			t.Fatal(err)
		}
		if page[128] != 'b' {
			t.Fatalf("recovered page at %d = %q, want b", offset, page[128])
		}
	}
}

func TestVFSFile_Hydration_CatchUpRefusesGap(t *testing.T) {
	client := newMockReplicaClient()
	initial := buildLTXFixtureWithPages(t, 1, 4096, []uint32{1, 2}, 'a')
	client.addFixture(t, initial)
	h := NewHydrator(filepath.Join(t.TempDir(), "hydrated.db"), false, 4096, client, slog.Default())
	if err := h.Init(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := h.Restore(context.Background(), []*ltx.FileInfo{initial.info}); err != nil {
		t.Fatal(err)
	}
	client.addFixture(t, buildLTXFixtureWithPages(t, 3, 4096, []uint32{1, 2}, 'c'))
	if err := h.CatchUp(context.Background(), 1, 3); err == nil {
		t.Fatal("hydration admitted a missing source transaction")
	}
}

func TestVFSFile_NonhydratedDowngradeKeepsPendingBehavior(t *testing.T) {
	client := newMockReplicaClient()
	initial := buildLTXFixtureWithPages(t, 1, 4096, []uint32{1, 2}, 'a')
	client.addFixture(t, initial)
	f := NewVFSFile(client, "nonhydrated.db", slog.Default())
	f.PollInterval = time.Hour
	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := f.Lock(sqlite3vfs.LockShared); err != nil {
		t.Fatal(err)
	}
	client.addFixture(t, buildLTXFixtureWithPages(t, 2, 4096, []uint32{1, 2}, 'b'))
	if err := f.pollReplicaClient(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.Unlock(sqlite3vfs.LockShared); err != nil {
		t.Fatal(err)
	}
	page := make([]byte, 4096)
	if _, err := f.ReadAt(page, 4096); err != nil {
		t.Fatal(err)
	}
	if page[128] != 'b' {
		t.Fatalf("nonhydrated downgrade changed existing pending publication: got %q", page[128])
	}
}

func TestVFSFile_Hydration_StateDoesNotWaitForFileIO(t *testing.T) {
	f := NewVFSFile(newMockReplicaClient(), "busy.db", slog.Default())
	f.hydrator = NewHydrator(filepath.Join(t.TempDir(), "hydrated.db"), false, 4096, f.client, slog.Default())
	// Restore holds this file lock during network decoding. Readiness must remain
	// observable while that work is in progress, including on a large database.
	f.hydrator.mu.Lock()
	defer f.hydrator.mu.Unlock()
	assertHydrationState(t, f, "hydrating")
}
