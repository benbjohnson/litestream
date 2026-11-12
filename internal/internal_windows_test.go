package internal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestFsyncDir_Windows(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, path := range []string{dir, filepath.Join(dir, "missing")} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			if err := FsyncDir(path); err != nil {
				t.Fatalf("FsyncDir(%q): %v", path, err)
			}
		})
	}
}

func TestLockFileExclusive_Windows(t *testing.T) {
	for _, offset := range []uint32{sqlitePendingByte, sqliteSharedFirst} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			t.Parallel()
			f, err := os.CreateTemp(t.TempDir(), "database")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			blocker, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Close()
			ol := windows.Overlapped{Offset: offset}
			if err := windows.LockFileEx(windows.Handle(blocker.Fd()), windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &ol); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			start := time.Now()
			if err := LockFileExclusive(ctx, f); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lock error=%v, want deadline exceeded", err)
			}
			if time.Since(start) > time.Second {
				t.Fatal("lock ignored context deadline")
			}
			if err := windows.UnlockFileEx(windows.Handle(blocker.Fd()), 0, 1, 0, &ol); err != nil {
				t.Fatal(err)
			}
			ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
			defer cancel2()
			if err := LockFileExclusive(ctx2, blocker); err != nil {
				t.Fatalf("lock after timeout: %v", err)
			}
			if err := UnlockFile(blocker); err != nil {
				t.Fatal(err)
			}
			cancel2()
			if err := LockFileExclusive(ctx2, f); !errors.Is(err, context.Canceled) {
				t.Fatalf("lock error=%v, want canceled", err)
			}
		})
	}
}
