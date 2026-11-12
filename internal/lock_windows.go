//go:build windows

package internal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

const (
	sqlitePendingByte = 0x40000000
	sqliteSharedFirst = sqlitePendingByte + 2
	sqliteSharedSize  = 510
)

func LockFileExclusive(ctx context.Context, f *os.File) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	h := windows.Handle(f.Fd())
	for _, region := range []struct{ start, size uint32 }{{sqlitePendingByte, 1}, {sqliteSharedFirst, sqliteSharedSize}} {
		ol := windows.Overlapped{Offset: region.start}
		for {
			err := ctx.Err()
			if err == nil {
				err = windows.LockFileEx(h, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, region.size, 0, &ol)
			}
			if err == nil {
				break
			}
			if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
				select {
				case <-ctx.Done():
					err = ctx.Err()
				case <-ticker.C:
					continue
				}
			}
			if region.start == sqliteSharedFirst {
				pendingOL := windows.Overlapped{Offset: sqlitePendingByte}
				err = errors.Join(err, windows.UnlockFileEx(h, 0, 1, 0, &pendingOL))
			}
			return fmt.Errorf("lock database byte range %d..%d: %w", region.start, region.start+region.size-1, err)
		}
	}
	return nil
}

func UnlockFile(f *os.File) error {
	h := windows.Handle(f.Fd())

	sharedOL := windows.Overlapped{Offset: sqliteSharedFirst}
	err1 := windows.UnlockFileEx(h, 0, sqliteSharedSize, 0, &sharedOL)

	pendingOL := windows.Overlapped{Offset: sqlitePendingByte}
	err2 := windows.UnlockFileEx(h, 0, 1, 0, &pendingOL)

	return errors.Join(err1, err2)
}
