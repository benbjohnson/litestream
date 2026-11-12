//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package internal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
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
	fd := int(f.Fd())
	for _, region := range []struct{ start, size int64 }{{sqlitePendingByte, 1}, {sqliteSharedFirst, sqliteSharedSize}} {
		for {
			err := ctx.Err()
			if err == nil {
				err = setFcntlLock(fd, unix.F_WRLCK, region.start, region.size)
			}
			if err == nil {
				break
			}
			if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
				select {
				case <-ctx.Done():
					err = ctx.Err()
				case <-ticker.C:
					continue
				}
			}
			if region.start == sqliteSharedFirst {
				err = errors.Join(err, setFcntlLock(fd, unix.F_UNLCK, sqlitePendingByte, 1))
			}
			return fmt.Errorf("lock database byte range %d..%d: %w", region.start, region.start+region.size-1, err)
		}
	}
	return nil
}

func UnlockFile(f *os.File) error {
	fd := int(f.Fd())
	err1 := setFcntlLock(fd, unix.F_UNLCK, sqliteSharedFirst, sqliteSharedSize)
	err2 := setFcntlLock(fd, unix.F_UNLCK, sqlitePendingByte, 1)
	return errors.Join(err1, err2)
}

func setFcntlLock(fd int, lockType int16, start int64, length int64) error {
	flock := unix.Flock_t{
		Type:   lockType,
		Whence: 0,
		Start:  start,
		Len:    length,
	}
	return unix.FcntlFlock(uintptr(fd), unix.F_SETLK, &flock)
}
