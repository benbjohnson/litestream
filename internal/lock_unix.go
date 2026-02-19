//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package internal

import (
	"os"

	"golang.org/x/sys/unix"
)

const (
	sqlitePendingByte = 0x40000000
	sqliteSharedFirst = sqlitePendingByte + 2
	sqliteSharedSize  = 510
)

func LockFileExclusive(f *os.File) error {
	fd := int(f.Fd())

	if err := setFcntlLock(fd, unix.F_WRLCK, sqlitePendingByte, 1); err != nil {
		return err
	}

	if err := setFcntlLock(fd, unix.F_WRLCK, sqliteSharedFirst, sqliteSharedSize); err != nil {
		_ = setFcntlLock(fd, unix.F_UNLCK, sqlitePendingByte, 1)
		return err
	}

	return nil
}

func UnlockFile(f *os.File) error {
	fd := int(f.Fd())
	err1 := setFcntlLock(fd, unix.F_UNLCK, sqliteSharedFirst, sqliteSharedSize)
	err2 := setFcntlLock(fd, unix.F_UNLCK, sqlitePendingByte, 1)
	if err1 != nil {
		return err1
	}
	return err2
}

func setFcntlLock(fd int, lockType int16, start int64, length int64) error {
	flock := unix.Flock_t{
		Type:   lockType,
		Whence: 0,
		Start:  start,
		Len:    length,
	}
	return unix.FcntlFlock(uintptr(fd), unix.F_SETLKW, &flock)
}
