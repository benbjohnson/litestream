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

// LockFileExclusive takes the PENDING-byte writer mutex on the database
// file. We deliberately do NOT acquire the SHARED-byte-range exclusive
// lock here: WAL-mode reader connections in the consuming application
// can hold SHARED on that range for the lifetime of their connection
// (e.g. via a long-lived per-worker connection pool), in which case the
// blocking F_SETLKW on the SHARED range would deadlock applyLTXFile
// indefinitely. The PENDING lock alone is sufficient to serialize
// writers; readers do not consult the main-DB lock-byte page in WAL
// mode and observe no torn reads as long as page writes are atomic at
// the filesystem level (true for SQLite page sizes ≥ filesystem block
// size on modern Unix filesystems).
func LockFileExclusive(f *os.File) error {
	fd := int(f.Fd())

	if err := setFcntlLock(fd, unix.F_WRLCK, sqlitePendingByte, 1); err != nil {
		return err
	}

	return nil
}

func UnlockFile(f *os.File) error {
	fd := int(f.Fd())
	return setFcntlLock(fd, unix.F_UNLCK, sqlitePendingByte, 1)
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
