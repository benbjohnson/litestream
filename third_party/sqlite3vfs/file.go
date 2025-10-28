package sqlite3vfs

import "fmt"

type File interface {
	Close() error

	// ReadAt reads len(p) bytes into p starting at offset off in the underlying input source.
	// It returns the number of bytes read (0 <= n <= len(p)) and any error encountered.
	// If n < len(p), SQLITE_IOERR_SHORT_READ will be returned to sqlite.
	ReadAt(p []byte, off int64) (n int, err error)

	// WriteAt writes len(p) bytes from p to the underlying data stream at offset off.
	// It returns the number of bytes written from p (0 <= n <= len(p)) and any error encountered that caused the write to stop early.
	// WriteAt must return a non-nil error if it returns n < len(p).
	WriteAt(p []byte, off int64) (n int, err error)

	Truncate(size int64) error

	Sync(flag SyncType) error

	FileSize() (int64, error)

	// Acquire or upgrade a lock.
	// elock can be one of the following:
	// LockShared, LockReserved, LockPending, LockExclusive.
	//
	// Additional states can be inserted between the current lock level
	// and the requested lock level. The locking might fail on one of the later
	// transitions leaving the lock state different from what it started but
	// still short of its goal.  The following chart shows the allowed
	// transitions and the inserted intermediate states:
	//
	//    UNLOCKED -> SHARED
	//    SHARED -> RESERVED
	//    SHARED -> (PENDING) -> EXCLUSIVE
	//    RESERVED -> (PENDING) -> EXCLUSIVE
	//    PENDING -> EXCLUSIVE
	//
	// This function should only increase a lock level.
	// See the sqlite source documentation for unixLock for more details.
	Lock(elock LockType) error

	// Lower the locking level on file to eFileLock. eFileLock must be
	// either NO_LOCK or SHARED_LOCK. If the locking level of the file
	// descriptor is already at or below the requested locking level,
	// this routine is a no-op.
	Unlock(elock LockType) error

	// Check whether any database connection, either in this process or
	// in some other process, is holding a RESERVED, PENDING, or
	// EXCLUSIVE lock on the file. It returns true if such a lock exists
	// and false otherwise.
	CheckReservedLock() (bool, error)

	// SectorSize returns the sector size of the device that underlies
	// the file. The sector size is the minimum write that can be
	// performed without disturbing other bytes in the file.
	SectorSize() int64

	// DeviceCharacteristics returns a bit vector describing behaviors
	// of the underlying device.
	DeviceCharacteristics() DeviceCharacteristic
}

type SyncType int

const (
	SyncNormal   SyncType = 0x00002
	SyncFull     SyncType = 0x00003
	SyncDataOnly SyncType = 0x00010
)

// https://www.sqlite.org/c3ref/c_lock_exclusive.html
type LockType int

const (
	LockNone      LockType = 0
	LockShared    LockType = 1
	LockReserved  LockType = 2
	LockPending   LockType = 3
	LockExclusive LockType = 4
)

func (lt LockType) String() string {
	switch lt {
	case LockNone:
		return "LockNone"
	case LockShared:
		return "LockShared"
	case LockReserved:
		return "LockReserved"
	case LockPending:
		return "LockPending"
	case LockExclusive:
		return "LockExclusive"
	default:
		return fmt.Sprintf("LockTypeUnknown<%d>", lt)
	}
}

// https://www.sqlite.org/c3ref/c_iocap_atomic.html
type DeviceCharacteristic int

const (
	IocapAtomic              DeviceCharacteristic = 0x00000001
	IocapAtomic512           DeviceCharacteristic = 0x00000002
	IocapAtomic1K            DeviceCharacteristic = 0x00000004
	IocapAtomic2K            DeviceCharacteristic = 0x00000008
	IocapAtomic4K            DeviceCharacteristic = 0x00000010
	IocapAtomic8K            DeviceCharacteristic = 0x00000020
	IocapAtomic16K           DeviceCharacteristic = 0x00000040
	IocapAtomic32K           DeviceCharacteristic = 0x00000080
	IocapAtomic64K           DeviceCharacteristic = 0x00000100
	IocapSafeAppend          DeviceCharacteristic = 0x00000200
	IocapSequential          DeviceCharacteristic = 0x00000400
	IocapUndeletableWhenOpen DeviceCharacteristic = 0x00000800
	IocapPowersafeOverwrite  DeviceCharacteristic = 0x00001000
	IocapImmutable           DeviceCharacteristic = 0x00002000
	IocapBatchAtomic         DeviceCharacteristic = 0x00004000
)
