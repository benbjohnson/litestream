package sqlite3vfs

import "fmt"

type File interface {
	Close() error
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	Truncate(size int64) error
	Sync(flag SyncType) error
	FileSize() (int64, error)
	Lock(elock LockType) error
	Unlock(elock LockType) error
	CheckReservedLock() (bool, error)
	SectorSize() int64
	DeviceCharacteristics() DeviceCharacteristic
}

type FileController interface {
	FileControl(op int, pragmaName string, pragmaValue *string) (*string, error)
}

type SyncType int

const (
	SyncNormal   SyncType = 0x00002
	SyncFull     SyncType = 0x00003
	SyncDataOnly SyncType = 0x00010
)

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
