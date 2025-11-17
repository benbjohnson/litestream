//go:build vfs
// +build vfs

package litestream

import (
	"os"
	"sync/atomic"

	"github.com/psanford/sqlite3vfs"
)

// localTempFile fulfills sqlite3vfs.File for SQLite temp & transient files.
// These files live entirely on the local filesystem and are deleted once the
// SQLite layer closes them (when requested via DeleteOnClose).
type localTempFile struct {
	f             *os.File
	deleteOnClose bool
	lockType      atomic.Int32
	onClose       func()
}

func newLocalTempFile(f *os.File, deleteOnClose bool, onClose func()) *localTempFile {
	return &localTempFile{f: f, deleteOnClose: deleteOnClose, onClose: onClose}
}

func (tf *localTempFile) Close() error {
	err := tf.f.Close()
	if tf.deleteOnClose {
		if removeErr := os.Remove(tf.f.Name()); removeErr != nil && !os.IsNotExist(removeErr) && err == nil {
			err = removeErr
		}
	}
	if tf.onClose != nil {
		tf.onClose()
	}
	return err
}

func (tf *localTempFile) ReadAt(p []byte, off int64) (n int, err error) {
	return tf.f.ReadAt(p, off)
}

func (tf *localTempFile) WriteAt(b []byte, off int64) (n int, err error) {
	return tf.f.WriteAt(b, off)
}

func (tf *localTempFile) Truncate(size int64) error {
	return tf.f.Truncate(size)
}

func (tf *localTempFile) Sync(flag sqlite3vfs.SyncType) error {
	return tf.f.Sync()
}

func (tf *localTempFile) FileSize() (int64, error) {
	info, err := tf.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (tf *localTempFile) Lock(elock sqlite3vfs.LockType) error {
	if elock == sqlite3vfs.LockNone {
		return nil
	}
	tf.lockType.Store(int32(elock))
	return nil
}

func (tf *localTempFile) Unlock(elock sqlite3vfs.LockType) error {
	tf.lockType.Store(int32(elock))
	return nil
}

func (tf *localTempFile) CheckReservedLock() (bool, error) {
	return sqlite3vfs.LockType(tf.lockType.Load()) >= sqlite3vfs.LockReserved, nil
}

func (tf *localTempFile) SectorSize() int64 {
	return 0
}

func (tf *localTempFile) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return 0
}
