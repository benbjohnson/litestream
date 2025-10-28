//go:build ignore

package sqlite3vfs

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

type TmpVFS struct {
	tmpdir string
}

func newTempVFS() *TmpVFS {
	dir, err := ioutil.TempDir("", "sqlite3vfs_test_tmpvfs")
	if err != nil {
		panic(err)
	}

	return &TmpVFS{
		tmpdir: dir,
	}
}

func (vfs *TmpVFS) Open(name string, flags OpenFlag) (File, OpenFlag, error) {
	var (
		f   *os.File
		err error
	)

	if name == "" {
		f, err = ioutil.TempFile(vfs.tmpdir, "")
		if err != nil {
			return nil, 0, CantOpenError
		}
	} else {
		fname := filepath.Join(vfs.tmpdir, name)
		if !filepath.HasPrefix(fname, vfs.tmpdir) {
			return nil, 0, PermError
		}
		var fileFlags int
		if flags&OpenExclusive != 0 {
			fileFlags |= os.O_EXCL
		}
		if flags&OpenCreate != 0 {
			fileFlags |= os.O_CREATE
		}
		if flags&OpenReadOnly != 0 {
			fileFlags |= os.O_RDONLY
		}
		if flags&OpenReadWrite != 0 {
			fileFlags |= os.O_RDWR
		}
		f, err = os.OpenFile(fname, fileFlags, 0600)
		if err != nil {
			return nil, 0, CantOpenError
		}
	}

	tf := &TmpFile{f: f}
	return tf, flags, nil
}

func (vfs *TmpVFS) Delete(name string, dirSync bool) error {
	fname := filepath.Join(vfs.tmpdir, name)
	if !filepath.HasPrefix(fname, vfs.tmpdir) {
		return errors.New("illegal path")
	}
	return os.Remove(fname)
}

func (vfs *TmpVFS) Access(name string, flag AccessFlag) (bool, error) {
	fname := filepath.Join(vfs.tmpdir, name)
	if !filepath.HasPrefix(fname, vfs.tmpdir) {
		return false, errors.New("illegal path")
	}

	exists := true
	_, err := os.Stat(fname)
	if err != nil && os.IsNotExist(err) {
		exists = false
	} else if err != nil {
		return false, err
	}

	if flag == AccessExists {
		return exists, nil
	}

	return true, nil
}

func (vfs *TmpVFS) FullPathname(name string) string {
	fname := filepath.Join(vfs.tmpdir, name)
	if !filepath.HasPrefix(fname, vfs.tmpdir) {
		return ""
	}

	return strings.TrimPrefix(fname, vfs.tmpdir)
}

type TmpFile struct {
	lockCount int64
	f         *os.File
}

func (tf *TmpFile) Close() error {
	return tf.f.Close()
}

func (tf *TmpFile) ReadAt(p []byte, off int64) (n int, err error) {
	return tf.f.ReadAt(p, off)
}

func (tf *TmpFile) WriteAt(b []byte, off int64) (n int, err error) {
	return tf.f.WriteAt(b, off)
}

func (tf *TmpFile) Truncate(size int64) error {
	return tf.f.Truncate(size)
}

func (tf *TmpFile) Sync(flag SyncType) error {
	return tf.f.Sync()
}

func (tf *TmpFile) FileSize() (int64, error) {
	cur, _ := tf.f.Seek(0, os.SEEK_CUR)
	end, err := tf.f.Seek(0, os.SEEK_END)
	if err != nil {
		return 0, err
	}

	tf.f.Seek(cur, os.SEEK_SET)
	return end, nil
}

func (tf *TmpFile) Lock(elock LockType) error {
	if elock == LockNone {
		return nil
	}
	atomic.AddInt64(&tf.lockCount, 1)
	return nil
}

func (tf *TmpFile) Unlock(elock LockType) error {
	if elock == LockNone {
		return nil
	}
	atomic.AddInt64(&tf.lockCount, -1)
	return nil
}

func (tf *TmpFile) CheckReservedLock() (bool, error) {
	count := atomic.LoadInt64(&tf.lockCount)
	return count > 0, nil
}

func (tf *TmpFile) SectorSize() int64 {
	return 0
}

func (tf *TmpFile) DeviceCharacteristics() DeviceCharacteristic {
	return 0
}
