package sqlite3vfs

/*
   #include "sqlite3vfs.h"
   #include <string.h>
   #include <stdlib.h>
*/
import "C"

import (
	"io"
	"sync"
	"time"
	"unsafe"
)

var (
	vfsMap = make(map[string]ExtendedVFSv1)

	fileMux    sync.Mutex
	nextFileID uint64
	fileMap    = make(map[uint64]File)
)

func newVFS(name string, goVFS ExtendedVFSv1, maxPathName int) error {
	vfsMap[name] = goVFS

	rc := C.s3vfsNew(C.CString(name), C.int(maxPathName))
	if rc == C.SQLITE_OK {
		return nil
	}

	return errFromCode(int(rc))
}

//export goVFSOpen
func goVFSOpen(cvfs *C.sqlite3_vfs, name *C.char, retFile *C.sqlite3_file, flags C.int, outFlags *C.int) C.int {

	fileName := C.GoString(name)

	vfs := vfsFromC(cvfs)

	file, retFlags, err := vfs.Open(fileName, OpenFlag(flags))
	if err != nil {
		return errToC(err)
	}

	if retFlags != 0 && outFlags != nil {
		*outFlags = C.int(retFlags)
	}

	cfile := (*C.s3vfsFile)(unsafe.Pointer(retFile))
	C.memset(unsafe.Pointer(cfile), 0, C.sizeof_s3vfsFile)

	fileMux.Lock()
	fileID := nextFileID
	nextFileID++
	cfile.id = C.sqlite3_uint64(fileID)
	fileMap[fileID] = file
	fileMux.Unlock()

	return sqliteOK
}

//export goVFSDelete
func goVFSDelete(cvfs *C.sqlite3_vfs, zName *C.char, syncDir C.int) C.int {
	vfs := vfsFromC(cvfs)

	fileName := C.GoString(zName)

	err := vfs.Delete(fileName, syncDir > 0)
	if err != nil {
		return errToC(err)
	}

	return sqliteOK
}

// The flags argument to xAccess() may be SQLITE_ACCESS_EXISTS to test
// for the existence of a file, or SQLITE_ACCESS_READWRITE to test
// whether a file is readable and writable, or SQLITE_ACCESS_READ to
// test whether a file is at least readable. The SQLITE_ACCESS_READ
// flag is never actually used and is not implemented in the built-in
// VFSes of SQLite. The file is named by the second argument and can
// be a directory. The xAccess method returns SQLITE_OK on success or
// some non-zero error code if there is an I/O error or if the name of
// the file given in the second argument is illegal. If SQLITE_OK is
// returned, then non-zero or zero is written into *pResOut to
// indicate whether or not the file is accessible.
//
//export goVFSAccess
func goVFSAccess(cvfs *C.sqlite3_vfs, zName *C.char, cflags C.int, pResOut *C.int) C.int {
	vfs := vfsFromC(cvfs)

	fileName := C.GoString(zName)
	flags := AccessFlag(cflags)

	ok, err := vfs.Access(fileName, flags)

	out := 0
	if ok {
		out = 1
	}
	*pResOut = C.int(out)

	if err != nil {
		return errToC(err)
	}

	return sqliteOK
}

//export goVFSFullPathname
func goVFSFullPathname(cvfs *C.sqlite3_vfs, zName *C.char, nOut C.int, zOut *C.char) C.int {
	vfs := vfsFromC(cvfs)

	fileName := C.GoString(zName)

	s := vfs.FullPathname(fileName)

	path := C.CString(s)
	defer C.free(unsafe.Pointer(path))

	if len(s)+1 >= int(nOut) {
		return errToC(TooBigError)
	}

	C.memcpy(unsafe.Pointer(zOut), unsafe.Pointer(path), C.size_t(len(s)+1))

	return sqliteOK
}

//export goVFSRandomness
func goVFSRandomness(cvfs *C.sqlite3_vfs, nByte C.int, zOut *C.char) C.int {
	vfs := vfsFromC(cvfs)

	buf := (*[1 << 28]byte)(unsafe.Pointer(zOut))[:int(nByte):int(nByte)]

	count := vfs.Randomness(buf)
	return C.int(count)
}

//export goVFSSleep
func goVFSSleep(cvfs *C.sqlite3_vfs, microseconds C.int) C.int {
	vfs := vfsFromC(cvfs)

	d := time.Duration(microseconds) * time.Microsecond

	vfs.Sleep(d)

	return sqliteOK
}

// Find the current time (in Universal Coordinated Time).  Write into *piNow
// the current time and date as a Julian Day number times 86_400_000.  In
// other words, write into *piNow the number of milliseconds since the Julian
// epoch of noon in Greenwich on November 24, 4714 B.C according to the
// proleptic Gregorian calendar.

// On success, return SQLITE_OK.  Return SQLITE_ERROR if the time and date
// cannot be found.
//
//export goVFSCurrentTimeInt64
func goVFSCurrentTimeInt64(cvfs *C.sqlite3_vfs, piNow *C.sqlite3_int64) C.int {
	vfs := vfsFromC(cvfs)

	ts := vfs.CurrentTime()

	unixEpoch := int64(24405875) * 8640000
	*piNow = C.sqlite3_int64(unixEpoch + ts.UnixNano()/1000000)

	return sqliteOK
}

//export goVFSClose
func goVFSClose(cfile *C.sqlite3_file) C.int {
	s3vfsFile := (*C.s3vfsFile)(unsafe.Pointer(cfile))

	fileID := uint64(s3vfsFile.id)

	fileMux.Lock()
	file := fileMap[fileID]
	delete(fileMap, fileID)
	fileMux.Unlock()

	if file == nil {
		return errToC(GenericError)
	}

	err := file.Close()
	if err != nil {
		return errToC(err)
	}

	return sqliteOK
}

//export goVFSRead
func goVFSRead(cfile *C.sqlite3_file, buf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	s3vfsFile := (*C.s3vfsFile)(unsafe.Pointer(cfile))

	fileID := uint64(s3vfsFile.id)

	fileMux.Lock()
	file := fileMap[fileID]
	fileMux.Unlock()

	if file == nil {
		return errToC(GenericError)
	}

	goBuf := (*[1 << 28]byte)(buf)[:int(iAmt):int(iAmt)]
	n, err := file.ReadAt(goBuf, int64(iOfst))
	if n < len(goBuf) {
		if err == nil {
			// io.ReadAt requires an error if n < len(goBuf)
			panic("ReadAt invalid semantics: returned n < len(p) but with a nil error")
		}
		// If xRead() returns SQLITE_IOERR_SHORT_READ it must also fill in the unread portions of the buffer with zeros.
		for i := n; i < len(goBuf); i++ {
			goBuf[i] = 0
		}
	}

	if err == io.EOF {
		return errToC(IOErrorShortRead)
	} else if err != nil {
		return errToC(err)
	}

	return sqliteOK
}

//export goVFSWrite
func goVFSWrite(cfile *C.sqlite3_file, buf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	s3vfsFile := (*C.s3vfsFile)(unsafe.Pointer(cfile))

	fileID := uint64(s3vfsFile.id)

	fileMux.Lock()
	file := fileMap[fileID]
	fileMux.Unlock()

	if file == nil {
		return errToC(GenericError)
	}

	goBuf := (*[1 << 28]byte)(buf)[:int(iAmt):int(iAmt)]

	_, err := file.WriteAt(goBuf, int64(iOfst))
	if err != nil {
		return errToC(IOErrorWrite)
	}

	return sqliteOK
}

//export goVFSTruncate
func goVFSTruncate(cfile *C.sqlite3_file, size C.sqlite3_int64) C.int {
	s3vfsFile := (*C.s3vfsFile)(unsafe.Pointer(cfile))

	fileID := uint64(s3vfsFile.id)

	fileMux.Lock()
	file := fileMap[fileID]
	fileMux.Unlock()

	if file == nil {
		return errToC(GenericError)
	}

	err := file.Truncate(int64(size))
	if err != nil {
		return errToC(err)
	}

	return sqliteOK
}

//export goVFSSync
func goVFSSync(cfile *C.sqlite3_file, flags C.int) C.int {
	s3vfsFile := (*C.s3vfsFile)(unsafe.Pointer(cfile))

	fileID := uint64(s3vfsFile.id)

	fileMux.Lock()
	file := fileMap[fileID]
	fileMux.Unlock()

	if file == nil {
		return errToC(GenericError)
	}

	err := file.Sync(SyncType(flags))
	if err != nil {
		return errToC(err)
	}

	return sqliteOK
}

//export goVFSFileSize
func goVFSFileSize(cfile *C.sqlite3_file, pSize *C.sqlite3_int64) C.int {
	s3vfsFile := (*C.s3vfsFile)(unsafe.Pointer(cfile))

	fileID := uint64(s3vfsFile.id)

	fileMux.Lock()
	file := fileMap[fileID]
	fileMux.Unlock()

	if file == nil {
		return errToC(GenericError)
	}

	n, err := file.FileSize()
	if err != nil {
		return errToC(err)
	}

	*pSize = C.sqlite3_int64(n)

	return sqliteOK
}

//export goVFSLock
func goVFSLock(cfile *C.sqlite3_file, eLock C.int) C.int {
	s3vfsFile := (*C.s3vfsFile)(unsafe.Pointer(cfile))

	fileID := uint64(s3vfsFile.id)

	fileMux.Lock()
	file := fileMap[fileID]
	fileMux.Unlock()

	if file == nil {
		return errToC(GenericError)
	}

	err := file.Lock(LockType(eLock))
	if err != nil {
		return errToC(err)
	}

	return sqliteOK
}

//export goVFSUnlock
func goVFSUnlock(cfile *C.sqlite3_file, eLock C.int) C.int {
	s3vfsFile := (*C.s3vfsFile)(unsafe.Pointer(cfile))

	fileID := uint64(s3vfsFile.id)

	fileMux.Lock()
	file := fileMap[fileID]
	fileMux.Unlock()

	if file == nil {
		return errToC(GenericError)
	}

	err := file.Unlock(LockType(eLock))
	if err != nil {
		return errToC(err)
	}

	return sqliteOK
}

//export goVFSCheckReservedLock
func goVFSCheckReservedLock(cfile *C.sqlite3_file, pResOut *C.int) C.int {
	s3vfsFile := (*C.s3vfsFile)(unsafe.Pointer(cfile))

	fileID := uint64(s3vfsFile.id)

	fileMux.Lock()
	file := fileMap[fileID]
	fileMux.Unlock()

	if file == nil {
		return errToC(GenericError)
	}

	locked, err := file.CheckReservedLock()
	if err != nil {
		return errToC(err)
	}

	if locked {
		*pResOut = C.int(0)
	} else {
		*pResOut = C.int(1)
	}

	return sqliteOK
}

//export goVFSSectorSize
func goVFSSectorSize(cfile *C.sqlite3_file) C.int {
	s3vfsFile := (*C.s3vfsFile)(unsafe.Pointer(cfile))

	fileID := uint64(s3vfsFile.id)

	fileMux.Lock()
	file := fileMap[fileID]
	fileMux.Unlock()

	if file == nil {
		return 1024
	}

	return C.int(file.SectorSize())
}

//export goVFSDeviceCharacteristics
func goVFSDeviceCharacteristics(cfile *C.sqlite3_file) C.int {
	s3vfsFile := (*C.s3vfsFile)(unsafe.Pointer(cfile))

	fileID := uint64(s3vfsFile.id)

	fileMux.Lock()
	file := fileMap[fileID]
	fileMux.Unlock()

	if file == nil {
		return 0
	}

	return C.int(file.DeviceCharacteristics())
}

func vfsFromC(cvfs *C.sqlite3_vfs) ExtendedVFSv1 {
	vfsName := C.GoString(cvfs.zName)
	return vfsMap[vfsName]
}

func errToC(err error) C.int {
	if e, ok := err.(sqliteError); ok {
		return C.int(e.code)
	}
	return C.int(GenericError.code)
}
