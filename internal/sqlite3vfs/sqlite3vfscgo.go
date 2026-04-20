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
	filename := openFilenameFromC(name)
	vfs := vfsFromC(cvfs)

	var (
		file     File
		retFlags OpenFlag
		err      error
	)
	if opener, ok := vfs.(FilenameOpener); ok {
		file, retFlags, err = opener.OpenFilename(filename, OpenFlag(flags))
	} else {
		file, retFlags, err = vfs.Open(filename.String(), OpenFlag(flags))
	}
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

	if err := vfs.Delete(fileName, syncDir > 0); err != nil {
		return errToC(err)
	}

	return sqliteOK
}

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
	return C.int(vfs.Randomness(buf))
}

//export goVFSSleep
func goVFSSleep(cvfs *C.sqlite3_vfs, microseconds C.int) C.int {
	vfs := vfsFromC(cvfs)
	vfs.Sleep(time.Duration(microseconds) * time.Microsecond)
	return sqliteOK
}

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

	if err := file.Close(); err != nil {
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
			panic("ReadAt invalid semantics: returned n < len(p) but with a nil error")
		}
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
	if _, err := file.WriteAt(goBuf, int64(iOfst)); err != nil {
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

	if err := file.Truncate(int64(size)); err != nil {
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

	if err := file.Sync(SyncType(flags)); err != nil {
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

	if err := file.Lock(LockType(eLock)); err != nil {
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

	if err := file.Unlock(LockType(eLock)); err != nil {
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

//export goVFSFileControl
func goVFSFileControl(cfile *C.sqlite3_file, op C.int, pArg unsafe.Pointer) C.int {
	s3vfsFile := (*C.s3vfsFile)(unsafe.Pointer(cfile))
	fileID := uint64(s3vfsFile.id)

	fileMux.Lock()
	file := fileMap[fileID]
	fileMux.Unlock()

	if file == nil {
		return C.SQLITE_NOTFOUND
	}

	const SQLITE_FCNTL_PRAGMA = 14
	if op != C.int(SQLITE_FCNTL_PRAGMA) {
		return C.SQLITE_NOTFOUND
	}

	fc, ok := file.(FileController)
	if !ok {
		return C.SQLITE_NOTFOUND
	}

	azArg := (**C.char)(pArg)
	ptrSize := unsafe.Sizeof(azArg)

	pragmaNamePtr := *(**C.char)(unsafe.Pointer(uintptr(pArg) + ptrSize))
	if pragmaNamePtr == nil {
		return C.SQLITE_NOTFOUND
	}
	pragmaName := C.GoString(pragmaNamePtr)

	var pragmaValue *string
	pragmaValuePtr := *(**C.char)(unsafe.Pointer(uintptr(pArg) + 2*ptrSize))
	if pragmaValuePtr != nil {
		val := C.GoString(pragmaValuePtr)
		pragmaValue = &val
	}

	result, err := fc.FileControl(int(op), pragmaName, pragmaValue)
	if err != nil {
		return errToC(err)
	}

	if result != nil {
		cResult := C.CString(*result)
		*azArg = C.s3vfsSqlite3Mprintf(cResult)
		C.free(unsafe.Pointer(cResult))
	}
	return C.SQLITE_OK
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

func openFilenameFromC(name *C.char) Filename {
	if name == nil {
		return NewFilename("", nil)
	}

	dbNamePtr := C.s3vfsFilenameDatabase(name)
	fileName := C.GoString(dbNamePtr)
	if fileName == "" {
		fileName = C.GoString(name)
	}

	var uriParameters map[string]string
	for i := 0; ; i++ {
		keyPtr := C.s3vfsURIKey(name, C.int(i))
		if keyPtr == nil {
			break
		}

		if uriParameters == nil {
			uriParameters = make(map[string]string)
		}

		key := C.GoString(keyPtr)
		valuePtr := C.s3vfsURIParameter(name, keyPtr)
		if valuePtr == nil {
			uriParameters[key] = ""
			continue
		}
		uriParameters[key] = C.GoString(valuePtr)
	}

	return NewFilename(fileName, uriParameters)
}
