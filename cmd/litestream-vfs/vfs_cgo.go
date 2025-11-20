//go:build vfs
// +build vfs

package main

/*
#cgo CFLAGS: -I../../src
#include "sqlite3.h"

typedef struct s3vfsFile {
	sqlite3_file base;
	sqlite3_uint64 id;
} s3vfsFile;

extern const sqlite3_io_methods s3vfs_io_methods;

static int litestream_file_id(sqlite3* db, sqlite3_uint64* out) {
	sqlite3_file* file = 0;
	int rc = sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &file);
	if (rc != SQLITE_OK || file == 0) {
		return rc;
	}
	if (file->pMethods != &s3vfs_io_methods) {
		return SQLITE_MISUSE;
	}
	*out = ((s3vfsFile*)file)->id;
	return SQLITE_OK;
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// LitestreamFileID returns the VFS file ID for a SQLite connection.
func LitestreamFileID(db unsafe.Pointer) (uint64, error) {
	var fileID C.sqlite3_uint64
	if rc := C.litestream_file_id((*C.sqlite3)(db), &fileID); rc != C.SQLITE_OK {
		return 0, fmt.Errorf("fetch file id: %d", int(rc))
	}
	return uint64(fileID), nil
}
