#include "sqlite3vfs.h"
#include <stdlib.h>
#include <stdio.h>

#ifdef SQLITE3VFS_LOADABLE_EXT
SQLITE_EXTENSION_INIT1
#endif

extern int goVFSOpen(sqlite3_vfs* vfs, const char * name, sqlite3_file* file, int flags, int *outFlags);
extern int goVFSDelete(sqlite3_vfs*, const char *zName, int syncDir);
extern int goVFSAccess(sqlite3_vfs*, const char *zName, int flags, int* pResOut);
extern int goVFSFullPathname(sqlite3_vfs*, const char *zName, int nOut, char* zOut);
extern int goVFSRandomness(sqlite3_vfs*, int nByte, char *zOut);
extern int goVFSSleep(sqlite3_vfs*, int microseconds);
extern int goVFSCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64* piNow);

extern int goVFSClose(sqlite3_file* file);
extern int goVFSRead(sqlite3_file* file, void* buf, int iAmt, sqlite3_int64 iOfst);
extern int goVFSWrite(sqlite3_file* file ,const void* buf, int iAmt, sqlite3_int64 iOfst);
extern int goVFSTruncate(sqlite3_file* file, sqlite3_int64 size);
extern int goVFSSync(sqlite3_file* file, int flags);
extern int goVFSFileSize(sqlite3_file* file, sqlite3_int64 *pSize);
extern int goVFSLock(sqlite3_file* file, int eLock);
extern int goVFSUnlock(sqlite3_file*, int eLock);
extern int goVFSCheckReservedLock(sqlite3_file* file, int *pResOut);
extern int goVFSSectorSize(sqlite3_file* file);
extern int goVFSDeviceCharacteristics(sqlite3_file* file);


int s3vfsNew(char* name, int maxPathName) {
  sqlite3_vfs* vfs;
  sqlite3_vfs* delegate;
  vfs = calloc(1, sizeof(sqlite3_vfs));
  if (vfs == NULL) {
    return SQLITE_ERROR;
  }

  delegate = sqlite3_vfs_find(0);
#ifndef SQLITE3VFS_LOADABLE_EXT
  if (delegate == NULL) {
    int init_rc = sqlite3_initialize();
    if (init_rc != SQLITE_OK) {
      free(vfs);
      return init_rc;
    }
    delegate = sqlite3_vfs_find(0);
  }
#endif

  if (delegate == NULL) {
    free(vfs);
    return SQLITE_ERROR;
  }

  vfs->iVersion = 2;
  vfs->szOsFile = sizeof(s3vfsFile);
  vfs->mxPathname = maxPathName;
  vfs->zName = name;
  vfs->xOpen = s3vfsOpen;
  vfs->xDelete = s3vfsDelete;
  vfs->xAccess = s3vfsAccess;
  vfs->xFullPathname = s3vfsFullPathname;
  vfs->xDlOpen = delegate->xDlOpen;
  vfs->xDlError = delegate->xDlError;
  vfs->xDlSym = delegate->xDlSym;
  vfs->xDlClose = delegate->xDlClose;
  vfs->xRandomness = s3vfsRandomness;
  vfs->xSleep = s3vfsSleep;
  vfs->xCurrentTime = s3vfsCurrentTime;
  vfs->xGetLastError = delegate->xGetLastError;
  vfs->xCurrentTimeInt64 = s3vfsCurrentTimeInt64;

  return sqlite3_vfs_register(vfs, 0);
}

int s3vfsOpen(sqlite3_vfs* vfs, const char * name, sqlite3_file* file, int flags, int *outFlags) {
  int ret = goVFSOpen(vfs, name, file, flags, outFlags);
  file->pMethods = &s3vfs_io_methods;
  return ret;
}

int s3vfsDelete(sqlite3_vfs* vfs, const char *zName, int syncDir) {
  int ret = goVFSDelete(vfs, zName, syncDir);
  return ret;
}

int s3vfsAccess(sqlite3_vfs* vfs, const char *zName, int flags, int* pResOut) {
  return goVFSAccess(vfs, zName, flags, pResOut);
}

int s3vfsFullPathname(sqlite3_vfs* vfs, const char *zName, int nOut, char* zOut) {
  return goVFSFullPathname(vfs, zName, nOut, zOut);
}

int s3vfsRandomness(sqlite3_vfs* vfs, int nByte, char *zOut) {
  return goVFSRandomness(vfs, nByte, zOut);
}

int s3vfsSleep(sqlite3_vfs* vfs, int microseconds) {
  return goVFSSleep(vfs, microseconds);
}

int s3vfsCurrentTime(sqlite3_vfs* vfs, double* prNow) {
  sqlite3_int64 i = 0;
  int rc;
  rc = s3vfsCurrentTimeInt64(0, &i);
  *prNow = i/86400000.0;
  return rc;
}

int s3vfsCurrentTimeInt64(sqlite3_vfs* vfs, sqlite3_int64* piNow) {
  return goVFSCurrentTimeInt64(vfs, piNow);
}

int s3vfsClose(sqlite3_file* file) {
  return goVFSClose(file);
}

int s3vfsRead(sqlite3_file* file, void* zBuf, int iAmt, sqlite3_int64 iOfst) {
  return goVFSRead(file, zBuf, iAmt, iOfst);
}

int s3vfsWrite(sqlite3_file* file, const void* zBuf, int iAmt, sqlite3_int64 iOfst) {
  return goVFSWrite(file, zBuf, iAmt, iOfst);
}

int s3vfsTruncate(sqlite3_file* file, sqlite3_int64 size) {
  return goVFSTruncate(file, size);
}

int s3vfsSync(sqlite3_file* file, int flags) {
  return goVFSSync(file, flags);
}

int s3vfsFileSize(sqlite3_file* file, sqlite3_int64 *pSize) {
  return goVFSFileSize(file, pSize);
}

int s3vfsLock(sqlite3_file* file, int eLock) {
  return goVFSLock(file, eLock);
}

int s3vfsUnlock(sqlite3_file* file , int eLock) {
  return goVFSUnlock(file, eLock);
}

int s3vfsCheckReservedLock(sqlite3_file* file, int *pResOut) {
  return goVFSCheckReservedLock(file, pResOut);
}

int s3vfsSectorSize(sqlite3_file* file) {
  return goVFSSectorSize(file);

}

int s3vfsDeviceCharacteristics(sqlite3_file* file) {
  return goVFSDeviceCharacteristics(file);
}

int s3vfsFileControl(sqlite3_file *pFile, int op, void *pArg){
  return SQLITE_NOTFOUND;
}

const sqlite3_io_methods s3vfs_io_methods = {
  1,                               /* iVersion */
  s3vfsClose,                      /* xClose */
  s3vfsRead,                       /* xRead */
  s3vfsWrite,                      /* xWrite */
  s3vfsTruncate,                   /* xTruncate */
  s3vfsSync,                       /* xSync */
  s3vfsFileSize,                   /* xFileSize */
  s3vfsLock,                       /* xLock */
  s3vfsUnlock,                     /* xUnlock */
  s3vfsCheckReservedLock,          /* xCheckReservedLock */
  s3vfsFileControl,                /* xFileControl */
  s3vfsSectorSize,                 /* xSectorSize */
  s3vfsDeviceCharacteristics,      /* xDeviceCharacteristics */
};
