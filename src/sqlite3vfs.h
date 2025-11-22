#ifndef SQLITE3VFS_H
#define SQLITE3VFS_H

#ifdef SQLITE3VFS_LOADABLE_EXT
#include "sqlite3ext.h"
#else
#include "sqlite3-binding.h"
#endif

typedef struct s3vfsFile {
  sqlite3_file base; /* IO methods */
  sqlite3_uint64 id; /* Go object id  */
} s3vfsFile;

int s3vfsNew(char* name, int maxPathName);

int s3vfsClose(sqlite3_file*);
int s3vfsRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
int s3vfsWrite(sqlite3_file*,const void*,int iAmt, sqlite3_int64 iOfst);
int s3vfsTruncate(sqlite3_file*, sqlite3_int64 size);
int s3vfsSync(sqlite3_file*, int flags);
int s3vfsFileSize(sqlite3_file*, sqlite3_int64 *pSize);
int s3vfsLock(sqlite3_file*, int);
int s3vfsUnlock(sqlite3_file*, int);
int s3vfsCheckReservedLock(sqlite3_file*, int *pResOut);
int s3vfsFileControl(sqlite3_file*, int op, void *pArg);
int s3vfsSectorSize(sqlite3_file*);
int s3vfsDeviceCharacteristics(sqlite3_file*);
int s3vfsShmMap(sqlite3_file*, int iPg, int pgsz, int, void volatile**);
int s3vfsShmLock(sqlite3_file*, int offset, int n, int flags);
void s3vfsShmBarrier(sqlite3_file*);
int s3vfsShmUnmap(sqlite3_file*, int deleteFlag);
int s3vfsFetch(sqlite3_file*, sqlite3_int64 iOfst, int iAmt, void **pp);
int s3vfsUnfetch(sqlite3_file*, sqlite3_int64 iOfst, void *p);


int s3vfsOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
int s3vfsDelete(sqlite3_vfs*, const char *, int);
int s3vfsAccess(sqlite3_vfs*, const char *, int, int *);
int s3vfsFullPathname(sqlite3_vfs*, const char *zName, int, char *zOut);
void *s3vfsDlOpen(sqlite3_vfs*, const char *zFilename);
void s3vfsDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
void (*s3vfsDlSym(sqlite3_vfs *pVfs, void *p, const char*zSym))(void);
void s3vfsDlClose(sqlite3_vfs*, void*);
int s3vfsRandomness(sqlite3_vfs*, int nByte, char *zOut);
int s3vfsSleep(sqlite3_vfs*, int microseconds);
int s3vfsCurrentTime(sqlite3_vfs*, double*);
int s3vfsGetLastError(sqlite3_vfs*, int, char *);
int s3vfsCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);

const extern sqlite3_io_methods s3vfs_io_methods;

#endif /* SQLITE3_VFS */
