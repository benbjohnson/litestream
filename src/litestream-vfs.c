#include "litestream-vfs.h"
#include "sqlite3.h"
#include "sqlite3ext.h"
#include <stdio.h>

/* sqlite3vfs already called SQLITE_EXTENSION_INIT1 */
extern const sqlite3_api_routines *sqlite3_api;

extern void Sqlite3HTTPRegister();

// This routine is called when the extension is loaded.
// Register the new VFS.
int sqlite3_litestreamvfs_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);

  // call into Go
  LitestreamVFSRegister();

  if( rc==SQLITE_OK ) rc = SQLITE_OK_LOAD_PERMANENTLY;
  return rc;
}
