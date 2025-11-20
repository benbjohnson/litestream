#include "litestream-vfs.h"
#include "sqlite3.h"
#include "sqlite3ext.h"
#include "sqlite3vfs.h"
#include <stdio.h>
#include <stdlib.h>

/* sqlite3vfs already called SQLITE_EXTENSION_INIT1 */
extern const sqlite3_api_routines *sqlite3_api;

extern char* GoLitestreamRegisterConnection(void* db, sqlite3_uint64 file_id);
extern char* GoLitestreamUnregisterConnection(void* db);
extern char* GoLitestreamSetTime(void* db, const char* timestamp);
extern char* GoLitestreamResetTime(void* db);
extern char* GoLitestreamCurrentTime(void* db, char** out);

static int litestream_register_connection(sqlite3* db);
static void litestream_set_time_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv);
static void litestream_reset_time_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv);
static void litestream_current_time_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv);
static void litestream_function_destroy(void* db);
static void litestream_auto_extension(sqlite3* db);

// This routine is called when the extension is loaded.
// Register the new VFS.
int sqlite3_litestreamvfs_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);

  // call into Go
  LitestreamVFSRegister();

  // Register SQL functions for new connections.
  rc = sqlite3_auto_extension((void (*)(void))litestream_auto_extension);

  if( rc==SQLITE_OK ) rc = SQLITE_OK_LOAD_PERMANENTLY;
  return rc;
}

static void litestream_auto_extension(sqlite3* db) {
  if (litestream_register_connection(db) != SQLITE_OK) {
    return;
  }

  sqlite3_create_function_v2(db, "litestream_set_time", 1, SQLITE_UTF8 | SQLITE_DIRECTONLY, db, litestream_set_time_impl, 0, 0, litestream_function_destroy);
  sqlite3_create_function_v2(db, "litestream_reset_time", 0, SQLITE_UTF8 | SQLITE_DIRECTONLY, db, litestream_reset_time_impl, 0, 0, litestream_function_destroy);
  sqlite3_create_function_v2(db, "litestream_current_time", 0, SQLITE_UTF8, db, litestream_current_time_impl, 0, 0, litestream_function_destroy);
}

static int litestream_register_connection(sqlite3* db) {
  sqlite3_file* file = 0;
  int rc = sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &file);
  if (rc != SQLITE_OK || file == 0) {
    return rc;
  }

  if (file->pMethods != &s3vfs_io_methods) {
    // Not using the litestream VFS.
    return SQLITE_DONE;
  }

  sqlite3_uint64 file_id = ((s3vfsFile*)file)->id;
  char* err = GoLitestreamRegisterConnection(db, file_id);
  if (err != 0) {
    free(err);
    return SQLITE_ERROR;
  }

  return SQLITE_OK;
}

static void litestream_set_time_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
  if (argc != 1) {
    sqlite3_result_error(ctx, "expected timestamp argument", -1);
    return;
  }

  const unsigned char* ts = sqlite3_value_text(argv[0]);
  if (!ts) {
    sqlite3_result_error(ctx, "timestamp required", -1);
    return;
  }

  sqlite3* db = sqlite3_context_db_handle(ctx);
  char* err = GoLitestreamSetTime(db, (const char*)ts);
  if (err != 0) {
    sqlite3_result_error(ctx, err, -1);
    free(err);
    return;
  }

  sqlite3_result_null(ctx);
}

static void litestream_reset_time_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
  (void)argc;
  (void)argv;

  sqlite3* db = sqlite3_context_db_handle(ctx);
  char* err = GoLitestreamResetTime(db);
  if (err != 0) {
    sqlite3_result_error(ctx, err, -1);
    free(err);
    return;
  }

  sqlite3_result_null(ctx);
}

static void litestream_current_time_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
  (void)argc;
  (void)argv;

  sqlite3* db = sqlite3_context_db_handle(ctx);
  char* out = 0;
  char* err = GoLitestreamCurrentTime(db, &out);
  if (err != 0) {
    sqlite3_result_error(ctx, err, -1);
    free(err);
    return;
  }

  sqlite3_result_text(ctx, out, -1, SQLITE_TRANSIENT);
  free(out);
}

static void litestream_function_destroy(void* db) {
  if (db == 0) {
    return;
  }
  char* err = GoLitestreamUnregisterConnection(db);
  if (err != 0) {
    free(err);
  }
}
