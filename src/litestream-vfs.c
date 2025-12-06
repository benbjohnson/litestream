#include "litestream-vfs.h"
#include "sqlite3.h"
#include "sqlite3ext.h"
#include "sqlite3vfs.h"
#include <stdio.h>
#include <stdlib.h>

/* sqlite3vfs already called SQLITE_EXTENSION_INIT1 */
extern const sqlite3_api_routines *sqlite3_api;

/* Go function declarations */
extern char* GoLitestreamRegisterConnection(void* db, sqlite3_uint64 file_id);
extern char* GoLitestreamUnregisterConnection(void* db);
extern char* GoLitestreamSetTime(void* db, char* timestamp);
extern char* GoLitestreamResetTime(void* db);
extern char* GoLitestreamTime(void* db, char** out);
extern char* GoLitestreamTxid(void* db, char** out);
extern char* GoLitestreamLag(void* db, sqlite3_int64* out);

/* Internal function declarations */
static int litestream_register_connection(sqlite3* db);
static void litestream_set_time_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv);
static void litestream_time_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv);
static void litestream_txid_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv);
static void litestream_lag_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv);
static void litestream_function_destroy(void* db);
static void litestream_auto_extension(sqlite3* db, const char** pzErrMsg, const struct sqlite3_api_routines* pApi);

/* This routine is called when the extension is loaded. */
int sqlite3_litestreamvfs_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);

  /* call into Go to register the VFS */
  LitestreamVFSRegister();

  /* Register SQL functions for new connections. */
  rc = sqlite3_auto_extension((void (*)(void))litestream_auto_extension);

  if( rc==SQLITE_OK ) rc = SQLITE_OK_LOAD_PERMANENTLY;
  return rc;
}

static void litestream_auto_extension(sqlite3* db, const char** pzErrMsg, const struct sqlite3_api_routines* pApi) {
  (void)pzErrMsg;
  (void)pApi;

  if (litestream_register_connection(db) != SQLITE_OK) {
    return;
  }

  /* litestream_set_time(timestamp) - for time travel */
  sqlite3_create_function_v2(db, "litestream_set_time", 1, SQLITE_UTF8 | SQLITE_DIRECTONLY, db, litestream_set_time_impl, 0, 0, litestream_function_destroy);

  /* Read-only functions: litestream_time(), litestream_txid(), litestream_lag() */
  sqlite3_create_function_v2(db, "litestream_time", 0, SQLITE_UTF8, db, litestream_time_impl, 0, 0, 0);
  sqlite3_create_function_v2(db, "litestream_txid", 0, SQLITE_UTF8, db, litestream_txid_impl, 0, 0, 0);
  sqlite3_create_function_v2(db, "litestream_lag", 0, SQLITE_UTF8, db, litestream_lag_impl, 0, 0, 0);
}

static int litestream_register_connection(sqlite3* db) {
  sqlite3_file* file = 0;
  int rc = sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &file);
  if (rc != SQLITE_OK || file == 0) {
    return rc;
  }

  if (file->pMethods != &s3vfs_io_methods) {
    /* Not using the litestream VFS. */
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

  /* Handle special 'LATEST' value */
  if (sqlite3_stricmp((const char*)ts, "LATEST") == 0) {
    sqlite3* db = sqlite3_context_db_handle(ctx);
    char* err = GoLitestreamResetTime(db);
    if (err != 0) {
      sqlite3_result_error(ctx, err, -1);
      free(err);
      return;
    }
    sqlite3_result_null(ctx);
    return;
  }

  sqlite3* db = sqlite3_context_db_handle(ctx);
  char* err = GoLitestreamSetTime(db, (char*)ts);
  if (err != 0) {
    sqlite3_result_error(ctx, err, -1);
    free(err);
    return;
  }

  sqlite3_result_null(ctx);
}

static void litestream_time_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
  (void)argc;
  (void)argv;

  sqlite3* db = sqlite3_context_db_handle(ctx);
  char* out = 0;
  char* err = GoLitestreamTime(db, &out);
  if (err != 0) {
    sqlite3_result_error(ctx, err, -1);
    free(err);
    return;
  }

  sqlite3_result_text(ctx, out, -1, SQLITE_TRANSIENT);
  free(out);
}

static void litestream_txid_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
  (void)argc;
  (void)argv;

  sqlite3* db = sqlite3_context_db_handle(ctx);
  char* out = 0;
  char* err = GoLitestreamTxid(db, &out);
  if (err != 0) {
    sqlite3_result_error(ctx, err, -1);
    free(err);
    return;
  }

  sqlite3_result_text(ctx, out, -1, SQLITE_TRANSIENT);
  free(out);
}

static void litestream_lag_impl(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
  (void)argc;
  (void)argv;

  sqlite3* db = sqlite3_context_db_handle(ctx);
  sqlite3_int64 out = 0;
  char* err = GoLitestreamLag(db, &out);
  if (err != 0) {
    sqlite3_result_error(ctx, err, -1);
    free(err);
    return;
  }

  sqlite3_result_int64(ctx, out);
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
