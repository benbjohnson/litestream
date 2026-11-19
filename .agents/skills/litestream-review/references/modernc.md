# Modernc and database/sql

Use the `modernc.org/sqlite` version pinned in `go.mod`. Fetch the corresponding version when the pin changes; current online package docs alone are insufficient.

The versioned [driver implementation](https://gitlab.com/cznic/sqlite/-/raw/v1.49.1/sqlite.go) registers `sqlite`. Its `applyQueryParams` accepts repeated `_pragma` values and `_txlock` values deferred, immediate, or exclusive. Busy timeout pragmas are applied first, then other pragmas are sorted. Do not rely on DSN text order or copy mattn's `_busy_timeout` options into a modernc connection.

The versioned [transaction implementation](https://gitlab.com/cznic/sqlite/-/raw/v1.49.1/tx.go) applies the configured transaction mode when `ReadOnly` is false. `ReadOnly` does not itself enforce a read-only database connection. Review the actual open mode and SQL if write prevention is required.

The versioned [error implementation](https://gitlab.com/cznic/sqlite/-/raw/v1.49.1/error.go) exposes `*sqlite.Error` and `Code()`. Match wrapped errors using the standard errors APIs and account for extended result codes where the behavior depends on a primary code. Production uses modernc, while tests and `litestream-test` also use mattn/go-sqlite3; an assertion against only mattn's error type cannot classify modernc errors. Inspect `isSQLiteBusyError` and nearby tests rather than importing a second driver to classify errors.

A [database/sql DB](https://pkg.go.dev/database/sql#DB) is a connection pool. Assess maximum open and idle connections, connection-local pragmas, and transaction ownership together. A single `Exec` on the pool does not configure every connection. A one-connection pool can block if a held transaction calls back into the pool. `BeginTx` context lifetime also controls transaction lifetime; check cancellation and cleanup when changing Litestream's long-lived read transaction.
