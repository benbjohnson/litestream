# SQLite internals for Litestream

Mirrors `docs/SQLITE_INTERNALS.md` in the Litestream repository. Plain source paths are relative to the repository root; links between bundled guides stay relative to this directory. Update both copies together.

Read this alongside `db.go`, `wal_reader.go`, and the SQLite sources linked below. The production driver is `modernc.org/sqlite`; its version and the supported Go version are pinned in `go.mod`.

## Database, WAL, and shared memory

The database holds checkpointed pages. The WAL can hold both committed transactions and an incomplete tail. The `-shm` file contains the wal-index and coordination state. Copying only the database while a WAL is active can omit committed data. Do not unlink a live database's sidecars to recover disk space; inspect connection ownership and the recovery path first. See [SQLite WAL](https://sqlite.org/wal.html) and [file format](https://sqlite.org/fileformat.html#walformat).

WAL headers are 32 bytes and frame headers are 24 bytes. A nonzero database-size field marks a commit frame. Stored multibyte header fields are big-endian; the WAL magic determines the byte order used to calculate checksums. Validating length alone is insufficient: check salts, checksum continuity, commit boundaries, and the readable end of the current generation. `WALReader.ReadFrame` validates salts and checksums and returns the commit field; `WALReader.PageMap` retains only committed frames within the readable boundary. See [WAL checksums](https://sqlite.org/fileformat.html#checksum_algorithm).

A WAL can restart without changing its allocated file length. `DB.Sync`, `verifyAndSyncWithExecutor`, and `detectFullCheckpoint` handle generation changes; a file-size-only polling example would miss them. Inspect the wal-index helpers and fallback behavior before treating shared-memory hints as authoritative.

The WAL header fields below are four bytes each, except the two eight-byte pairs. Offsets are from the start of the WAL. See [SQLite's WAL header layout](https://sqlite.org/fileformat.html#walformat) and `WALReader.readHeader`.

| Offset | Bytes | Field |
|--------|-------|-------|
| 0 | 4 | Magic, `0x377f0682` or `0x377f0683` |
| 4 | 4 | Format version, currently 3007000 |
| 8 | 4 | Database page size |
| 12 | 4 | Checkpoint sequence number |
| 16 | 8 | Two salts |
| 24 | 8 | Two checksum words over the first 24 bytes |

Each frame contains this 24-byte header followed by one page of data. Offsets are from the start of the frame header; see `WALReader.ReadFrame`.

| Offset | Bytes | Field |
|--------|-------|-------|
| 0 | 4 | Database page number |
| 4 | 4 | Database size in pages after commit, or zero for a non-commit frame |
| 8 | 8 | Salts matching the WAL header |
| 16 | 8 | Cumulative checksum words |

## Pages and the lock byte

SQLite page numbers start at one; supported page sizes are powers of two from 512 through 65536 bytes. The page containing byte `0x40000000` is reserved. Its number is `uint32(0x40000000/pageSize) + 1`, or 262145 for 4096-byte pages. Use `ltx.LockPgno(uint32(pageSize))` and skip that page during database iteration, LTX encoding, and compaction. See [database pages](https://sqlite.org/fileformat.html#pages) and [lock-byte page](https://sqlite.org/fileformat.html#the_lock_byte_page).

B-tree page headers describe only B-tree pages; they are not a universal header for freelist, overflow, or lock pages. Page one also begins with the database header. Tests for page iteration must include databases spanning the reserved page and supported page sizes; see `tests/integration/boundary_test.go`.

## Transactions and Litestream's read lock

`DB.acquireReadLock` begins a long-lived transaction and reads `_litestream_seq` to establish the snapshot. A constant-only query does not replace that database read. The transaction's lifetime is deliberately detached from the requesting context; statement execution still uses the caller's context. `releaseReadLock`, checkpoint coordination, and shutdown own its cleanup. This pins checkpoint progress without preventing application writes. See [WAL concurrency](https://sqlite.org/wal.html#concurrency).

Keep rollback-journal locking descriptions separate from WAL locking. In WAL mode, IMMEDIATE and EXCLUSIVE transactions have the same writer behavior; neither is a recipe for blocking all readers. See [SQLite transactions](https://sqlite.org/lang_transaction.html). Check transaction mode and busy handling against the actual driver's pinned implementation before changing them.

## Checkpoints

| Mode | Behavior |
|------|----------|
| PASSIVE | Makes available progress without waiting for readers or writers; it may leave frames uncheckpointed. |
| FULL | Waits through the busy handler for the writer and suitable reader snapshots, then checkpoints all frames; new readers remain allowed. |
| RESTART | Adds a wait for readers to leave the WAL so the next writer can restart it. |
| TRUNCATE | Adds truncation of the WAL to zero bytes after successful restart conditions. |

These distinctions come from [SQLite's checkpoint API](https://sqlite.org/c3ref/wal_checkpoint_v2.html). A successful SQL statement alone does not prove the checkpoint finished; inspect returned busy/frame counts.

`DB.checkpointIfNeeded` uses PASSIVE for routine thresholds and intervals. At the emergency truncate threshold it first attempts PASSIVE, then may use blocking TRUNCATE; the executor coordinates the required sync and snapshot boundary. Read that function and `checkpointWithExecutor` for current policy. The automatic policy does not use RESTART. The public `DB.Checkpoint` takes a context and mode.

## Driver and pragma scope

Production opens `database/sql` with driver name `sqlite`. `DB.init` uses repeated `_pragma` parameters for `busy_timeout(...)` and `wal_autocheckpoint(0)`, sets persistent WAL through the driver connection, enables WAL, and checks the returned mode. Tests and `litestream-test` also use the distinct mattn `sqlite3` driver. Their DSN options and error types are not interchangeable.

Connection-local pragmas must reach every connection that needs them. One pooled `Exec` does not initialize all future connections. Avoid importing mattn `_busy_timeout` or `_journal` options into a modernc DSN. Verify the pinned driver's `applyQueryParams` and transaction implementation before changing connection setup.

`PRAGMA synchronous=NORMAL` in WAL mode can lose recently committed transactions after a power failure; choose durability settings explicitly. `busy_timeout` installs a connection busy handler, while `wal_autocheckpoint` configures that connection's automatic checkpoint threshold. See [SQLite pragmas](https://sqlite.org/pragma.html#pragma_synchronous).

## Review entry points

Use the shared litestream-review skill (`.agents/skills/litestream-review/SKILL.md`) for a review. Follow `DB.Sync` through WAL verification, committed-frame selection, LTX creation, replica upload, and checkpoint coordination. Return correctness errors to callers; preserve documented best-effort fallbacks. Avoid reproducing these algorithms as unchecked illustrative code.
