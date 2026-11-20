# SQLite correctness

Recheck the relevant SQLite source when behavior or dependencies change.

Review WAL parsing against [SQLite's file format](https://sqlite.org/fileformat.html#walformat): 32-byte WAL header, 24-byte frame header, salts, cumulative checksums, and commit frames. The stored header integers are big-endian; checksum input order depends on the magic. Page sizes range from 512 to 65536 bytes in powers of two. The reserved lock-byte page contains byte `0x40000000`; use `ltx.LockPgno` rather than a page-size-specific constant.

Check snapshot visibility and WAL reuse against [WAL concurrency](https://sqlite.org/wal.html#concurrency). A reader can pin checkpoint progress while writers continue. File size does not identify the current generation. Copying only the database or deleting a live WAL can discard committed state. Review sidecar moves, replacement, and unlinking with connection ownership and recovery ordering in view.

For wal-index access, verify lock ownership, byte order, header consistency, and fallbacks against the [WAL-index format](https://sqlite.org/fileformat.html#walidxfmt). A hint must not bypass committed-frame validation.

Use the [checkpoint API](https://sqlite.org/c3ref/wal_checkpoint_v2.html) to distinguish PASSIVE progress from a complete checkpoint, FULL's writer wait, RESTART's reader wait, and TRUNCATE's additional truncation. Check busy results and frame counts, not only the SQL execution error.

[Transaction modes](https://sqlite.org/lang_transaction.html) distinguish deferred acquisition from an immediate writer transaction. EXCLUSIVE and IMMEDIATE behave alike in WAL mode. A read-to-write upgrade can fail under contention; a timeout does not prove every locking failure is retryable.

[Pragma semantics](https://sqlite.org/pragma.html) govern connection scope and durability. Verify journal-mode results; assess power-loss durability before changing synchronous mode. Trace `DB.acquireReadLock`, `checkpointWithExecutor`, `WALReader.ReadFrame`, and the sidecar handling in restore against `docs/SQLITE_INTERNALS.md`. Require boundary coverage when page loops change.
