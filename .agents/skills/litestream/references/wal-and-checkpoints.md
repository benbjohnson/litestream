# WAL and checkpoints

## Implementation

For SQLite implementation or debugging, start with [SQLite internals](../../../../docs/SQLITE_INTERNALS.md) and the [SQLite file format](https://sqlite.org/fileformat2.html). Trace WAL capture and checkpoint coordination in `db.go` and frame validation in `wal_reader.go`.

- The lock-byte page starts at byte offset 1,073,741,824. Use `ltx.LockPgno` to identify it for the database's page size and skip it in page-copy loops. SQLite page sizes are powers of two from 512 to 65,536 bytes.
- WAL file and frame headers are 32 and 24 bytes, respectively. Decode stored integers as big-endian. Compute checksums using the byte order indicated by the WAL magic. Verify salts and use commit boundaries to distinguish committed frames from pending writes.
- For changes to page iteration, use the repository's large-database and page-size cases in `tests/integration/boundary_test.go`. For checkpoint or WAL changes, exercise the affected cases in `db_internal_test.go` and the behavioral gate described in [CONTRIBUTING.md](../../../../CONTRIBUTING.md).

## Diagnose WAL growth

Record WAL size, database page size, timestamps, and local and replicated TXIDs. If the socket is enabled, use [control socket diagnostics](runtime-debugging.md#inspect-the-control-socket): `/txid` reports the local position, `/debug/sync-status` reports local sync/checkpoint activity, and `/list` includes the last successful replica sync time. These endpoints do not report a remote TXID. Inspect replica ranges with `litestream ltx -level all -json`, supplying the configured database or replica as in [LTX inspection](ltx-and-compaction.md#inspect-files-and-restore-coverage); library callers can use `DB.SyncStatus(ctx)` to read both positions. Identify whether capture has stopped, uploads are behind, or a checkpoint is blocked.

- Trace `DB.monitor`, `DB.Sync`, and WAL verification in `db.go`. Inspect commit boundaries, WAL end offsets, salts, and checksum handling in `wal_reader.go`. The monitor interval defaults to one second. Check whether a sync error triggered retries or backoff.
- Check `DB.Notify` and `applySyncExecutor` for local sync notification. The channel closes on a successful capture and is replaced. Follow `Replica.monitor` for the separate upload and retry loop.
- Trace `checkpointIfNeeded` and `checkpointWithExecutor` before changing checkpoint behavior. The regular page threshold is 1,000 and the truncate threshold is 121,359 by default. Check the time trigger, whether a passive checkpoint made progress, and which readers or writers remain active. Use the `min-checkpoint-page-count` and `truncate-page-n` database config keys.
- On a test copy, inspect `PRAGMA journal_mode`, `page_size`, and `wal_autocheckpoint`. `PRAGMA wal_checkpoint` and `PRAGMA wal_checkpoint(PASSIVE)` can perform checkpoint work. SQLite describes each mode in its [PRAGMA reference](https://sqlite.org/pragma.html#pragma_wal_checkpoint).

```bash
go test -race -v -run '^(TestDB_SyncChunksWALAtCommitBoundary|TestDB_CheckpointPassiveRestartSkipsTruncate|TestDB_CheckpointTruncateSkipsRepeatedPassiveWithoutProgress|TestDB_MonitorRetriesAndRecoversFromLTXStagingDiskFull)$' .
```

Use the corresponding case in `db_internal_test.go` to reproduce the observed failure and the behavioral gate in [CONTRIBUTING.md](../../../../CONTRIBUTING.md) for core WAL changes.
