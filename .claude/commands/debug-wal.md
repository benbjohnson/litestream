---
description: Diagnose WAL monitoring, checkpoint delays, and replication lag.
---

Record WAL size, database page size, timestamps, and local and replicated TXIDs. If the socket is enabled, start with [Debug IPC](debug-ipc.md) and inspect `/debug/sync-status`. Identify whether the database has stopped capturing commits, the replica is behind, or a checkpoint is blocked.

- Trace `DB.monitor`, `DB.Sync`, and WAL verification in `db.go`. Inspect commit boundaries, WAL end offsets, salts, and checksum handling in `wal_reader.go`. The monitor interval defaults to one second. Check whether a sync error triggered retries or backoff.
- Check `DB.Notify` and `applySyncExecutor` for local sync notification. The channel closes on a successful capture and is replaced. Follow `Replica.monitor` for the separate upload and retry loop.
- Trace `checkpointIfNeeded` and `checkpointWithExecutor` before changing checkpoint behavior. The regular page threshold is 1,000 and the truncate threshold is 121,359 by default. Check the time trigger, whether a passive checkpoint made progress, and which readers or writers remain active. Use the `min-checkpoint-page-count` and `truncate-page-n` database config keys.
- On a test copy, inspect `PRAGMA journal_mode`, `page_size`, and `wal_autocheckpoint`. `PRAGMA wal_checkpoint` and `PRAGMA wal_checkpoint(PASSIVE)` can perform checkpoint work. SQLite describes each mode in its [PRAGMA reference](https://sqlite.org/pragma.html#pragma_wal_checkpoint).

```bash
go test -race -v -run '^(TestDB_SyncChunksWALAtCommitBoundary|TestDB_CheckpointPassiveRestartSkipsTruncate|TestDB_CheckpointTruncateSkipsRepeatedPassiveWithoutProgress|TestDB_MonitorRetriesAndRecoversFromLTXStagingDiskFull)$' .
```

Use the corresponding case in `db_internal_test.go` to reproduce the observed failure and the behavioral gate in [CONTRIBUTING.md](../../CONTRIBUTING.md) for core WAL changes.
