---
description: Trace a SQLite write from WAL capture through replica upload and compaction.
---

1. In WAL mode, SQLite writes frames to the WAL. Verify that the application transaction committed.
2. Follow `DB.monitor` and `DB.Sync` in `db.go`. The monitor runs on a timer and backs off after sync failures. Sync validates WAL state, captures committed frames in local L0 LTX files, and can checkpoint according to page and time thresholds. A successful capture closes the current notification channel.
3. Follow `Replica.monitor` and `syncOnce` in `replica.go`. The replica checks the database TXID, waits as needed for the configured interval or notification, and calls `uploadLTXFile` for pending L0 files. It advances its replicated position after a successful upload.
4. Follow `WriteLTXFile` in the configured backend. Look for upload errors, retry behavior, and the stored TXID range. For scheduled compaction and snapshots, see `Store.CompactDB`, `Compactor.Compact`, and `DB.Snapshot`.

On a test database, run replication with debug logging. Write a committed transaction from another connection, then inspect the remote listing and restore to a fresh file.

```bash
litestream replicate -log-level debug /path/to/test.db file:///path/to/replica
```

```bash
litestream ltx -level all file:///path/to/replica
litestream restore -integrity-check full -o /path/to/new-restored.db file:///path/to/replica
```

If the control socket is enabled, [Debug IPC](debug-ipc.md) covers sync diagnostics and replica upload health. See [Debug WAL](debug-wal.md) for capture and checkpoint delays.
