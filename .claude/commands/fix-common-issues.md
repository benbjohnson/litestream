---
description: Diagnose common Litestream replication, recovery, and deployment failures.
---

Record the Litestream version, effective configuration, database path or replica, and exact error. Identify the affected stage before changing configuration or local state.

- **WAL growth or checkpoint delay.** Use [Debug WAL](debug-wal.md) to inspect active readers, WAL capture, and checkpoint progress. Check the actual WAL file size. The database page count describes the main database and can differ from the number of WAL frames.
- **Replication position or partial uploads.** Use [Trace Replication](trace-replication.md). For code changes, compare the locks in `Replica.Pos` and `Replica.SetPos` and test the actual backend's write and retry behavior.
- **LTX corruption, gaps, or timestamp mismatches.** Use [Analyze LTX](analyze-ltx.md) to distinguish local state from replica history. Inspect local reset with `litestream reset -dry-run -config /path/to/litestream.yml /path/to/database.db`. A reset removes local LTX files, including transactions that might not have uploaded. Inspect backup coverage before an authorized reset. For unattended recovery from eligible local LTX errors, set `auto-recover: true` under `dbs[].replica`.
- **Socket connection failures.** Use [Debug IPC](debug-ipc.md) to check configuration, path, and permissions. Startup removes existing Unix sockets at the configured path and rejects other file types there.
- **Growing backup storage.** Check the effective `retention.enabled` value, the snapshot and compaction schedules, and cleanup errors. Retention defaults to enabled. When disabled, the deployment's storage lifecycle policy is responsible for cleanup.
- **v0.3.x backups are present but restore fails.** Check that the backend implements `ReplicaClientV3`. The restore path can choose v0.3.x when it has the better backup for the requested time or latest restore, and uses the LTX path for explicit TXIDs. See `Replica.restore` and `shouldUseV3Restore` in `replica.go`.
- **Memory growth.** Capture a heap profile and compare retained allocations for the same workload. Use the performance optimizer's profiling references if the source is unclear.

Confirm recovery by restoring to a fresh file and checking its contents. On page-copy changes, test with a database beyond the 1 GiB lock-byte boundary and the page sizes relevant to the bug.
