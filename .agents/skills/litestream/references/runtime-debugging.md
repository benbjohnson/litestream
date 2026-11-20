# Runtime debugging

## Inspect the control socket

Check `socket.enabled`, `socket.path`, and `socket.permissions` in the effective configuration. The socket defaults to disabled, with path `/var/run/litestream.sock` and mode `0600`. For a startup error, check the parent directory and the type of any existing path. `Server.Start` removes an existing socket and rejects an existing non-socket path.

```bash
curl --unix-socket /var/run/litestream.sock http://localhost/info
curl --unix-socket /var/run/litestream.sock http://localhost/list
curl --unix-socket /var/run/litestream.sock --get --data-urlencode 'path=/path/to/database.db' http://localhost/txid
curl --unix-socket /var/run/litestream.sock --get --data-urlencode 'path=/path/to/database.db' http://localhost/debug/sync-status
```

`/list` shows database paths, status, and the last successful replica sync when known. Use the exact database path it reports. `/txid` reports the local TXID, and `/debug/sync-status` reports local sync/checkpoint diagnostics; neither exposes the remote TXID. Use replica LTX listings or the library's `DB.SyncStatus(ctx)` for the remote position. If the socket is inaccessible, check the running process, configured socket path, directory permissions, and the mode and owner of the socket.

`server.go` defines the request and response types for `POST /start`, `/stop`, `/register`, `/unregister`, and `/sync`. Registration of an existing database returns HTTP 200 with status `already_registered`. Start, stop, and unregister responses include a TXID. Run state-changing requests only when the investigation calls for them and the operator has authorized the change.

For a goroutine dump, use `/debug/pprof/goroutine?debug=2`. For CPU and trace captures, use `/debug/pprof/profile` and `/debug/pprof/trace` and supply `seconds` to set the capture length. Use the heap endpoint for memory profiling.

## Trace replication

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

If the control socket is enabled, [Control socket diagnostics](runtime-debugging.md#inspect-the-control-socket) covers sync diagnostics and replica upload health. See [WAL diagnostics](wal-and-checkpoints.md#diagnose-wal-growth) for capture and checkpoint delays.

## Triage common failures

Record the Litestream version, effective configuration, database path or replica, and exact error. Identify the affected stage before changing configuration or local state.

- **WAL growth or checkpoint delay.** Use [WAL diagnostics](wal-and-checkpoints.md#diagnose-wal-growth) to inspect active readers, WAL capture, and checkpoint progress. Check the actual WAL file size. The database page count describes the main database and can differ from the number of WAL frames.
- **Replication position or partial uploads.** Use [Replication tracing](runtime-debugging.md#trace-replication). For code changes, compare the locks in `Replica.Pos` and `Replica.SetPos` and test the actual backend's write and retry behavior.
- **LTX corruption, gaps, or timestamp mismatches.** Use [LTX inspection](ltx-and-compaction.md#inspect-files-and-restore-coverage) to distinguish local state from replica history. Inspect local reset with `litestream reset -dry-run -config /path/to/litestream.yml /path/to/database.db`. A reset removes local LTX files, including transactions that might not have uploaded. Inspect backup coverage before an authorized reset. For unattended recovery from eligible local LTX errors, set `auto-recover: true` under `dbs[].replica`.
- **Socket connection failures.** Use [Control socket diagnostics](runtime-debugging.md#inspect-the-control-socket) to check configuration, path, and permissions. Startup removes existing Unix sockets at the configured path and rejects other file types there.
- **Growing backup storage.** Check the effective `retention.enabled` value, the snapshot and compaction schedules, and cleanup errors. Retention defaults to enabled. When disabled, the deployment's storage lifecycle policy is responsible for cleanup.
- **v0.3.x backups are present but restore fails.** Check that the backend implements `ReplicaClientV3`. The restore path can choose v0.3.x when it has the better backup for the requested time or latest restore, and uses the LTX path for explicit TXIDs. See `Replica.restore` and `shouldUseV3Restore` in `replica.go`.
- **Memory growth.** Capture a heap profile and compare retained allocations for the same workload. Follow the [performance workflow](../../litestream-review/references/performance.md), including approval before profiling.

Confirm recovery by restoring to a fresh file and checking its contents. On page-copy changes, test with a database beyond the 1 GiB lock-byte boundary and the page sizes relevant to the bug.
