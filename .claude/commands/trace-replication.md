Trace the complete replication flow in Litestream. This command helps understand how changes flow from SQLite through to storage backends.

Follow the replication path step by step:

1. **Application writes to SQLite**:
```sql
-- Application performs write
INSERT INTO table VALUES (...);
-- SQLite appends to WAL file
```

2. **DB.monitor() syncs the shadow WAL** (db.go:1499):
```go
ticker := time.NewTicker(db.MonitorInterval) // default 1s
for {
    select {
    case <-db.ctx.Done():
        return
    case <-ticker.C:
    }

    if err := db.Sync(db.ctx); err != nil && !errors.Is(err, context.Canceled) {
        db.Logger.Error("sync error", "error", err)
    }
}
```

3. **Replica.monitor() responds** (replica.go):
```go
ticker := time.NewTicker(r.SyncInterval)
defer ticker.Stop()

notify := r.db.Notify()
for {
    select {
    case <-ctx.Done():
        return
    case <-ticker.C:
        // Enforce minimum sync interval
    case <-notify:
        // WAL changed, time to sync
    }

    notify = r.db.Notify()

    if err := r.Sync(ctx); err != nil && !errors.Is(err, context.Canceled) {
        r.Logger().Error("monitor error", "error", err)
    }
}
```

4. **Replica.Sync() uploads new L0 files** (replica.go):
```go
// Determine local database position
dpos, err := r.db.Pos()
if err != nil {
    return err
}
if dpos.IsZero() {
    return fmt.Errorf("no position, waiting for data")
}

// Upload each unreplicated L0 file
for txID := r.Pos().TXID + 1; txID <= dpos.TXID; txID = r.Pos().TXID + 1 {
    if err := r.uploadLTXFile(ctx, 0, txID, txID); err != nil {
        return err
    }
    r.SetPos(ltx.Pos{TXID: txID})
}
```

5. **ReplicaClient uploads to storage**:
```go
func (c *S3Client) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
    // Stream LTX data to storage and return metadata (size, CreatedAt, checksums)
}
```

6. **Checkpoint when thresholds are hit**:
```go
if walPageCount > db.MinCheckpointPageN {
    db.Checkpoint(ctx, litestream.CheckpointModePassive)
}
if walPageCount > db.TruncatePageN {
    db.Checkpoint(ctx, litestream.CheckpointModeTruncate)
}
```

Key synchronization points:
- WAL monitoring (1s intervals)
- Replica sync (configurable, default 1s)
- Checkpoint triggers (page thresholds)
- Compaction (hourly/daily)

Trace with logging:
```go
// Enable debug logging
slog.SetLogLevel(slog.LevelDebug)

// Key log points:
slog.Debug("wal changed", "size", walSize)
slog.Debug("syncing replica", "pos", r.Pos())
slog.Debug("ltx uploaded", "txid", maxTXID)
slog.Debug("checkpoint complete", "mode", mode)
```

Performance metrics to monitor:
- WAL growth rate
- Sync latency
- Upload throughput
- Checkpoint frequency
- Compaction duration

Common bottlenecks:
1. Slow storage uploads
2. Large transactions causing big LTX files
3. Long-running read transactions blocking checkpoints
4. Eventual consistency delays
5. Network latency to storage

Test replication flow:
```bash
# Start replication with verbose logging
litestream replicate -v

# In another terminal, write to database
sqlite3 test.db "INSERT INTO test VALUES (1, 'data');"

# Watch logs for flow:
# - WAL change detected
# - Replica sync triggered
# - LTX file uploaded
# - Position updated
```

Verify replication:
```bash
# List replicated files
aws s3 ls s3://bucket/path/ltx/0000/

# Restore and verify
litestream restore -o restored.db s3://bucket/path
sqlite3 restored.db "SELECT * FROM test;"
```
