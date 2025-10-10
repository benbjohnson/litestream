Trace the complete replication flow in Litestream. This command helps understand how changes flow from SQLite through to storage backends.

Follow the replication path step by step:

1. **Application writes to SQLite**:
```sql
-- Application performs write
INSERT INTO table VALUES (...);
-- SQLite appends to WAL file
```

2. **DB.monitor() detects changes** (db.go:1499):
```go
ticker := time.NewTicker(db.MonitorInterval) // Every 1s
changed, err := db.checkWAL()
if changed {
    db.notifyReplicas() // Signal replicas
}
```

3. **Replica.monitor() responds** (replica.go):
```go
select {
case <-db.notify:
    // WAL changed, time to sync
case <-ticker.C:
    // Regular sync interval
}
```

4. **Replica.Sync() processes changes**:
```go
// Read WAL pages since last position
reader := db.WALReader(r.pos.PageNo)

// Convert to LTX format
ltxData := convertWALToLTX(reader)

// Write to storage backend
info, err := r.Client.WriteLTXFile(ctx, level, minTXID, maxTXID, ltxData)

// Update position
r.SetPos(newPos)
```

5. **ReplicaClient uploads to storage**:
```go
// Backend-specific upload
func (c *S3Client) WriteLTXFile(...) (*ltx.FileInfo, error) {
    // Upload to S3
    // Return file metadata
}
```

6. **Checkpoint when threshold reached**:
```go
if walPageCount > db.MinCheckpointPageN {
    db.Checkpoint("PASSIVE") // Try checkpoint
}
if walPageCount > db.MaxCheckpointPageN {
    db.Checkpoint("RESTART") // Force checkpoint
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
slog.Debug("syncing replica", "pos", r.pos)
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
