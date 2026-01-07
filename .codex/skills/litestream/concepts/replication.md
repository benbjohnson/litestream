# Replication Mechanics

How Litestream replicates SQLite databases.

## Replication Flow

```
1. Application writes to SQLite
          ↓
2. SQLite writes to WAL file
          ↓
3. Litestream monitors WAL (MonitorInterval)
          ↓
4. Changes converted to LTX format
          ↓
5. LTX files uploaded (SyncInterval)
          ↓
6. Storage backend stores files
          ↓
7. Periodic checkpoint & compaction
```

## WAL Monitoring

Litestream monitors the WAL file for changes:

```yaml
monitor-interval: 10ms  # How often to check WAL (default)
```

The monitor:
1. Checks WAL file size and modification time
2. Compares with previous state
3. Reads new frames when changes detected
4. Notifies replicas of pending changes

## Sync Process

Syncing uploads changes to the storage backend:

```yaml
sync-interval: 1s  # How often to sync (default)
```

The sync process:
1. Collects pending WAL frames
2. Creates LTX file with page data
3. Uploads to storage backend
4. Updates replication position

## Position Tracking

Litestream tracks replication position using:

- **TXID**: Transaction ID (monotonically increasing)
- **PageNo**: Current page number within transaction
- **Checksum**: Running checksum for validation

Position is stored locally and used for:
- Resuming replication after restart
- Detecting gaps in replication
- Point-in-time recovery

## LTX File Creation

WAL frames are converted to LTX format:

1. Group frames by transaction
2. Skip lock page (at 1GB mark)
3. Write header with TXID range
4. Write page frames with data
5. Generate page index
6. Write trailer with checksums

## Checkpoint Coordination

Litestream coordinates with SQLite checkpoints:

### When Checkpoints Occur

1. **Page threshold**: `min-checkpoint-page-count` reached
2. **Time interval**: `checkpoint-interval` elapsed
3. **Emergency**: `truncate-page-n` exceeded

### Checkpoint Process

1. Release read transaction temporarily
2. Execute `PRAGMA wal_checkpoint(PASSIVE)`
3. Restart read transaction
4. Continue monitoring

### Why PASSIVE Mode

Litestream uses PASSIVE checkpoints:
- Non-blocking (doesn't wait for readers)
- Doesn't interrupt application writes
- May not fully checkpoint if readers present

## Compaction

Compaction merges LTX files to reduce storage:

### Compaction Levels

| Level | Typical Interval | Purpose |
|-------|-----------------|---------|
| L0 | - | Raw files from sync |
| L1 | 30s | Reduce file count |
| L2 | 5m | Further consolidation |
| L3 | 1h | Long-term storage |

### Compaction Process

1. List files at level N-1
2. Read local files (preferred) or remote
3. Merge pages, keeping latest version
4. Write compacted file at level N
5. Delete old files (after retention)

### Immutability

LTX files are **never modified** after creation:
- New data creates new files
- Compaction creates new merged files
- Old files deleted only after retention

## Eventual Consistency

Storage backends may have eventual consistency:

### During Compaction

Litestream handles this by:
1. Preferring local files when available
2. Falling back to remote only when necessary
3. Using retries with backoff

### File Listing

Some backends don't immediately show new files:
- S3: Usually consistent, some edge cases
- GCS: Usually consistent
- SFTP: Immediate consistency

## Recovery Position

When restarting, Litestream:

1. Reads local metadata for last position
2. Validates against storage backend
3. Resumes from validated position
4. Creates new LTX files from WAL

## Shadow WAL

Litestream maintains a "shadow" of WAL state:

- Prevents SQLite from checkpointing unsynced data
- Ensures all changes are captured
- Uses long-running read transaction

## Sync Guarantees

Litestream provides:

- **Durability**: Data synced to storage survives local failure
- **Ordering**: Transactions applied in order
- **Completeness**: All committed transactions replicated

It does **not** provide:
- Real-time sync (subject to sync-interval)
- Multi-master replication
- Conflict resolution

## Configuration Example

```yaml
dbs:
  - path: /data/app.db
    monitor-interval: 10ms       # WAL check frequency
    checkpoint-interval: 1m      # Time-based checkpoint
    min-checkpoint-page-count: 1000  # Page-based checkpoint
    replica:
      url: s3://bucket/backup
      sync-interval: 1s          # Upload frequency
```

## See Also

- [Architecture](architecture.md)
- [SQLite WAL](sqlite-wal.md)
- [LTX Format](ltx-format.md)
