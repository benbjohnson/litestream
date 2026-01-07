# Generations and Compaction

Understanding TXID, generations, and compaction levels.

## Transaction IDs (TXID)

Every transaction in Litestream has a unique Transaction ID (TXID):

- Monotonically increasing 64-bit integer
- Starts at 1 for new databases
- Increments with each transaction
- Used for ordering and recovery

### TXID Format

Displayed as 16-character hex strings:
```
0000000000000001  (TXID 1)
0000000000001234  (TXID 4660)
FFFFFFFFFFFFFFFF  (max TXID)
```

## LTX File Naming

LTX files are named by their transaction range:

```
{MinTXID}-{MaxTXID}.ltx
```

Examples:
```
0000000000000001-0000000000000100.ltx  # TXIDs 1-256
0000000000000101-0000000000000200.ltx  # TXIDs 257-512
```

## Generations

A generation represents a continuous sequence of transactions:

- Starts with a base snapshot
- Contains all changes since snapshot
- New generation created when:
  - Fresh database (no prior backup)
  - Snapshot created
  - Replication restarted from scratch

### Generation Structure

```
generations/
├── 0000000000000001/     # First generation
│   ├── 0/                # Level 0 (raw)
│   ├── 1/                # Level 1 (compacted)
│   └── 2/                # Level 2 (more compacted)
└── 0000000000000002/     # Second generation (after snapshot)
    ├── 0/
    ├── 1/
    └── 2/
```

## Compaction Levels

### Level Overview

| Level | Contents | Typical Interval |
|-------|----------|-----------------|
| L0 | Raw LTX files from sync | - |
| L1 | L0 files compacted | 30s |
| L2 | L1 files compacted | 5m |
| L3 | L2 files compacted | 1h |

### Why Compact?

1. **Reduce file count**: Many small files → fewer large files
2. **Save storage**: Eliminate duplicate pages
3. **Faster restore**: Fewer files to download

### Compaction Process

```
L0: [1-10] [11-20] [21-30] [31-40] [41-50]
         ↓ Compact
L1: [1-50]
         ↓ Compact (with more L1 files)
L2: [1-500]
```

During compaction:
1. Read files from lower level
2. Merge pages (keep latest version of each page)
3. Write single file at higher level
4. Delete old files (after retention)

### Page Deduplication

If a page is modified multiple times:

```
L0 File 1: Page 5 (version A)
L0 File 2: Page 5 (version B)
L0 File 3: Page 5 (version C)

After compaction:
L1 File: Page 5 (version C only)
```

## Configuration

### Compaction Levels

```yaml
levels:
  - interval: 30s    # L1: Compact every 30 seconds
  - interval: 5m     # L2: Compact every 5 minutes
  - interval: 1h     # L3: Compact hourly
```

### L0 Retention

```yaml
l0-retention: 1h                    # Keep L0 files for 1 hour
l0-retention-check-interval: 5m     # Check retention every 5 minutes
```

### Snapshots

```yaml
snapshot:
  interval: 24h      # Create snapshot daily
  retention: 24h     # Keep snapshots for 24 hours
```

## Recovery and Generations

### Latest Recovery

Uses most recent generation:
1. Find latest snapshot (or start of generation)
2. Apply all LTX files in order
3. Result: Latest state

### Point-in-Time Recovery

Uses specific TXID or timestamp:
1. Find generation containing target
2. Find snapshot before target
3. Apply LTX files up to target
4. Result: State at that point

### Example

```bash
# Find available TXIDs
litestream ltx /data/app.db

# Output:
min_txid              max_txid              size     created
0000000000000001      0000000000000100      4096     2024-01-15T10:00:00Z
0000000000000101      0000000000000200      8192     2024-01-15T10:30:00Z

# Restore to TXID 150
litestream restore -txid 150 -o /tmp/db s3://bucket/backup
```

## Timestamp Preservation

During compaction, the original timestamp is preserved:

- **CreatedAt**: When the original data was written
- Used for point-in-time recovery
- Preserved even when files are merged

## Storage Layout

Full storage structure:

```
s3://bucket/backup/
├── generations/
│   ├── 0000000000000001/
│   │   ├── 0/
│   │   │   ├── 00000001-00000100.ltx
│   │   │   └── 00000101-00000200.ltx
│   │   ├── 1/
│   │   │   └── 00000001-00001000.ltx
│   │   └── 2/
│   │       └── 00000001-00010000.ltx
│   └── 0000000000000002/
│       └── ...
└── snapshots/
    └── ...
```

## Best Practices

### High-Frequency Writes

```yaml
levels:
  - interval: 15s    # More frequent L1 compaction
  - interval: 2m
  - interval: 30m
```

### Long-Term Retention

```yaml
levels:
  - interval: 1m
  - interval: 15m
  - interval: 6h     # Higher level for long retention
  - interval: 24h

snapshot:
  interval: 168h     # Weekly snapshots
  retention: 720h    # Keep for 30 days
```

### Minimize Storage Costs

```yaml
levels:
  - interval: 30s
  - interval: 5m
  - interval: 1h

l0-retention: 30m    # Delete L0 quickly after compaction
```

## See Also

- [LTX Format](ltx-format.md)
- [Replication](replication.md)
- [Recovery](../operations/recovery.md)
