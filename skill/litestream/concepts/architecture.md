# Litestream Architecture

## System Overview

Litestream follows a layered architecture with clear separation of concerns:

```
Application Layer
├── CLI Commands (replicate, restore, status, ltx)
└── Configuration (YAML parsing, validation)
        ↓
Core Layer
├── Store (multi-database coordination, compaction)
├── DB (single database management, WAL monitoring)
└── Replica (replication to single destination)
        ↓
Storage Abstraction
└── ReplicaClient Interface
        ↓
Storage Implementations
├── S3, GCS, Azure, OSS (cloud object storage)
├── SFTP, WebDAV (file transfer protocols)
├── NATS (message streaming)
└── File (local filesystem)
```

## Core Components

### Store

The Store coordinates multiple databases and system-wide resources:

- Manages list of monitored databases
- Schedules compaction across all databases
- Handles snapshot creation and retention
- Configures compaction levels (L0, L1, L2...)

### DB (Database Manager)

The DB component manages a single SQLite database:

- Opens and maintains SQLite connection
- Monitors WAL for changes at `MonitorInterval`
- Performs checkpoints when thresholds are reached
- Converts WAL changes to LTX format
- Coordinates with replicas for sync

Key configuration:
- `MonitorInterval`: How often to check for WAL changes (default: 10ms)
- `CheckpointInterval`: Time-based checkpoint trigger
- `MinCheckpointPageN`: Minimum pages before passive checkpoint
- `TruncatePageN`: Emergency checkpoint threshold

### Replica

The Replica manages replication to a single destination:

- Tracks replication position (TXID, PageNo, Checksum)
- Uploads LTX files to storage backend
- Handles sync intervals and retries

Key configuration:
- `SyncInterval`: How often to sync changes (default: 1s)
- `ValidationInterval`: How often to validate replica integrity

## Data Flow

### Replication Flow

1. **Application writes** to SQLite database
2. SQLite writes changes to **WAL file**
3. Litestream's **monitor goroutine** detects WAL changes
4. Changes are converted to **LTX format**
5. LTX files are uploaded via **ReplicaClient**
6. Storage backend stores **immutable LTX files**
7. Periodic **checkpoint** moves WAL changes to main database
8. **Compaction** merges LTX files to reduce storage

### Restoration Flow

1. User requests restore with `litestream restore`
2. Litestream queries storage for **available LTX files**
3. Files are downloaded and **applied in order**
4. Pages are written to **restored database**
5. Lock page at 1GB is **always skipped**
6. Final database is ready for use

## LTX File Format

LTX (Log Transaction) files are immutable containers for database changes:

```
+------------------+
|     Header       |  Magic, version, page size, TXID range
+------------------+
|   Page Frames    |  Page number + page data (variable count)
+------------------+
|   Page Index     |  Binary search index for page lookup
+------------------+
|     Trailer      |  Checksums and metadata
+------------------+
```

### File Naming

LTX files are named by their transaction range: `{MinTXID}-{MaxTXID}.ltx`

Example: `0000000000000001-0000000000000100.ltx`

### Compaction Levels

- **L0**: Raw LTX files from WAL changes
- **L1**: Hourly compaction (merges L0 files)
- **L2**: Daily compaction (merges L1 files)
- Higher levels for longer retention

## Layer Responsibilities

| Layer | Responsibility | Files |
|-------|---------------|-------|
| Store | Multi-DB coordination, compaction scheduling | `store.go` |
| DB | WAL monitoring, checkpointing, LTX creation | `db.go` |
| Replica | Sync mechanics, position tracking | `replica.go` |
| ReplicaClient | Storage backend implementation | `*/replica_client.go` |

**Important**: Database state logic (restoration, position) belongs in the DB layer, not the Replica layer.

## Concurrency Model

Litestream uses Go's concurrency primitives:

- **sync.RWMutex** for protecting shared state
- **Goroutines** for background monitoring and sync
- **Channels** for shutdown coordination
- **Context** for cancellation propagation

Lock ordering to prevent deadlocks:
1. Store.mu
2. DB.mu
3. DB.chkMu (checkpoint)
4. Replica.mu

## Metrics and Monitoring

Prometheus metrics exposed on configured address:

- `litestream_db_size_bytes` - Database file size
- `litestream_wal_size_bytes` - WAL file size
- `litestream_replica_lag_seconds` - Replication lag
- `litestream_sync_count_total` - Total sync operations
- `litestream_checkpoint_count_total` - Total checkpoints

## Error Handling

Litestream categorizes errors:

1. **Recoverable**: Network timeouts, temporary unavailability (retried with backoff)
2. **Fatal**: Database corruption, invalid configuration (stops operation)
3. **Operational**: Checkpoint failures, sync delays (logged, continues)

Exponential backoff: 1s initial, 1m max, doubles on each retry.
