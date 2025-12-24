# Litestream Library Usage Examples

These examples demonstrate how to use Litestream as a Go library instead of as a
standalone CLI tool.

## API Stability Warning

The Litestream library API is not considered stable and may change between
versions. The CLI interface is more stable for production use. Use the library
API at your own risk, and pin to specific versions.

Note (macOS): macOS uses per-process locks for SQLite, not per-handle locks.
If you open the same database with two different SQLite driver implementations
in the same process and close one of them, you can hit locking issues. Prefer
using the same driver for your app and Litestream (these examples use
`modernc.org/sqlite`).

## Examples

### Basic (File Backend)

The simplest example using local filesystem replication.

```bash
cd basic
go run main.go
```

This creates:

- `myapp.db` - The SQLite database
- `replica/` - Directory containing replicated LTX files

### S3 Backend

A more complete example showing the restore-on-startup pattern with S3.

```bash
cd s3

# Set required environment variables
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export LITESTREAM_BUCKET="your-bucket-name"
export LITESTREAM_PATH="databases/myapp"  # optional, defaults to "litestream"
export AWS_REGION="us-east-1"             # optional, defaults to "us-east-1"

go run main.go
```

This example:

1. Checks if the local database exists
2. If not, attempts to restore from S3
3. Starts background replication to S3
4. Inserts sample data every 2 seconds
5. Gracefully shuts down on Ctrl+C

## Core API Pattern

```go
import (
    "context"
    "database/sql"
    "github.com/benbjohnson/litestream"
    "github.com/benbjohnson/litestream/file"  // or s3, gs, abs, etc.
    _ "modernc.org/sqlite"
)

// 1. Create database wrapper
db := litestream.NewDB("/path/to/db.sqlite")

// 2. Create replica client
client := file.NewReplicaClient("/path/to/replica")
// OR from URL:
// client, _ := litestream.NewReplicaClientFromURL("s3://bucket/path")

// 3. Attach replica to database
replica := litestream.NewReplicaWithClient(db, client)
db.Replica = replica
client.Replica = replica // file backend only; preserves ownership/permissions

// 4. Create compaction levels (L0 required, plus at least one more)
levels := litestream.CompactionLevels{
    {Level: 0},
    {Level: 1, Interval: 10 * time.Second},
}

// 5. Create Store to manage DB and background compaction
store := litestream.NewStore([]*litestream.DB{db}, levels)

// 6. Open Store (opens all DBs, starts background monitors)
if err := store.Open(ctx); err != nil { ... }
defer store.Close(context.Background())

// 7. Open your app's SQLite connection for normal database operations
sqlDB, err := sql.Open("sqlite", "/path/to/db.sqlite")
if err != nil { ... }
if _, err := sqlDB.ExecContext(ctx, `PRAGMA journal_mode = wal;`); err != nil { ... }
if _, err := sqlDB.ExecContext(ctx, `PRAGMA busy_timeout = 5000;`); err != nil { ... }
```

## Restore Pattern

```go
// Create replica without database for restore
replica := litestream.NewReplicaWithClient(nil, client)

opt := litestream.NewRestoreOptions()
opt.OutputPath = "/path/to/restored.db"
// Optional: point-in-time restore
// opt.Timestamp = time.Now().Add(-1 * time.Hour)

if err := replica.Restore(ctx, opt); err != nil {
    if errors.Is(err, litestream.ErrTxNotAvailable) || errors.Is(err, litestream.ErrNoSnapshots) {
        // No backup available, create fresh database
    }
    return err
}
```

## Supported Backends

- `file` - Local filesystem
- `s3` - AWS S3 and S3-compatible storage
- `gs` - Google Cloud Storage
- `abs` - Azure Blob Storage
- `oss` - Alibaba Cloud OSS
- `sftp` - SFTP servers
- `nats` - NATS JetStream
- `webdav` - WebDAV servers

## Key Configuration Options

### Store Settings

```go
store.SnapshotInterval  = 24 * time.Hour  // How often to create snapshots
store.SnapshotRetention = 24 * time.Hour  // How long to keep snapshots
store.L0Retention       = 5 * time.Minute // How long to keep L0 files after compaction
```

### DB Settings

```go
db.MonitorInterval    = 1 * time.Second   // How often to check for changes
db.CheckpointInterval = 1 * time.Minute   // Time-based checkpoint interval
db.MinCheckpointPageN = 1000              // Page threshold for checkpoint
db.BusyTimeout        = 1 * time.Second   // SQLite busy timeout
```

### Replica Settings

```go
replica.SyncInterval   = 1 * time.Second  // Time between syncs
replica.MonitorEnabled = true             // Auto-sync in background
```

## Resources

- [Litestream Documentation](https://litestream.io)
- [GitHub Repository](https://github.com/benbjohnson/litestream)
