# Litestream VFS

The Litestream VFS (Virtual File System) is a SQLite extension that allows applications to read directly
from Litestream replica storage (S3, GCS, Azure Blob, etc.) without restoring to local disk. It also
supports write mode for remote-first SQLite databases.

## Table of Contents

- [Overview](#overview)
- [Building](#building)
- [Configuration](#configuration)
- [Usage](#usage)
- [SQL Functions](#sql-functions)
- [Time Travel](#time-travel)
- [Write Mode](#write-mode)
- [Supported Storage Backends](#supported-storage-backends)

## Overview

The VFS extension provides:

- **Direct replica reading**: Read SQLite databases directly from cloud storage
- **Automatic polling**: Background polling for new LTX files from the primary
- **Page caching**: LRU cache for frequently accessed pages (default 10MB)
- **Time travel**: Query historical database states at specific timestamps
- **Write support**: Write changes that sync back to remote storage (experimental)

### How It Works

1. The VFS loads as a SQLite extension
2. When opening a database, it reads LTX files from the configured replica URL
3. Page requests are satisfied from cached data or fetched from remote storage
4. A background goroutine polls for new LTX files at a configurable interval

## Building

### Prerequisites

- Go 1.21+
- GCC (Linux) or Clang (macOS)
- CGO enabled

### Build Commands

**macOS (current architecture):**

```bash
make vfs
```

This creates:
- `dist/litestream-vfs.a` - Static library
- `dist/litestream-vfs.so` - Loadable SQLite extension

**Platform-specific builds:**

```bash
# macOS ARM64 (Apple Silicon)
make vfs-darwin-arm64
# Output: dist/litestream-vfs-darwin-arm64.dylib

# macOS AMD64 (Intel)
make vfs-darwin-amd64
# Output: dist/litestream-vfs-darwin-amd64.dylib

# Linux AMD64
make vfs-linux-amd64
# Output: dist/litestream-vfs-linux-amd64.so

# Linux ARM64
make vfs-linux-arm64
# Output: dist/litestream-vfs-linux-arm64.so
```

### Running Tests

```bash
make vfs-test
```

## Configuration

The VFS is configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `LITESTREAM_REPLICA_URL` | Replica storage URL (required) | - |
| `LITESTREAM_LOG_LEVEL` | Log level: `DEBUG` or `INFO` | `INFO` |
| `LITESTREAM_WRITE_ENABLED` | Enable write mode: `true` or `false` | `false` |
| `LITESTREAM_SYNC_INTERVAL` | Write sync interval (e.g., `1s`, `500ms`) | `1s` |
| `LITESTREAM_BUFFER_PATH` | Local write buffer file path | temp file |

### Replica URL Format

```
# Amazon S3
s3://bucket-name/path/to/db

# Google Cloud Storage
gs://bucket-name/path/to/db

# Azure Blob Storage
abs://container-name/path/to/db

# Alibaba OSS
oss://bucket-name/path/to/db

# Local filesystem
file:///path/to/replica

# SFTP
sftp://user@host:port/path/to/db

# NATS JetStream
nats://host:port/bucket/path

# WebDAV
webdav://host:port/path/to/db
```

## Usage

### Loading the Extension

```sql
-- Load the extension (adjust path as needed)
.load ./dist/litestream-vfs.so
```

### Opening a Database

Before loading the extension, set the replica URL:

```bash
export LITESTREAM_REPLICA_URL="s3://my-bucket/mydb"
```

Then in SQLite:

```sql
.load ./dist/litestream-vfs.so
.open file:mydb.db?vfs=litestream
SELECT * FROM my_table;
```

### Python Example

```python
import os
import sqlite3

os.environ["LITESTREAM_REPLICA_URL"] = "s3://my-bucket/mydb"

conn = sqlite3.connect(":memory:")
conn.enable_load_extension(True)
conn.load_extension("./dist/litestream-vfs.so")

# Open database using the litestream VFS
conn = sqlite3.connect("file:mydb.db?vfs=litestream")
cursor = conn.execute("SELECT * FROM users")
for row in cursor:
    print(row)
```

### Go Example

```go
import (
    "database/sql"
    "os"

    _ "github.com/mattn/go-sqlite3"
)

func main() {
    os.Setenv("LITESTREAM_REPLICA_URL", "s3://my-bucket/mydb")

    db, err := sql.Open("sqlite3", "file:mydb.db?vfs=litestream")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    rows, err := db.Query("SELECT * FROM users")
    // ...
}
```

## SQL Functions

The VFS extension provides SQL functions for observability and time travel:

### `litestream_txid()`

Returns the current transaction ID as a hex string.

```sql
SELECT litestream_txid();
-- Returns: "0000000000000042"
```

### `litestream_time()`

Returns the current view timestamp (RFC3339 format) or `"latest"`.

```sql
SELECT litestream_time();
-- Returns: "2024-01-15T10:30:00.123456789Z" or "latest"
```

### `litestream_lag()`

Returns seconds since the last successful poll for new LTX files. Returns `-1` if no successful poll has occurred.

```sql
SELECT litestream_lag();
-- Returns: 2 (seconds behind primary)
```

### `litestream_set_time(timestamp)`

Sets the view time for time travel queries. See [Time Travel](#time-travel) for details.

## Time Travel

The VFS supports querying historical database states by setting a target timestamp.

### Setting a Target Time

```sql
-- View database as of a specific timestamp (RFC3339 format)
SELECT litestream_set_time('2024-01-15T10:30:00Z');

-- Relative time expressions are also supported
SELECT litestream_set_time('5 minutes ago');
SELECT litestream_set_time('yesterday');
SELECT litestream_set_time('2 hours ago');

-- Return to latest state
SELECT litestream_set_time('LATEST');
```

### Example: Comparing Historical Data

```sql
-- Check current count
SELECT COUNT(*) FROM orders;
-- Returns: 1000

-- Go back in time
SELECT litestream_set_time('1 hour ago');

-- Check historical count
SELECT COUNT(*) FROM orders;
-- Returns: 950

-- Return to present
SELECT litestream_set_time('LATEST');
```

### Time Travel Limitations

- Time travel rebuilds the page index, which may take time for large databases
- Historical data is only available if LTX files haven't been compacted away
- L0 retention settings affect how far back you can travel
- Time travel is read-only (writes are disabled while viewing historical state)

## Write Mode

Write mode allows the VFS to accept writes and sync them back to remote storage. This is experimental.

### Enabling Write Mode

```bash
export LITESTREAM_REPLICA_URL="s3://my-bucket/mydb"
export LITESTREAM_WRITE_ENABLED="true"
export LITESTREAM_SYNC_INTERVAL="1s"
```

### How Write Mode Works

1. Writes are captured to a local buffer file for durability
2. Dirty pages are tracked in memory
3. Periodically (or on close), dirty pages are packaged into an LTX file
4. The LTX file is uploaded to remote storage
5. Conflict detection prevents overwrites if the remote has newer transactions

### Write Mode Considerations

- **Connection pooling**: Multiple connections can be opened in write mode (for example, by `database/sql`)
- **Single writer**: Write contention is enforced at lock acquisition. If another connection already holds write intent, SQLite returns `SQLITE_BUSY`
- **Conflict detection**: If the remote has advanced unexpectedly, `ErrConflict` is returned
- **Buffer durability**: The local buffer file provides crash recovery for uncommitted writes
- **Sync interval**: Balance between durability (shorter) and performance (longer)
- **New databases**: Write mode can create new databases from scratch if no LTX files exist

### Creating a New Database

With write mode enabled, you can create a new database that doesn't exist yet:

```sql
.load ./dist/litestream-vfs.so
.open file:newdb.db?vfs=litestream

CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users (name) VALUES ('Alice');
-- Data is synced to remote storage automatically
```

## Supported Storage Backends

The VFS supports all Litestream storage backends:

| Backend | URL Scheme | Notes |
|---------|-----------|-------|
| Amazon S3 | `s3://` | Supports S3-compatible services (MinIO, DigitalOcean Spaces, etc.) |
| Google Cloud Storage | `gs://` | Requires `GOOGLE_APPLICATION_CREDENTIALS` |
| Azure Blob Storage | `abs://` | Requires `AZURE_STORAGE_ACCOUNT` and credentials |
| Alibaba OSS | `oss://` | Object Storage Service |
| Local filesystem | `file://` | Useful for testing and development |
| SFTP | `sftp://` | SSH File Transfer Protocol |
| NATS JetStream | `nats://` | Object store via NATS |
| WebDAV | `webdav://` | Web Distributed Authoring and Versioning |

### S3 Configuration

For S3 and S3-compatible services, set credentials via environment variables:

```bash
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_REGION="us-east-1"

# For S3-compatible services:
export LITESTREAM_REPLICA_URL="s3://bucket/path?endpoint=https://custom.endpoint.com"
```

### GCS Configuration

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export LITESTREAM_REPLICA_URL="gs://bucket/path"
```

## Troubleshooting

### Extension fails to load

Ensure the extension file matches your platform:
- macOS: `.dylib` or `.so`
- Linux: `.so`

### "no backup files available"

The VFS waits for LTX files to become available. Ensure:
1. The replica URL is correct
2. Litestream has replicated at least one transaction
3. Credentials are properly configured

### High latency reads

- Increase `CacheSize` for larger page cache
- Reduce `PollInterval` for more responsive updates
- Consider using a closer storage region

### Debug logging

```bash
export LITESTREAM_LOG_LEVEL="DEBUG"
```

This enables verbose logging of VFS operations, page fetches, and cache hits/misses.
