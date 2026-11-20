# Litestream VFS

The Litestream VFS (Virtual File System) is a SQLite extension that reads from Litestream replica storage on demand. It can also hydrate a local SQLite file in the background for subsequent reads or run in experimental write mode.

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
- **Page caching**: LRU cache for frequently accessed pages (default 10 MiB)
- **Hydration**: Build a local read copy in the background while serving reads from the replica
- **Time travel**: Query historical database states at specific timestamps
- **Write support**: Write changes that sync back to remote storage (experimental)

### How It Works

1. The VFS loads as a SQLite extension
2. When opening a database, it reads LTX files from the configured replica URL
3. Page requests are satisfied from cached data or fetched from remote storage
4. A background goroutine polls for new LTX files at a configurable interval

## Building

### Prerequisites

- SQLite 3.31.0+ runtime
- Go 1.26 or later. See [`go.mod`](../go.mod) for the pinned toolchain.
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

Environment variables supply defaults. Set the replica URL in the environment or the database URI.

| Variable | Description | Default |
|----------|-------------|---------|
| `LITESTREAM_REPLICA_URL` | Default replica storage URL | - |
| `LITESTREAM_LOG_LEVEL` | Log level (`DEBUG` or `INFO`) | `INFO` |
| `LITESTREAM_LOG_FILE` | Append logs to this file | Standard output |
| `LITESTREAM_WRITE_ENABLED` | Enable write mode: `true` or `false` | `false` |
| `LITESTREAM_SYNC_INTERVAL` | Write sync interval (e.g., `1s`, `500ms`) | `1s` |
| `LITESTREAM_BUFFER_PATH` | Local write buffer file path | Temporary file |
| `LITESTREAM_HYDRATION_ENABLED` | Enable background hydration | `false` |
| `LITESTREAM_HYDRATION_PATH` | Persistent local hydration file path | Temporary file |

The extension accepts per-database URI options `replica_url`, `write_enabled`, `sync_interval`, `buffer_path`, `hydration_enabled`, `hydration_path`, `poll_interval`, and `cache_size`. Percent-encode the replica URL when embedding it in the database URI. Polling defaults to one second and `cache_size` is measured in bytes. See [`vfs_config.go`](../vfs_config.go) and [`cmd/litestream-vfs/main.go`](../cmd/litestream-vfs/main.go).

### Hydration

When enabled, hydration builds a local SQLite file in the background. Reads continue through the remote page index and cache until the local copy catches up. After cutover, reads use the local file and polling applies new replica transactions. A configured hydration path supports reuse after reopening. `PRAGMA litestream_hydration_progress` and `PRAGMA litestream_hydration_file` report progress and path. See [`vfs.go`](../vfs.go).

### Replica URL Format

```
# Amazon S3
s3://bucket-name/path/to/db

# Google Cloud Storage
gs://bucket-name/path/to/db

# Azure Blob Storage
abs://account-name@container-name/path/to/db

# Alibaba OSS
oss://bucket-name/path/to/db

# Local filesystem
file:///path/to/replica

# SFTP
sftp://user@host:port/path/to/db

# NATS JetStream
nats://host:port/bucket

# WebDAV over HTTPS
webdavs://host:port/path/to/db
```

## Usage

### Loading the Extension

```sql
-- Load the extension (adjust path as needed)
.load ./dist/litestream-vfs.so
```

### Opening a Database

To use the environment default, set the replica URL before loading the extension:

```bash
export LITESTREAM_REPLICA_URL="s3://my-bucket/mydb"
```

Then in SQLite:

```sql
.load ./dist/litestream-vfs.so
.open file:mydb.db?vfs=litestream
SELECT * FROM my_table;
```

To override the replica URL for a connection, use a SQLite URI such as `file:mydb.db?vfs=litestream&replica_url=s3%3A%2F%2Fmy-bucket%2Fmydb`.

### Python Example

```python
import os
import sqlite3

os.environ["LITESTREAM_REPLICA_URL"] = "s3://my-bucket/mydb"

bootstrap = sqlite3.connect(":memory:")
try:
    bootstrap.enable_load_extension(True)
    bootstrap.load_extension("./dist/litestream-vfs.so")
finally:
    bootstrap.close()

conn = sqlite3.connect("file:mydb.db?vfs=litestream", uri=True)
try:
    for row in conn.execute("SELECT * FROM users"):
        print(row)
finally:
    conn.close()
```

### Go Library Usage

The Go library provides `litestream.NewVFS` for registering with `sqlite3vfs.RegisterVFS`. The extension tests show registration, opening a connection with `vfs=litestream`, and querying a replica. See [`cmd/litestream-vfs/time_travel_test.go`](../cmd/litestream-vfs/time_travel_test.go).

## SQL Functions

The VFS extension provides SQL functions for observability and time travel:

### `litestream_txid()`

Returns the current transaction ID as a hex string.

```sql
SELECT litestream_txid();
-- Returns: "0000000000000042"
```

### `litestream_time()`

Returns the requested historical timestamp or the latest observed LTX timestamp in RFC3339 format. Before a timestamp is known, it returns `"latest"`.

```sql
SELECT litestream_time();
-- Returns: "2024-01-15T10:30:00.123456789Z" or "latest"
```

### `litestream_lag()`

Returns seconds since the last successful poll for new LTX files. Returns `-1` if no successful poll has occurred.

```sql
SELECT litestream_lag();
-- Returns: 2 (seconds since the last successful poll)
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
- Available restore points depend on retained LTX files and their compaction levels
- The connection pauses follower updates while viewing a historical state
- Leave write mode disabled on connections used for historical queries

## Write Mode

Write mode allows the VFS to accept writes and sync them back to remote storage. This is experimental.

### Enabling Write Mode

```bash
export LITESTREAM_REPLICA_URL="s3://my-bucket/mydb"
export LITESTREAM_WRITE_ENABLED="true"
export LITESTREAM_SYNC_INTERVAL="1s"
```

### How Write Mode Works

1. The VFS stages changed pages in a local buffer file and tracks their offsets in memory.
2. On the sync interval or at close, the VFS checks the remote L0 TXID for conflicts.
3. If the check passes, the VFS packages dirty pages into an LTX file and uploads it.

### Write Mode Considerations

- **Connection pooling**: Multiple connections can be opened in write mode (for example, by `database/sql`)
- **Single writer**: Connections sharing a VFS instance acquire write intent one at a time. Contending connections receive `SQLITE_BUSY`.
- **Conflict detection**: Sync returns `ErrConflict` if the remote L0 listing shows a newer TXID than expected. Configure one writer for a replica destination.
- **Write buffer**: The buffer is cleared on open. A restart loses changes that were still waiting for upload.
- **Sync interval**: A shorter interval reduces how long changes wait for remote upload.
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
| Google Cloud Storage | `gs://` | Uses Application Default Credentials |
| Azure Blob Storage | `abs://` | Pass the account in the URL or specify an endpoint. Uses SAS, account key, or default credentials. |
| Alibaba OSS | `oss://` | Object Storage Service |
| Local filesystem | `file://` | Useful for testing and development |
| SFTP | `sftp://` | SSH File Transfer Protocol |
| NATS JetStream | `nats://` | Object store via NATS |
| WebDAV | `webdav://`, `webdavs://` | HTTP or HTTPS |

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

- Increase `cache_size` in the database URI to keep more pages in memory.
- Use hydration for local reads after the replica copy catches up.
- Reduce `poll_interval` in the database URI to check for updates more frequently.

### Debug logging

```bash
export LITESTREAM_LOG_LEVEL="DEBUG"
```

This enables verbose logging of VFS operations, page fetches, and cache hits/misses.
