# Litestream Code Patterns and Anti-Patterns

This document contains detailed code patterns, examples, and anti-patterns for working with Litestream. For a quick overview, see [AGENTS.md](../AGENTS.md).

## Table of Contents

- [Architectural Boundaries](#architectural-boundaries)
- [Atomic File Operations](#atomic-file-operations)
- [Error Handling](#error-handling)
- [Locking Patterns](#locking-patterns)
- [Compaction Inputs](#compaction-inputs)
- [Resumable Reader Pattern](#resumable-reader-pattern)
- [Retention Bypass Pattern](#retention-bypass-pattern)
- [Conditional Write Pattern (Distributed Locking)](#conditional-write-pattern-distributed-locking)
- [Timestamp Preservation](#timestamp-preservation)
- [Common Pitfalls](#common-pitfalls)
- [Component Reference](#component-reference)

## Architectural Boundaries

### Layer Responsibilities

```text
DB Layer (db.go)          → Database state, restoration, monitoring
Replica Layer (replica.go) → Replication mechanics only
Storage Layer             → ReplicaClient implementations
```

### Refresh local replication state in the DB layer

`DB.checkDatabaseBehindReplica` compares the latest local and remote L0 positions before replica monitoring starts. When the remote position is ahead, the DB clears its local L0 cache, invalidates the cached position, and downloads the newest remote L0 file using an atomic write. `Replica.Restore` handles reconstruction of a SQLite database from replica storage. See [`db.go`](../db.go) and [`replica.go`](../replica.go).

## Atomic File Operations

Publish completed files with an atomic rename. Use a temporary file in the destination directory and handle write, sync, and close errors before publishing. Atomic visibility does not by itself guarantee crash durability of the directory entry; that also requires syncing the containing directory where supported. See [`file/replica_client.go`](../file/replica_client.go).

### DO: Write to temp file, then rename

```go
// CORRECT - Atomic file write pattern
func writeFileAtomic(path string, data []byte) error {
    // Create temp file in same directory (for atomic rename)
    dir := filepath.Dir(path)
    tmpFile, err := os.CreateTemp(dir, ".tmp-*")
    if err != nil {
        return fmt.Errorf("create temp file: %w", err)
    }
    tmpPath := tmpFile.Name()

    // Clean up temp file on error
    defer func() {
        if tmpFile != nil {
            tmpFile.Close()
            os.Remove(tmpPath)
        }
    }()

    // Write data to temp file
    if _, err := tmpFile.Write(data); err != nil {
        return fmt.Errorf("write temp file: %w", err)
    }

    // Sync to ensure data is on disk
    if err := tmpFile.Sync(); err != nil {
        return fmt.Errorf("sync temp file: %w", err)
    }

    // Close before rename
    if err := tmpFile.Close(); err != nil {
        return fmt.Errorf("close temp file: %w", err)
    }
    tmpFile = nil // Prevent defer cleanup

    // Atomic rename (on same filesystem)
    if err := os.Rename(tmpPath, path); err != nil {
        os.Remove(tmpPath)
        return fmt.Errorf("rename to final path: %w", err)
    }

    return nil
}
```

### DON'T: Write directly to final location

```go
// WRONG - Can leave partial files on failure
func writeFileDirect(path string, data []byte) error {
    return os.WriteFile(path, data, 0644)  // Not atomic!
}
```

## Error Handling

### Decision Rule

When you handle an error, ask: "Does the caller need to know about this failure?"

- **Yes → return the error.** This is the default for virtually all cases in Litestream.
  - The result is needed for correctness
  - Failure could corrupt data or state
  - You're in a loop processing items

- **No → DEBUG log only.** This is rare. Only when ALL of these are true:
  - A valid fallback path exists that doesn't depend on the result
  - Failure cannot affect correctness
  - The operation is purely supplementary (e.g., reading an optimization hint)

When in doubt, return the error. In a disaster recovery tool, silent failures are worse than noisy ones.

### Return errors to the caller

`DB.SyncAndWait` returns local sync or replica sync failures with stage context. Background monitors report failures where they decide whether and when to retry. See [`db.go`](../db.go) and [`replica.go`](../replica.go).

### DON'T: Ignore errors and continue in loops

```go
// WRONG - Continuing after error can corrupt state
func (db *DB) processFiles() {
    for _, file := range files {
        if err := processFile(file); err != nil {
            log.Printf("error: %v", err)  // Just logging!
            // Continuing to next file is dangerous
        }
    }
}
```

### DO: Return errors properly in loops

```go
// CORRECT - Let caller decide how to handle errors
func (db *DB) processFiles() error {
    for _, file := range files {
        if err := processFile(file); err != nil {
            return fmt.Errorf("process file %s: %w", file, err)
        }
    }
    return nil
}
```

## Locking Patterns

### DO: Use proper lock types

```go
// CORRECT - Use Lock() for writes
r.mu.Lock()
defer r.mu.Unlock()
r.pos = pos
```

### DON'T: Use RLock for write operations

```go
// WRONG - Race condition
r.mu.RLock()  // Should be Lock() for writes
defer r.mu.RUnlock()
r.pos = pos   // Writing with RLock!
```

## Compaction Inputs

`Compactor.Compact` prefers local copies when the DB has them. If a local file is missing, the compactor opens the remote LTX file and wraps it in a resumable reader. Other local open errors propagate. Source selection and merge behavior are in [`compactor.go`](../compactor.go).

## Resumable Reader Pattern

During restore, LTX file streams from S3/Tigris may sit idle while the compactor processes lower-numbered pages from the snapshot. Storage providers close these idle connections, causing "unexpected EOF" errors.

The `ResumableReader` (`internal/resumable_reader.go`) wraps `io.ReadCloser` with automatic reconnection:

### Interface

```go
type LTXFileOpener interface {
    OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
}
```

### Two Failure Modes

1. **Non-EOF errors** (connection reset, timeout) — stream broke mid-transfer
2. **Premature EOF** — server closed cleanly, but `offset < size` (known file size)

### Key Behaviors

- Max 3 retries (`resumableReaderMaxRetries = 3`)
- Tracks current byte `offset` for range-request resume
- Returns partial reads without error so callers like `io.ReadFull` naturally retry
- Reopens from current offset using `OpenLTXFile(ctx, level, min, max, offset, 0)`

### Opening restore streams

`Replica.openRestoreFiles` creates each resumable reader with its expected file size and calls `Open`. If an initial open fails, a subsequent read can retry it. When the reader owns the stream's request context, a stalled read is cancelled after 30 seconds so the reader can reconnect. Retries back off from 250 milliseconds. See [`replica.go`](../replica.go) and [`internal/resumable_reader.go`](../internal/resumable_reader.go).

## Retention Bypass Pattern

When using cloud provider lifecycle policies (S3 lifecycle rules, R2 auto-cleanup), Litestream's active file deletion can be disabled:

```yaml
retention:
  enabled: false
```

### Propagation Chain

1. `RetentionConfig{Enabled *bool}` in YAML config (`cmd/litestream/main.go`)
2. `Store.SetRetentionEnabled(bool)` propagates to all DBs and their compactors (`store.go`)
3. `Compactor.RetentionEnabled` guards three remote deletion points in `compactor.go`. Local cleanup still runs.

### DO: Disable retention when cloud lifecycle handles cleanup

```go
store.SetRetentionEnabled(false) // Delegates deletion to cloud lifecycle policies
```

### DON'T: Disable retention without cloud lifecycle policies

Disabling retention without cloud lifecycle policies causes unbounded storage growth. Litestream logs a warning: "retention disabled; cloud provider lifecycle policies must handle retention".

## Conditional Write Pattern (Distributed Locking)

The S3 leaser (`s3/leaser.go`) uses S3 conditional writes (`If-Match`/`If-None-Match`) for distributed locking without an external coordination service.

### Acquire Pattern

```go
input := &s3.PutObjectInput{
    Bucket: aws.String(l.Bucket),
    Key:    aws.String(key),
    Body:   bytes.NewReader(data),
}
if etag == "" {
    input.IfNoneMatch = aws.String("*") // First acquire: only if key doesn't exist
} else {
    input.IfMatch = aws.String(etag)     // Expired takeover: only if ETag matches
}
```

### Release Pattern

```go
_, err := l.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
    Bucket:  aws.String(l.Bucket),
    Key:     aws.String(key),
    IfMatch: aws.String(lease.ETag), // Only delete if we still hold the lease
})
```

### Error Handling

- Acquisition maps HTTP 412 to `LeaseExistsError`. Renewal and release map HTTP 412 to `ErrLeaseNotHeld`.
- Release returns `s3.ErrLeaseAlreadyReleased` if the lease is missing.

## Timestamp Preservation

Storage clients preserve the timestamp in the incoming LTX header. The file and SFTP clients record it as modification time. The S3 client records it as object metadata and retrieves it for `LTXFiles` calls with `useMetadata=true`. Compacted files get the timestamp of their final input in TXID order from `ltx.Compactor`. See [`compactor.go`](../compactor.go), [`file/replica_client.go`](../file/replica_client.go), and [`s3/replica_client.go`](../s3/replica_client.go).

## Common Pitfalls

### Snapshot creation

`DB.verify` checks WAL and LTX continuity. The sync path uses that result to decide how to capture changes. To write a full L9 snapshot explicitly, use `DB.Snapshot(ctx)`. See [`db.go`](../db.go).

### Skipping the lock page

The lock page at 1GB (0x40000000) must always be skipped:

```go
lockPgno := ltx.LockPgno(uint32(db.pageSize))
if pgno == lockPgno {
    continue // Skip this page - it's reserved by SQLite
}
```

Lock page numbers by page size:

| Page Size | Lock Page Number |
|-----------|------------------|
| 4KB | 262145 |
| 8KB | 131073 |
| 16KB | 65537 |
| 32KB | 32769 |

## Component Reference

### DB Component (db.go)

**Responsibilities:**

- Manages SQLite database connection (via `modernc.org/sqlite` - no CGO)
- Monitors WAL for changes
- Performs checkpoints
- Maintains long-running read transaction
- Converts WAL pages to LTX format

**Key Fields:**

```go
type DB struct {
    path     string      // Database file path
    db       *sql.DB     // SQLite connection
    rtx      *sql.Tx     // Long-running read transaction
    pageSize int         // Database page size (critical for lock page)
    notify   chan struct{} // Notifies on WAL changes
}
```

**Initialization sequence:** `DB.Open` starts the monitor when configured. The first sync that finds the database opens SQLite, enables WAL, acquires the long-running read transaction, loads the page size and metadata state, and starts the replica monitor. See [`db.go`](../db.go).

### Replica Component (replica.go)

**Responsibilities:**

- Manages replication to a single destination (one replica per DB)
- Tracks replication position (ltx.Pos)
- Handles sync intervals

**Key Operations:**

- `Sync(ctx)` uploads pending local L0 files.
- `SetPos(pos)` updates its tracked replication position under a write lock.

### ReplicaClient Interface (replica_client.go)

**Required Methods:**

```go
type ReplicaClient interface {
    Type() string
    Init(ctx context.Context) error
    SetLogger(logger *slog.Logger)

    // File operations
    LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error)
    OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
    WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error)
    DeleteLTXFiles(ctx context.Context, files []*ltx.FileInfo) error
    DeleteAll(ctx context.Context) error
}
```

**useMetadata Parameter:**

- `useMetadata=true`: Fetch accurate timestamps from backend metadata (required for point-in-time restores)
- `useMetadata=false`: Use fast timestamps for normal operations

### Compactor Component (compactor.go)

**Responsibilities:**

- Compaction and retention for LTX files
- Operates solely through `ReplicaClient` interface
- Suitable for both DB (with local file caching) and VFS (remote-only)

**Key Fields:**

```go
type Compactor struct {
    client           ReplicaClient
    VerifyCompaction bool  // Post-compaction TXID consistency check
    RetentionEnabled bool  // Default: true. Controls active file deletion

    // Local file optimization (set by DB layer)
    LocalFileOpener  func(level int, minTXID, maxTXID ltx.TXID) (io.ReadCloser, error)
    LocalFileDeleter func(level int, minTXID, maxTXID ltx.TXID) error

    // Level max-file-info caching
    CacheGetter func(level int) (*ltx.FileInfo, bool)
    CacheSetter func(level int, info *ltx.FileInfo)
}
```

### Store Component (store.go)

The default compaction intervals are 30 seconds for L1, five minutes for L2, and one hour for L3. L0 stores raw changes. L9 snapshots default to an interval and retention of 24 hours each. See [`compaction_level.go`](../compaction_level.go) and [`store.go`](../store.go).

## Testing Patterns

### Race Condition Testing

```bash
# Always run with race detector
go test -race -v ./...

# Specific race-prone areas
go test -race -v -run TestReplica_Sync ./...
go test -race -v -run TestDB_Sync ./...
go test -race -v -run TestStore_CompactDB ./...
```

### Lock Page Testing

Populate a database larger than 1 GB, replicate it, and validate the restored copy against the source.

```bash
# Test with various page sizes
./bin/litestream-test populate -db test.db -page-size 4096 -target-size 2GB
./bin/litestream-test populate -db test.db -page-size 8192 -target-size 2GB

# Validate lock page handling
./bin/litestream-test validate -source-db test.db -replica-url file:///tmp/replica
```

### Integration Testing

Run the shared replica client tests from the root package. Select the backend and supply its test connection settings as needed.

```bash
go test -v . -run '^TestReplicaClient_' -args -integration -replica-clients=file
```
