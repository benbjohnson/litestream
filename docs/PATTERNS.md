# Litestream Code Patterns and Anti-Patterns

This document contains detailed code patterns, examples, and anti-patterns for working with Litestream. For a quick overview, see [AGENTS.md](../AGENTS.md).

## Table of Contents

- [Architectural Boundaries](#architectural-boundaries)
- [Atomic File Operations](#atomic-file-operations)
- [Error Handling](#error-handling)
- [Locking Patterns](#locking-patterns)
- [Compaction and Eventual Consistency](#compaction-and-eventual-consistency)
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

### DO: Handle database state in DB layer

Database restoration logic belongs in the DB layer, not the Replica layer.

When the database is behind the replica (local TXID < remote TXID):

1. **Clear local L0 cache**: Remove the entire L0 directory and recreate it
2. **Fetch latest L0 file from replica**: Download the most recent L0 LTX file
3. **Write using atomic file operations**: Prevent partial/corrupted files

```go
// CORRECT - DB layer handles database state
func (db *DB) init() error {
    // DB layer handles database state
    if db.needsRestore() {
        if err := db.restore(); err != nil {
            return err
        }
    }
    // Then start replica for replication only
    return db.replica.Start()
}

func (r *Replica) Start() error {
    // Replica focuses only on replication
    return r.startSync()
}
```

Reference: `DB.checkDatabaseBehindReplica()` in db.go:670-737

### DON'T: Put database state logic in Replica layer

```go
// WRONG - Replica should only handle replication concerns
func (r *Replica) Start() error {
    // DON'T check database state here
    if needsRestore() {  // Wrong layer!
        restoreDatabase()  // Wrong layer!
    }
    // Replica should focus only on replication mechanics
}
```

## Atomic File Operations

Always use atomic writes to prevent partial/corrupted files.

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

### DO: Return errors immediately

```go
// CORRECT - Return error for caller to handle
func (db *DB) validatePosition() error {
    dpos, err := db.Pos()
    if err != nil {
        return err
    }
    rpos := replica.Pos()
    if dpos.TXID < rpos.TXID {
        return fmt.Errorf("database position (%v) behind replica (%v)", dpos, rpos)
    }
    return nil
}
```

### DON'T: Continue on critical errors

```go
// WRONG - Silently continuing can cause data corruption
func (db *DB) validatePosition() {
    if dpos, _ := db.Pos(); dpos.TXID < replica.Pos().TXID {
        log.Printf("warning: position mismatch")  // Don't just log!
        // Continuing here is dangerous
    }
}
```

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

## Compaction and Eventual Consistency

Many storage backends (S3, R2, etc.) are eventually consistent:

- A file you just wrote might not be immediately readable
- A file might be listed but only partially available
- Reads might return stale or incomplete data

### DO: Read from local when available

```go
// CORRECT - Check local first during compaction
// db.go:1280-1294 - ALWAYS read from local disk when available
f, err := os.Open(db.LTXPath(info.Level, info.MinTXID, info.MaxTXID))
if err == nil {
    // Use local file - it's complete and consistent
    return f, nil
}
// Only fall back to remote if local doesn't exist
return replica.Client.OpenLTXFile(...)
```

### DON'T: Read from remote during compaction

```go
// WRONG - Can get partial/corrupt data from eventually consistent storage
f, err := client.OpenLTXFile(ctx, level, minTXID, maxTXID, 0, 0)
```

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

### DO: Use ResumableReader for restore streams

```go
rc, _ := client.OpenLTXFile(ctx, level, min, max, 0, fileInfo.Size)
rr := internal.NewResumableReader(ctx, client, level, min, max, fileInfo.Size, rc, logger)
defer rr.Close()
```

### DON'T: Use raw OpenLTXFile during long restore operations

```go
rc, _ := client.OpenLTXFile(ctx, level, min, max, 0, 0)
// Risk: connection may drop during idle periods in multi-file restore
io.ReadFull(rc, buf) // unexpected EOF!
```

## Retention Bypass Pattern

When using cloud provider lifecycle policies (S3 lifecycle rules, R2 auto-cleanup), Litestream's active file deletion can be disabled:

```yaml
retention:
  enabled: false
```

### Propagation Chain

1. `RetentionConfig{Enabled *bool}` in YAML config (`cmd/litestream/main.go`)
2. `Store.SetRetentionEnabled(bool)` propagates to all DBs and their compactors (`store.go`)
3. `Compactor.RetentionEnabled` guards 3 deletion points in `compactor.go`

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

- **HTTP 412 (PreconditionFailed)**: Another instance acquired/renewed the lease
- **HTTP 404 (NoSuchKey)**: Lease already released

## Timestamp Preservation

During compaction, preserve the earliest CreatedAt timestamp from source files to maintain temporal granularity for point-in-time restoration.

### DO: Preserve earliest timestamp

```go
// CORRECT - Preserve temporal information
info, err := replica.Client.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
if err != nil {
    return fmt.Errorf("write ltx: %w", err)
}
info.CreatedAt = oldestSourceFile.CreatedAt
```

### DON'T: Ignore CreatedAt preservation

```go
// WRONG - Loses timestamp granularity for point-in-time restores
info := &ltx.FileInfo{
    CreatedAt: time.Now(), // Don't use current time during compaction
}
```

## Common Pitfalls

### 1. Mixing architectural concerns

```go
// WRONG - Database state logic in Replica layer
func (r *Replica) Start() error {
    if db.needsRestore() {  // Wrong layer for DB state!
        r.restoreDatabase()  // Replica shouldn't manage DB state!
    }
    return r.sync()
}
```

### 2. Recreating existing functionality

```go
// WRONG - Don't reimplement what already exists
func customSnapshotTrigger() {
    // Complex custom logic to trigger snapshots
    // when db.verify() already does this!
}
```

### DO: Leverage existing mechanisms

```go
// CORRECT - Use what's already there
func triggerSnapshot() error {
    return db.verify()  // Already handles snapshot logic correctly
}
```

### 3. Skipping the lock page

The lock page at 1GB (0x40000000) must always be skipped:

```go
// db.go:951-953 - Must skip lock page during replication
lockPgno := ltx.LockPgno(pageSize)
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

**Initialization Sequence:**

1. Open database connection
2. Read page size from database
3. Initialize long-running read transaction
4. Start monitor goroutine
5. Initialize replicas

### Replica Component (replica.go)

**Responsibilities:**

- Manages replication to a single destination (one replica per DB)
- Tracks replication position (ltx.Pos)
- Handles sync intervals
- Manages encryption (if configured)

**Key Operations:**

- `Sync()`: Synchronizes pending changes
- `SetPos()`: Updates replication position (must use Lock, not RLock!)
- `Snapshot()`: Creates full database snapshot

### ReplicaClient Interface (replica_client.go)

**Required Methods:**

```go
type ReplicaClient interface {
    Type() string  // Client type identifier

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

**Default Compaction Levels:**

```go
var defaultLevels = CompactionLevels{
    {Level: 0, Interval: 0},        // Raw LTX files (no compaction)
    {Level: 1, Interval: 30*Second},
    {Level: 2, Interval: 5*Minute},
    {Level: 3, Interval: 1*Hour},
    // Snapshots created daily (24h retention)
}
```

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

```bash
# Test with various page sizes
./bin/litestream-test populate -db test.db -page-size 4096 -target-size 2GB
./bin/litestream-test populate -db test.db -page-size 8192 -target-size 2GB

# Validate lock page handling
./bin/litestream-test validate -source-db test.db -replica-url file:///tmp/replica
```

### Integration Testing

```bash
# Test specific backend
go test -v ./replica_client_test.go -integration s3
go test -v ./replica_client_test.go -integration gcs
go test -v ./replica_client_test.go -integration abs
go test -v ./replica_client_test.go -integration oss
go test -v ./replica_client_test.go -integration sftp
```
