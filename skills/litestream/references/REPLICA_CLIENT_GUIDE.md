# ReplicaClient Implementation Reference

Condensed reference for agents implementing or modifying Litestream storage backends.

## Interface Contract

From `replica_client.go`:

```go
type ReplicaClient interface {
    Type() string
    Init(ctx context.Context) error
    LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error)
    OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
    WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error)
    DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error
    DeleteAll(ctx context.Context) error
}
```

### Method Contracts

**Type()**: Return identifier string (e.g., "s3", "gcs", "file").

**Init()**: Initialize connection. Must be idempotent (no-op if already initialized).

**LTXFiles()**: Return iterator sorted by MinTXID. `seek` starts from given TXID.
`useMetadata=true` fetches accurate timestamps (required for point-in-time restore).
`useMetadata=false` uses fast timestamps for normal operations.

**OpenLTXFile()**: Open file for reading. Must support partial reads via `offset`/`size`.
Return `os.ErrNotExist` if file is missing.

**WriteLTXFile()**: Write file to storage. Set `CreatedAt` from backend
metadata or upload time.

**DeleteLTXFiles()**: Delete one or more files. Batch if possible.

**DeleteAll()**: Delete all files for this database.

## Implementation Checklist

### Required

- [ ] All interface methods implemented
- [ ] `Init()` is idempotent
- [ ] Partial reads supported (`offset`/`size` in `OpenLTXFile`)
- [ ] `os.ErrNotExist` returned for missing files
- [ ] Context cancellation handled in all methods
- [ ] `CreatedAt` timestamps preserved in `WriteLTXFile`
- [ ] Concurrent operations supported
- [ ] Proper cleanup in `DeleteAll`

### Optional

- [ ] Connection pooling
- [ ] Retry logic with exponential backoff
- [ ] Request batching for deletes
- [ ] Compression / encryption at rest
- [ ] Bandwidth throttling

## Eventual Consistency

Many cloud backends (S3, R2, etc.) are eventually consistent:
- Recently written files may not be immediately visible
- Listed files may be only partially readable
- Deletes may not take effect immediately

### Pattern: Read Local First

During compaction, always prefer local files:

```go
f, err := os.Open(db.LTXPath(info.Level, info.MinTXID, info.MaxTXID))
if err == nil {
    return f, nil
}
return replica.Client.OpenLTXFile(...)
```

### Pattern: Retry with Backoff

```go
backoff := 100 * time.Millisecond
for i := 0; i < 5; i++ {
    reader, err := c.openFile(ctx, path, offset, size)
    if err == nil {
        return reader, nil
    }
    if errors.Is(err, os.ErrNotExist) {
        return nil, err // Don't retry definitive errors
    }
    select {
    case <-ctx.Done():
        return nil, ctx.Err()
    case <-time.After(backoff):
        backoff *= 2
    }
}
```

### Pattern: Lazy Iterator

```go
func (c *Client) LTXFiles(...) (ltx.FileIterator, error) {
    return &lazyIterator{
        client:   c,
        level:    level,
        seek:     seek,
        pageSize: 1000, // Paginate, don't load all at once
    }, nil
}
```

## Error Handling

### Standard Error Types

```go
// File not found
return nil, os.ErrNotExist

// Permission denied
return nil, os.ErrPermission

// Context cancelled
return nil, ctx.Err()

// Wrapped errors
return nil, fmt.Errorf("s3 download failed: %w", err)
```

### Retryable Error Classification

```go
func isRetryable(err error) bool {
    var netErr net.Error
    if errors.As(err, &netErr) && netErr.Temporary() {
        return true
    }
    // HTTP 429, 500, 502, 503, 504
    if errors.Is(err, context.DeadlineExceeded) {
        return true
    }
    return false
}
```

## Common Mistakes

### 1. Not Handling Partial Reads

```go
// WRONG - Ignores offset/size
return c.storage.Download(path)

// CORRECT
if offset == 0 && size == 0 {
    return c.storage.Download(path)
}
return c.storage.DownloadRange(path, offset, offset+size-1)
```

### 2. Not Preserving CreatedAt

```go
// WRONG
return &ltx.FileInfo{CreatedAt: time.Now()}

// CORRECT - Use backend metadata
return &ltx.FileInfo{CreatedAt: modTime}
```

### 3. Wrong Error Types

```go
// WRONG
return nil, fmt.Errorf("not found")

// CORRECT
if resp.StatusCode == 404 {
    return nil, os.ErrNotExist
}
```

### 4. Ignoring Context

```go
// WRONG - Could run forever
for i := 0; i < 1000000; i++ {
    doWork()
}

// CORRECT
select {
case <-ctx.Done():
    return nil, ctx.Err()
default:
}
```

### 5. Loading All Files at Once

```go
// WRONG - Could be millions of files
allFiles, _ := c.loadAllFiles(level)

// CORRECT - Lazy pagination
return &lazyIterator{pageSize: 1000}, nil
```

## Path Construction

```go
func (c *Client) ltxDir(level int) string {
    if level == SnapshotLevel {
        return path.Join(c.Path, "snapshots")
    }
    return path.Join(c.Path, "ltx", fmt.Sprintf("%04d", level))
}
```

## Reference Implementations

- **Simplest**: `file/replica_client.go` (direct file I/O, no network)
- **Most complete**: `s3/replica_client.go` (multipart uploads, retries, signing)

Start with the file backend for understanding, then study S3 for advanced
patterns.

## Testing Requirements

- Unit tests with >80% coverage
- Integration tests with build tag
- Test partial reads, concurrent operations, missing files
- Run with `-race` flag
- Verify `os.ErrNotExist` for missing files
- Test context cancellation
