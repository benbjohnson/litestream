# ReplicaClient Implementation Guide

This guide provides comprehensive instructions for implementing new storage backends for Litestream replication.

## Table of Contents

- [Interface Contract](#interface-contract)
- [Implementation Checklist](#implementation-checklist)
- [Eventual Consistency Handling](#eventual-consistency-handling)
- [Error Handling](#error-handling)
- [Testing Requirements](#testing-requirements)
- [Common Implementation Mistakes](#common-implementation-mistakes)
- [Reference Implementations](#reference-implementations)

## Interface Contract

All replica clients MUST implement the `ReplicaClient` interface defined in `replica_client.go`:

```go
type ReplicaClient interface {
    // Returns the type identifier (e.g., "s3", "gcs", "file")
    Type() string

    // Returns iterator of LTX files at given level
    // seek: Start from this TXID (0 = beginning)
    // useMetadata: When true, fetch accurate timestamps from backend metadata (required for PIT restore)
    LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error)

    // Opens an LTX file for reading
    // Returns os.ErrNotExist if file doesn't exist
    OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)

    // Writes an LTX file to storage
    // SHOULD set CreatedAt based on backend metadata or upload time
    WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error)

    // Deletes one or more LTX files
    DeleteLTXFiles(ctx context.Context, files []*ltx.FileInfo) error

    // Deletes all files for this database
    DeleteAll(ctx context.Context) error
}
```

## Implementation Checklist

### Required Features

- [ ] Implement all interface methods
- [ ] Support partial reads (offset/size in OpenLTXFile)
- [ ] Return proper error types (especially os.ErrNotExist)
- [ ] Handle context cancellation
- [ ] Preserve file timestamps (CreatedAt)
- [ ] Support concurrent operations
- [ ] Implement proper cleanup in DeleteAll

### Optional Features

- [ ] Connection pooling
- [ ] Retry logic with exponential backoff
- [ ] Request batching
- [ ] Compression
- [ ] Encryption at rest
- [ ] Bandwidth throttling

## Eventual Consistency Handling

Many cloud storage services exhibit eventual consistency, where:
- A file you just wrote might not be immediately visible
- A file might be listed but only partially readable
- Deletes might not take effect immediately

### Best Practices

#### 1. Write-After-Write Consistency

```go
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
    // Buffer the entire content first
    data, err := io.ReadAll(r)
    if err != nil {
        return nil, fmt.Errorf("buffer ltx data: %w", err)
    }

    // Calculate checksum before upload
    checksum := crc64.Checksum(data, crc64.MakeTable(crc64.ECMA))

    // Upload with checksum verification
    err = c.uploadWithVerification(ctx, path, data, checksum)
    if err != nil {
        return nil, err
    }

    // Verify the file is readable before returning
    return c.verifyUpload(ctx, path, int64(len(data)), checksum)
}

func (c *ReplicaClient) verifyUpload(ctx context.Context, path string, expectedSize int64, expectedChecksum uint64) (*ltx.FileInfo, error) {
    // Implement retry loop with backoff
    backoff := 100 * time.Millisecond
    for i := 0; i < 10; i++ {
        info, err := c.statFile(ctx, path)
        if err == nil {
            if info.Size == expectedSize {
                rc, err := c.openFile(ctx, path, 0, 0)
                if err != nil {
                    return nil, fmt.Errorf("open uploaded file: %w", err)
                }
                data, err := io.ReadAll(rc)
                rc.Close()
                if err != nil {
                    return nil, fmt.Errorf("read uploaded file: %w", err)
                }
                if crc64.Checksum(data, crc64.MakeTable(crc64.ECMA)) == expectedChecksum {
                    return info, nil
                }
            }
        }

        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-time.After(backoff):
            backoff *= 2
        }
    }
    return nil, errors.New("upload verification failed")
}
```

#### 2. List-After-Write Consistency

```go
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
    // List files from storage
    files, err := c.listFiles(ctx, level, useMetadata)
    if err != nil {
        return nil, err
    }

    // Sort by TXID for consistent ordering
    sort.Slice(files, func(i, j int) bool {
        if files[i].MinTXID != files[j].MinTXID {
            return files[i].MinTXID < files[j].MinTXID
        }
        return files[i].MaxTXID < files[j].MaxTXID
    })

    // Filter by seek position
    var filtered []*ltx.FileInfo
    for _, f := range files {
        if f.MinTXID >= seek {
            filtered = append(filtered, f)
        }
    }

    return ltx.NewFileInfoSliceIterator(filtered), nil
}
```

#### 3. Read-After-Write Consistency

```go
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
    path := c.ltxPath(level, minTXID, maxTXID)

    // For eventually consistent backends, implement retry
    var lastErr error
    backoff := 100 * time.Millisecond

    for i := 0; i < 5; i++ {
        reader, err := c.openFile(ctx, path, offset, size)
        if err == nil {
            return reader, nil
        }

        // Don't retry on definitive errors
        if errors.Is(err, os.ErrNotExist) {
            return nil, err
        }

        lastErr = err
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-time.After(backoff):
            backoff *= 2
        }
    }

    return nil, fmt.Errorf("open file after retries: %w", lastErr)
}
```

## Error Handling

### Standard Error Types

Always return appropriate standard errors:

```go
// File not found
return nil, os.ErrNotExist

// Permission denied
return nil, os.ErrPermission

// Context cancelled
return nil, ctx.Err()

// Custom errors should wrap standard ones
return nil, fmt.Errorf("s3 download failed: %w", err)
```

### Error Classification

```go
// Retryable errors
func isRetryable(err error) bool {
    // Network errors
    var netErr net.Error
    if errors.As(err, &netErr) && netErr.Temporary() {
        return true
    }

    // Specific HTTP status codes
    if httpErr, ok := err.(HTTPError); ok {
        switch httpErr.StatusCode {
        case 429, 500, 502, 503, 504:
            return true
        }
    }

    // Timeout errors
    if errors.Is(err, context.DeadlineExceeded) {
        return true
    }

    return false
}
```

### Logging Best Practices

```go
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
    logger := slog.Default().With(
        "replica", c.Type(),
        "level", level,
        "minTXID", minTXID,
        "maxTXID", maxTXID,
    )

    logger.Debug("starting ltx upload")

    info, err := c.upload(ctx, level, minTXID, maxTXID, r)
    if err != nil {
        logger.Error("ltx upload failed", "error", err)
        return nil, err
    }

    logger.Info("ltx upload complete", "size", info.Size)
    return info, nil
}
```

## Testing Requirements

### Unit Tests

Every replica client MUST have comprehensive unit tests:

```go
// replica_client_test.go
func TestReplicaClient_WriteLTXFile(t *testing.T) {
    client := NewReplicaClient(testConfig)
    ctx := context.Background()

    // Test data
    data := []byte("test ltx content")
    reader := bytes.NewReader(data)

    // Write file
    info, err := client.WriteLTXFile(ctx, 0, 1, 100, reader)
    assert.NoError(t, err)
    assert.Equal(t, int64(len(data)), info.Size)

    // Verify file exists
    rc, err := client.OpenLTXFile(ctx, 0, 1, 100, 0, 0)
    assert.NoError(t, err)
    defer rc.Close()

    // Read and verify content
    content, err := io.ReadAll(rc)
    assert.NoError(t, err)
    assert.Equal(t, data, content)
}

func TestReplicaClient_PartialRead(t *testing.T) {
    client := NewReplicaClient(testConfig)
    ctx := context.Background()

    // Write test file
    data := bytes.Repeat([]byte("x"), 1000)
    _, err := client.WriteLTXFile(ctx, 0, 1, 100, bytes.NewReader(data))
    require.NoError(t, err)

    // Test partial read
    rc, err := client.OpenLTXFile(ctx, 0, 1, 100, 100, 50)
    require.NoError(t, err)
    defer rc.Close()

    partial, err := io.ReadAll(rc)
    assert.NoError(t, err)
    assert.Equal(t, 50, len(partial))
    assert.Equal(t, data[100:150], partial)
}

func TestReplicaClient_NotFound(t *testing.T) {
    client := NewReplicaClient(testConfig)
    ctx := context.Background()

    // Try to open non-existent file
    _, err := client.OpenLTXFile(ctx, 0, 999, 999, 0, 0)
    assert.True(t, errors.Is(err, os.ErrNotExist))
}
```

### Integration Tests

Integration tests run against real backends:

```go
// +build integration

func TestReplicaClient_Integration(t *testing.T) {
    // Skip if not in integration mode
    if testing.Short() {
        t.Skip("skipping integration test")
    }

    // Get credentials from environment
    config := ConfigFromEnv(t)
    client := NewReplicaClient(config)
    ctx := context.Background()

    t.Run("Concurrent Writes", func(t *testing.T) {
        var wg sync.WaitGroup
        errors := make(chan error, 10)

        for i := 0; i < 10; i++ {
            wg.Add(1)
            go func(n int) {
                defer wg.Done()

                data := []byte(fmt.Sprintf("concurrent %d", n))
                minTXID := ltx.TXID(n * 100)
                maxTXID := ltx.TXID((n + 1) * 100)

                _, err := client.WriteLTXFile(ctx, 0, minTXID, maxTXID,
                    bytes.NewReader(data))
                if err != nil {
                    errors <- err
                }
            }(i)
        }

        wg.Wait()
        close(errors)

        for err := range errors {
            t.Error(err)
        }
    })

    t.Run("Large File", func(t *testing.T) {
        // Test with 100MB file
        data := bytes.Repeat([]byte("x"), 100*1024*1024)

        info, err := client.WriteLTXFile(ctx, 0, 1000, 2000,
            bytes.NewReader(data))
        require.NoError(t, err)
        assert.Equal(t, int64(len(data)), info.Size)
    })

    t.Run("Cleanup", func(t *testing.T) {
        err := client.DeleteAll(ctx)
        assert.NoError(t, err)

        // Verify cleanup
        iter, err := client.LTXFiles(ctx, 0, 0, false)
        require.NoError(t, err)
        defer iter.Close()

        assert.False(t, iter.Next(), "files should be deleted")
    })
}
```

### Mock Client for Testing

Provide a mock implementation for testing:

```go
// mock/replica_client.go
type ReplicaClient struct {
    mu     sync.Mutex
    files  map[string]*ltx.FileInfo
    data   map[string][]byte
    errors map[string]error  // Inject errors for testing
}

func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
    c.mu.Lock()
    defer c.mu.Unlock()

    // Check for injected error
    key := fmt.Sprintf("write-%d-%d-%d", level, minTXID, maxTXID)
    if err, ok := c.errors[key]; ok {
        return nil, err
    }

    // Store data
    data, err := io.ReadAll(r)
    if err != nil {
        return nil, err
    }

    path := ltxPath(level, minTXID, maxTXID)
    c.data[path] = data

    info := &ltx.FileInfo{
        Level:     level,
        MinTXID:   minTXID,
        MaxTXID:   maxTXID,
        Size:      int64(len(data)),
        CreatedAt: time.Now(),
    }
    c.files[path] = info

    return info, nil
}
```

## Common Implementation Mistakes

### ❌ Mistake 1: Not Handling Partial Reads

```go
// WRONG - Always reads entire file
func (c *Client) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
    return c.storage.Download(path)  // Ignores offset/size!
}
```

```go
// CORRECT - Respects offset and size
func (c *Client) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
    if offset == 0 && size == 0 {
        // Full file
        return c.storage.Download(path)
    }

    // Partial read using Range header or equivalent
    end := offset + size - 1
    if size == 0 {
        end = 0  // Read to end
    }
    return c.storage.DownloadRange(path, offset, end)
}
```

### ❌ Mistake 2: Not Preserving CreatedAt

```go
// WRONG - Uses current time
func (c *Client) WriteLTXFile(...) (*ltx.FileInfo, error) {
    // Upload file...

    return &ltx.FileInfo{
        CreatedAt: time.Now(),  // Wrong! Loses temporal info
    }, nil
}
```

```go
// CORRECT - Preserves original timestamp
func (c *Client) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
    // Upload file...
    uploadedSize, modTime, err := c.storage.Upload(path, r)
    if err != nil {
        return nil, err
    }

    return &ltx.FileInfo{
        Level:     level,
        MinTXID:   minTXID,
        MaxTXID:   maxTXID,
        Size:      uploadedSize,
        CreatedAt: modTime,
    }, nil
}
```

### ❌ Mistake 3: Wrong Error Types

```go
// WRONG - Generic error
func (c *Client) OpenLTXFile(...) (io.ReadCloser, error) {
    resp, err := c.get(path)
    if err != nil {
        return nil, fmt.Errorf("not found")  // Wrong type!
    }
}
```

```go
// CORRECT - Proper error type
func (c *Client) OpenLTXFile(...) (io.ReadCloser, error) {
    resp, err := c.get(path)
    if err != nil {
        if resp.StatusCode == 404 {
            return nil, os.ErrNotExist  // Correct type
        }
        return nil, fmt.Errorf("download failed: %w", err)
    }
}
```

### ❌ Mistake 4: Not Handling Context

```go
// WRONG - Ignores context
func (c *Client) WriteLTXFile(ctx context.Context, ...) (*ltx.FileInfo, error) {
    // Long operation without checking context
    for i := 0; i < 1000000; i++ {
        doWork()  // Could run forever!
    }
}
```

```go
// CORRECT - Respects context
func (c *Client) WriteLTXFile(ctx context.Context, ...) (*ltx.FileInfo, error) {
    // Check context periodically
    for i := 0; i < 1000000; i++ {
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        default:
            // Continue work
        }

        if err := doWork(ctx); err != nil {
            return nil, err
        }
    }
}
```

### ❌ Mistake 5: Blocking in Iterator

```go
// WRONG - Loads all files at once
func (c *Client) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
    allFiles, err := c.loadAllFiles(level)  // Could be millions!
    if err != nil {
        return nil, err
    }

    return NewIterator(allFiles), nil
}
```

```go
// CORRECT - Lazy loading with pagination
func (c *Client) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
    return &lazyIterator{
        client:      c,
        level:       level,
        seek:        seek,
        useMetadata: useMetadata,
        pageSize:    1000,
    }, nil
}

type lazyIterator struct {
    client   *Client
    level    int
    seek     ltx.TXID
    pageSize int
    current  []*ltx.FileInfo
    index    int
    done     bool
}

func (i *lazyIterator) Next() bool {
    if i.index >= len(i.current) && !i.done {
        // Load next page
        i.loadNextPage()
    }
    return i.index < len(i.current)
}
```

## Reference Implementations

### File System Client (Simplest)

See `file/replica_client.go` for the simplest implementation:
- Direct file I/O operations
- No network complexity
- Good starting reference

### S3 Client (Most Complex)

See `s3/replica_client.go` for advanced features:
- Multipart uploads for large files
- Retry logic with exponential backoff
- Request signing
- Eventual consistency handling

### Key Patterns from S3 Implementation

```go
// Path construction
func (c *ReplicaClient) ltxDir(level int) string {
    if level == SnapshotLevel {
        return path.Join(c.Path, "snapshots")
    }
    return path.Join(c.Path, "ltx", fmt.Sprintf("%04d", level))
}

// Metadata handling
func (c *ReplicaClient) WriteLTXFile(...) (*ltx.FileInfo, error) {
    // Add metadata to object
    metadata := map[string]string{
        "min-txid": fmt.Sprintf("%d", minTXID),
        "max-txid": fmt.Sprintf("%d", maxTXID),
        "level":    fmt.Sprintf("%d", level),
    }

    // Upload with metadata
    _, err := c.s3.PutObjectWithContext(ctx, &s3.PutObjectInput{
        Bucket:   &c.Bucket,
        Key:      &key,
        Body:     r,
        Metadata: metadata,
    })
}

// Error mapping
func mapS3Error(err error) error {
    if aerr, ok := err.(awserr.Error); ok {
        switch aerr.Code() {
        case s3.ErrCodeNoSuchKey:
            return os.ErrNotExist
        case s3.ErrCodeAccessDenied:
            return os.ErrPermission
        }
    }
    return err
}
```

## Performance Optimization

### Connection Pooling

```go
type ReplicaClient struct {
    pool *ConnectionPool
}

func NewReplicaClient(config Config) *ReplicaClient {
    pool := &ConnectionPool{
        MaxConnections: config.MaxConnections,
        IdleTimeout:    config.IdleTimeout,
    }

    return &ReplicaClient{
        pool: pool,
    }
}
```

### Request Batching

```go
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, files []*ltx.FileInfo) error {
    // Batch deletes for efficiency
    const batchSize = 100

    for i := 0; i < len(files); i += batchSize {
        end := i + batchSize
        if end > len(files) {
            end = len(files)
        }

        batch := files[i:end]
        if err := c.deleteBatch(ctx, batch); err != nil {
            return fmt.Errorf("delete batch %d: %w", i/batchSize, err)
        }
    }

    return nil
}
```

### Caching

```go
type ReplicaClient struct {
    cache *FileInfoCache
}

func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
    // Check cache first (only cache when useMetadata=false for fast queries)
    cacheKey := fmt.Sprintf("%d-%d", level, seek)
    if !useMetadata {
        if cached, ok := c.cache.Get(cacheKey); ok {
            return ltx.NewFileInfoSliceIterator(cached), nil
        }
    }

    // Load from storage
    files, err := c.loadFiles(ctx, level, seek, useMetadata)
    if err != nil {
        return nil, err
    }

    // Cache for future requests
    c.cache.Set(cacheKey, files, 5*time.Minute)

    return ltx.NewFileInfoSliceIterator(files), nil
}
```

## Checklist for New Implementations

Before submitting a new replica client:

- [ ] All interface methods implemented
- [ ] Unit tests with >80% coverage
- [ ] Integration tests (with build tag)
- [ ] Mock client for testing
- [ ] Handles partial reads correctly
- [ ] Returns proper error types
- [ ] Preserves timestamps
- [ ] Handles context cancellation
- [ ] Documents eventual consistency behavior
- [ ] Includes retry logic for transient errors
- [ ] Logs appropriately (debug/info/error)
- [ ] README with configuration examples
- [ ] Added to main configuration parser

## Getting Help

1. Study existing implementations (start with `file/`, then `s3/`)
2. Check test files for expected behavior
3. Run integration tests against your backend
4. Use the mock client for rapid development
5. Ask in GitHub discussions for design feedback
