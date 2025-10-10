Validate a ReplicaClient implementation in Litestream. This command helps ensure a replica client correctly implements the interface and handles edge cases.

First, identify what needs validation:
- Which replica client implementation?
- What storage backend specifics?
- Any known issues or concerns?

Then validate the implementation:

1. **Interface compliance check**:
```go
// Ensure all methods are implemented
var _ litestream.ReplicaClient = (*YourClient)(nil)
```

2. **Verify error types**:
```go
// OpenLTXFile must return os.ErrNotExist for missing files
_, err := client.OpenLTXFile(ctx, 0, 999, 999, 0, 0)
if !errors.Is(err, os.ErrNotExist) {
    t.Errorf("Expected os.ErrNotExist, got %v", err)
}
```

3. **Test partial reads**:
```go
// Must support offset and size parameters
rc, err := client.OpenLTXFile(ctx, 0, 1, 100, 50, 25)
data, _ := io.ReadAll(rc)
if len(data) != 25 {
    t.Errorf("Expected 25 bytes, got %d", len(data))
}
```

4. **Verify timestamp preservation**:
```go
// WriteLTXFile must preserve CreatedAt if provided
createdAt := time.Now().Add(-24 * time.Hour)
info, _ := client.WriteLTXFile(ctx, 0, 1, 100, reader, &createdAt)
if !info.CreatedAt.Equal(createdAt) {
    t.Error("CreatedAt not preserved")
}
```

5. **Test eventual consistency handling**:
- Implement retry logic for transient failures
- Handle partial file availability
- Verify write-after-write consistency

6. **Validate cleanup**:
```go
// DeleteAll must remove everything
err := client.DeleteAll(ctx)
files, _ := client.LTXFiles(ctx, 0, 0)
if files.Next() {
    t.Error("Files remain after DeleteAll")
}
```

Key validation points:
- Proper error types (os.ErrNotExist, os.ErrPermission)
- Context cancellation handling
- Concurrent operation safety
- Iterator doesn't load all files at once
- Proper path construction for storage backend

Run integration tests:
```bash
go test -v ./[backend]/replica_client_test.go -integration
```
