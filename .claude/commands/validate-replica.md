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

1. **Verify error types**:

   ```go
   // OpenLTXFile must return os.ErrNotExist for missing files
   _, err := client.OpenLTXFile(ctx, 0, 999, 999, 0, 0)
   if !errors.Is(err, os.ErrNotExist) {
       t.Errorf("Expected os.ErrNotExist, got %v", err)
   }
   ```

1. **Test partial reads**:

   ```go
   // Must support offset and size parameters
   rc, err := client.OpenLTXFile(ctx, 0, 1, 100, 50, 25)
   data, _ := io.ReadAll(rc)
   if len(data) != 25 {
       t.Errorf("Expected 25 bytes, got %d", len(data))
   }
   ```

1. **Verify timestamp preservation**:

   ```go
   // CreatedAt should reflect remote object metadata (or upload time)
   start := time.Now()
   info, _ := client.WriteLTXFile(ctx, 0, 1, 100, reader)
   if info.CreatedAt.IsZero() || info.CreatedAt.Before(start.Add(-time.Second)) {
       t.Error("unexpected CreatedAt timestamp")
   }
   ```

1. **Test eventual consistency handling**:
   - Implement retry logic for transient failures
   - Handle partial file availability
   - Verify write-after-write consistency

1. **Validate cleanup**:

   ```go
   // DeleteAll must remove everything
   err := client.DeleteAll(ctx)
   files, _ := client.LTXFiles(ctx, 0, 0, false)
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
