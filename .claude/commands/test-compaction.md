Test Litestream compaction logic. This command helps test and debug compaction issues, especially with eventually consistent storage backends.

First, understand the test scenario:
- What storage backend needs testing?
- What size database is involved?
- Are there eventual consistency concerns?

Then create comprehensive tests:

1. **Test basic compaction**:
```go
func TestCompaction_Basic(t *testing.T) {
    // Create multiple LTX files at level 0
    // Run compaction to level 1
    // Verify merged file is correct
}
```

2. **Test with eventual consistency**:
```go
func TestStore_CompactDB_RemotePartialRead(t *testing.T) {
    // Use mock client that returns partial data initially
    // Verify compaction prefers local files
    // Ensure no corruption occurs
}
```

3. **Test lock page handling during compaction**:
```go
func TestCompaction_LockPage(t *testing.T) {
    // Create database > 1GB
    // Compact with data around lock page
    // Verify lock page is skipped (page at 0x40000000)
}
```

4. **Test timestamp preservation**:
```go
func TestCompaction_PreserveTimestamps(t *testing.T) {
    // Compact files with different CreatedAt times
    // Verify earliest timestamp is preserved
}
```

Key areas to test:
- Reading from local files first (db.go:1280-1294)
- Skipping lock page at 1GB boundary
- Preserving CreatedAt timestamps
- Handling partial/incomplete remote files
- Concurrent compaction safety

Run with race detector:
```bash
go test -race -v -run TestStore_CompactDB ./...
```

Use the test harness for large databases:
```bash
./bin/litestream-test populate -db test.db -target-size 1.5GB
./bin/litestream-test validate -source-db test.db -replica-url file:///tmp/replica
```
