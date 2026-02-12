# Litestream Testing Reference

Condensed reference for agents writing and debugging Litestream tests.

## Testing Philosophy

1. Test at multiple levels: unit, integration, end-to-end
2. Focus on edge cases: >1 GB databases, eventual consistency
3. Use real SQLite: avoid mocking SQLite behavior
4. Always run with `-race` flag
5. Use fixed seeds and timestamps for deterministic tests

## Essential Commands

```bash
# Full test suite with race detection
go test -race -v ./...

# Specific test areas
go test -race -v -run TestReplica_Sync ./...
go test -race -v -run TestDB_Sync ./...
go test -race -v -run TestStore_CompactDB ./...

# Coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
go tool cover -func=coverage.out | grep total

# Integration tests (backend-specific)
go test -v ./replica_client_test.go -integration s3
go test -v ./replica_client_test.go -integration gcs
```

## Lock Page Testing

The lock page at 1 GB must be skipped. Test with databases spanning this
boundary using multiple page sizes:

```bash
./bin/litestream-test populate -db test.db -page-size 4096 -target-size 2GB
./bin/litestream-test validate -source-db test.db -replica-url file:///tmp/replica
```

Test all page size variants:

| Page Size | Lock Page | Test Target |
|-----------|-----------|-------------|
| 4 KB      | 262145    | >1.0 GB     |
| 8 KB      | 131073    | >1.0 GB     |
| 16 KB     | 65537     | >1.0 GB     |
| 32 KB     | 32769     | >1.0 GB     |

Verify lock page is not present in any LTX file after replication.

## Race Condition Testing

Race-prone areas (always run with `-race`):

1. **Position updates**: Concurrent `SetPos()` + `Pos()` on Replica
2. **WAL monitoring**: Concurrent writes + monitor + checkpoint
3. **Compaction**: Concurrent compaction at different levels

### Pattern: Concurrent Test

```go
func TestConcurrent(t *testing.T) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    var wg sync.WaitGroup
    // Start writer, monitor, checkpoint goroutines
    // Use ctx for shutdown, wg for cleanup
    wg.Wait()
}
```

## Common Test Failures

### 1. Database Locked

```go
// WRONG - WAL mode not enabled
db, _ := sql.Open("sqlite", "test.db")

// CORRECT
db, _ := sql.Open("sqlite", "test.db?_journal=WAL")
```

### 2. Timing Issues

```go
// WRONG - Data may not be replicated yet
WriteData(db)
result := ReadReplica()

// CORRECT - Explicit sync
WriteData(db)
db.Sync(context.Background())
result := ReadReplica()
```

### 3. Goroutine Leaks

```go
// WRONG - Goroutine outlives test
go func() {
    time.Sleep(10 * time.Second)
    doWork()
}()

// CORRECT - Use context + WaitGroup
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
var wg sync.WaitGroup
wg.Add(1)
go func() {
    defer wg.Done()
    select {
    case <-ctx.Done():
        return
    case <-time.After(10 * time.Second):
        doWork()
    }
}()
cancel()
wg.Wait()
```

### 4. File Handle Leaks

```go
// Always use defer for cleanup
f, err := os.Open("test.db")
require.NoError(t, err)
defer f.Close()
```

## Test Helper Patterns

```go
func NewTestDB(t testing.TB) *litestream.DB {
    t.Helper()
    path := filepath.Join(t.TempDir(), "test.db")
    conn, err := sql.Open("sqlite", path+"?_journal=WAL")
    require.NoError(t, err)
    _, err = conn.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, data BLOB)")
    require.NoError(t, err)
    conn.Close()

    db := litestream.NewDB(path)
    db.MonitorInterval = 10 * time.Millisecond
    db.MinCheckpointPageN = 100
    err = db.Open()
    require.NoError(t, err)
    t.Cleanup(func() { db.Close(context.Background()) })
    return db
}
```

## Coverage Requirements

| Package            | Target |
|--------------------|--------|
| Core (db, replica) | >80%   |
| Replica clients    | >70%   |
| Utilities          | >60%   |

## Table-Driven Tests

```go
func TestCheckpoint(t *testing.T) {
    tests := []struct {
        name    string
        mode    string
        wantErr bool
    }{
        {"Passive", "PASSIVE", false},
        {"Full", "FULL", false},
        {"Truncate", "TRUNCATE", false},
        {"Invalid", "INVALID", true},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // ...
        })
    }
}
```
