# Litestream Testing Guide

Comprehensive guide for testing Litestream components and handling edge cases.

## Table of Contents

- [Testing Philosophy](#testing-philosophy)
- [1GB Database Testing](#1gb-database-testing)
- [Race Condition Testing](#race-condition-testing)
- [Integration Testing](#integration-testing)
- [Performance Testing](#performance-testing)
- [Mock Usage Patterns](#mock-usage-patterns)
- [Test Utilities](#test-utilities)
- [Common Test Failures](#common-test-failures)

## Testing Philosophy

Litestream testing follows these principles:

1. **Test at Multiple Levels**: Unit, integration, and end-to-end
2. **Focus on Edge Cases**: Especially >1GB databases and eventual consistency
3. **Use Real SQLite**: Avoid mocking SQLite behavior
4. **Race Detection**: Always run with `-race` flag
5. **Deterministic Tests**: Use fixed seeds and timestamps where possible

## 1GB Database Testing

### The Lock Page Problem

SQLite reserves a special lock page at exactly 1GB (0x40000000 bytes). This page cannot contain data and must be skipped during replication.

### Test Requirements

#### Creating Test Databases

```bash
# Use litestream-test tool for large databases
./bin/litestream-test populate \
    -db test.db \
    -target-size 1.5GB \
    -page-size 4096
```

#### Manual Test Database Creation

```go
func createLargeTestDB(t *testing.T, path string, targetSize int64) {
    db, err := sql.Open("sqlite", path+"?_journal=WAL")
    require.NoError(t, err)
    defer db.Close()

    // Set page size
    _, err = db.Exec("PRAGMA page_size = 4096")
    require.NoError(t, err)

    // Create test table
    _, err = db.Exec(`
        CREATE TABLE test_data (
            id INTEGER PRIMARY KEY,
            data BLOB NOT NULL
        )
    `)
    require.NoError(t, err)

    // Calculate rows needed
    rowSize := 4000 // bytes per row
    rowsNeeded := targetSize / int64(rowSize)

    // Batch insert for performance
    tx, err := db.Begin()
    require.NoError(t, err)

    stmt, err := tx.Prepare("INSERT INTO test_data (data) VALUES (?)")
    require.NoError(t, err)

    for i := int64(0); i < rowsNeeded; i++ {
        data := make([]byte, rowSize)
        rand.Read(data)
        _, err = stmt.Exec(data)
        require.NoError(t, err)

        // Commit periodically
        if i%1000 == 0 {
            err = tx.Commit()
            require.NoError(t, err)
            tx, err = db.Begin()
            require.NoError(t, err)
            stmt, err = tx.Prepare("INSERT INTO test_data (data) VALUES (?)")
            require.NoError(t, err)
        }
    }

    err = tx.Commit()
    require.NoError(t, err)

    // Verify size
    var pageCount, pageSize int
    db.QueryRow("PRAGMA page_count").Scan(&pageCount)
    db.QueryRow("PRAGMA page_size").Scan(&pageSize)

    actualSize := int64(pageCount * pageSize)
    t.Logf("Created database: %d bytes (%d pages of %d bytes)",
        actualSize, pageCount, pageSize)

    // Verify lock page is in range
    lockPgno := ltx.LockPgno(pageSize)
    if pageCount > lockPgno {
        t.Logf("Database spans lock page at page %d", lockPgno)
    }
}
```

#### Lock Page Test Cases

```go
func TestDB_LockPageHandling(t *testing.T) {
    testCases := []struct {
        name     string
        pageSize int
        lockPgno uint32
    }{
        {"4KB pages", 4096, 262145},
        {"8KB pages", 8192, 131073},
        {"16KB pages", 16384, 65537},
        {"32KB pages", 32768, 32769},
    }

    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            // Create database larger than 1GB
            dbPath := filepath.Join(t.TempDir(), "test.db")
            createLargeTestDB(t, dbPath, 1100*1024*1024) // 1.1GB

            // Open with Litestream
            db := litestream.NewDB(dbPath)
            err := db.Open()
            require.NoError(t, err)
            defer db.Close(context.Background())

            // Start replication
            replica := litestream.NewReplicaWithClient(db, newMockClient())
            err = replica.Start(context.Background())
            require.NoError(t, err)

            // Perform writes that span the lock page
            conn, err := sql.Open("sqlite", dbPath)
            require.NoError(t, err)

            tx, err := conn.Begin()
            require.NoError(t, err)

            // Write data around lock page boundary
            for i := tc.lockPgno - 10; i < tc.lockPgno+10; i++ {
                if i == tc.lockPgno {
                    continue // Skip lock page
                }

                _, err = tx.Exec(fmt.Sprintf(
                    "INSERT INTO test_data (id, data) VALUES (%d, randomblob(4000))",
                    i))
                require.NoError(t, err)
            }

            err = tx.Commit()
            require.NoError(t, err)

            // Wait for sync
            err = db.Sync(context.Background())
            require.NoError(t, err)

            // Verify replication skipped lock page
            verifyLockPageSkipped(t, replica, tc.lockPgno)
        })
    }
}

func verifyLockPageSkipped(t *testing.T, replica *litestream.Replica, lockPgno uint32) {
    // Get LTX files
    files, err := replica.Client.LTXFiles(context.Background(), 0, 0, false)
    require.NoError(t, err)

    // Check each file
    for files.Next() {
        info := files.Item()

        // Read page index
        pageIndex, err := litestream.FetchPageIndex(context.Background(),
            replica.Client, info)
        require.NoError(t, err)

        // Verify lock page not present
        _, hasLockPage := pageIndex[lockPgno]
        assert.False(t, hasLockPage,
            "Lock page %d should not be in LTX file", lockPgno)
    }
}
```

### Restoration Testing

```go
func TestDB_RestoreLargeDatabase(t *testing.T) {
    // Create and replicate large database
    srcPath := filepath.Join(t.TempDir(), "source.db")
    createLargeTestDB(t, srcPath, 1500*1024*1024) // 1.5GB

    // Setup replication
    db := litestream.NewDB(srcPath)
    err := db.Open()
    require.NoError(t, err)

    client := file.NewReplicaClient(filepath.Join(t.TempDir(), "replica"))
    replica := litestream.NewReplicaWithClient(db, client)

    err = replica.Start(context.Background())
    require.NoError(t, err)

    // Let it replicate
    err = db.Sync(context.Background())
    require.NoError(t, err)

    db.Close(context.Background())

    // Restore to new location
    dstPath := filepath.Join(t.TempDir(), "restored.db")
    err = litestream.Restore(context.Background(), client, dstPath, nil)
    require.NoError(t, err)

    // Verify restoration
    verifyDatabasesMatch(t, srcPath, dstPath)
}

func verifyDatabasesMatch(t *testing.T, path1, path2 string) {
    // Compare checksums
    checksum1 := calculateDBChecksum(t, path1)
    checksum2 := calculateDBChecksum(t, path2)
    assert.Equal(t, checksum1, checksum2, "Database checksums should match")

    // Compare page counts
    pageCount1 := getPageCount(t, path1)
    pageCount2 := getPageCount(t, path2)
    assert.Equal(t, pageCount1, pageCount2, "Page counts should match")

    // Run integrity check
    db, err := sql.Open("sqlite", path2)
    require.NoError(t, err)
    defer db.Close()

    var result string
    err = db.QueryRow("PRAGMA integrity_check").Scan(&result)
    require.NoError(t, err)
    assert.Equal(t, "ok", result, "Integrity check should pass")
}
```

## Race Condition Testing

### Running with Race Detector

```bash
# Always run tests with race detector
go test -race -v ./...

# Run specific race-prone tests
go test -race -v -run TestReplica_Sync ./...
go test -race -v -run TestDB_Sync ./...
go test -race -v -run TestStore_CompactDB ./...
```

### Common Race Conditions

#### 1. Position Updates

```go
func TestReplica_ConcurrentPositionUpdate(t *testing.T) {
    replica := litestream.NewReplica(nil)
    var wg sync.WaitGroup
    errors := make(chan error, 100)

    // Concurrent writers
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(n int) {
            defer wg.Done()

            pos := ltx.NewPos(ltx.TXID(n), ltx.Checksum(uint64(n)))

            // This should use proper locking
            replica.SetPos(pos)

            // Verify position
            readPos := replica.Pos()
            if readPos.TXID < ltx.TXID(n) {
                errors <- fmt.Errorf("position went backwards")
            }
        }(i)
    }

    // Concurrent readers
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()

            for j := 0; j < 100; j++ {
                _ = replica.Pos()
                time.Sleep(time.Microsecond)
            }
        }()
    }

    wg.Wait()
    close(errors)

    for err := range errors {
        t.Error(err)
    }
}
```

#### 2. WAL Monitoring

```go
func TestDB_ConcurrentWALAccess(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close(context.Background())

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    var wg sync.WaitGroup

    // Writer goroutine
    wg.Add(1)
    go func() {
        defer wg.Done()

        conn, err := sql.Open("sqlite", db.Path())
        if err != nil {
            return
        }
        defer conn.Close()

        for i := 0; i < 100; i++ {
            _, _ = conn.Exec("INSERT INTO test VALUES (?)", i)
            time.Sleep(10 * time.Millisecond)
        }
    }()

    // Monitor goroutine
    wg.Add(1)
    go func() {
        defer wg.Done()

        notifyCh := db.Notify()

        for {
            select {
            case <-ctx.Done():
                return
            case <-notifyCh:
                // Process WAL changes
                _ = db.Sync(context.Background())
            }
        }
    }()

    // Checkpoint goroutine
    wg.Add(1)
    go func() {
        defer wg.Done()

        ticker := time.NewTicker(100 * time.Millisecond)
        defer ticker.Stop()

        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                _ = db.Checkpoint(context.Background(), litestream.CheckpointModePassive)
            }
        }
    }()

    wg.Wait()
}
```

### Test Cleanup

```go
func TestStore_Integration(t *testing.T) {
    // Setup
    tmpDir := t.TempDir()
    db := setupTestDB(t, tmpDir)

    // Use defer with error channel for cleanup
    insertErr := make(chan error, 1)

    // Cleanup function
    cleanup := func() {
        select {
        case err := <-insertErr:
            if err != nil {
                t.Errorf("insert error during test: %v", err)
            }
        default:
        }
    }
    defer cleanup()

    // Test with timeout
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    // Run test...
}
```

## Integration Testing

### Test Structure

```go
// +build integration

package litestream_test

import (
    "context"
    "os"
    "testing"
)

func TestIntegration_S3(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }

    // Check for credentials
    if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
        t.Skip("AWS_ACCESS_KEY_ID not set")
    }

    // Run test against real S3
    runIntegrationTest(t, setupS3Client())
}

func runIntegrationTest(t *testing.T, client ReplicaClient) {
    ctx := context.Background()

    t.Run("BasicReplication", func(t *testing.T) {
        // Test basic write/read cycle
    })

    t.Run("Compaction", func(t *testing.T) {
        // Test compaction with remote storage
    })

    t.Run("EventualConsistency", func(t *testing.T) {
        // Test handling of eventual consistency
    })

    t.Run("LargeFiles", func(t *testing.T) {
        // Test with files > 100MB
    })

    t.Run("Cleanup", func(t *testing.T) {
        err := client.DeleteAll(ctx)
        require.NoError(t, err)
    })
}
```

### Environment-Based Configuration

```go
func setupS3Client() *s3.ReplicaClient {
    return &s3.ReplicaClient{
        AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
        SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
        Region:          getEnvOrDefault("AWS_REGION", "us-east-1"),
        Bucket:          getEnvOrDefault("TEST_S3_BUCKET", "litestream-test"),
        Path:            fmt.Sprintf("test-%d", time.Now().Unix()),
    }
}

func getEnvOrDefault(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}
```

## Performance Testing

### Benchmarks

```go
func BenchmarkDB_Sync(b *testing.B) {
    db := setupBenchDB(b)
    defer db.Close(context.Background())

    // Prepare test data
    conn, _ := sql.Open("sqlite", db.Path())
    defer conn.Close()

    for i := 0; i < 1000; i++ {
        conn.Exec("INSERT INTO test VALUES (?)", i)
    }

    b.ResetTimer()

    for i := 0; i < b.N; i++ {
        err := db.Sync(context.Background())
        if err != nil {
            b.Fatal(err)
        }
    }

    b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "syncs/sec")
}

func BenchmarkCompaction(b *testing.B) {
    benchmarks := []struct {
        name      string
        fileCount int
        fileSize  int
    }{
        {"Small-Many", 1000, 1024},        // Many small files
        {"Medium", 100, 10 * 1024},        // Medium files
        {"Large-Few", 10, 100 * 1024},     // Few large files
    }

    for _, bm := range benchmarks {
        b.Run(bm.name, func(b *testing.B) {
            for i := 0; i < b.N; i++ {
                b.StopTimer()
                files := generateTestFiles(bm.fileCount, bm.fileSize)
                b.StartTimer()

                _, err := compact(files)
                if err != nil {
                    b.Fatal(err)
                }
            }

            totalSize := int64(bm.fileCount * bm.fileSize)
            b.ReportMetric(float64(totalSize)/float64(b.Elapsed().Nanoseconds()), "bytes/ns")
        })
    }
}
```

### Load Testing

```go
func TestDB_LoadTest(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping load test")
    }

    db := setupTestDB(t)
    defer db.Close(context.Background())

    // Configure load
    config := LoadConfig{
        Duration:      5 * time.Minute,
        WriteRate:     100,  // writes/sec
        ReadRate:      500,  // reads/sec
        Workers:       10,
        DataSize:      4096,
        BurstPattern:  true,
    }

    results := runLoadTest(t, db, config)

    // Verify results
    assert.Greater(t, results.TotalWrites, int64(20000))
    assert.Less(t, results.P99Latency, 100*time.Millisecond)
    assert.Zero(t, results.Errors)

    t.Logf("Load test results: %+v", results)
}

type LoadResults struct {
    TotalWrites   int64
    TotalReads    int64
    Errors        int64
    P50Latency    time.Duration
    P99Latency    time.Duration
    BytesReplicated int64
}

func runLoadTest(t *testing.T, db *DB, config LoadConfig) LoadResults {
    ctx, cancel := context.WithTimeout(context.Background(), config.Duration)
    defer cancel()

    var results LoadResults
    var mu sync.Mutex
    latencies := make([]time.Duration, 0, 100000)

    // Start workers
    var wg sync.WaitGroup
    for i := 0; i < config.Workers; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()

            conn, err := sql.Open("sqlite", db.Path())
            if err != nil {
                return
            }
            defer conn.Close()

            ticker := time.NewTicker(time.Second / time.Duration(config.WriteRate))
            defer ticker.Stop()

            for {
                select {
                case <-ctx.Done():
                    return
                case <-ticker.C:
                    start := time.Now()
                    data := make([]byte, config.DataSize)
                    rand.Read(data)

                    _, err := conn.Exec("INSERT INTO test (data) VALUES (?)", data)
                    latency := time.Since(start)

                    mu.Lock()
                    if err != nil {
                        results.Errors++
                    } else {
                        results.TotalWrites++
                        latencies = append(latencies, latency)
                    }
                    mu.Unlock()
                }
            }
        }(i)
    }

    wg.Wait()

    // Calculate percentiles
    sort.Slice(latencies, func(i, j int) bool {
        return latencies[i] < latencies[j]
    })

    if len(latencies) > 0 {
        results.P50Latency = latencies[len(latencies)*50/100]
        results.P99Latency = latencies[len(latencies)*99/100]
    }

    return results
}
```

## Mock Usage Patterns

### Mock ReplicaClient

```go
type MockReplicaClient struct {
    mu    sync.Mutex
    files map[string]*ltx.FileInfo
    data  map[string][]byte

    // Control behavior
    FailureRate   float64
    Latency       time.Duration
    EventualDelay time.Duration
}

func (m *MockReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
    // Simulate latency
    if m.Latency > 0 {
        time.Sleep(m.Latency)
    }

    // Simulate failures
    if m.FailureRate > 0 && rand.Float64() < m.FailureRate {
        return nil, errors.New("simulated failure")
    }

    // Simulate eventual consistency
    if m.EventualDelay > 0 {
        time.AfterFunc(m.EventualDelay, func() {
            m.mu.Lock()
            defer m.mu.Unlock()
            // Make file available after delay
        })
    }

    // Store file
    data, err := io.ReadAll(r)
    if err != nil {
        return nil, err
    }

    m.mu.Lock()
    defer m.mu.Unlock()

    key := fmt.Sprintf("%d-%016x-%016x", level, minTXID, maxTXID)
    info := &ltx.FileInfo{
        Level:     level,
        MinTXID:   minTXID,
        MaxTXID:   maxTXID,
        Size:      int64(len(data)),
        CreatedAt: time.Now(),
    }

    m.files[key] = info
    m.data[key] = data

    return info, nil
}
```

### Mock Database

```go
type MockDB struct {
    mu       sync.Mutex
    path     string
    replicas []*Replica
    closed   bool

    // Control behavior
    CheckpointFailures int
    SyncDelay         time.Duration
}

func (m *MockDB) Sync(ctx context.Context) error {
    if m.SyncDelay > 0 {
        select {
        case <-time.After(m.SyncDelay):
        case <-ctx.Done():
            return ctx.Err()
        }
    }

    m.mu.Lock()
    defer m.mu.Unlock()

    if m.closed {
        return errors.New("database closed")
    }

    for _, r := range m.replicas {
        if err := r.Sync(ctx); err != nil {
            return err
        }
    }

    return nil
}
```

## Test Utilities

### Helper Functions

```go
// testutil/db.go
package testutil

import (
    "database/sql"
    "testing"
    "path/filepath"
)

func NewTestDB(t testing.TB) *litestream.DB {
    t.Helper()

    path := filepath.Join(t.TempDir(), "test.db")

    // Create SQLite database
    conn, err := sql.Open("sqlite", path+"?_journal=WAL")
    require.NoError(t, err)

    _, err = conn.Exec(`
        CREATE TABLE test (
            id INTEGER PRIMARY KEY,
            data BLOB
        )
    `)
    require.NoError(t, err)
    conn.Close()

    // Open with Litestream
    db := litestream.NewDB(path)
    db.MonitorInterval = 10 * time.Millisecond  // Speed up for tests
    db.MinCheckpointPageN = 100  // Lower threshold for tests

    err = db.Open()
    require.NoError(t, err)

    t.Cleanup(func() {
        db.Close(context.Background())
    })

    return db
}

func WriteTestData(t testing.TB, db *litestream.DB, count int) {
    t.Helper()

    conn, err := sql.Open("sqlite", db.Path())
    require.NoError(t, err)
    defer conn.Close()

    tx, err := conn.Begin()
    require.NoError(t, err)

    for i := 0; i < count; i++ {
        data := make([]byte, 100)
        rand.Read(data)
        _, err = tx.Exec("INSERT INTO test (data) VALUES (?)", data)
        require.NoError(t, err)
    }

    err = tx.Commit()
    require.NoError(t, err)
}
```

### Test Fixtures

```go
// testdata/fixtures.go
package testdata

import _ "embed"

//go:embed small.db
var SmallDB []byte

//go:embed large.db
var LargeDB []byte

//go:embed corrupted.db
var CorruptedDB []byte

func ExtractFixture(name string, path string) error {
    var data []byte

    switch name {
    case "small":
        data = SmallDB
    case "large":
        data = LargeDB
    case "corrupted":
        data = CorruptedDB
    default:
        return fmt.Errorf("unknown fixture: %s", name)
    }

    return os.WriteFile(path, data, 0600)
}
```

## Common Test Failures

### 1. Database Locked Errors

```go
// Problem: Multiple connections without proper WAL mode
func TestBroken(t *testing.T) {
    db1, _ := sql.Open("sqlite", "test.db")    // Wrong! WAL disabled
    db2, _ := sql.Open("sqlite", "test.db")    // Will fail
}

// Solution: Use WAL mode
func TestFixed(t *testing.T) {
    db1, _ := sql.Open("sqlite", "test.db?_journal=WAL")
    db2, _ := sql.Open("sqlite", "test.db?_journal=WAL")
}
```

### 2. Timing Issues

```go
// Problem: Race between write and sync
func TestBroken(t *testing.T) {
    WriteData(db)
    result := ReadReplica()  // May not see data yet!
}

// Solution: Explicit sync
func TestFixed(t *testing.T) {
    WriteData(db)
    err := db.Sync(context.Background())
    require.NoError(t, err)
    result := ReadReplica()  // Now guaranteed to see data
}
```

### 3. Cleanup Issues

```go
// Problem: Goroutine outlives test
func TestBroken(t *testing.T) {
    go func() {
        time.Sleep(10 * time.Second)
        doWork()  // Test already finished!
    }()
}

// Solution: Use context and wait
func TestFixed(t *testing.T) {
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

    // Test work...

    cancel()  // Signal shutdown
    wg.Wait() // Wait for goroutine
}
```

### 4. File Handle Leaks

```go
// Problem: Not closing files
func TestBroken(t *testing.T) {
    f, _ := os.Open("test.db")
    // Missing f.Close()!
}

// Solution: Always use defer
func TestFixed(t *testing.T) {
    f, err := os.Open("test.db")
    require.NoError(t, err)
    defer f.Close()
}
```

## Test Coverage

### Running Coverage

```bash
# Generate coverage report
go test -coverprofile=coverage.out ./...

# View coverage in browser
go tool cover -html=coverage.out

# Check coverage percentage
go tool cover -func=coverage.out | grep total

# Coverage by package
go test -cover ./...
```

### Coverage Requirements

- Core packages (`db.go`, `replica.go`, `store.go`): >80%
- Replica clients: >70%
- Utilities: >60%
- Mock implementations: Not required

### Improving Coverage

```go
// Use test tables for comprehensive coverage
func TestDB_Checkpoint(t *testing.T) {
    tests := []struct {
        name     string
        mode     string
        walSize  int
        wantErr  bool
    }{
        {"Passive", "PASSIVE", 100, false},
        {"Full", "FULL", 1000, false},
        {"Restart", "RESTART", 5000, false},
        {"Truncate", "TRUNCATE", 10000, false},
        {"Invalid", "INVALID", 100, true},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            db := setupTestDB(t)
            generateWAL(t, db, tt.walSize)

            err := db.Checkpoint(tt.mode)
            if tt.wantErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
            }
        })
    }
}
```
