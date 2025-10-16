# Integration Tests

Go-based integration tests for Litestream. These tests replace the previous bash-based test scripts with proper Go testing infrastructure.

## Overview

This package contains comprehensive integration tests organized by test type:

- **scenario_test.go** - Core functionality scenarios (fresh start, integrity, deletion, failover)
- **concurrent_test.go** - Concurrency and stress tests (rapid checkpoints, WAL growth, concurrent ops, busy timeout)
- **quick_test.go** - Quick validation tests (30 minutes configurable)
- **overnight_test.go** - Long-running stability tests (8+ hours)
- **boundary_test.go** - Edge cases (1GB boundary, different page sizes)
- **helpers.go** - Shared test utilities and helpers
- **fixtures.go** - Test data generators and scenarios

## Prerequisites

Build the required binaries:

```bash
go build -o bin/litestream ./cmd/litestream
go build -o bin/litestream-test ./cmd/litestream-test
```

## Running Tests

### Quick Tests (Default)

Run fast integration tests suitable for CI:

```bash
go test -v -tags=integration -timeout=30m ./tests/integration/... \
  -run="TestFreshStart|TestDatabaseIntegrity|TestRapidCheckpoints"
```

### All Scenario Tests

Run all scenario tests (excluding long-running):

```bash
go test -v -tags=integration -timeout=1h ./tests/integration/...
```

### Long-Running Tests

Run overnight and boundary tests:

```bash
go test -v -tags="integration,long" -timeout=10h ./tests/integration/... \
  -run="TestOvernight|Test1GBBoundary"
```

### Specific Tests

Run individual test functions:

```bash
# Fresh start test
go test -v -tags=integration ./tests/integration/... -run=TestFreshStart

# Rapid checkpoints test
go test -v -tags=integration ./tests/integration/... -run=TestRapidCheckpoints

# 1GB boundary test
go test -v -tags=integration ./tests/integration/... -run=Test1GBBoundary
```

### Short Mode

Run abbreviated versions with `-short`:

```bash
go test -v -tags=integration -short ./tests/integration/...
```

This reduces test durations by 10x (e.g., 8 hours becomes 48 minutes).

## Test Categories

### Scenario Tests

Core functionality tests that run in seconds to minutes:

- `TestFreshStart` - Starting replication before database exists
- `TestDatabaseIntegrity` - Complex schema and data integrity
- `TestDatabaseDeletion` - Source database deletion during replication
- `TestReplicaFailover` - Multiple replica configuration and failover

### Concurrent Tests

Stress and concurrency tests:

- `TestRapidCheckpoints` - Rapid checkpoint operations under load
- `TestWALGrowth` - Large WAL file handling (100MB+)
- `TestConcurrentOperations` - Multiple databases replicating simultaneously
- `TestBusyTimeout` - Database busy timeout and lock handling

### Quick Tests

Configurable duration validation (default 30 minutes):

- `TestQuickValidation` - Comprehensive validation with wave pattern load

### Overnight Tests

Long-running stability tests (default 8 hours):

- `TestOvernightFile` - 8-hour file-based replication test
- `TestOvernightComprehensive` - 8-hour comprehensive test with large database

### Boundary Tests

Edge case and boundary condition tests:

- `Test1GBBoundary` - SQLite 1GB lock page boundary (page #262145 with 4KB pages)
- `TestLockPageWithDifferentPageSizes` - Lock page handling with various page sizes

## CI Integration

### Automatic (Pull Requests)

Quick tests run automatically on PRs modifying Go code:

```yaml
- Quick integration tests (TestFreshStart, TestDatabaseIntegrity, TestRapidCheckpoints)
- Timeout: 30 minutes
```

### Manual Workflows

Trigger via GitHub Actions UI:

**Quick Tests:**
```
workflow_dispatch → test_type: quick
```

**All Scenario Tests:**
```
workflow_dispatch → test_type: all
```

**Long-Running Tests:**
```
workflow_dispatch → test_type: long
```

## Test Infrastructure

### Helpers (helpers.go)

- `SetupTestDB(t, name)` - Create test database instance
- `TestDB.Create()` - Create database with WAL mode
- `TestDB.Populate(size)` - Populate to target size
- `TestDB.StartLitestream()` - Start replication
- `TestDB.StopLitestream()` - Stop replication
- `TestDB.Restore(path)` - Restore from replica
- `TestDB.Validate(path)` - Full validation (integrity, checksum, data)
- `TestDB.QuickValidate(path)` - Quick validation
- `TestDB.GenerateLoad(...)` - Generate database load
- `GetTestDuration(t, default)` - Get configurable test duration
- `RequireBinaries(t)` - Check for required binaries

### Fixtures (fixtures.go)

- `DefaultLoadConfig()` - Load generation configuration
- `DefaultPopulateConfig()` - Database population configuration
- `CreateComplexTestSchema(db)` - Multi-table schema with foreign keys
- `PopulateComplexTestData(db, ...)` - Populate complex data
- `LargeWALScenario()` - Large WAL test scenario
- `RapidCheckpointsScenario()` - Rapid checkpoint scenario

## Test Artifacts

Tests create temporary directories via `t.TempDir()`:

```
/tmp/<test-temp-dir>/
├── <name>.db          # Test database
├── <name>.db-wal      # WAL file
├── <name>.db-shm      # Shared memory
├── replica/           # Replica directory
│   └── ltx/0/        # LTX files
├── litestream.log     # Litestream output
└── *-restored.db      # Restored databases
```

Artifacts are automatically cleaned up after tests complete.

## Debugging Tests

### View Litestream Logs

```go
log, err := db.GetLitestreamLog()
fmt.Println(log)
```

### Check for Errors

```go
errors, err := db.CheckForErrors()
for _, e := range errors {
    t.Logf("Error: %s", e)
}
```

### Inspect Replica

```go
fileCount, _ := db.GetReplicaFileCount()
t.Logf("LTX files: %d", fileCount)
```

### Check Database Size

```go
size, _ := db.GetDatabaseSize()
t.Logf("DB size: %.2f MB", float64(size)/(1024*1024))
```

## Migration from Bash

These Go tests replace the following bash scripts:

### Migrated Scripts

- `scripts/test-quick-validation.sh` → `quick_test.go::TestQuickValidation`
- `scripts/test-overnight.sh` → `overnight_test.go::TestOvernightFile`
- `cmd/litestream-test/scripts/test-fresh-start.sh` → `scenario_test.go::TestFreshStart`
- `cmd/litestream-test/scripts/test-database-integrity.sh` → `scenario_test.go::TestDatabaseIntegrity`
- `cmd/litestream-test/scripts/test-database-deletion.sh` → `scenario_test.go::TestDatabaseDeletion`
- `cmd/litestream-test/scripts/test-replica-failover.sh` → `scenario_test.go::TestReplicaFailover`
- `cmd/litestream-test/scripts/test-rapid-checkpoints.sh` → `concurrent_test.go::TestRapidCheckpoints`
- `cmd/litestream-test/scripts/test-wal-growth.sh` → `concurrent_test.go::TestWALGrowth`
- `cmd/litestream-test/scripts/test-concurrent-operations.sh` → `concurrent_test.go::TestConcurrentOperations`
- `cmd/litestream-test/scripts/test-busy-timeout.sh` → `concurrent_test.go::TestBusyTimeout`
- `cmd/litestream-test/scripts/test-1gb-boundary.sh` → `boundary_test.go::Test1GBBoundary`

### Pending Migration

Future PRs will add:
- S3/retention tests (requires S3 mock setup)
- Format isolation and upgrade tests
- Bug reproduction tests

## Benefits Over Bash

1. **Type Safety** - Compile-time error checking
2. **Better Debugging** - Use standard Go debugging tools
3. **Code Reuse** - Shared helpers and fixtures
4. **Parallel Execution** - Tests can run concurrently
5. **CI Integration** - Run automatically on PRs
6. **Test Coverage** - Measure code coverage
7. **Consistent Patterns** - Standard Go testing conventions
8. **Better Error Messages** - Structured, clear reporting
9. **Platform Independent** - Works on Linux, macOS, Windows
10. **IDE Integration** - Full editor support

## Contributing

When adding new integration tests:

1. Use appropriate build tags (`//go:build integration` or `//go:build integration && long`)
2. Call `RequireBinaries(t)` to check prerequisites
3. Use `SetupTestDB(t, name)` for test setup
4. Call `defer db.Cleanup()` for automatic cleanup
5. Log test progress with descriptive messages
6. Use `GetTestDuration(t, default)` for configurable durations
7. Add test to CI workflow if appropriate
8. Update this README with new test documentation

## Related Documentation

- [cmd/litestream-test README](../../cmd/litestream-test/README.md) - Testing harness CLI
- [scripts/README.md](../../scripts/README.md) - Legacy bash test scripts
- [GitHub Issue #798](https://github.com/benbjohnson/litestream/issues/798) - Migration tracking
