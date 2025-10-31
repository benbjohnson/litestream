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

This is part of an ongoing effort to migrate bash test scripts to Go integration tests. This migration improves maintainability, enables CI integration, and provides platform independence.

### Test Directory Organization

Two distinct script directories serve different purposes:

**`scripts/` (top-level)** - Long-running **soak tests** (2-8 hours) for pre-release validation:
- File-based replication: `test-quick-validation.sh`, `test-overnight.sh`, `test-comprehensive.sh`
- S3 replication: `test-minio-s3.sh`, `test-overnight-s3.sh`
- Utilities: `analyze-test-results.sh`, `setup-homebrew-tap.sh`

**`cmd/litestream-test/scripts/`** - Scenario and debugging tests (seconds to ~30 minutes):
- Bug reproduction scripts for specific issues (#752, #754)
- Format & upgrade tests for version compatibility
- S3 retention tests with Python mock
- Quick validation and setup utilities

### Migration Status

**Migrated from `scripts/` (5 scripts):**
- `test-quick-validation.sh` → `quick_test.go::TestQuickValidation` (CI: ✅)
- `test-overnight.sh` → `overnight_test.go::TestOvernightFile` (CI: ❌ too long)
- `test-comprehensive.sh` → `comprehensive_soak_test.go::TestComprehensiveSoak` (CI: ❌ soak test)
- `test-minio-s3.sh` → `minio_soak_test.go::TestMinIOSoak` (CI: ❌ soak test, requires Docker)
- `test-overnight-s3.sh` → `overnight_s3_soak_test.go::TestOvernightS3Soak` (CI: ❌ soak test, 8 hours)

**Migrated from `cmd/litestream-test/scripts/` (9 scripts):**
- `test-fresh-start.sh` → `scenario_test.go::TestFreshStart`
- `test-database-integrity.sh` → `scenario_test.go::TestDatabaseIntegrity`
- `test-database-deletion.sh` → `scenario_test.go::TestDatabaseDeletion`
- `test-replica-failover.sh` → NOT MIGRATED (feature removed from Litestream)
- `test-rapid-checkpoints.sh` → `concurrent_test.go::TestRapidCheckpoints`
- `test-wal-growth.sh` → `concurrent_test.go::TestWALGrowth`
- `test-concurrent-operations.sh` → `concurrent_test.go::TestConcurrentOperations`
- `test-busy-timeout.sh` → `concurrent_test.go::TestBusyTimeout`
- `test-1gb-boundary.sh` → `boundary_test.go::Test1GBBoundary`

**Remaining Bash Scripts:**

_scripts/_ (2 scripts remaining):
- `analyze-test-results.sh` - Post-test analysis utility (may stay as bash)
- `setup-homebrew-tap.sh` - Packaging script (not a test)

_cmd/litestream-test/scripts/_ (16 scripts remaining):
- Bug reproduction scripts: `reproduce-critical-bug.sh`, `test-754-*.sh`, `test-v0.5-*.sh`
- Format & upgrade tests: `test-format-isolation.sh`, `test-upgrade-*.sh`, `test-massive-upgrade.sh`
- S3 retention tests: `test-s3-retention-*.sh` (4 scripts, use Python S3 mock)
- Utility: `verify-test-setup.sh`

### Why Some Tests Aren't in CI

Per industry best practices, CI tests should complete in < 1 hour (ideally < 10 minutes):
- ✅ **Quick tests** (< 5 min) - Run on every PR
- ❌ **Soak tests** (2-8 hours) - Run locally before releases only
- ❌ **Long-running tests** (> 30 min) - Too slow for CI feedback loop

Soak tests are migrated to Go for maintainability but run **locally only**. See "Soak Tests" section below.

## Soak Tests (Long-Running Stability Tests)

Soak tests run for 2-8 hours to validate long-term stability under sustained load. These tests are **NOT run in CI** per industry best practices (effective CI requires tests to complete in < 1 hour).

### Purpose

Soak tests validate:
- Long-term replication stability
- Memory leak detection over time
- Compaction effectiveness across multiple cycles
- Checkpoint behavior under sustained load
- Recovery from transient issues
- Storage growth patterns

### When to Run Soak Tests

- ✅ Before major releases
- ✅ After significant replication changes
- ✅ To reproduce stability issues
- ✅ For performance benchmarking
- ❌ NOT on every commit (too slow for CI)

### Running Soak Tests Locally

**File-based comprehensive test (2 hours):**
```bash
go test -v -tags="integration,soak" -timeout=3h -run=TestComprehensiveSoak ./tests/integration/
```

**MinIO S3 test (2 hours, requires Docker):**
```bash
# Ensure Docker is running
go test -v -tags="integration,soak,docker" -timeout=3h -run=TestMinIOSoak ./tests/integration/
```

**Overnight S3 test (8 hours, requires AWS):**
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export S3_BUCKET=your-test-bucket
export AWS_REGION=us-east-1

go test -v -tags="integration,soak,aws" -timeout=10h -run=TestOvernightS3Soak ./tests/integration/
```

**Run all soak tests:**
```bash
go test -v -tags="integration,soak,docker,aws" -timeout=15h ./tests/integration/
```

### Adjust Duration for Testing

Tests respect the `-test.short` flag to run abbreviated versions:

```bash
# Run comprehensive test for 30 minutes instead of 2 hours
go test -v -tags="integration,soak" -timeout=1h -run=TestComprehensiveSoak ./tests/integration/ -test.short
```

You can also set `TEST_DURATION` environment variable:

```bash
# Run for custom duration
TEST_DURATION=30m go test -v -tags="integration,soak" -timeout=1h -run=TestComprehensiveSoak ./tests/integration/
```

### Soak Test Build Tags

Soak tests use multiple build tags to control execution:

- `integration` - Required for all integration tests
- `soak` - Marks long-running stability tests (2-8 hours)
- `docker` - Requires Docker (MinIO test)
- `aws` - Requires AWS credentials (S3 tests)

### Monitoring Soak Tests

All soak tests log progress every 60 seconds:

```bash
# Watch test progress in real-time
go test -v -tags="integration,soak" -run=TestComprehensiveSoak ./tests/integration/ 2>&1 | tee soak-test.log
```

Metrics reported during execution:
- Database size and WAL size
- Row count
- Replica statistics (snapshots, LTX segments)
- Operation counts (checkpoints, compactions, syncs)
- Error counts
- Write rate

### Soak Test Summary

| Test | Duration | Requirements | What It Tests |
|------|----------|--------------|---------------|
| TestComprehensiveSoak | 2h | None | File-based replication with aggressive compaction |
| TestMinIOSoak | 2h | Docker | S3-compatible storage via MinIO container |
| TestOvernightS3Soak | 8h | AWS credentials | Real S3 replication, overnight stability |

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
