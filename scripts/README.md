# Utility Scripts

Utility scripts for Litestream testing and distribution.

## Overview

This directory contains utility scripts for post-test analysis and packaging. All long-running soak tests have been migrated to Go integration tests in `tests/integration/`.

> **Note:** For all soak tests (2-8 hours), see the Go-based test suite in [tests/integration/](../tests/integration/README.md). The bash soak tests have been migrated to Go for better maintainability and cross-platform support

## Prerequisites

```bash
go build -o bin/litestream ./cmd/litestream
go build -o bin/litestream-test ./cmd/litestream-test
```

## Available Scripts

### analyze-test-results.sh

Post-test analysis tool for examining overnight test results.

```bash
./scripts/analyze-test-results.sh /tmp/litestream-overnight-<timestamp>
```

**Analyzes:**
1. Test duration and timeline
2. Compaction statistics
3. Checkpoint frequency
4. Error analysis and categorization
5. Performance metrics
6. Database growth patterns
7. Replica file statistics
8. Final validation results

**Output:**
- Analysis report written to `<test-dir>/analysis-report.txt`
- Console summary with key findings
- Error categorization and counts
- Performance statistics

**What it Reports:**
- Total test duration
- Number of compactions by interval
- Checkpoint count and frequency
- Error types and severity
- Database size growth
- WAL file patterns
- Replica size and file counts
- Success/failure summary

**Use Cases:**
- Post-overnight-test analysis
- Comparing test runs
- Identifying performance trends
- Debugging test failures
- Documenting test results

### setup-homebrew-tap.sh

Homebrew tap setup script for packaging and distribution.

```bash
./scripts/setup-homebrew-tap.sh
```

**Purpose:** Automates Homebrew tap setup for Litestream distribution. Not a test script per se, but part of the release process.

## Usage

### Analyzing Test Results

```bash
ls /tmp/litestream-overnight-* -dt | head -1

./scripts/analyze-test-results.sh $(ls /tmp/litestream-overnight-* -dt | head -1)
```

## Test Duration Guide

| Duration | Use Case | Test Type | Expected Results |
|----------|----------|-----------|------------------|
| 5 minutes | CI/CD smoke test | Go integration tests | Basic functionality |
| 30 minutes | Short integration | Go integration tests | Pattern detection |
| 2-8 hours | Soak testing | Go soak tests (local only) | Full validation |

> **Note:** All soak tests are now Go-based in `tests/integration/`. See [tests/integration/README.md](../tests/integration/README.md) for details on running comprehensive, MinIO, and overnight S3 soak tests.

## Monitoring and Debugging

### Real-time Monitoring

All tests create timestamped directories in `/tmp/`:

```bash
LATEST=$(ls /tmp/litestream-* -dt | head -1)
tail -f $LATEST/logs/monitor.log
tail -f $LATEST/logs/litestream.log
```

### Key Metrics to Watch

**Database Growth:**
- Should grow steadily
- WAL file size should cycle (grow, checkpoint, reset)

**Replica Statistics:**
- Snapshot count should increase over time
- LTX file count should grow then stabilize (compaction)
- Replica size should be similar to database size

**Operations:**
- Compactions should occur at scheduled intervals
- Checkpoints should happen regularly
- Sync operations should complete successfully

**Errors:**
- Should be minimal or zero
- Transient errors OK if recovered
- Persistent errors indicate issues

### Common Issues

#### Test Fails to Start

Check binaries:
```bash
ls -la bin/litestream bin/litestream-test
```

Rebuild if needed:
```bash
go build -o bin/litestream ./cmd/litestream
go build -o bin/litestream-test ./cmd/litestream-test
```

#### S3 Test Fails

Verify credentials:
```bash
aws s3 ls s3://$S3_BUCKET/
```

Check environment variables:
```bash
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY
echo $S3_BUCKET
```

#### High Error Counts

Check log for error details:
```bash
grep -i error /tmp/litestream-*/logs/litestream.log | head -20
```

#### Validation Fails

Compare databases manually:
```bash
sqlite3 /tmp/litestream-*/test.db "SELECT COUNT(*) FROM test_data"
sqlite3 /tmp/litestream-*/restored.db "SELECT COUNT(*) FROM test_data"
```

### Stopping Tests Early

Go tests can be interrupted with Ctrl+C. They will cleanup gracefully via defer statements.

## Test Artifacts

All tests create timestamped directories with comprehensive artifacts:

```
/tmp/litestream-overnight-<timestamp>/
├── logs/
│   ├── litestream.log      # Litestream replication log
│   ├── load.log            # Load generator log
│   ├── monitor.log         # Real-time monitoring log
│   ├── populate.log        # Initial population log
│   └── validate.log        # Final validation log
├── test.db                 # Source database
├── test.db-wal             # Write-ahead log
├── test.db-shm             # Shared memory file
├── replica/                # Replica directory (file tests)
│   └── ltx/               # LTX files
└── restored.db            # Restored database for validation
```

## Integration with Go Tests

These utility scripts complement the Go integration test suite:

**Test Locations:**
- `tests/integration/` → All integration and soak tests (Go-based)
- `cmd/litestream-test/scripts/` → Scenario and debugging tests (bash, being phased out)
- `scripts/` → Utilities only (this directory)

**Testing Workflow:**
1. Run quick integration tests during development
2. Run full integration test suite before major changes
3. Run soak tests (2-8h) locally before releases: `TestComprehensiveSoak`, `TestMinIOSoak`, `TestOvernightS3Soak`
4. Analyze results with `analyze-test-results.sh`

## Related Documentation

- [Go Integration Tests](../tests/integration/README.md) - Complete Go-based test suite including soak tests
- [litestream-test CLI Tool](../cmd/litestream-test/README.md) - Testing harness documentation
- [Scenario Test Scripts](../cmd/litestream-test/scripts/README.md) - Focused test scenarios
- [S3 Retention Testing](../cmd/litestream-test/S3-RETENTION-TESTING.md) - S3-specific testing
