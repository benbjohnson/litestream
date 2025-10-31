# Integration Test Scripts

Long-running integration test scripts for comprehensive Litestream validation. These scripts are designed for extended testing scenarios, including overnight tests and production-like workloads.

## Overview

This directory contains integration test scripts that run for extended periods (30 minutes to 8+ hours) to validate Litestream's behavior under sustained load and realistic production scenarios.

**Key Difference from `cmd/litestream-test/scripts/`:**
- **This directory:** Long-running integration tests (minutes to hours)
- **`cmd/litestream-test/scripts/`:** Focused scenario tests (seconds to minutes)

## Prerequisites

```bash
go build -o bin/litestream ./cmd/litestream
go build -o bin/litestream-test ./cmd/litestream-test
```

## Test Scripts

> **Note:** Some tests have been migrated to Go integration tests in `tests/integration/`. See [tests/integration/README.md](../tests/integration/README.md) for the Go-based test suite.

### test-overnight-s3.sh

Comprehensive 8-hour test with S3 replication.

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export S3_BUCKET=your-test-bucket
export AWS_REGION=us-east-1

./scripts/test-overnight-s3.sh
```

**Configuration:**
- Duration: 8 hours
- Database: 100MB initial population
- Write rate: 100 writes/second (higher than file test)
- Pattern: Wave (simulates varying load)
- Payload size: 4KB (larger than file test)
- Workers: 8 (more than file test)
- Replica: S3 bucket with unique timestamped path

**S3-Specific Settings:**
- Force path style: false
- Skip verify: false
- Optional SSE encryption support
- Region configurable via environment

**Features:**
- Higher load than file-based test (S3 can handle more)
- S3 connectivity validation before start
- S3-specific error monitoring (403, 404, 500, 503)
- Upload operation tracking
- S3 object count monitoring
- Restoration from S3 after completion
- Automatic row count comparison

**Real-time Monitoring:**
```bash
tail -f /tmp/litestream-overnight-s3-*/logs/monitor.log
tail -f /tmp/litestream-overnight-s3-*/logs/litestream.log

aws s3 ls s3://your-bucket/litestream-overnight-<timestamp>/ --recursive
```

**What it Tests:**
- S3 replication stability
- Network resilience over 8 hours
- S3 API call efficiency
- Multipart upload handling
- S3-specific error recovery
- Cross-region replication (if configured)
- S3 cost implications (API calls, storage)
- Restoration from cloud storage

**S3 Monitoring Includes:**
- Snapshot count in S3
- WAL segment count in S3
- Total S3 object count
- S3 storage size
- Upload operation count
- S3-specific errors

**Expected Behavior:**
- Successful S3 connectivity throughout
- Regular S3 uploads without failures
- S3 object counts grow over time
- Compaction reduces old S3 objects
- Successful S3 restore at end
- Row count match between source and restored

**Prerequisites:**
- Valid AWS credentials
- S3 bucket with write permissions
- Network connectivity to S3
- AWS CLI installed (for monitoring)

**Cost Considerations:**
- ~8 hours of continuous uploads
- Estimated API calls: Thousands of PUTs/GETs
- Storage: 100MB+ depending on replication
- Consider using a test/dev account

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

## Usage Patterns

### Running Overnight S3 Tests

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export S3_BUCKET=your-test-bucket
export AWS_REGION=us-east-1

./scripts/test-overnight-s3.sh
```

### Analyzing Results

```bash
ls /tmp/litestream-overnight-* -dt | head -1

./scripts/analyze-test-results.sh $(ls /tmp/litestream-overnight-* -dt | head -1)
```

## Test Duration Guide

| Duration | Use Case | Test Type | Expected Results |
|----------|----------|-----------|------------------|
| 5 minutes | CI/CD smoke test | Go integration tests | Basic functionality |
| 30 minutes | Short integration | Go integration tests | Pattern detection |
| 8 hours | Overnight stability | S3 overnight test | Full validation |
| 12+ hours | Stress testing | S3 overnight test | Edge case discovery |

> **Note:** For shorter validation tests, use the Go integration test suite in `tests/integration/`.

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

Tests can be interrupted with Ctrl+C. They will cleanup gracefully:
```bash
./scripts/test-overnight.sh
^C
Cleaning up...
```

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

## Integration with Other Tests

These scripts complement the scenario tests in `cmd/litestream-test/scripts/`:

**Relationship:**
- `cmd/litestream-test/scripts/` → Focused scenarios (seconds to ~30 minutes)
- `scripts/` → Integration tests (30 minutes to 8+ hours)

**Workflow:**
1. Run focused scenario tests during development (Go tests in `tests/integration/`)
2. Run Go integration tests before major changes
3. Run overnight S3 tests (8h) before releases
4. Analyze results with analysis script

## Success Criteria

### Overnight S3 Tests (8h)

✅ Pass Criteria:
- No process crashes
- Error count < 10 (excluding transient)
- Steady database growth
- Regular snapshots (40+)
- Active compaction visible
- Successful final restoration
- Row count match
- Memory usage stable

## Related Documentation

- [litestream-test CLI Tool](../cmd/litestream-test/README.md) - Testing harness documentation
- [Scenario Test Scripts](../cmd/litestream-test/scripts/README.md) - Focused test scenarios
- [S3 Retention Testing](../cmd/litestream-test/S3-RETENTION-TESTING.md) - S3-specific testing

## Contributing

When adding new integration scripts:

1. Follow naming conventions (`test-*.sh`)
2. Include clear duration estimates in comments
3. Create comprehensive monitoring
4. Generate timestamped test directories
5. Implement graceful cleanup with `trap`
6. Provide clear success/failure output
7. Update this README with script documentation
8. Consider both file and S3 variants if applicable
