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

### test-quick-validation.sh

Quick validation test that runs for a configurable duration (default: 30 minutes).

```bash
./scripts/test-quick-validation.sh

TEST_DURATION=2h ./scripts/test-quick-validation.sh

TEST_DURATION=1h ./scripts/test-quick-validation.sh
```

**Default Configuration:**
- Duration: 30 minutes (configurable via `TEST_DURATION`)
- Database: 10MB initial population
- Write rate: 100 writes/second
- Pattern: Wave (simulates varying load)
- Payload size: 4KB
- Workers: 4
- Replica: File-based

**Features:**
- Aggressive test settings for quick feedback
- Very frequent snapshots (1 minute intervals)
- Rapid compaction cycles (30s, 1m, 5m, 15m)
- Real-time monitoring every 30 seconds
- Automatic validation and restore testing
- Comprehensive final report

**Monitoring:**
```bash
tail -f /tmp/litestream-quick-*/logs/monitor.log
tail -f /tmp/litestream-quick-*/logs/litestream.log
```

**What it Tests:**
- Snapshot creation frequency
- Compaction behavior across multiple intervals
- LTX file generation and management
- Checkpoint behavior under load
- Replication integrity
- Restoration success
- Error handling

**When to Use:**
- Before running overnight tests
- Validating configuration changes
- Quick regression testing
- CI/CD integration (with short duration)
- Pre-release validation

**Success Criteria:**
- LTX segments created (>0)
- No critical errors in logs
- Successful restoration
- Row counts match between source and restored database

### test-overnight.sh

Comprehensive 8-hour test with file-based replication.

```bash
./scripts/test-overnight.sh
```

**Configuration:**
- Duration: 8 hours
- Database: 100MB initial population
- Write rate: 50 writes/second
- Pattern: Wave (simulates varying load)
- Payload size: 2KB
- Workers: 4
- Replica: File-based (`/tmp/litestream-overnight-*/replica`)

**Features:**
- Extended monitoring with 1-minute updates
- Snapshot every 10 minutes
- Aggressive compaction intervals:
  - 30 seconds → 30s duration
  - 1 minute → 1m duration
  - 5 minutes → 5m duration
  - 15 minutes → 1h duration
  - 30 minutes → 6h duration
  - 1 hour → 24h duration
- 720-hour retention (30 days)
- Checkpoint every 30 seconds
- Automatic validation after completion

**Real-time Monitoring:**
```bash
tail -f /tmp/litestream-overnight-*/logs/monitor.log
tail -f /tmp/litestream-overnight-*/logs/litestream.log
tail -f /tmp/litestream-overnight-*/logs/load.log
```

**What it Tests:**
- Long-term replication stability
- Compaction effectiveness over time
- Memory stability under sustained load
- WAL file management
- Checkpoint consistency
- Replica file count growth patterns
- Error accumulation over time
- Recovery from transient issues

**Expected Behavior:**
- Steady database growth over 8 hours
- Regular snapshot creation (48 total)
- Active compaction reducing old LTX files
- Stable memory usage
- No error accumulation
- Successful final validation

**Artifacts:**
- Test directory: `/tmp/litestream-overnight-<timestamp>/`
- Logs: Monitor, litestream, load, populate, validate
- Database: Source and restored versions
- Replica: Full replica directory with LTX files

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

### Quick Validation Before Overnight Test

```bash
TEST_DURATION=30m ./scripts/test-quick-validation.sh
```

If this passes, proceed to overnight:
```bash
./scripts/test-overnight.sh
```

### Running Multiple Overnight Tests

File and S3 tests can run concurrently (different machines recommended):

```bash
./scripts/test-overnight.sh &
./scripts/test-overnight-s3.sh &
```

### Custom Duration Testing

```bash
TEST_DURATION=2h ./scripts/test-quick-validation.sh
TEST_DURATION=4h ./scripts/test-quick-validation.sh
TEST_DURATION=12h ./scripts/test-quick-validation.sh
```

### Analyzing Results

```bash
ls /tmp/litestream-overnight-* -dt | head -1

./scripts/analyze-test-results.sh $(ls /tmp/litestream-overnight-* -dt | head -1)
```

### Continuous Integration

For CI/CD, use shorter durations:

```bash
TEST_DURATION=5m ./scripts/test-quick-validation.sh
TEST_DURATION=15m ./scripts/test-quick-validation.sh
```

## Test Duration Guide

| Duration | Use Case | Test Type | Expected Results |
|----------|----------|-----------|------------------|
| 5 minutes | CI/CD smoke test | Quick validation | Basic functionality |
| 30 minutes | Pre-overnight validation | Quick validation | Config verification |
| 1 hour | Short integration | Quick validation | Pattern detection |
| 2 hours | Extended integration | Quick validation | Compaction cycles |
| 8 hours | Overnight stability | Overnight test | Full validation |
| 12+ hours | Stress testing | Overnight test | Edge case discovery |

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
1. Run focused scenario tests during development
2. Run quick validation (30min) before major changes
3. Run overnight tests (8h) before releases
4. Analyze results with analysis script

## Success Criteria

### Quick Validation (30min)

✅ Pass Criteria:
- LTX segments created (>0)
- At least 1 snapshot created
- Multiple compaction cycles completed
- No critical errors
- Successful restoration
- Row count matches

### Overnight Tests (8h)

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
