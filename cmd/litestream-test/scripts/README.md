# Litestream Test Scripts

Comprehensive test scripts for validating Litestream functionality across various scenarios. These scripts use the `litestream-test` CLI tool to orchestrate complex testing scenarios.

## Prerequisites

```bash
go build -o bin/litestream ./cmd/litestream
go build -o bin/litestream-test ./cmd/litestream-test

./cmd/litestream-test/scripts/verify-test-setup.sh
```

## Quick Reference

| Script | Purpose | Duration | Status |
|--------|---------|----------|--------|
| verify-test-setup.sh | Environment validation | ~5s | ✅ Stable |
| test-fresh-start.sh | Fresh database creation | ~30s | ✅ Stable |
| test-rapid-checkpoints.sh | Checkpoint stress test | ~2min | ✅ Stable |
| test-wal-growth.sh | Large WAL handling (100MB+) | ~5min | ✅ Stable |
| test-concurrent-operations.sh | Multi-database concurrent replication | ~5min | ✅ Stable |
| test-database-integrity.sh | Complex data integrity validation | ~3min | ✅ Stable |
| test-database-deletion.sh | Database deletion scenarios | ~2min | ✅ Stable |
| test-replica-failover.sh | Replica failover testing | ~3min | ✅ Stable |
| test-busy-timeout.sh | Database busy timeout handling | ~2min | ✅ Stable |
| test-1gb-boundary.sh | SQLite 1GB lock page boundary | ~10min | ⚠️ Blocked by #754 |
| reproduce-critical-bug.sh | Checkpoint during downtime bug | ~2min | 🐛 Reproduces #752 |
| test-754-s3-scenarios.sh | Issue #754 S3 vs file replication | ~10min | 🐛 Tests #754 |
| test-754-restore-focus.sh | Issue #754 restore focus | ~5min | 🐛 Tests #754 |
| test-simple-754-reproduction.sh | Minimal #754 reproduction | ~3min | 🐛 Tests #754 |
| test-v0.5-flag-reproduction.sh | ltx v0.5.0 flag issue | ~5min | 🐛 Tests #754 |
| test-v0.5-restart-scenarios.sh | v0.5 restart scenarios | ~5min | 🐛 Tests #754 |
| test-format-isolation.sh | Format version isolation | ~3min | ✅ Stable |
| test-quick-format-check.sh | Quick format validation | ~30s | ✅ Stable |
| test-upgrade-v0.3-to-v0.5.sh | v0.3 to v0.5 upgrade | ~10min | ✅ Stable |
| test-upgrade-large-db.sh | Large database upgrade | ~15min | ✅ Stable |
| test-massive-upgrade.sh | Massive database upgrade | ~20min | ✅ Stable |
| test-s3-retention-cleanup.sh | Basic S3 retention | ~8min | ✅ Stable |
| test-s3-retention-small-db.sh | S3 retention 50MB | ~8min | ✅ Stable |
| test-s3-retention-large-db.sh | S3 retention 1.5GB | ~20min | ✅ Stable |
| test-s3-retention-comprehensive.sh | Full S3 retention suite | ~30min | ✅ Stable |

## Test Categories

### Setup & Validation

#### verify-test-setup.sh
Verifies that the test environment is properly configured with required binaries and dependencies.

```bash
./cmd/litestream-test/scripts/verify-test-setup.sh
```

**Checks:**
- Litestream binary exists
- litestream-test binary exists
- SQLite3 available
- Python dependencies for S3 mock

### Core Functionality Tests

#### test-fresh-start.sh
Tests replication with a fresh database that doesn't exist when Litestream starts.

```bash
./cmd/litestream-test/scripts/test-fresh-start.sh
```

**Tests:**
- Starting Litestream before database exists
- Database creation while Litestream is running
- Automatic detection of new database
- Replication and restore integrity

#### test-database-integrity.sh
Creates complex data patterns and verifies integrity after restore.

```bash
./cmd/litestream-test/scripts/test-database-integrity.sh
```

**Tests:**
- Complex data patterns (multiple tables, indexes)
- SQLite PRAGMA integrity_check
- Full database restoration
- Data consistency verification

#### test-database-deletion.sh
Tests scenarios where the source database is deleted during replication.

```bash
./cmd/litestream-test/scripts/test-database-deletion.sh
```

**Tests:**
- Database deletion during active replication
- Recovery behavior
- Replica consistency

#### test-replica-failover.sh
Tests replica failover scenarios with multiple replicas.

```bash
./cmd/litestream-test/scripts/test-replica-failover.sh
```

**Tests:**
- Multiple replica configuration
- Failover when primary replica fails
- Data consistency across replicas

### Stress & Performance Tests

#### test-rapid-checkpoints.sh
Tests Litestream under rapid checkpoint pressure with continuous writes.

```bash
./cmd/litestream-test/scripts/test-rapid-checkpoints.sh
```

**Tests:**
- 100+ writes/second
- Forced rapid checkpoints
- Replication under checkpoint pressure
- Data integrity under stress

**Key Metrics:**
- Checkpoint frequency
- WAL file growth
- Replication lag
- Error rates

#### test-wal-growth.sh
Tests handling of large WAL files (100MB+) under sustained write load.

```bash
./cmd/litestream-test/scripts/test-wal-growth.sh
```

**Tests:**
- Sustained high write rates (400+ writes/sec)
- Large WAL file creation and handling
- Checkpoint behavior with large WALs
- Replication performance with large data

**Key Findings:**
- Successfully handles 100MB+ WAL files
- Maintains data integrity
- Handles 400+ writes/second

#### test-concurrent-operations.sh
Tests multiple databases replicating simultaneously with competing operations.

```bash
./cmd/litestream-test/scripts/test-concurrent-operations.sh
```

**Tests:**
- Multiple databases (3-5) replicating concurrently
- Mixed read/write operations
- Competing checkpoints
- Resource contention handling

#### test-busy-timeout.sh
Tests database busy timeout handling with concurrent access.

```bash
./cmd/litestream-test/scripts/test-busy-timeout.sh
```

**Tests:**
- Concurrent database access
- Busy timeout configuration
- Lock contention handling
- Recovery from busy states

### Boundary & Edge Case Tests

#### test-1gb-boundary.sh
Tests SQLite's 1GB lock page boundary handling.

```bash
./cmd/litestream-test/scripts/test-1gb-boundary.sh
```

**Tests:**
- Database growth beyond 1GB (with 4KB pages)
- Lock page at #262145 properly skipped
- Replication across lock page boundary
- Restoration integrity after crossing boundary

**Status:** ⚠️ Currently blocked by ltx v0.5.0 flag compatibility issue (#754)

**Lock Page Numbers by Page Size:**
| Page Size | Lock Page # |
|-----------|-------------|
| 4KB       | 262145      |
| 8KB       | 131073      |
| 16KB      | 65537       |
| 32KB      | 32769       |

### Bug Reproduction Scripts

#### reproduce-critical-bug.sh
Reproduces checkpoint during downtime bug that causes restore failures.

```bash
./cmd/litestream-test/scripts/reproduce-critical-bug.sh
```

**Reproduces:** Issue #752

**Scenario:**
1. Litestream is killed (simulating crash)
2. Writes continue and a checkpoint occurs
3. Litestream is restarted
4. Restore fails with "nonsequential page numbers" error

**Expected:** Database should restore successfully
**Actual:** Restore fails, causing data loss

#### test-754-s3-scenarios.sh
Tests Issue #754 flag compatibility with S3 replication versus file replication.

```bash
./cmd/litestream-test/scripts/test-754-s3-scenarios.sh
```

**Tests:**
- S3 replica behavior with ltx v0.5.0
- File replica behavior comparison
- LTX file cleanup and retention
- Flag compatibility issues

#### test-754-restore-focus.sh
Focused testing of Issue #754 restore failures.

```bash
./cmd/litestream-test/scripts/test-754-restore-focus.sh
```

**Tests:**
- Restore failures with pre-existing databases
- Flag mismatch detection
- Recovery scenarios

#### test-simple-754-reproduction.sh
Minimal reproduction case for Issue #754.

```bash
./cmd/litestream-test/scripts/test-simple-754-reproduction.sh
```

**Reproduces:** Issue #754 with minimal steps for debugging

#### test-v0.5-flag-reproduction.sh
Reproduces ltx v0.5.0 flag compatibility issue.

```bash
./cmd/litestream-test/scripts/test-v0.5-flag-reproduction.sh
```

**Tests:**
- Pre-existing database behavior with ltx v0.5.0
- Flag mismatch scenarios
- Upgrade path issues

#### test-v0.5-restart-scenarios.sh
Tests various restart scenarios with ltx v0.5.0.

```bash
./cmd/litestream-test/scripts/test-v0.5-restart-scenarios.sh
```

**Tests:**
- Clean restart
- Restart after checkpoint
- Restart with pending data
- Flag persistence across restarts

### Format & Upgrade Tests

#### test-format-isolation.sh
Tests isolation between different LTX format versions.

```bash
./cmd/litestream-test/scripts/test-format-isolation.sh
```

**Tests:**
- Multiple format versions coexisting
- Format detection and handling
- Migration between formats
- Backward compatibility

#### test-quick-format-check.sh
Quick validation of LTX format handling.

```bash
./cmd/litestream-test/scripts/test-quick-format-check.sh
```

**Duration:** ~30 seconds

**Tests:**
- Format version detection
- Basic format integrity
- Quick validation workflow

#### test-upgrade-v0.3-to-v0.5.sh
Tests upgrade path from ltx v0.3 to v0.5.

```bash
./cmd/litestream-test/scripts/test-upgrade-v0.3-to-v0.5.sh
```

**Tests:**
- Migration from v0.3 to v0.5
- Data preservation during upgrade
- Flag handling in upgrade process
- Backward compatibility verification

#### test-upgrade-large-db.sh
Tests upgrade process with large databases (1GB+).

```bash
./cmd/litestream-test/scripts/test-upgrade-large-db.sh
```

**Tests:**
- Large database upgrade performance
- Data integrity during upgrade
- Lock page handling in upgrades
- Resource usage during migration

#### test-massive-upgrade.sh
Tests upgrade with very large databases and long-running scenarios.

```bash
./cmd/litestream-test/scripts/test-massive-upgrade.sh
```

**Tests:**
- Multi-GB database upgrades
- Extended migration scenarios
- Performance under scale
- Memory and disk usage

### S3 & Retention Tests

For detailed S3 retention testing documentation, see [S3-RETENTION-TESTING.md](../S3-RETENTION-TESTING.md).

#### test-s3-retention-cleanup.sh
Basic S3 LTX retention cleanup testing.

```bash
./cmd/litestream-test/scripts/test-s3-retention-cleanup.sh
```

**Tests:**
- Basic retention cleanup behavior
- Old LTX file removal
- Retention period enforcement

#### test-s3-retention-small-db.sh
S3 retention testing with 50MB database.

```bash
./cmd/litestream-test/scripts/test-s3-retention-small-db.sh
```

**Configuration:**
- Database size: 50MB
- Retention period: 2 minutes
- Duration: ~8 minutes

**Tests:**
- Small database retention cleanup
- Quick retention cycles
- S3 mock integration

#### test-s3-retention-large-db.sh
S3 retention testing with 1.5GB database crossing lock page boundary.

```bash
./cmd/litestream-test/scripts/test-s3-retention-large-db.sh
```

**Configuration:**
- Database size: 1.5GB
- Page size: 4KB (lock page at #262145)
- Retention period: 3 minutes
- Duration: ~20 minutes

**Tests:**
- Large database retention cleanup
- Lock page boundary handling
- Extended monitoring
- Scale behavior

#### test-s3-retention-comprehensive.sh
Master script running both small and large database retention tests with analysis.

```bash
./cmd/litestream-test/scripts/test-s3-retention-comprehensive.sh

./cmd/litestream-test/scripts/test-s3-retention-comprehensive.sh --small-only
./cmd/litestream-test/scripts/test-s3-retention-comprehensive.sh --large-only
./cmd/litestream-test/scripts/test-s3-retention-comprehensive.sh --no-cleanup
```

**Duration:** ~30 minutes for full suite

**Features:**
- Runs both small and large DB tests
- Comparative analysis
- Detailed reports
- Configurable execution

## Usage Patterns

### Running Individual Tests

```bash
./cmd/litestream-test/scripts/test-fresh-start.sh
```

### Verify Environment First

```bash
./cmd/litestream-test/scripts/verify-test-setup.sh
./cmd/litestream-test/scripts/test-rapid-checkpoints.sh
```

### Running Multiple Tests

```bash
for script in test-fresh-start.sh test-rapid-checkpoints.sh test-database-integrity.sh; do
    echo "Running $script..."
    ./cmd/litestream-test/scripts/$script
    echo ""
done
```

### S3 Testing with Local Mock

The S3 tests automatically use the Python S3 mock server (`./etc/s3_mock.py`) for isolated testing:

```bash
./cmd/litestream-test/scripts/test-s3-retention-small-db.sh
```

### Debugging Failed Tests

Most tests create logs in `/tmp/`:

```bash
tail -f /tmp/fresh-test.log

tail -f /tmp/checkpoint-cycle.log

grep -i error /tmp/*.log
```

## Test Artifacts

Tests typically create artifacts in `/tmp/`:

- **Databases:** `/tmp/*-test.db`
- **Replicas:** `/tmp/*-replica/`
- **Logs:** `/tmp/*-test.log`
- **Configs:** `/tmp/*.yml`
- **Restored DBs:** `/tmp/*-restored.db`

## Test Results & Analysis

Historical test results and analysis are stored in `.local/test-results/` (git-ignored):

- `final-test-summary.md` - Comprehensive test findings
- `validation-results-after-ltx-v0.5.0.md` - ltx v0.5.0 impact analysis
- `comprehensive-test-findings.md` - Initial test results
- `critical-bug-analysis.md` - Detailed bug analysis

## Key Findings Summary

### Performance ✅
- Successfully handles 400+ writes/second
- Manages 100MB+ WAL files
- Multiple concurrent databases replicate cleanly

### Fresh Databases ✅
- Work perfectly with ltx v0.5.0
- Clean replication and restore
- No flag issues

### Pre-existing Databases ❌
- Broken due to ltx flag compatibility (#754)
- Restore failures
- Upgrade path issues

### Checkpoint During Downtime ❌
- Worse with ltx v0.5.0 (#752)
- Causes restore failures
- Data loss risk

### S3 Retention ✅
- LTX cleanup works correctly
- Handles lock page boundary
- Scale testing successful

## Related Issues

- [#752](https://github.com/benbjohnson/litestream/issues/752) - Checkpoint during downtime bug
- [#753](https://github.com/benbjohnson/litestream/issues/753) - Transaction numbering (FIXED)
- [#754](https://github.com/benbjohnson/litestream/issues/754) - ltx v0.5.0 flag compatibility (CRITICAL)

## Related Documentation

- [litestream-test CLI Documentation](../README.md) - CLI tool reference
- [S3 Retention Testing Guide](../S3-RETENTION-TESTING.md) - Detailed S3 testing
- [Top-level Integration Scripts](../../../scripts/README.md) - Long-running tests

## Contributing

When adding new test scripts:

1. Follow existing naming conventions (`test-*.sh`)
2. Include clear comments explaining what is being tested
3. Use `/tmp/` for test artifacts
4. Create cleanup handlers with `trap`
5. Provide clear success/failure output
6. Update this README with script documentation
7. Add entry to Quick Reference table
