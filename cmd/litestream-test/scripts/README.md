# Litestream Test Scripts

This directory contains comprehensive test scripts for validating Litestream functionality across various scenarios.

## Prerequisites

```bash
# Build required binaries
go build -o bin/litestream ./cmd/litestream
go build -o bin/litestream-test ./cmd/litestream-test

# Verify setup
./verify-test-setup.sh
```

## Test Scripts

### Core Functionality Tests

- **`verify-test-setup.sh`** - Verifies test environment is properly configured
- **`test-fresh-start.sh`** - Tests replication with fresh database creation
- **`reproduce-critical-bug.sh`** - Reproduces checkpoint during downtime bug

### Stress & Performance Tests

- **`test-rapid-checkpoints.sh`** - Tests rapid checkpoint cycling under load
- **`test-wal-growth.sh`** - Tests handling of large WAL files (100MB+)
- **`test-concurrent-operations.sh`** - Tests multiple database concurrent replication

### Boundary & Edge Case Tests

- **`test-1gb-boundary.sh`** - Tests SQLite 1GB lock page boundary handling
  - Note: Currently blocked by ltx v0.5.0 flag compatibility issue

### S3 Retention Tests (NEW)

- **`test-s3-retention-small-db.sh`** - Tests S3 LTX retention cleanup with 50MB database (2min retention)
- **`test-s3-retention-large-db.sh`** - Tests S3 LTX retention cleanup with 1.5GB database crossing lock page (3min retention)
- **`test-s3-retention-comprehensive.sh`** - Master script running both tests with comparative analysis
- **`test-s3-retention-cleanup.sh`** - Original basic S3 retention test

These scripts test that old LTX files are properly cleaned up after their retention period expires, using the local Python S3 mock for isolated testing.

## Usage

All scripts are designed to be run from the repository root:

```bash
# Run individual tests
./cmd/litestream-test/scripts/test-fresh-start.sh
./cmd/litestream-test/scripts/test-rapid-checkpoints.sh

# Verify environment first
./cmd/litestream-test/scripts/verify-test-setup.sh
```

## Test Results

Detailed test results and analysis are stored in `.local/test-results/`:

- `final-test-summary.md` - Comprehensive test findings
- `validation-results-after-ltx-v0.5.0.md` - ltx v0.5.0 impact analysis
- `comprehensive-test-findings.md` - Initial test results
- `critical-bug-analysis.md` - Detailed bug analysis

## Key Findings

- ✅ **Performance**: Handles 400+ writes/sec, 100MB WAL files
- ✅ **Fresh databases**: Work perfectly with ltx v0.5.0
- ❌ **Pre-existing databases**: Broken due to ltx flag compatibility
- ❌ **Checkpoint during downtime**: Worse with ltx v0.5.0

## Related Issues

- [#752](https://github.com/benbjohnson/litestream/issues/752) - Checkpoint during downtime bug
- [#753](https://github.com/benbjohnson/litestream/issues/753) - Transaction numbering (FIXED)
- [#754](https://github.com/benbjohnson/litestream/issues/754) - ltx v0.5.0 flag compatibility (CRITICAL)
