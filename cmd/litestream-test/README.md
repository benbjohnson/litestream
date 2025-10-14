# litestream-test

A CLI testing harness for Litestream that provides tools for database population, load generation, and replication validation.

## Overview

`litestream-test` is a purpose-built tool for testing Litestream's replication functionality across various scenarios. It provides commands for:

- Quickly populating databases to specific sizes
- Generating continuous load with configurable patterns
- Shrinking databases to test compaction scenarios
- Validating replication integrity after restore

## Installation

```bash
go build -o bin/litestream-test ./cmd/litestream-test
```

## Commands

### populate

Quickly populate a database to a target size with structured test data.

```bash
litestream-test populate -db <path> [options]
```

**Options:**
- `-db` - Database path (required)
- `-target-size` - Target database size (default: "100MB", examples: "1GB", "500MB", "50MB")
- `-row-size` - Average row size in bytes (default: 1024)
- `-batch-size` - Rows per transaction (default: 1000)
- `-table-count` - Number of tables to create (default: 1)
- `-index-ratio` - Percentage of columns to index, 0.0-1.0 (default: 0.2)
- `-page-size` - SQLite page size in bytes (default: 4096)

**Examples:**
```bash
litestream-test populate -db /tmp/test.db -target-size 1GB
litestream-test populate -db /tmp/test.db -target-size 50MB -batch-size 10000
litestream-test populate -db /tmp/test.db -target-size 1.5GB -page-size 4096
```

**Use Cases:**
- Creating test databases of specific sizes
- Testing SQLite lock page boundary (1GB with 4KB pages)
- Generating initial data before replication tests

### load

Generate continuous write and read load on a database with configurable patterns.

```bash
litestream-test load -db <path> [options]
```

**Options:**
- `-db` - Database path (required, must exist)
- `-write-rate` - Writes per second (default: 100)
- `-duration` - How long to run (default: 1m, examples: "30s", "5m", "2h", "8h")
- `-pattern` - Write pattern (default: "constant")
  - `constant` - Steady write rate
  - `burst` - Periodic bursts of activity
  - `random` - Random write intervals
  - `wave` - Sinusoidal pattern simulating varying load
- `-payload-size` - Size of each write in bytes (default: 1024)
- `-read-ratio` - Read/write ratio, 0.0-1.0 (default: 0.2)
- `-workers` - Number of concurrent workers (default: 1)

**Examples:**
```bash
litestream-test load -db /tmp/test.db -write-rate 50 -duration 5m
litestream-test load -db /tmp/test.db -write-rate 100 -duration 2h -pattern wave
litestream-test load -db /tmp/test.db -write-rate 200 -duration 8h -workers 4 -pattern burst
```

**Use Cases:**
- Stress testing replication under sustained load
- Testing checkpoint behavior with various patterns
- Simulating production workloads for overnight tests
- Testing concurrent operations with multiple workers

### shrink

Shrink a database by deleting data, useful for testing compaction scenarios.

```bash
litestream-test shrink -db <path> [options]
```

**Use Cases:**
- Testing database shrinkage and compaction
- Simulating data deletion scenarios
- Testing retention cleanup behavior

### validate

Validate that a replica can be restored and matches the source database.

```bash
litestream-test validate [options]
```

**Options:**
- `-source-db` - Original database path (required)
- `-replica-url` - Replica URL to validate (e.g., "file:///path", "s3://bucket/path")
- `-restored-db` - Path for restored database (default: source-db + ".restored")
- `-check-type` - Type of validation (default: "quick")
  - `quick` - Fast row count comparison
  - `integrity` - SQLite PRAGMA integrity_check
  - `checksum` - Full database checksum comparison
  - `full` - All validation types
- `-ltx-continuity` - Check LTX file continuity (default: false)
- `-config` - Litestream config file path (alternative to replica-url)

**Examples:**
```bash
litestream-test validate -source-db /tmp/test.db -replica-url file:///tmp/replica
litestream-test validate -source-db /tmp/test.db -replica-url s3://bucket/path -check-type full
litestream-test validate -source-db /tmp/test.db -config /tmp/litestream.yml -ltx-continuity
```

**Use Cases:**
- Verifying replication integrity after tests
- Testing restore functionality
- Validating data consistency across replicas
- Checking LTX file continuity

### version

Show version information.

```bash
litestream-test version
```

## Usage Patterns

### Basic Test Workflow

```bash
litestream-test populate -db /tmp/test.db -target-size 100MB

litestream replicate /tmp/test.db file:///tmp/replica &
LITESTREAM_PID=$!

litestream-test load -db /tmp/test.db -duration 5m -write-rate 50

kill $LITESTREAM_PID
wait

litestream-test validate -source-db /tmp/test.db -replica-url file:///tmp/replica
```

### Overnight Test Pattern

```bash
litestream-test populate -db /tmp/test.db -target-size 100MB

litestream replicate -config litestream.yml &

litestream-test load -db /tmp/test.db \
  -duration 8h \
  -write-rate 100 \
  -pattern wave \
  -workers 4
```

### Large Database with Lock Page Testing

```bash
litestream-test populate -db /tmp/test.db \
  -target-size 1.5GB \
  -page-size 4096 \
  -batch-size 10000

litestream replicate /tmp/test.db s3://bucket/path &

litestream-test validate -source-db /tmp/test.db \
  -replica-url s3://bucket/path \
  -check-type full
```

## Integration with Test Scripts

The `litestream-test` tool is used by all scripts in the `scripts/` directory. These scripts orchestrate full test scenarios:

- `scripts/*.sh` - Use litestream-test for database operations
- `scripts/verify-test-setup.sh` - Checks that litestream-test is built
- See `scripts/README.md` for detailed test scenario documentation

## Related Documentation

- [Test Scripts Documentation](./scripts/README.md) - Comprehensive test scenarios
- [S3 Retention Testing](./S3-RETENTION-TESTING.md) - S3-specific test documentation
- [Top-level Integration Scripts](../../scripts/README.md) - Long-running test documentation

## Development

### Building

```bash
go build -o bin/litestream-test ./cmd/litestream-test
```

### Testing

```bash
go test ./cmd/litestream-test/...
```

### Adding New Commands

1. Create a new file in `cmd/litestream-test/` (e.g., `mycommand.go`)
2. Implement the command struct and Run method
3. Add command to switch statement in `main.go`
4. Update this README with command documentation
