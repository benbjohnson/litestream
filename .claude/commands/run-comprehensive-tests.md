---
description: Run comprehensive test suite for Litestream
---

# Run Comprehensive Tests Command

Execute a full test suite including unit tests, integration tests, race detection, and large database tests.

## Quick Test Suite

```bash
# Basic tests with race detection
go test -race -v ./...

# With coverage
go test -race -cover -v ./...
```

## Full Test Suite

### 1. Unit Tests
```bash
echo "=== Running Unit Tests ==="
go test -v ./... -short
```

### 2. Race Condition Tests
```bash
echo "=== Testing for Race Conditions ==="
go test -race -v -run TestReplica_Sync ./...
go test -race -v -run TestDB_Sync ./...
go test -race -v -run TestStore_CompactDB ./...
go test -race -v ./...
```

### 3. Integration Tests
```bash
echo "=== Running Integration Tests ==="

# S3 (requires AWS credentials)
AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=yyy \
  go test -v ./replica_client_test.go -integration s3

# Google Cloud Storage (requires credentials)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json \
  go test -v ./replica_client_test.go -integration gcs

# Azure Blob Storage
AZURE_STORAGE_ACCOUNT=xxx AZURE_STORAGE_KEY=yyy \
  go test -v ./replica_client_test.go -integration abs

# SFTP (requires SSH server)
go test -v ./replica_client_test.go -integration sftp

# File system (always available)
go test -v ./replica_client_test.go -integration file
```

### 4. Large Database Tests (>1GB)
```bash
echo "=== Testing Large Databases ==="

# Create test database for each page size
for pagesize in 4096 8192 16384 32768; do
  echo "Testing page size: $pagesize"

  # Create >1GB database
  sqlite3 test-${pagesize}.db <<EOF
PRAGMA page_size=${pagesize};
CREATE TABLE test(id INTEGER PRIMARY KEY, data BLOB);
WITH RECURSIVE generate_series(value) AS (
  SELECT 1 UNION ALL SELECT value+1 FROM generate_series LIMIT 300000
)
INSERT INTO test SELECT value, randomblob(4000) FROM generate_series;
EOF

  # Test replication
  ./bin/litestream replicate test-${pagesize}.db file:///tmp/replica-${pagesize} &
  PID=$!
  sleep 10
  kill $PID

  # Test restoration
  ./bin/litestream restore -o restored-${pagesize}.db file:///tmp/replica-${pagesize}

  # Verify integrity
  sqlite3 restored-${pagesize}.db "PRAGMA integrity_check;" | grep -q "ok" || echo "FAILED: Page size $pagesize"

  # Cleanup
  rm -f test-${pagesize}.db restored-${pagesize}.db
  rm -rf /tmp/replica-${pagesize}
done
```

### 5. Compaction Tests
```bash
echo "=== Testing Compaction ==="

# Test store compaction
go test -v -run TestStore_CompactDB ./...

# Test with eventual consistency simulation
go test -v -run TestStore_CompactDB_RemotePartialRead ./...

# Test parallel compaction
go test -race -v -run TestStore_ParallelCompact ./...
```

### 6. Benchmark Tests
```bash
echo "=== Running Benchmarks ==="

# Core operations
go test -bench=BenchmarkWALRead -benchmem
go test -bench=BenchmarkLTXWrite -benchmem
go test -bench=BenchmarkCompaction -benchmem
go test -bench=BenchmarkPageIteration -benchmem

# Compare with previous results
go test -bench=. -benchmem | tee bench_new.txt
# benchcmp bench_old.txt bench_new.txt  # if you have previous results
```

### 7. Memory and CPU Profiling
```bash
echo "=== Profiling ==="

# Memory profile
go test -memprofile=mem.prof -bench=. -run=^$
go tool pprof -top mem.prof

# CPU profile
go test -cpuprofile=cpu.prof -bench=. -run=^$
go tool pprof -top cpu.prof
```

### 8. Build Tests
```bash
echo "=== Testing Builds ==="

# Test main build (no CGO)
go build -o bin/litestream ./cmd/litestream

# Test VFS build (requires CGO)
CGO_ENABLED=1 go build -tags vfs -o bin/litestream-vfs ./cmd/litestream-vfs

# Test cross-compilation
GOOS=linux GOARCH=amd64 go build -o bin/litestream-linux-amd64 ./cmd/litestream
GOOS=darwin GOARCH=arm64 go build -o bin/litestream-darwin-arm64 ./cmd/litestream
GOOS=windows GOARCH=amd64 go build -o bin/litestream-windows-amd64.exe ./cmd/litestream
```

### 9. Linting and Static Analysis
```bash
echo "=== Running Linters ==="

# Format check
gofmt -d .
goimports -local github.com/benbjohnson/litestream -d .

# Vet
go vet ./...

# Static check
staticcheck ./...

# Pre-commit hooks
pre-commit run --all-files
```

## Coverage Report

```bash
# Generate coverage for all packages
go test -coverprofile=coverage.out ./...

# View coverage in terminal
go tool cover -func=coverage.out

# Generate HTML report
go tool cover -html=coverage.out -o coverage.html
open coverage.html  # macOS
# xdg-open coverage.html  # Linux
```

## CI-Friendly Test Script

```bash
#!/bin/bash
set -e

echo "Running Litestream Test Suite"

# Unit tests with race and coverage
go test -race -cover -v ./... | tee test_results.txt

# Check coverage threshold
coverage=$(go test -cover ./... | grep -oE '[0-9]+\.[0-9]+%' | head -1 | tr -d '%')
if (( $(echo "$coverage < 70" | bc -l) )); then
  echo "Coverage $coverage% is below 70% threshold"
  exit 1
fi

# Large database test
./scripts/test-large-db.sh

echo "All tests passed!"
```

## Expected Results

✅ All tests should pass
✅ No race conditions detected
✅ Coverage >70% for core packages
✅ Lock page correctly skipped for all page sizes
✅ Restoration works for databases >1GB
✅ No memory leaks in benchmarks
