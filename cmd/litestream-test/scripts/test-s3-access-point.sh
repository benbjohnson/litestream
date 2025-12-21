#!/bin/bash
set -e

# Test S3 Access Point ARN support (Issue #923)
# This script tests that Litestream can replicate to S3 using Access Point ARNs
# without requiring manual endpoint configuration.

echo "=========================================="
echo "S3 Access Point ARN Test (Issue #923)"
echo "=========================================="
echo ""
echo "This test verifies that S3 Access Point ARNs work automatically"
echo "without requiring manual endpoint configuration."
echo ""

# Configuration
DB="/tmp/access-point-test.db"
RESTORED_DB="/tmp/access-point-restored.db"
LITESTREAM="./bin/litestream"
LOG="/tmp/access-point-test.log"
CONFIG="/tmp/access-point-config.yml"

# S3 Access Point Configuration
# The ARN format: arn:aws:s3:REGION:ACCOUNT_ID:accesspoint/ACCESS_POINT_NAME
S3_ACCESS_POINT_ARN="${LITESTREAM_S3_ACCESS_POINT_ARN:-}"
S3_PREFIX="${LITESTREAM_S3_PREFIX:-litestream-access-point-test}"
S3_REGION="${LITESTREAM_S3_REGION:-}"

# Check prerequisites
check_prerequisites() {
    echo "[Prerequisites]"

    if [ ! -f "$LITESTREAM" ]; then
        echo "❌ Litestream binary not found at $LITESTREAM"
        echo "   Run: go build -o bin/litestream ./cmd/litestream"
        exit 1
    fi
    echo "  ✓ Litestream binary found"

    if ! command -v sqlite3 &> /dev/null; then
        echo "❌ sqlite3 not found"
        exit 1
    fi
    echo "  ✓ sqlite3 found"

    if [ -z "$S3_ACCESS_POINT_ARN" ]; then
        echo ""
        echo "⚠️  S3 Access Point ARN not configured!"
        echo ""
        echo "Please set the following environment variables:"
        echo ""
        echo "  export LITESTREAM_S3_ACCESS_POINT_ARN='arn:aws:s3:REGION:ACCOUNT:accesspoint/NAME'"
        echo "  export LITESTREAM_S3_REGION='us-east-1'  # Optional, extracted from ARN"
        echo "  export LITESTREAM_S3_PREFIX='test-prefix'  # Optional"
        echo ""
        echo "You also need AWS credentials configured via:"
        echo "  - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)"
        echo "  - AWS credentials file (~/.aws/credentials)"
        echo "  - IAM role (if running on AWS)"
        echo ""
        echo "Example:"
        echo "  export LITESTREAM_S3_ACCESS_POINT_ARN='arn:aws:s3:us-east-2:123456789012:accesspoint/my-access-point'"
        echo "  ./cmd/litestream-test/scripts/test-s3-access-point.sh"
        echo ""
        exit 1
    fi
    echo "  ✓ Access Point ARN configured"

    # Extract region from ARN if not explicitly set
    if [ -z "$S3_REGION" ]; then
        # ARN format: arn:aws:s3:REGION:ACCOUNT:accesspoint/NAME
        S3_REGION=$(echo "$S3_ACCESS_POINT_ARN" | cut -d: -f4)
        echo "  ✓ Region extracted from ARN: $S3_REGION"
    fi

    echo ""
    echo "Configuration:"
    echo "  Access Point ARN: $S3_ACCESS_POINT_ARN"
    echo "  Region: $S3_REGION"
    echo "  Prefix: $S3_PREFIX"
    echo ""
}

# Cleanup function
cleanup() {
    echo ""
    echo "[Cleanup]"
    pkill -f "litestream replicate.*access-point-test" 2>/dev/null || true
    rm -f "$DB"* "$RESTORED_DB"* "$LOG" "$CONFIG"
    echo "  ✓ Cleaned up test artifacts"
}

trap cleanup EXIT

# Run prerequisites check
check_prerequisites

# Clean up any previous test artifacts
cleanup 2>/dev/null || true

echo "=========================================="
echo "Test 1: Replication to Access Point ARN"
echo "=========================================="
echo ""

echo "[1] Creating test database..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
CREATE TABLE access_point_test (
    id INTEGER PRIMARY KEY,
    data TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO access_point_test (data) SELECT 'initial data ' || value FROM generate_series(1, 100);
EOF
echo "  ✓ Database created with 100 rows"

# Create config using Access Point ARN WITHOUT explicit endpoint
# This is the key test - it should work automatically with UseARNRegion=true
echo "[2] Creating config with Access Point ARN (no explicit endpoint)..."
cat > "$CONFIG" <<EOF
dbs:
  - path: $DB
    replicas:
      - type: s3
        bucket: $S3_ACCESS_POINT_ARN
        path: $S3_PREFIX
        region: $S3_REGION
        sync-interval: 1s
EOF
echo "  ✓ Config created"
echo ""
echo "  Config contents:"
cat "$CONFIG" | sed 's/^/    /'
echo ""

echo "[3] Starting replication..."
$LITESTREAM replicate -config "$CONFIG" > "$LOG" 2>&1 &
REPL_PID=$!
echo "  ✓ Litestream started (PID: $REPL_PID)"

# Wait for initial sync
echo "[4] Waiting for initial sync (10 seconds)..."
sleep 10

# Check if Litestream is still running
if ! kill -0 $REPL_PID 2>/dev/null; then
    echo "❌ Litestream exited unexpectedly!"
    echo ""
    echo "Log output:"
    cat "$LOG" | sed 's/^/    /'
    exit 1
fi
echo "  ✓ Litestream still running"

echo "[5] Adding more data..."
sqlite3 "$DB" <<EOF
INSERT INTO access_point_test (data) SELECT 'batch 2 data ' || value FROM generate_series(1, 100);
EOF
echo "  ✓ Added 100 more rows"

# Wait for sync
echo "[6] Waiting for sync (5 seconds)..."
sleep 5

# Stop replication
echo "[7] Stopping replication..."
kill $REPL_PID 2>/dev/null || true
wait $REPL_PID 2>/dev/null || true
echo "  ✓ Litestream stopped"

# Check for errors in log
echo "[8] Checking for errors in log..."
if grep -qi "error\|fail\|403\|AccessDenied" "$LOG"; then
    echo "❌ Errors found in log!"
    echo ""
    echo "Log output:"
    grep -i "error\|fail\|403\|AccessDenied" "$LOG" | sed 's/^/    /'
    echo ""
    echo "Full log:"
    cat "$LOG" | sed 's/^/    /'
    exit 1
fi
echo "  ✓ No errors in log"

echo ""
echo "=========================================="
echo "Test 2: Restore from Access Point ARN"
echo "=========================================="
echo ""

echo "[1] Restoring database from Access Point..."
RESTORE_URL="s3://${S3_ACCESS_POINT_ARN}/${S3_PREFIX}"
echo "  Restore URL: $RESTORE_URL"

if ! $LITESTREAM restore -o "$RESTORED_DB" "$RESTORE_URL" 2>&1; then
    echo "❌ Restore failed!"
    exit 1
fi
echo "  ✓ Restore completed"

echo "[2] Verifying restored data..."
ORIGINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM access_point_test")
RESTORED_COUNT=$(sqlite3 "$RESTORED_DB" "SELECT COUNT(*) FROM access_point_test")

echo "  Original rows: $ORIGINAL_COUNT"
echo "  Restored rows: $RESTORED_COUNT"

if [ "$ORIGINAL_COUNT" != "$RESTORED_COUNT" ]; then
    echo "❌ Row count mismatch!"
    exit 1
fi
echo "  ✓ Row counts match"

echo "[3] Running integrity check..."
INTEGRITY=$(sqlite3 "$RESTORED_DB" "PRAGMA integrity_check")
if [ "$INTEGRITY" != "ok" ]; then
    echo "❌ Integrity check failed: $INTEGRITY"
    exit 1
fi
echo "  ✓ Integrity check passed"

echo ""
echo "=========================================="
echo "✅ All Tests Passed!"
echo "=========================================="
echo ""
echo "S3 Access Point ARN works correctly without manual endpoint configuration."
echo "The UseARNRegion=true fix (Issue #923) is working as expected."
echo ""
echo "Test Summary:"
echo "  - Replication to Access Point ARN: ✓"
echo "  - Automatic endpoint resolution: ✓"
echo "  - Restore from Access Point ARN: ✓"
echo "  - Data integrity: ✓"
echo ""
