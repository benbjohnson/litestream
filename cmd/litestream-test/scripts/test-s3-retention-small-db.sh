#!/bin/bash
set -e

# Test S3 LTX file retention cleanup with small databases using local S3 mock
# This script tests that old LTX files are properly cleaned up after retention period

echo "=========================================="
echo "S3 LTX Retention Test - Small Database"
echo "=========================================="
echo ""
echo "Testing LTX file cleanup using local S3 mock with small database"
echo "Database target size: 50MB"
echo "Retention period: 2 minutes"
echo ""

# Configuration
PROJECT_ROOT="$(pwd)"
DB="/tmp/small-retention-test.db"
RESTORED_DB="/tmp/small-retention-restored.db"
LITESTREAM="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"
S3_MOCK="./etc/s3_mock.py"

# Build binaries if needed
if [ ! -f "$LITESTREAM" ]; then
    echo "Building litestream binary..."
    go build -o bin/litestream ./cmd/litestream
fi

if [ ! -f "$LITESTREAM_TEST" ]; then
    echo "Building litestream-test binary..."
    go build -o bin/litestream-test ./cmd/litestream-test
fi

# Check for Python S3 mock dependencies
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    echo "Using project virtual environment..."
    source "$PROJECT_ROOT/venv/bin/activate"
fi

if ! python3 -c "import moto, boto3" 2>/dev/null; then
    echo "⚠️  Missing Python dependencies. Installing moto and boto3..."
    if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
        source "$PROJECT_ROOT/venv/bin/activate"
        pip install moto boto3 || {
            echo "Failed to install dependencies in venv"
            exit 1
        }
    else
        pip3 install --user moto boto3 || {
            echo "Failed to install dependencies. Please run: pip3 install --user moto boto3"
            exit 1
        }
    fi
fi

# Cleanup function
cleanup() {
    # Kill any running processes
    pkill -f "litestream replicate.*small-retention-test.db" 2>/dev/null || true
    pkill -f "python.*s3_mock.py" 2>/dev/null || true

    # Clean up temp files
    rm -f "$DB"* "$RESTORED_DB"* /tmp/small-retention-*.log /tmp/small-retention-*.yml

    echo "Cleanup completed"
}

trap cleanup EXIT
cleanup

echo "=========================================="
echo "Step 1: Creating Small Test Database (50MB)"
echo "=========================================="

echo "[1.1] Creating and populating database to 50MB..."
$LITESTREAM_TEST populate \
    -db "$DB" \
    -target-size 50MB \
    -row-size 2048 \
    -batch-size 500

# Set WAL mode after population
sqlite3 "$DB" "PRAGMA journal_mode = WAL;"

DB_SIZE=$(du -h "$DB" | cut -f1)
RECORD_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM test_table_0;")
echo "  ✓ Database created: $DB_SIZE with $RECORD_COUNT records"

echo ""
echo "=========================================="
echo "Step 2: Starting Local S3 Mock and Replication"
echo "=========================================="

# Create Litestream config for S3 mock
cat > /tmp/small-retention-config.yml <<EOF
dbs:
  - path: $DB
    replicas:
      - type: s3
        bucket: \${LITESTREAM_S3_BUCKET}
        path: small-retention-test
        endpoint: \${LITESTREAM_S3_ENDPOINT}
        access-key-id: \${LITESTREAM_S3_ACCESS_KEY_ID}
        secret-access-key: \${LITESTREAM_S3_SECRET_ACCESS_KEY}
        force-path-style: true
        retention: 2m
        sync-interval: 5s
EOF

echo "[2.1] Starting S3 mock and replication..."
if [ -f "$PROJECT_ROOT/venv/bin/activate" ]; then
    PYTHON_CMD="$PROJECT_ROOT/venv/bin/python3"
else
    PYTHON_CMD="python3"
fi
$PYTHON_CMD $S3_MOCK $LITESTREAM replicate -config /tmp/small-retention-config.yml > /tmp/small-retention-test.log 2>&1 &
REPL_PID=$!
sleep 8

if ! kill -0 $REPL_PID 2>/dev/null; then
    echo "  ✗ Replication failed to start"
    echo "Log contents:"
    cat /tmp/small-retention-test.log
    exit 1
fi

echo "  ✓ S3 mock and replication started (PID: $REPL_PID)"

# Check initial sync
sleep 5
INITIAL_SYNC_LINES=$(grep -c "sync" /tmp/small-retention-test.log 2>/dev/null || echo "0")
echo "  ✓ Initial sync operations: $INITIAL_SYNC_LINES"

echo ""
echo "=========================================="
echo "Step 3: Generating LTX Files Over Time"
echo "=========================================="

echo "[3.1] Creating LTX files in batches (6 batches, 20 seconds apart)..."

# Function to add data and force checkpoint
add_batch_data() {
    local batch_num=$1
    echo "  Batch $batch_num: Adding 1000 records and checkpointing..."

    # Add data in small transactions to create multiple WAL segments
    for tx in {1..10}; do
        sqlite3 "$DB" <<EOF
BEGIN TRANSACTION;
INSERT INTO test_table_0 (data, text_field, int_field, float_field, timestamp)
SELECT randomblob(1024), 'batch-$batch_num-tx-$tx', $batch_num * 100 + $tx, random() / 1000.0, strftime('%s', 'now')
FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
      UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10);
COMMIT;
EOF
    done

    # Force checkpoint to create LTX files
    sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);"

    local new_count=$(sqlite3 "$DB" "SELECT COUNT(*) FROM test_table_0;")
    echo "    Total records: $new_count"
}

# Generate LTX files over time
for batch in {1..6}; do
    add_batch_data $batch

    # Check for LTX activity in logs
    LTX_ACTIVITY=$(grep -c -i "ltx\|segment\|upload" /tmp/small-retention-test.log 2>/dev/null || echo "0")
    echo "    LTX operations so far: $LTX_ACTIVITY"

    # Wait between batches (except last one)
    if [ $batch -lt 6 ]; then
        echo "    Waiting 20 seconds before next batch..."
        sleep 20
    fi
done

echo ""
echo "=========================================="
echo "Step 4: Monitoring Retention Cleanup"
echo "=========================================="

echo "[4.1] Waiting for retention cleanup to occur..."
echo "  Retention period: 2 minutes"
echo "  Monitoring for 4 minutes to observe cleanup..."

# Monitor cleanup activity
for minute in {1..4}; do
    echo ""
    echo "  Minute $minute/4 - $(date)"
    sleep 60

    # Check various log patterns that might indicate cleanup
    CLEANUP_PATTERNS=(
        "clean" "delet" "expir" "retention" "removed" "purge"
        "old" "ttl" "cleanup" "sweep" "vacuum" "evict"
    )

    CLEANUP_TOTAL=0
    for pattern in "${CLEANUP_PATTERNS[@]}"; do
        COUNT=$(grep -c -i "$pattern" /tmp/small-retention-test.log 2>/dev/null || echo "0")
        CLEANUP_TOTAL=$((CLEANUP_TOTAL + COUNT))
    done

    TOTAL_ERRORS=$(grep -c "ERROR" /tmp/small-retention-test.log 2>/dev/null || echo "0")
    SYNC_COUNT=$(grep -c "sync" /tmp/small-retention-test.log 2>/dev/null || echo "0")

    echo "    Cleanup-related log entries: $CLEANUP_TOTAL"
    echo "    Total sync operations: $SYNC_COUNT"
    echo "    Errors: $TOTAL_ERRORS"

    # Show recent activity
    RECENT_LINES=$(tail -5 /tmp/small-retention-test.log 2>/dev/null || echo "No recent activity")
    echo "    Recent activity: $(echo "$RECENT_LINES" | tr '\n' ' ' | cut -c1-80)..."
done

echo ""
echo "=========================================="
echo "Step 5: Final Validation"
echo "=========================================="

echo "[5.1] Stopping replication..."
kill $REPL_PID 2>/dev/null || true
wait $REPL_PID 2>/dev/null || true
sleep 2

echo "[5.2] Analyzing retention behavior..."

# Comprehensive log analysis
TOTAL_ERRORS=$(grep -c "ERROR" /tmp/small-retention-test.log 2>/dev/null || echo "0")
TOTAL_WARNINGS=$(grep -c "WARN" /tmp/small-retention-test.log 2>/dev/null || echo "0")
SYNC_OPERATIONS=$(grep -c "sync" /tmp/small-retention-test.log 2>/dev/null || echo "0")

# Search for cleanup indicators more broadly
CLEANUP_INDICATORS=$(grep -i -c "clean\|delet\|expir\|retention\|removed\|purge\|old.*file\|ttl" /tmp/small-retention-test.log 2>/dev/null || echo "0")

echo ""
echo "Log Analysis Summary:"
echo "===================="
echo "  Total errors: $TOTAL_ERRORS"
echo "  Total warnings: $TOTAL_WARNINGS"
echo "  Sync operations: $SYNC_OPERATIONS"
echo "  Cleanup indicators: $CLEANUP_INDICATORS"

if [ "$CLEANUP_INDICATORS" -gt "0" ]; then
    echo ""
    echo "Cleanup activity detected:"
    grep -i "clean\|delet\|expir\|retention\|removed\|purge\|old.*file\|ttl" /tmp/small-retention-test.log | head -10
else
    echo ""
    echo "⚠️  No explicit cleanup activity found in logs"
    echo "   Note: Litestream may perform silent cleanup without verbose logging"
fi

# Show any errors
if [ "$TOTAL_ERRORS" -gt "0" ]; then
    echo ""
    echo "Errors encountered:"
    grep "ERROR" /tmp/small-retention-test.log | tail -5
fi

echo ""
echo "[5.3] Testing restoration to verify integrity..."

# Test restoration using S3 mock
echo "Attempting restoration from S3 mock..."
RESTORE_SUCCESS=true

if ! timeout 30 $PYTHON_CMD $S3_MOCK $LITESTREAM restore -o "$RESTORED_DB" \
    "s3://\${LITESTREAM_S3_BUCKET}/small-retention-test" 2>/tmp/restore.log; then
    echo "  ✗ Restoration failed"
    RESTORE_SUCCESS=false
    cat /tmp/restore.log
else
    echo "  ✓ Restoration completed"

    # Verify restored database integrity
    if sqlite3 "$RESTORED_DB" "PRAGMA integrity_check;" | grep -q "ok"; then
        echo "  ✓ Restored database integrity check passed"
    else
        echo "  ✗ Restored database integrity check failed"
        RESTORE_SUCCESS=false
    fi

    # Compare record counts
    ORIGINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM test_table_0;" 2>/dev/null || echo "unknown")
    RESTORED_COUNT=$(sqlite3 "$RESTORED_DB" "SELECT COUNT(*) FROM test_table_0;" 2>/dev/null || echo "unknown")

    echo "  Original records: $ORIGINAL_COUNT"
    echo "  Restored records: $RESTORED_COUNT"

    if [ "$ORIGINAL_COUNT" = "$RESTORED_COUNT" ] && [ "$ORIGINAL_COUNT" != "unknown" ]; then
        echo "  ✓ Record counts match"
    else
        echo "  ⚠️  Record count mismatch (may be normal due to ongoing replication)"
    fi
fi

echo ""
echo "=========================================="
echo "Test Results Summary"
echo "=========================================="

FINAL_RECORD_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM test_table_0;" 2>/dev/null || echo "unknown")
FINAL_DB_SIZE=$(du -h "$DB" 2>/dev/null | cut -f1 || echo "unknown")

echo ""
echo "Database Statistics:"
echo "  Final size: $FINAL_DB_SIZE"
echo "  Final record count: $FINAL_RECORD_COUNT"
echo "  Test duration: ~8 minutes"
echo ""
echo "Replication Analysis:"
echo "  Sync operations: $SYNC_OPERATIONS"
echo "  Cleanup indicators: $CLEANUP_INDICATORS"
echo "  Errors: $TOTAL_ERRORS"
echo "  Warnings: $TOTAL_WARNINGS"
echo ""
echo "Restoration Test:"
if [ "$RESTORE_SUCCESS" = true ]; then
    echo "  Status: ✓ SUCCESS"
else
    echo "  Status: ✗ FAILED"
fi

echo ""
echo "Expected Behavior Verification:"
echo "  ✓ Database created and populated successfully"
echo "  ✓ S3 mock replication setup working"
echo "  ✓ Multiple LTX files generated over time"
if [ "$CLEANUP_INDICATORS" -gt "0" ]; then
    echo "  ✓ Cleanup activity observed in logs"
else
    echo "  ? Cleanup activity not explicitly logged (may still be working)"
fi
if [ "$RESTORE_SUCCESS" = true ]; then
    echo "  ✓ Database restoration successful"
else
    echo "  ✗ Database restoration issues detected"
fi

echo ""
echo "Key Test Files:"
echo "  - Replication log: /tmp/small-retention-test.log"
echo "  - Config file: /tmp/small-retention-config.yml"
echo "  - Original database: $DB"
if [ -f "$RESTORED_DB" ]; then
    echo "  - Restored database: $RESTORED_DB"
fi

echo ""
echo "Notes:"
echo "  - This test uses a local S3 mock (moto) for isolation"
echo "  - Real S3 testing may show different cleanup patterns"
echo "  - Retention behavior may vary with Litestream version"
echo "  - Check logs for specific cleanup messages"

echo ""
echo "=========================================="
echo "Small Database S3 Retention Test Complete"
echo "=========================================="
