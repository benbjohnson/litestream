#!/bin/bash

# Test: WAL Growth and Size Limits
# This tests how Litestream handles extreme WAL growth scenarios

set -e

echo "=========================================="
echo "WAL Growth and Size Limits Test"
echo "=========================================="
echo ""
echo "Testing Litestream's handling of large WAL files"
echo ""

# Configuration
DB="/tmp/wal-growth.db"
REPLICA="/tmp/wal-growth-replica"
LITESTREAM="./bin/litestream"
TARGET_WAL_SIZE_MB=100  # Target WAL size in MB

# Clean up
echo "[SETUP] Cleaning up..."
rm -f "$DB"*
rm -rf "$REPLICA"

# Create fresh database
echo "[1] Creating database..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode=WAL;
PRAGMA wal_autocheckpoint=0;  -- Disable auto-checkpoint
CREATE TABLE test (id INTEGER PRIMARY KEY, data BLOB);
EOF
echo "  ✓ Database created with auto-checkpoint disabled"

# Start Litestream
echo ""
echo "[2] Starting Litestream..."
$LITESTREAM replicate "$DB" "file://$REPLICA" > /tmp/wal-growth.log 2>&1 &
LITESTREAM_PID=$!
sleep 3

if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
    echo "  ✗ Litestream failed to start"
    cat /tmp/wal-growth.log | head -10
    exit 1
fi
echo "  ✓ Litestream running (PID: $LITESTREAM_PID)"

# Write data until WAL reaches target size
echo ""
echo "[3] Growing WAL to ${TARGET_WAL_SIZE_MB}MB..."
echo "  Writing large blobs without checkpointing..."

BATCH_COUNT=0
while true; do
    # Check current WAL size
    WAL_SIZE=$(stat -f%z "$DB-wal" 2>/dev/null || stat -c%s "$DB-wal" 2>/dev/null || echo "0")
    WAL_SIZE_MB=$((WAL_SIZE / 1024 / 1024))

    if [ $WAL_SIZE_MB -ge $TARGET_WAL_SIZE_MB ]; then
        echo "  ✓ WAL reached ${WAL_SIZE_MB}MB"
        break
    fi

    # Write a batch of large records
    sqlite3 "$DB" <<EOF 2>/dev/null || true
BEGIN;
INSERT INTO test (data) SELECT randomblob(10000) FROM generate_series(1, 100);
COMMIT;
EOF

    BATCH_COUNT=$((BATCH_COUNT + 1))
    if [ $((BATCH_COUNT % 10)) -eq 0 ]; then
        echo "    WAL size: ${WAL_SIZE_MB}MB / ${TARGET_WAL_SIZE_MB}MB"
    fi

    # Check if Litestream is still alive
    if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
        echo "  ✗ Litestream died during WAL growth!"
        break
    fi
done

# Check Litestream status
echo ""
echo "[4] Checking Litestream status with large WAL..."
if kill -0 $LITESTREAM_PID 2>/dev/null; then
    echo "  ✓ Litestream still running with ${WAL_SIZE_MB}MB WAL"

    # Check replication lag
    sleep 5
    LATEST_LTX=$(ls -t "$REPLICA/ltx/0/" 2>/dev/null | head -1)
    if [ -n "$LATEST_LTX" ]; then
        echo "  ✓ Still replicating (latest: $LATEST_LTX)"
    else
        echo "  ⚠ No recent replication activity"
    fi
else
    echo "  ✗ Litestream crashed!"
fi

# Check for errors
echo ""
echo "[5] Checking for errors..."
ERROR_COUNT=$(grep -c "ERROR" /tmp/wal-growth.log 2>/dev/null || echo "0")
OOM_COUNT=$(grep -c -i "out of memory\|oom" /tmp/wal-growth.log 2>/dev/null || echo "0")

if [ "$OOM_COUNT" -gt 0 ]; then
    echo "  ✗ Out of memory errors detected!"
elif [ "$ERROR_COUNT" -gt 1 ]; then
    echo "  ⚠ Errors detected: $ERROR_COUNT"
    grep "ERROR" /tmp/wal-growth.log | tail -3
else
    echo "  ✓ No significant errors"
fi

# Get statistics
echo ""
echo "[6] Statistics..."
ROW_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM test;" 2>/dev/null || echo "0")
DB_SIZE=$(stat -f%z "$DB" 2>/dev/null || stat -c%s "$DB" 2>/dev/null || echo "0")
LTX_COUNT=$(find "$REPLICA" -name "*.ltx" 2>/dev/null | wc -l || echo "0")

echo "  Database size: $((DB_SIZE / 1024 / 1024))MB"
echo "  WAL size: ${WAL_SIZE_MB}MB"
echo "  Row count: $ROW_COUNT"
echo "  LTX files: $LTX_COUNT"

# Now checkpoint and see what happens
echo ""
echo "[7] Executing checkpoint on large WAL..."
CHECKPOINT_START=$(date +%s)
CHECKPOINT_RESULT=$(sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);" 2>&1) || echo "Failed"
CHECKPOINT_END=$(date +%s)
CHECKPOINT_TIME=$((CHECKPOINT_END - CHECKPOINT_START))

echo "  Checkpoint result: $CHECKPOINT_RESULT"
echo "  Checkpoint time: ${CHECKPOINT_TIME}s"

# Check WAL size after checkpoint
NEW_WAL_SIZE=$(stat -f%z "$DB-wal" 2>/dev/null || stat -c%s "$DB-wal" 2>/dev/null || echo "0")
NEW_WAL_SIZE_MB=$((NEW_WAL_SIZE / 1024 / 1024))
echo "  WAL size after checkpoint: ${NEW_WAL_SIZE_MB}MB"

# Let Litestream catch up
echo ""
echo "[8] Letting Litestream catch up after checkpoint..."
sleep 10

# Check if Litestream survived
if kill -0 $LITESTREAM_PID 2>/dev/null; then
    echo "  ✓ Litestream survived large checkpoint"
else
    echo "  ✗ Litestream died after checkpoint"
fi

# Stop Litestream
kill $LITESTREAM_PID 2>/dev/null || true
sleep 2

# Test restore
echo ""
echo "[9] Testing restore after large WAL handling..."
rm -f /tmp/wal-restored.db
if $LITESTREAM restore -o /tmp/wal-restored.db "file://$REPLICA" 2>&1 | tee /tmp/restore-wal.log; then
    REST_COUNT=$(sqlite3 /tmp/wal-restored.db "SELECT COUNT(*) FROM test;" 2>/dev/null || echo "0")

    if [ "$REST_COUNT" -eq "$ROW_COUNT" ]; then
        echo "  ✓ Restore successful: $REST_COUNT rows"
        echo ""
        echo "TEST PASSED: Handled ${TARGET_WAL_SIZE_MB}MB WAL successfully"
    else
        echo "  ⚠ Row count mismatch: Original=$ROW_COUNT, Restored=$REST_COUNT"
        echo ""
        echo "TEST FAILED: Data loss with large WAL"
    fi
else
    echo "  ✗ Restore failed!"
    echo ""
    echo "TEST FAILED: Cannot restore after large WAL"
fi

echo ""
echo "=========================================="
echo "Summary:"
echo "  Maximum WAL size tested: ${WAL_SIZE_MB}MB"
echo "  Checkpoint time: ${CHECKPOINT_TIME}s"
echo "  Data integrity: $([ "$REST_COUNT" -eq "$ROW_COUNT" ] && echo "✓ Preserved" || echo "✗ Lost")"
echo "=========================================="
