#!/bin/bash

# Test: Rapid Checkpoint Cycling
# This tests Litestream's behavior under rapid checkpoint pressure

set -e

echo "=========================================="
echo "Rapid Checkpoint Cycling Test"
echo "=========================================="
echo ""
echo "Testing Litestream under rapid checkpoint pressure"
echo ""

# Configuration
DB="/tmp/checkpoint-cycle.db"
REPLICA="/tmp/checkpoint-cycle-replica"
LITESTREAM="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# Clean up
echo "[SETUP] Cleaning up..."
rm -f "$DB"*
rm -rf "$REPLICA"

# Start with fresh database
echo "[1] Creating initial database..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode=WAL;
CREATE TABLE test (id INTEGER PRIMARY KEY, data BLOB);
EOF
echo "  ✓ Database created"

# Start Litestream
echo ""
echo "[2] Starting Litestream..."
$LITESTREAM replicate "$DB" "file://$REPLICA" > /tmp/checkpoint-cycle.log 2>&1 &
LITESTREAM_PID=$!
sleep 3

if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
    echo "  ✗ Litestream failed to start"
    cat /tmp/checkpoint-cycle.log
    exit 1
fi
echo "  ✓ Litestream running (PID: $LITESTREAM_PID)"

# Start continuous writes in background
echo ""
echo "[3] Starting continuous writes..."
(
    while kill -0 $LITESTREAM_PID 2>/dev/null; do
        sqlite3 "$DB" "INSERT INTO test (data) VALUES (randomblob(1000));" 2>/dev/null || true
        sleep 0.01  # 100 writes/sec attempt
    done
) &
WRITE_PID=$!
echo "  ✓ Write loop started"

# Rapid checkpoint cycling
echo ""
echo "[4] Starting rapid checkpoint cycling (30 seconds)..."
echo "  Testing all checkpoint modes in rapid succession..."

CHECKPOINT_COUNT=0
ERRORS=0
START_TIME=$(date +%s)

while [ $(($(date +%s) - START_TIME)) -lt 30 ]; do
    # Cycle through different checkpoint modes
    for MODE in PASSIVE FULL RESTART TRUNCATE; do
        if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
            echo "  ✗ Litestream crashed during checkpoint!"
            break 2
        fi

        # Execute checkpoint
        OUTPUT=$(sqlite3 "$DB" "PRAGMA wal_checkpoint($MODE);" 2>&1) || {
            ERRORS=$((ERRORS + 1))
            echo "  ⚠ Checkpoint $MODE error: $OUTPUT"
        }
        CHECKPOINT_COUNT=$((CHECKPOINT_COUNT + 1))

        # Very brief pause
        sleep 0.1
    done
done

echo "  Executed $CHECKPOINT_COUNT checkpoints with $ERRORS errors"

# Stop writes
kill $WRITE_PID 2>/dev/null || true

# Let Litestream catch up
echo ""
echo "[5] Letting Litestream stabilize..."
sleep 5

# Check Litestream health
if kill -0 $LITESTREAM_PID 2>/dev/null; then
    echo "  ✓ Litestream survived rapid checkpointing"
else
    echo "  ✗ Litestream died during test"
fi

# Check for sync errors
echo ""
echo "[6] Checking for sync errors..."
SYNC_ERRORS=$(grep -c "sync error" /tmp/checkpoint-cycle.log 2>/dev/null || echo "0")
FLAGS_ERRORS=$(grep -c "no flags allowed" /tmp/checkpoint-cycle.log 2>/dev/null || echo "0")

if [ "$FLAGS_ERRORS" -gt 0 ]; then
    echo "  ✗ ltx v0.5.0 flag errors detected: $FLAGS_ERRORS"
elif [ "$SYNC_ERRORS" -gt 0 ]; then
    echo "  ⚠ Sync errors detected: $SYNC_ERRORS"
else
    echo "  ✓ No sync errors"
fi

# Check replica status
echo ""
echo "[7] Checking replica status..."
if [ -d "$REPLICA/ltx" ]; then
    LTX_COUNT=$(find "$REPLICA/ltx" -name "*.ltx" | wc -l)
    echo "  ✓ Replica has $LTX_COUNT LTX files"
else
    echo "  ✗ No replica created!"
fi

# Get final stats
ROW_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM test;" 2>/dev/null || echo "0")
WAL_SIZE=$(stat -f%z "$DB-wal" 2>/dev/null || stat -c%s "$DB-wal" 2>/dev/null || echo "0")
echo "  Final row count: $ROW_COUNT"
echo "  Final WAL size: $((WAL_SIZE / 1024))KB"

# Stop Litestream
kill $LITESTREAM_PID 2>/dev/null || true
sleep 2

# Test restore
echo ""
echo "[8] Testing restore after rapid checkpointing..."
rm -f /tmp/checkpoint-restored.db
if $LITESTREAM restore -o /tmp/checkpoint-restored.db "file://$REPLICA" 2>&1 | tee /tmp/restore-checkpoint.log; then
    REST_COUNT=$(sqlite3 /tmp/checkpoint-restored.db "SELECT COUNT(*) FROM test;" 2>/dev/null || echo "0")

    if [ "$REST_COUNT" -eq "$ROW_COUNT" ]; then
        echo "  ✓ Restore successful: $REST_COUNT rows"
        echo ""
        echo "TEST PASSED: Survived $CHECKPOINT_COUNT rapid checkpoints"
    else
        echo "  ⚠ Row count mismatch: Original=$ROW_COUNT, Restored=$REST_COUNT"
        LOSS=$((ROW_COUNT - REST_COUNT))
        echo "  Data loss: $LOSS rows"
        echo ""
        echo "TEST FAILED: Data loss after rapid checkpointing"
    fi
else
    echo "  ✗ Restore failed!"
    cat /tmp/restore-checkpoint.log
    echo ""
    echo "TEST FAILED: Cannot restore after rapid checkpointing"
fi

echo ""
echo "=========================================="
echo "Summary:"
echo "  Checkpoints executed: $CHECKPOINT_COUNT"
echo "  Checkpoint errors: $ERRORS"
echo "  Sync errors: $SYNC_ERRORS"
echo "  Flag errors: $FLAGS_ERRORS"
echo "  Rows written: $ROW_COUNT"
echo "=========================================="
