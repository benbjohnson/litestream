#!/bin/bash
set -euo pipefail

# Quick validation test - runs for 30 minutes with aggressive settings
# Use this to validate configuration before overnight runs

TEST_DURATION="${TEST_DURATION:-30m}"
TEST_DIR="/tmp/litestream-quick-$(date +%Y%m%d-%H%M%S)"
DB_PATH="$TEST_DIR/test.db"
REPLICA_PATH="$TEST_DIR/replica"
CONFIG_FILE="$TEST_DIR/litestream.yml"
LOG_DIR="$TEST_DIR/logs"

echo "================================================"
echo "Litestream Quick Validation Test"
echo "================================================"
echo "Duration: $TEST_DURATION"
echo "Test directory: $TEST_DIR"
echo "Start time: $(date)"
echo ""

cleanup() {
    echo ""
    echo "Cleaning up..."

    # Kill all spawned processes
    jobs -p | xargs -r kill 2>/dev/null || true
    wait

    echo "Test completed at: $(date)"
    echo "Results saved in: $TEST_DIR"
}

trap cleanup EXIT INT TERM

# Create directories
mkdir -p "$TEST_DIR" "$LOG_DIR" "$REPLICA_PATH"

# Build binaries if needed
echo "Building binaries..."
if [ ! -f bin/litestream ]; then
    go build -o bin/litestream ./cmd/litestream
fi
if [ ! -f bin/litestream-test ]; then
    go build -o bin/litestream-test ./cmd/litestream-test
fi

# Create test database
echo "Creating test database..."
sqlite3 "$DB_PATH" <<EOF
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS test_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data BLOB,
    created_at INTEGER
);
EOF

# Create aggressive test configuration
echo "Creating test configuration..."
cat > "$CONFIG_FILE" <<EOF
dbs:
  - path: $DB_PATH
    replicas:
      - type: file
        path: $REPLICA_PATH

        # Very aggressive settings for quick testing
        snapshot-interval: 2m
        retention: 1h
        retention-check-interval: 5m

        # Frequent compaction for testing
        compaction:
          - duration: 30s
            interval: 30s
          - duration: 1m
            interval: 1m
          - duration: 5m
            interval: 5m
          - duration: 15m
            interval: 10m

    # Aggressive checkpoint settings
    checkpoint-interval: 15s
    min-checkpoint-page-count: 100
    max-checkpoint-page-count: 1000
EOF

echo "Starting litestream..."
LOG_LEVEL=debug bin/litestream replicate -config "$CONFIG_FILE" > "$LOG_DIR/litestream.log" 2>&1 &
LITESTREAM_PID=$!

sleep 3

if ! kill -0 "$LITESTREAM_PID" 2>/dev/null; then
    echo "ERROR: Litestream failed to start!"
    tail -50 "$LOG_DIR/litestream.log"
    exit 1
fi

echo "Litestream running (PID: $LITESTREAM_PID)"
echo ""

# Quick populate
echo "Populating database (10MB)..."
bin/litestream-test populate -db "$DB_PATH" -target-size 10MB -batch-size 1000 > "$LOG_DIR/populate.log" 2>&1

# Start load generator
echo "Starting load generator..."
bin/litestream-test load \
    -db "$DB_PATH" \
    -write-rate 20 \
    -duration "$TEST_DURATION" \
    -pattern wave \
    -payload-size 1024 \
    -read-ratio 0.3 \
    -workers 2 \
    > "$LOG_DIR/load.log" 2>&1 &
LOAD_PID=$!

echo "Load generator running (PID: $LOAD_PID)"
echo ""

# Monitor function
monitor_quick() {
    while true; do
        sleep 30

        echo "[$(date +%H:%M:%S)] Status check"

        # Check database size
        if [ -f "$DB_PATH" ]; then
            DB_SIZE=$(stat -f%z "$DB_PATH" 2>/dev/null || stat -c%s "$DB_PATH" 2>/dev/null)
            echo "  Database: $(numfmt --to=iec-i --suffix=B $DB_SIZE 2>/dev/null || echo "$DB_SIZE bytes")"
        fi

        # Count replica files
        if [ -d "$REPLICA_PATH" ]; then
            SNAPSHOTS=$(find "$REPLICA_PATH" -name "*.snapshot.lz4" | wc -l | tr -d ' ')
            WALS=$(find "$REPLICA_PATH" -name "*.wal.lz4" | wc -l | tr -d ' ')
            echo "  Snapshots: $SNAPSHOTS, WAL segments: $WALS"
        fi

        # Check for compaction
        COMPACT_COUNT=$(grep -c "compacting" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        echo "  Compactions so far: $COMPACT_COUNT"

        # Check for errors
        ERROR_COUNT=$(grep -c "ERROR\|error" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        if [ "$ERROR_COUNT" -gt 0 ]; then
            echo "  ⚠ Errors detected: $ERROR_COUNT"
            grep "ERROR\|error" "$LOG_DIR/litestream.log" | tail -2
        fi

        # Check processes
        if ! kill -0 "$LITESTREAM_PID" 2>/dev/null; then
            echo "  ✗ Litestream stopped unexpectedly!"
            break
        fi

        if ! kill -0 "$LOAD_PID" 2>/dev/null; then
            echo "  ✓ Load test completed"
            break
        fi

        echo ""
    done
}

echo "Running test for $TEST_DURATION..."
echo "================================================"
echo ""

# Start monitoring in background
monitor_quick &
MONITOR_PID=$!

# Wait for load test to complete
wait "$LOAD_PID" 2>/dev/null || true

# Stop the monitor
kill $MONITOR_PID 2>/dev/null || true
wait $MONITOR_PID 2>/dev/null || true

echo ""
echo "================================================"
echo "Test Results"
echo "================================================"

# Final statistics
echo "Database Statistics:"
if [ -f "$DB_PATH" ]; then
    DB_SIZE=$(stat -f%z "$DB_PATH" 2>/dev/null || stat -c%s "$DB_PATH" 2>/dev/null)
    ROW_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM test_data" 2>/dev/null || echo "Unknown")
    echo "  Final size: $(numfmt --to=iec-i --suffix=B $DB_SIZE 2>/dev/null || echo "$DB_SIZE bytes")"
    echo "  Total rows: $ROW_COUNT"
fi

echo ""
echo "Replication Statistics:"
if [ -d "$REPLICA_PATH" ]; then
    SNAPSHOT_COUNT=$(find "$REPLICA_PATH" -name "*.snapshot.lz4" | wc -l | tr -d ' ')
    WAL_COUNT=$(find "$REPLICA_PATH" -name "*.wal.lz4" | wc -l | tr -d ' ')
    REPLICA_SIZE=$(du -sh "$REPLICA_PATH" | cut -f1)
    echo "  Snapshots created: $SNAPSHOT_COUNT"
    echo "  WAL segments: $WAL_COUNT"
    echo "  Replica size: $REPLICA_SIZE"
fi

echo ""
echo "Operation Counts:"
COMPACTION_COUNT=$(grep -c "compacting" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
CHECKPOINT_COUNT=$(grep -c "checkpoint" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
ERROR_COUNT=$(grep -c "ERROR\|error" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
echo "  Compactions: $COMPACTION_COUNT"
echo "  Checkpoints: $CHECKPOINT_COUNT"
echo "  Errors: $ERROR_COUNT"

# Quick validation
echo ""
echo "Validation:"
bin/litestream-test validate \
    -source "$DB_PATH" \
    -replica "$REPLICA_PATH" \
    > "$LOG_DIR/validate.log" 2>&1

if [ $? -eq 0 ]; then
    echo "  ✓ Validation passed!"
else
    echo "  ✗ Validation failed!"
    tail -10 "$LOG_DIR/validate.log"
fi

# Test restoration
echo ""
echo "Testing restoration..."
RESTORE_DB="$TEST_DIR/restored.db"
bin/litestream restore -o "$RESTORE_DB" "file://$REPLICA_PATH" > "$LOG_DIR/restore.log" 2>&1

if [ $? -eq 0 ]; then
    RESTORED_COUNT=$(sqlite3 "$RESTORE_DB" "SELECT COUNT(*) FROM test_data" 2>/dev/null || echo "0")
    ORIGINAL_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM test_data" 2>/dev/null || echo "0")

    if [ "$RESTORED_COUNT" = "$ORIGINAL_COUNT" ]; then
        echo "  ✓ Restoration successful! ($RESTORED_COUNT rows)"
    else
        echo "  ⚠ Row count mismatch! Original: $ORIGINAL_COUNT, Restored: $RESTORED_COUNT"
    fi
else
    echo "  ✗ Restoration failed!"
fi

# Summary
echo ""
echo "================================================"
if [ "$ERROR_COUNT" -eq 0 ] && [ "$COMPACTION_COUNT" -gt 0 ] && [ "$SNAPSHOT_COUNT" -gt 0 ]; then
    echo "✓ Quick validation PASSED!"
    echo ""
    echo "The configuration appears ready for overnight testing."
    echo "Run the overnight test with:"
    echo "  ./test-overnight.sh"
else
    echo "⚠ Quick validation completed with issues:"
    [ "$ERROR_COUNT" -gt 0 ] && echo "  - Errors detected: $ERROR_COUNT"
    [ "$COMPACTION_COUNT" -eq 0 ] && echo "  - No compactions occurred"
    [ "$SNAPSHOT_COUNT" -eq 0 ] && echo "  - No snapshots created"
    echo ""
    echo "Review the logs before running overnight tests:"
    echo "  $LOG_DIR/litestream.log"
fi

echo ""
echo "Full results available in: $TEST_DIR"
echo "================================================"
