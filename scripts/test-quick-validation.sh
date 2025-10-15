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

# Create test database and populate BEFORE starting litestream
echo "Creating test database..."
sqlite3 "$DB_PATH" <<EOF
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS test_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data BLOB,
    created_at INTEGER
);
EOF

# Populate database BEFORE litestream starts
echo "Populating database (10MB)..."
bin/litestream-test populate -db "$DB_PATH" -target-size 10MB -batch-size 1000 > "$LOG_DIR/populate.log" 2>&1
if [ $? -ne 0 ]; then
    echo "Warning: Population failed, but continuing..."
    cat "$LOG_DIR/populate.log"
fi

# Create aggressive test configuration
echo "Creating test configuration..."
cat > "$CONFIG_FILE" <<EOF
# Very aggressive snapshot settings for quick testing
snapshot:
  interval: 1m       # Snapshots every minute
  retention: 30m     # Keep data for 30 minutes

# Frequent compaction levels for testing
levels:
  - interval: 30s
  - interval: 1m
  - interval: 5m
  - interval: 10m

dbs:
  - path: $DB_PATH
    # Aggressive checkpoint settings
    checkpoint-interval: 30s
    min-checkpoint-page-count: 10
    max-checkpoint-page-count: 10000

    replicas:
      - type: file
        path: $REPLICA_PATH
        retention-check-interval: 2m
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

# Start load generator with more aggressive settings
echo "Starting load generator..."
bin/litestream-test load \
    -db "$DB_PATH" \
    -write-rate 100 \
    -duration "$TEST_DURATION" \
    -pattern wave \
    -payload-size 4096 \
    -read-ratio 0.2 \
    -workers 4 \
    > "$LOG_DIR/load.log" 2>&1 &
LOAD_PID=$!

echo "Load generator running (PID: $LOAD_PID)"
echo ""

# Monitor function
monitor_quick() {
    while true; do
        sleep 30

        echo "[$(date +%H:%M:%S)] Status check"

        # Check database size and WAL size
        if [ -f "$DB_PATH" ]; then
            DB_SIZE=$(stat -f%z "$DB_PATH" 2>/dev/null || stat -c%s "$DB_PATH" 2>/dev/null)
            echo "  Database: $(numfmt --to=iec-i --suffix=B $DB_SIZE 2>/dev/null || echo "$DB_SIZE bytes")"

            # Check WAL file size
            if [ -f "$DB_PATH-wal" ]; then
                WAL_SIZE=$(stat -f%z "$DB_PATH-wal" 2>/dev/null || stat -c%s "$DB_PATH-wal" 2>/dev/null)
                echo "  WAL size: $(numfmt --to=iec-i --suffix=B $WAL_SIZE 2>/dev/null || echo "$WAL_SIZE bytes")"
            fi
        fi

        # Count replica files (for file replica type, count LTX files)
        if [ -d "$REPLICA_PATH" ]; then
            # Count snapshot files (snapshot.ltx files)
            SNAPSHOTS=$(find "$REPLICA_PATH" -name "*snapshot*.ltx" 2>/dev/null | wc -l | tr -d ' ')
            # Count LTX files (WAL segments)
            LTX_FILES=$(find "$REPLICA_PATH" -name "*.ltx" 2>/dev/null | wc -l | tr -d ' ')
            echo "  Snapshots: $SNAPSHOTS, LTX segments: $LTX_FILES"

            # Show replica directory size
            REPLICA_SIZE=$(du -sh "$REPLICA_PATH" 2>/dev/null | cut -f1)
            echo "  Replica size: $REPLICA_SIZE"
        fi

        # Check for compaction (look for "compaction complete")
        COMPACT_COUNT=$(grep -c "compaction complete" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        echo "  Compactions: $COMPACT_COUNT"

        # Check for checkpoints (look for various checkpoint patterns)
        CHECKPOINT_COUNT=$(grep -iE "checkpoint|checkpointed" "$LOG_DIR/litestream.log" 2>/dev/null | wc -l | tr -d ' ')
        echo "  Checkpoints: $CHECKPOINT_COUNT"

        # Check sync activity
        SYNC_COUNT=$(grep -c "replica sync" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        echo "  Syncs: $SYNC_COUNT"

        # Check for errors (exclude known non-critical errors)
        ERROR_COUNT=$(grep -i "ERROR" "$LOG_DIR/litestream.log" 2>/dev/null | grep -v "page size not initialized" | wc -l | tr -d ' ')
        if [ "$ERROR_COUNT" -gt 0 ]; then
            echo "  ⚠ Critical errors: $ERROR_COUNT"
            grep -i "ERROR" "$LOG_DIR/litestream.log" | grep -v "page size not initialized" | tail -2
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
    # Find the actual table name - tables are space-separated on one line
    TABLES=$(sqlite3 "$DB_PATH" ".tables" 2>/dev/null)
    # Look for the main data table
    if echo "$TABLES" | grep -q "load_test"; then
        ROW_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM load_test" 2>/dev/null || echo "0")
    elif echo "$TABLES" | grep -q "test_table_0"; then
        ROW_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM test_table_0" 2>/dev/null || echo "0")
    elif echo "$TABLES" | grep -q "test_data"; then
        ROW_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM test_data" 2>/dev/null || echo "0")
    else
        ROW_COUNT="0"
    fi
    echo "  Final size: $(numfmt --to=iec-i --suffix=B $DB_SIZE 2>/dev/null || echo "$DB_SIZE bytes")"
    echo "  Total rows: $ROW_COUNT"
fi

echo ""
echo "Replication Statistics:"
if [ -d "$REPLICA_PATH" ]; then
    SNAPSHOT_COUNT=$(find "$REPLICA_PATH" -name "*snapshot*.ltx" 2>/dev/null | wc -l | tr -d ' ')
    LTX_COUNT=$(find "$REPLICA_PATH" -name "*.ltx" 2>/dev/null | wc -l | tr -d ' ')
    REPLICA_SIZE=$(du -sh "$REPLICA_PATH" | cut -f1)
    echo "  Snapshots created: $SNAPSHOT_COUNT"
    echo "  LTX segments: $LTX_COUNT"
    echo "  Replica size: $REPLICA_SIZE"
fi

echo ""
echo "Operation Counts:"
# Count operations from log
if [ -f "$LOG_DIR/litestream.log" ]; then
    COMPACTION_COUNT=$(grep -c "compaction complete" "$LOG_DIR/litestream.log" || echo "0")
    CHECKPOINT_COUNT=$(grep -iE "checkpoint|checkpointed" "$LOG_DIR/litestream.log" | wc -l | tr -d ' ' || echo "0")
    ERROR_COUNT=$(grep -i "ERROR" "$LOG_DIR/litestream.log" | grep -v "page size not initialized" | wc -l | tr -d ' ' || echo "0")
else
    COMPACTION_COUNT="0"
    CHECKPOINT_COUNT="0"
    ERROR_COUNT="0"
fi
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
# Count critical errors (exclude known non-critical ones)
CRITICAL_ERROR_COUNT=$(grep -i "ERROR" "$LOG_DIR/litestream.log" 2>/dev/null | grep -v "page size not initialized" | wc -l | tr -d ' ')

if [ "$CRITICAL_ERROR_COUNT" -eq 0 ] && [ "$LTX_COUNT" -gt 0 ]; then
    echo "✓ Quick validation PASSED!"
    echo ""
    echo "Summary:"
    echo "  - Litestream successfully replicated data"
    echo "  - Created $LTX_COUNT LTX segments"
    [ "$SNAPSHOT_COUNT" -gt 0 ] && echo "  - Created $SNAPSHOT_COUNT snapshots"
    [ "$COMPACTION_COUNT" -gt 0 ] && echo "  - Performed $COMPACTION_COUNT compactions"
    echo ""
    echo "The configuration appears ready for overnight testing."
    echo "Run the overnight test with:"
    echo "  ./test-overnight.sh"
else
    echo "⚠ Quick validation completed with issues:"
    [ "$CRITICAL_ERROR_COUNT" -gt 0 ] && echo "  - Critical errors detected: $CRITICAL_ERROR_COUNT"
    [ "$LTX_COUNT" -eq 0 ] && echo "  - No LTX segments created (replication not working)"
    [ "$SNAPSHOT_COUNT" -eq 0 ] && echo "  - No snapshots created (may be normal for short tests)"
    [ "$COMPACTION_COUNT" -eq 0 ] && echo "  - No compactions occurred (may be normal for short tests)"
    echo ""
    echo "Review the logs before running overnight tests:"
    echo "  $LOG_DIR/litestream.log"
fi

echo ""
echo "Full results available in: $TEST_DIR"
echo "================================================"
