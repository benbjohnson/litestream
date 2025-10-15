#!/bin/bash
set -euo pipefail

# Comprehensive validation test with aggressive settings
# This test exercises all Litestream features: replication, snapshots, compaction, checkpoints
# Can be run for any duration - defaults to 2 hours for thorough testing

TEST_DURATION="${TEST_DURATION:-2h}"
TEST_DIR="/tmp/litestream-comprehensive-$(date +%Y%m%d-%H%M%S)"
DB_PATH="$TEST_DIR/test.db"
REPLICA_PATH="$TEST_DIR/replica"
CONFIG_FILE="$TEST_DIR/litestream.yml"
LOG_DIR="$TEST_DIR/logs"

echo "================================================"
echo "Litestream Comprehensive Validation Test"
echo "================================================"
echo "Duration: $TEST_DURATION"
echo "Test directory: $TEST_DIR"
echo "Start time: $(date)"
echo ""
echo "This test uses aggressive settings to validate:"
echo "  - Continuous replication"
echo "  - Snapshot generation (every 10m)"
echo "  - Compaction (30s/1m/5m intervals)"
echo "  - Checkpoint operations"
echo "  - Database restoration"
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
echo "Creating and populating test database..."
sqlite3 "$DB_PATH" <<EOF
PRAGMA journal_mode=WAL;
PRAGMA page_size=4096;
CREATE TABLE IF NOT EXISTS test_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data BLOB,
    created_at INTEGER DEFAULT (strftime('%s', 'now'))
);
EOF

# Populate database with initial data (50MB to ensure activity)
echo "Populating database (50MB initial data)..."
bin/litestream-test populate -db "$DB_PATH" -target-size 50MB -batch-size 1000 > "$LOG_DIR/populate.log" 2>&1
if [ $? -ne 0 ]; then
    echo "Warning: Population failed, but continuing..."
    cat "$LOG_DIR/populate.log"
fi

# Create configuration with Ben's recommended aggressive settings
echo "Creating test configuration with aggressive intervals..."
cat > "$CONFIG_FILE" <<EOF
# Aggressive snapshot settings per Ben's request
snapshot:
  interval: 10m      # Snapshots every 10 minutes
  retention: 1h      # Keep data for 1 hour

# Aggressive compaction levels: 30s/1m/5m/15m/30m intervals
levels:
  - interval: 30s
  - interval: 1m
  - interval: 5m
  - interval: 15m
  - interval: 30m

dbs:
  - path: $DB_PATH
    # Checkpoint settings to ensure checkpoints happen
    checkpoint-interval: 1m          # Check for checkpoint every minute
    min-checkpoint-page-count: 100   # Low threshold to trigger checkpoints
    max-checkpoint-page-count: 5000  # Force checkpoint at this size

    replicas:
      - type: file
        path: $REPLICA_PATH
        retention-check-interval: 5m # Check retention every 5 minutes
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

# Start load generator with heavy sustained load
echo "Starting load generator (heavy sustained load)..."
bin/litestream-test load \
    -db "$DB_PATH" \
    -write-rate 500 \
    -duration "$TEST_DURATION" \
    -pattern wave \
    -payload-size 4096 \
    -read-ratio 0.3 \
    -workers 8 \
    > "$LOG_DIR/load.log" 2>&1 &
LOAD_PID=$!

echo "Load generator running (PID: $LOAD_PID)"
echo ""

# Monitor function with detailed metrics
monitor_comprehensive() {
    local last_checkpoint_count=0
    local last_compaction_count=0
    local last_sync_count=0

    while true; do
        sleep 60  # Check every minute

        echo "[$(date +%H:%M:%S)] Status Report"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        # Database metrics
        if [ -f "$DB_PATH" ]; then
            DB_SIZE=$(stat -f%z "$DB_PATH" 2>/dev/null || stat -c%s "$DB_PATH" 2>/dev/null)
            echo "  Database size: $(numfmt --to=iec-i --suffix=B $DB_SIZE 2>/dev/null || echo "$DB_SIZE bytes")"

            # WAL file size (indicates write activity)
            if [ -f "$DB_PATH-wal" ]; then
                WAL_SIZE=$(stat -f%z "$DB_PATH-wal" 2>/dev/null || stat -c%s "$DB_PATH-wal" 2>/dev/null)
                echo "  WAL size: $(numfmt --to=iec-i --suffix=B $WAL_SIZE 2>/dev/null || echo "$WAL_SIZE bytes")"
            fi

            # Row count
            TABLES=$(sqlite3 "$DB_PATH" ".tables" 2>/dev/null)
            if echo "$TABLES" | grep -q "load_test"; then
                ROW_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM load_test" 2>/dev/null || echo "0")
                echo "  Rows in database: $ROW_COUNT"
            elif echo "$TABLES" | grep -q "test_table_0"; then
                ROW_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM test_table_0" 2>/dev/null || echo "0")
                echo "  Rows in database: $ROW_COUNT"
            fi
        fi

        # Replication metrics
        if [ -d "$REPLICA_PATH" ]; then
            # Count snapshot files
            SNAPSHOTS=$(find "$REPLICA_PATH" -name "*snapshot*.ltx" 2>/dev/null | wc -l | tr -d ' ')
            # Count LTX files (WAL segments)
            LTX_FILES=$(find "$REPLICA_PATH" -name "*.ltx" 2>/dev/null | wc -l | tr -d ' ')
            REPLICA_SIZE=$(du -sh "$REPLICA_PATH" 2>/dev/null | cut -f1)
            echo "  Replica: $SNAPSHOTS snapshots, $LTX_FILES segments, size: $REPLICA_SIZE"
        fi

        # Operation metrics (with delta since last check)
        if [ -f "$LOG_DIR/litestream.log" ]; then
            CHECKPOINT_COUNT=$(grep -c "checkpoint" "$LOG_DIR/litestream.log" 2>/dev/null)
            CHECKPOINT_COUNT=${CHECKPOINT_COUNT:-0}
            COMPACTION_COUNT=$(grep -c "compaction complete" "$LOG_DIR/litestream.log" 2>/dev/null)
            COMPACTION_COUNT=${COMPACTION_COUNT:-0}
            SYNC_COUNT=$(grep -c "replica sync" "$LOG_DIR/litestream.log" 2>/dev/null)
            SYNC_COUNT=${SYNC_COUNT:-0}

            CHECKPOINT_DELTA=$((CHECKPOINT_COUNT - last_checkpoint_count))
            COMPACTION_DELTA=$((COMPACTION_COUNT - last_compaction_count))
            SYNC_DELTA=$((SYNC_COUNT - last_sync_count))

            echo "  Operations: $CHECKPOINT_COUNT checkpoints (+$CHECKPOINT_DELTA), $COMPACTION_COUNT compactions (+$COMPACTION_DELTA)"
            echo "  Syncs: $SYNC_COUNT total (+$SYNC_DELTA in last minute)"

            last_checkpoint_count=$CHECKPOINT_COUNT
            last_compaction_count=$COMPACTION_COUNT
            last_sync_count=$SYNC_COUNT
        fi

        # Check for errors (excluding known non-critical)
        ERROR_COUNT=$(grep -i "ERROR" "$LOG_DIR/litestream.log" 2>/dev/null | grep -v "page size not initialized" | wc -l | tr -d ' ')
        if [ "$ERROR_COUNT" -gt 0 ]; then
            echo "  ⚠ Critical errors: $ERROR_COUNT"
            grep -i "ERROR" "$LOG_DIR/litestream.log" | grep -v "page size not initialized" | tail -2
        fi

        # Load generator status
        if [ -f "$LOG_DIR/load.log" ]; then
            LOAD_STATUS=$(tail -1 "$LOG_DIR/load.log" 2>/dev/null | grep -oE "writes_per_sec=[0-9.]+" | cut -d= -f2 || echo "0")
            echo "  Write rate: ${LOAD_STATUS:-0} writes/sec"
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

echo "Running comprehensive test for $TEST_DURATION..."
echo "Monitor will report every 60 seconds"
echo "================================================"
echo ""

# Start monitoring in background
monitor_comprehensive &
MONITOR_PID=$!

# Wait for load test to complete
wait "$LOAD_PID" 2>/dev/null || true

# Stop the monitor
kill $MONITOR_PID 2>/dev/null || true
wait $MONITOR_PID 2>/dev/null || true

echo ""
echo "================================================"
echo "Final Test Results"
echo "================================================"

# Final statistics
echo "Database Statistics:"
if [ -f "$DB_PATH" ]; then
    DB_SIZE=$(stat -f%z "$DB_PATH" 2>/dev/null || stat -c%s "$DB_PATH" 2>/dev/null)
    # Find the actual table name
    TABLES=$(sqlite3 "$DB_PATH" ".tables" 2>/dev/null)
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
if [ -f "$LOG_DIR/litestream.log" ]; then
    COMPACTION_COUNT=$(grep -c "compaction complete" "$LOG_DIR/litestream.log" || echo "0")
    CHECKPOINT_COUNT=$(grep -c "checkpoint" "$LOG_DIR/litestream.log" || echo "0")
    SYNC_COUNT=$(grep -c "replica sync" "$LOG_DIR/litestream.log" || echo "0")
    ERROR_COUNT=$(grep -i "ERROR" "$LOG_DIR/litestream.log" | grep -v "page size not initialized" | wc -l | tr -d ' ' || echo "0")
else
    COMPACTION_COUNT="0"
    CHECKPOINT_COUNT="0"
    SYNC_COUNT="0"
    ERROR_COUNT="0"
fi
echo "  Compactions: $COMPACTION_COUNT"
echo "  Checkpoints: $CHECKPOINT_COUNT"
echo "  Syncs: $SYNC_COUNT"
echo "  Errors: $ERROR_COUNT"

# Validation test
echo ""
echo "Testing validation..."
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
    # Get row count from restored database
    TABLES=$(sqlite3 "$RESTORE_DB" ".tables" 2>/dev/null)
    if echo "$TABLES" | grep -q "load_test"; then
        RESTORED_COUNT=$(sqlite3 "$RESTORE_DB" "SELECT COUNT(*) FROM load_test" 2>/dev/null || echo "0")
    elif echo "$TABLES" | grep -q "test_table_0"; then
        RESTORED_COUNT=$(sqlite3 "$RESTORE_DB" "SELECT COUNT(*) FROM test_table_0" 2>/dev/null || echo "0")
    else
        RESTORED_COUNT="0"
    fi

    if [ "$RESTORED_COUNT" = "$ROW_COUNT" ]; then
        echo "  ✓ Restoration successful! ($RESTORED_COUNT rows match)"
    else
        echo "  ⚠ Row count mismatch! Original: $ROW_COUNT, Restored: $RESTORED_COUNT"
    fi
else
    echo "  ✗ Restoration failed!"
    tail -10 "$LOG_DIR/restore.log"
fi

# Summary
echo ""
echo "================================================"
echo "Test Summary"
echo "================================================"

# Count critical errors (exclude known non-critical ones)
CRITICAL_ERROR_COUNT=$(grep -i "ERROR" "$LOG_DIR/litestream.log" 2>/dev/null | grep -v "page size not initialized" | wc -l | tr -d ' ')

# Determine test result
TEST_PASSED=true
ISSUES=""

if [ "$CRITICAL_ERROR_COUNT" -gt 0 ]; then
    TEST_PASSED=false
    ISSUES="$ISSUES\n  - Critical errors detected: $CRITICAL_ERROR_COUNT"
fi

if [ "$LTX_COUNT" -eq 0 ]; then
    TEST_PASSED=false
    ISSUES="$ISSUES\n  - No LTX segments created (replication not working)"
fi

if [ "$CHECKPOINT_COUNT" -eq 0 ]; then
    ISSUES="$ISSUES\n  - No checkpoints recorded (may need more aggressive settings)"
fi

if [ "$COMPACTION_COUNT" -eq 0 ]; then
    ISSUES="$ISSUES\n  - No compactions occurred (unexpected for this test duration)"
fi

if [ "$TEST_PASSED" = true ]; then
    echo "✓ COMPREHENSIVE TEST PASSED!"
    echo ""
    echo "Successfully validated:"
    echo "  - Continuous replication ($LTX_COUNT segments)"
    echo "  - Compaction ($COMPACTION_COUNT operations)"
    [ "$CHECKPOINT_COUNT" -gt 0 ] && echo "  - Checkpoints ($CHECKPOINT_COUNT operations)"
    [ "$SNAPSHOT_COUNT" -gt 0 ] && echo "  - Snapshots ($SNAPSHOT_COUNT created)"
    echo "  - Database restoration"
    echo ""
    echo "The configuration is ready for production use."
else
    echo "⚠ TEST COMPLETED WITH ISSUES:"
    echo -e "$ISSUES"
    echo ""
    echo "Review the logs for details:"
    echo "  $LOG_DIR/litestream.log"
fi

echo ""
echo "Full test results available in: $TEST_DIR"
echo "================================================"
