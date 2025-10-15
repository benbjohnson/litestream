#!/bin/bash
set -euo pipefail

TEST_DIR="/tmp/litestream-overnight-$(date +%Y%m%d-%H%M%S)"
DB_PATH="$TEST_DIR/test.db"
REPLICA_PATH="$TEST_DIR/replica"
LOG_DIR="$TEST_DIR/logs"
CONFIG_FILE="$TEST_DIR/litestream.yml"
MONITOR_PID=""
LITESTREAM_PID=""
LOAD_PID=""

echo "================================================"
echo "Litestream Overnight Test Suite"
echo "================================================"
echo "Test directory: $TEST_DIR"
echo "Start time: $(date)"
echo ""

cleanup() {
    echo ""
    echo "================================================"
    echo "Cleaning up..."
    echo "================================================"

    if [ -n "$LOAD_PID" ] && kill -0 "$LOAD_PID" 2>/dev/null; then
        echo "Stopping load generator..."
        kill "$LOAD_PID" 2>/dev/null || true
        wait "$LOAD_PID" 2>/dev/null || true
    fi

    if [ -n "$LITESTREAM_PID" ] && kill -0 "$LITESTREAM_PID" 2>/dev/null; then
        echo "Stopping litestream..."
        kill "$LITESTREAM_PID" 2>/dev/null || true
        wait "$LITESTREAM_PID" 2>/dev/null || true
    fi

    if [ -n "$MONITOR_PID" ] && kill -0 "$MONITOR_PID" 2>/dev/null; then
        echo "Stopping monitor..."
        kill "$MONITOR_PID" 2>/dev/null || true
    fi

    echo ""
    echo "Test Summary:"
    echo "============="
    if [ -f "$LOG_DIR/monitor.log" ]; then
        echo "Final statistics from monitor log:"
        tail -20 "$LOG_DIR/monitor.log"
    fi

    echo ""
    echo "Test artifacts saved in: $TEST_DIR"
    echo "End time: $(date)"
}

trap cleanup EXIT INT TERM

mkdir -p "$TEST_DIR" "$LOG_DIR" "$REPLICA_PATH"

echo "Creating initial database..."
sqlite3 "$DB_PATH" <<EOF
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS test_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data BLOB,
    created_at INTEGER
);
EOF

echo "Creating litestream configuration with frequent intervals..."
cat > "$CONFIG_FILE" <<EOF
# Litestream configuration for overnight testing
# with aggressive compaction and snapshot intervals

# Snapshot every 10 minutes
snapshot:
  interval: 10m
  retention: 720h    # Keep everything for analysis

# Compaction settings - very frequent for testing
levels:
  - interval: 30s
  - interval: 1m
  - interval: 5m
  - interval: 15m
  - interval: 30m
  - interval: 1h

dbs:
  - path: $DB_PATH
    # Checkpoint after every 1000 frames (frequent for testing)
    checkpoint-interval: 30s
    min-checkpoint-page-count: 1000
    max-checkpoint-page-count: 10000

    replicas:
      - type: file
        path: $REPLICA_PATH
        retention-check-interval: 1h
EOF

echo ""
echo "Configuration created at: $CONFIG_FILE"
cat "$CONFIG_FILE"
echo ""

echo "Building litestream if needed..."
if [ ! -f bin/litestream ]; then
    go build -o bin/litestream ./cmd/litestream
fi

echo "Starting litestream replication..."
LOG_LEVEL=debug bin/litestream replicate -config "$CONFIG_FILE" > "$LOG_DIR/litestream.log" 2>&1 &
LITESTREAM_PID=$!
echo "Litestream started with PID: $LITESTREAM_PID"

sleep 5

if ! kill -0 "$LITESTREAM_PID" 2>/dev/null; then
    echo "ERROR: Litestream failed to start. Check logs:"
    tail -50 "$LOG_DIR/litestream.log"
    exit 1
fi

monitor_test() {
    while true; do
        echo "================================================" | tee -a "$LOG_DIR/monitor.log"
        echo "Monitor Update: $(date)" | tee -a "$LOG_DIR/monitor.log"
        echo "================================================" | tee -a "$LOG_DIR/monitor.log"

        # Database size
        if [ -f "$DB_PATH" ]; then
            DB_SIZE=$(stat -f%z "$DB_PATH" 2>/dev/null || stat -c%s "$DB_PATH" 2>/dev/null || echo "0")
            echo "Database size: $(numfmt --to=iec-i --suffix=B $DB_SIZE 2>/dev/null || echo "$DB_SIZE bytes")" | tee -a "$LOG_DIR/monitor.log"
        fi

        # WAL size
        if [ -f "$DB_PATH-wal" ]; then
            WAL_SIZE=$(stat -f%z "$DB_PATH-wal" 2>/dev/null || stat -c%s "$DB_PATH-wal" 2>/dev/null || echo "0")
            echo "WAL size: $(numfmt --to=iec-i --suffix=B $WAL_SIZE 2>/dev/null || echo "$WAL_SIZE bytes")" | tee -a "$LOG_DIR/monitor.log"
        fi

        # Replica statistics
        echo "" | tee -a "$LOG_DIR/monitor.log"
        echo "Replica Statistics:" | tee -a "$LOG_DIR/monitor.log"

        # Count snapshots (for file replica, look for snapshot.ltx files)
        SNAPSHOT_COUNT=$(find "$REPLICA_PATH" -name "*snapshot*.ltx" 2>/dev/null | wc -l | tr -d ' ')
        echo "  Snapshots: $SNAPSHOT_COUNT" | tee -a "$LOG_DIR/monitor.log"

        # Count LTX segments by age (file replicas use .ltx not .wal.lz4)
        if [ -d "$REPLICA_PATH" ]; then
            LTX_30S=$(find "$REPLICA_PATH" -name "*.ltx" -mmin -0.5 2>/dev/null | wc -l | tr -d ' ')
            LTX_1M=$(find "$REPLICA_PATH" -name "*.ltx" -mmin -1 2>/dev/null | wc -l | tr -d ' ')
            LTX_5M=$(find "$REPLICA_PATH" -name "*.ltx" -mmin -5 2>/dev/null | wc -l | tr -d ' ')
            LTX_TOTAL=$(find "$REPLICA_PATH" -name "*.ltx" 2>/dev/null | wc -l | tr -d ' ')

            echo "  LTX segments (last 30s): $LTX_30S" | tee -a "$LOG_DIR/monitor.log"
            echo "  LTX segments (last 1m): $LTX_1M" | tee -a "$LOG_DIR/monitor.log"
            echo "  LTX segments (last 5m): $LTX_5M" | tee -a "$LOG_DIR/monitor.log"
            echo "  LTX segments (total): $LTX_TOTAL" | tee -a "$LOG_DIR/monitor.log"

            # Replica size
            REPLICA_SIZE=$(du -sh "$REPLICA_PATH" 2>/dev/null | cut -f1)
            echo "  Total replica size: $REPLICA_SIZE" | tee -a "$LOG_DIR/monitor.log"
        fi

        # Count operations
        echo "" | tee -a "$LOG_DIR/monitor.log"
        echo "Operations:" | tee -a "$LOG_DIR/monitor.log"
        if [ -f "$LOG_DIR/litestream.log" ]; then
            COMPACTION_COUNT=$(grep -c "compaction complete" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
            CHECKPOINT_COUNT=$(grep -iE "checkpoint|checkpointed" "$LOG_DIR/litestream.log" 2>/dev/null | wc -l | tr -d ' ' || echo "0")
            SYNC_COUNT=$(grep -c "replica sync" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
            echo "  Compactions: $COMPACTION_COUNT" | tee -a "$LOG_DIR/monitor.log"
            echo "  Checkpoints: $CHECKPOINT_COUNT" | tee -a "$LOG_DIR/monitor.log"
            echo "  Syncs: $SYNC_COUNT" | tee -a "$LOG_DIR/monitor.log"
        fi

        # Check for errors in litestream log (exclude known non-critical)
        echo "" | tee -a "$LOG_DIR/monitor.log"
        ERROR_COUNT=$(grep -i "ERROR" "$LOG_DIR/litestream.log" 2>/dev/null | grep -v "page size not initialized" | wc -l | tr -d ' ' || echo "0")
        echo "Critical errors in litestream log: $ERROR_COUNT" | tee -a "$LOG_DIR/monitor.log"

        if [ "$ERROR_COUNT" -gt 0 ]; then
            echo "Recent errors:" | tee -a "$LOG_DIR/monitor.log"
            grep -i "ERROR" "$LOG_DIR/litestream.log" | grep -v "page size not initialized" | tail -5 | tee -a "$LOG_DIR/monitor.log"
        fi

        # Process status
        echo "" | tee -a "$LOG_DIR/monitor.log"
        echo "Process Status:" | tee -a "$LOG_DIR/monitor.log"

        if kill -0 "$LITESTREAM_PID" 2>/dev/null; then
            echo "  Litestream: Running (PID: $LITESTREAM_PID)" | tee -a "$LOG_DIR/monitor.log"
        else
            echo "  Litestream: STOPPED" | tee -a "$LOG_DIR/monitor.log"
        fi

        if [ -n "$LOAD_PID" ] && kill -0 "$LOAD_PID" 2>/dev/null; then
            echo "  Load generator: Running (PID: $LOAD_PID)" | tee -a "$LOG_DIR/monitor.log"
        else
            echo "  Load generator: STOPPED" | tee -a "$LOG_DIR/monitor.log"
        fi

        echo "" | tee -a "$LOG_DIR/monitor.log"
        sleep 60
    done
}

echo "Starting monitor process..."
monitor_test &
MONITOR_PID=$!
echo "Monitor started with PID: $MONITOR_PID"

echo ""
echo "Initial database population (before starting litestream)..."
# Kill litestream temporarily to populate database
kill "$LITESTREAM_PID" 2>/dev/null || true
wait "$LITESTREAM_PID" 2>/dev/null || true

bin/litestream-test populate -db "$DB_PATH" -target-size 100MB -batch-size 10000 > "$LOG_DIR/populate.log" 2>&1
if [ $? -ne 0 ]; then
    echo "Warning: Population failed, but continuing..."
    cat "$LOG_DIR/populate.log"
fi

# Restart litestream
echo "Restarting litestream after population..."
LOG_LEVEL=debug bin/litestream replicate -config "$CONFIG_FILE" > "$LOG_DIR/litestream.log" 2>&1 &
LITESTREAM_PID=$!
sleep 3

echo ""
echo "Starting load generator for overnight test..."
echo "Configuration:"
echo "  - Duration: 8 hours"
echo "  - Write rate: 50 writes/second"
echo "  - Pattern: wave (simulates varying load)"
echo "  - Workers: 4"
echo ""

# Run load test for 8 hours with varying patterns
bin/litestream-test load \
    -db "$DB_PATH" \
    -write-rate 50 \
    -duration 8h \
    -pattern wave \
    -payload-size 2048 \
    -read-ratio 0.3 \
    -workers 4 \
    > "$LOG_DIR/load.log" 2>&1 &

LOAD_PID=$!
echo "Load generator started with PID: $LOAD_PID"

echo ""
echo "================================================"
echo "Overnight test is running!"
echo "================================================"
echo ""
echo "Monitor the test with:"
echo "  tail -f $LOG_DIR/monitor.log"
echo ""
echo "View litestream logs:"
echo "  tail -f $LOG_DIR/litestream.log"
echo ""
echo "View load generator logs:"
echo "  tail -f $LOG_DIR/load.log"
echo ""
echo "The test will run for 8 hours. Press Ctrl+C to stop early."
echo ""

wait "$LOAD_PID"

echo ""
echo "Load generation completed."

# Final statistics
echo ""
echo "================================================"
echo "Final Statistics"
echo "================================================"

if [ -f "$DB_PATH" ]; then
    DB_SIZE=$(stat -f%z "$DB_PATH" 2>/dev/null || stat -c%s "$DB_PATH" 2>/dev/null)
    # Find actual table name
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
    echo "Database size: $(numfmt --to=iec-i --suffix=B $DB_SIZE 2>/dev/null || echo "$DB_SIZE bytes")"
    echo "Total rows: $ROW_COUNT"
fi

if [ -d "$REPLICA_PATH" ]; then
    SNAPSHOT_COUNT=$(find "$REPLICA_PATH" -name "*snapshot*.ltx" 2>/dev/null | wc -l | tr -d ' ')
    LTX_COUNT=$(find "$REPLICA_PATH" -name "*.ltx" 2>/dev/null | wc -l | tr -d ' ')
    REPLICA_SIZE=$(du -sh "$REPLICA_PATH" | cut -f1)
    echo "Snapshots created: $SNAPSHOT_COUNT"
    echo "LTX segments: $LTX_COUNT"
    echo "Replica size: $REPLICA_SIZE"
fi

if [ -f "$LOG_DIR/litestream.log" ]; then
    COMPACTION_COUNT=$(grep -c "compaction complete" "$LOG_DIR/litestream.log" || echo "0")
    CHECKPOINT_COUNT=$(grep -iE "checkpoint|checkpointed" "$LOG_DIR/litestream.log" | wc -l | tr -d ' ' || echo "0")
    ERROR_COUNT=$(grep -i "ERROR" "$LOG_DIR/litestream.log" | grep -v "page size not initialized" | wc -l | tr -d ' ' || echo "0")
    echo "Compactions: $COMPACTION_COUNT"
    echo "Checkpoints: $CHECKPOINT_COUNT"
    echo "Critical errors: $ERROR_COUNT"
fi

echo ""
echo "Running validation..."
bin/litestream-test validate \
    -source "$DB_PATH" \
    -replica "$REPLICA_PATH" \
    > "$LOG_DIR/validate.log" 2>&1

if [ $? -eq 0 ]; then
    echo "✓ Validation passed!"
else
    echo "✗ Validation failed! Check $LOG_DIR/validate.log"
fi
