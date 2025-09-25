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

dbs:
  - path: $DB_PATH
    replicas:
      - type: file
        path: $REPLICA_PATH

        # Snapshot every 10 minutes
        snapshot-interval: 10m

        # Retention settings - keep everything for analysis
        retention: 720h
        retention-check-interval: 1h

        # Compaction settings - very frequent for testing
        compaction:
          - duration: 30s
            interval: 30s
          - duration: 1m
            interval: 1m
          - duration: 5m
            interval: 5m
          - duration: 1h
            interval: 15m
          - duration: 6h
            interval: 30m
          - duration: 24h
            interval: 1h

    # Checkpoint after every 1000 frames (frequent for testing)
    checkpoint-interval: 30s
    min-checkpoint-page-count: 1000
    max-checkpoint-page-count: 10000
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

        # Count snapshots
        SNAPSHOT_COUNT=$(find "$REPLICA_PATH" -name "*.snapshot.lz4" 2>/dev/null | wc -l | tr -d ' ')
        echo "  Snapshots: $SNAPSHOT_COUNT" | tee -a "$LOG_DIR/monitor.log"

        # Count WAL segments by age
        if [ -d "$REPLICA_PATH" ]; then
            WAL_30S=$(find "$REPLICA_PATH" -name "*.wal.lz4" -mmin -0.5 2>/dev/null | wc -l | tr -d ' ')
            WAL_1M=$(find "$REPLICA_PATH" -name "*.wal.lz4" -mmin -1 2>/dev/null | wc -l | tr -d ' ')
            WAL_5M=$(find "$REPLICA_PATH" -name "*.wal.lz4" -mmin -5 2>/dev/null | wc -l | tr -d ' ')
            WAL_TOTAL=$(find "$REPLICA_PATH" -name "*.wal.lz4" 2>/dev/null | wc -l | tr -d ' ')

            echo "  WAL segments (last 30s): $WAL_30S" | tee -a "$LOG_DIR/monitor.log"
            echo "  WAL segments (last 1m): $WAL_1M" | tee -a "$LOG_DIR/monitor.log"
            echo "  WAL segments (last 5m): $WAL_5M" | tee -a "$LOG_DIR/monitor.log"
            echo "  WAL segments (total): $WAL_TOTAL" | tee -a "$LOG_DIR/monitor.log"

            # Replica size
            REPLICA_SIZE=$(du -sh "$REPLICA_PATH" 2>/dev/null | cut -f1)
            echo "  Total replica size: $REPLICA_SIZE" | tee -a "$LOG_DIR/monitor.log"
        fi

        # Check for errors in litestream log
        echo "" | tee -a "$LOG_DIR/monitor.log"
        ERROR_COUNT=$(grep -c "ERROR\|error" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        echo "Errors in litestream log: $ERROR_COUNT" | tee -a "$LOG_DIR/monitor.log"

        if [ "$ERROR_COUNT" -gt 0 ]; then
            echo "Recent errors:" | tee -a "$LOG_DIR/monitor.log"
            grep "ERROR\|error" "$LOG_DIR/litestream.log" | tail -5 | tee -a "$LOG_DIR/monitor.log"
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
echo "Initial database population..."
bin/litestream-test populate -db "$DB_PATH" -target-size 100MB -batch-size 10000 > "$LOG_DIR/populate.log" 2>&1

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
echo "Load generation completed. Running validation..."
bin/litestream-test validate \
    -source "$DB_PATH" \
    -replica "$REPLICA_PATH" \
    > "$LOG_DIR/validate.log" 2>&1

if [ $? -eq 0 ]; then
    echo "✓ Validation passed!"
else
    echo "✗ Validation failed! Check $LOG_DIR/validate.log"
fi
