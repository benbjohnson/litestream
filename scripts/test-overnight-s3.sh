#!/bin/bash
set -euo pipefail

# Check for required environment variables
if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ] || [ -z "${S3_BUCKET:-}" ]; then
    echo "Error: Required environment variables not set"
    echo "Please set: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET"
    echo ""
    echo "Example:"
    echo "  export AWS_ACCESS_KEY_ID=your_key"
    echo "  export AWS_SECRET_ACCESS_KEY=your_secret"
    echo "  export S3_BUCKET=your-test-bucket"
    echo "  export AWS_REGION=us-east-1  # optional, defaults to us-east-1"
    exit 1
fi

AWS_REGION="${AWS_REGION:-us-east-1}"
S3_PATH="s3://${S3_BUCKET}/litestream-overnight-$(date +%Y%m%d-%H%M%S)"

TEST_DIR="/tmp/litestream-overnight-s3-$(date +%Y%m%d-%H%M%S)"
DB_PATH="$TEST_DIR/test.db"
LOG_DIR="$TEST_DIR/logs"
CONFIG_FILE="$TEST_DIR/litestream.yml"
MONITOR_PID=""
LITESTREAM_PID=""
LOAD_PID=""

echo "================================================"
echo "Litestream Overnight S3 Test Suite"
echo "================================================"
echo "Test directory: $TEST_DIR"
echo "S3 destination: $S3_PATH"
echo "AWS Region: $AWS_REGION"
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
    echo "S3 Final Statistics:"
    aws s3 ls "${S3_PATH}/" --recursive --summarize 2>/dev/null | tail -5 || true

    echo ""
    echo "Test artifacts saved locally in: $TEST_DIR"
    echo "S3 replica data in: $S3_PATH"
    echo "End time: $(date)"
}

trap cleanup EXIT INT TERM

mkdir -p "$TEST_DIR" "$LOG_DIR"

echo "Creating initial database..."
sqlite3 "$DB_PATH" <<EOF
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS test_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data BLOB,
    created_at INTEGER
);
EOF

echo "Creating litestream configuration for S3 with frequent intervals..."
cat > "$CONFIG_FILE" <<EOF
# Litestream S3 configuration for overnight testing
# with aggressive compaction and snapshot intervals

# Optional: Access key configuration (can also use environment variables)
# access-key-id: ${AWS_ACCESS_KEY_ID}
# secret-access-key: ${AWS_SECRET_ACCESS_KEY}

dbs:
  - path: $DB_PATH
    replicas:
      - url: ${S3_PATH}
        region: ${AWS_REGION}

        # Snapshot every 10 minutes
        snapshot-interval: 10m

        # Retention settings - keep data for 30 days
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

        # S3-specific settings
        force-path-style: false
        skip-verify: false

        # Optional: Server-side encryption
        # sse: AES256
        # sse-kms-key-id: your-kms-key-id

    # Checkpoint settings - frequent for testing
    checkpoint-interval: 30s
    min-checkpoint-page-count: 1000
    max-checkpoint-page-count: 10000
EOF

echo ""
echo "Configuration created at: $CONFIG_FILE"
echo ""

echo "Testing S3 connectivity..."
if aws s3 ls "s3://${S3_BUCKET}/" > /dev/null 2>&1; then
    echo "✓ S3 bucket accessible"
else
    echo "✗ Failed to access S3 bucket: ${S3_BUCKET}"
    exit 1
fi

echo "Building litestream if needed..."
if [ ! -f bin/litestream ]; then
    go build -o bin/litestream ./cmd/litestream
fi

echo "Starting litestream replication to S3..."
LOG_LEVEL=debug bin/litestream replicate -config "$CONFIG_FILE" > "$LOG_DIR/litestream.log" 2>&1 &
LITESTREAM_PID=$!
echo "Litestream started with PID: $LITESTREAM_PID"

sleep 5

if ! kill -0 "$LITESTREAM_PID" 2>/dev/null; then
    echo "ERROR: Litestream failed to start. Check logs:"
    tail -50 "$LOG_DIR/litestream.log"
    exit 1
fi

monitor_s3_test() {
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

        # S3 statistics
        echo "" | tee -a "$LOG_DIR/monitor.log"
        echo "S3 Replica Statistics:" | tee -a "$LOG_DIR/monitor.log"

        # Count objects in S3
        SNAPSHOT_COUNT=$(aws s3 ls "${S3_PATH}/" --recursive 2>/dev/null | grep -c "\.snapshot\.lz4" || echo "0")
        WAL_COUNT=$(aws s3 ls "${S3_PATH}/" --recursive 2>/dev/null | grep -c "\.wal\.lz4" || echo "0")
        TOTAL_OBJECTS=$(aws s3 ls "${S3_PATH}/" --recursive 2>/dev/null | wc -l | tr -d ' ' || echo "0")

        echo "  Snapshots in S3: $SNAPSHOT_COUNT" | tee -a "$LOG_DIR/monitor.log"
        echo "  WAL segments in S3: $WAL_COUNT" | tee -a "$LOG_DIR/monitor.log"
        echo "  Total objects in S3: $TOTAL_OBJECTS" | tee -a "$LOG_DIR/monitor.log"

        # Get S3 storage size (if possible)
        S3_SIZE=$(aws s3 ls "${S3_PATH}/" --recursive --summarize 2>/dev/null | grep "Total Size" | awk '{print $3}' || echo "0")
        if [ "$S3_SIZE" != "0" ]; then
            echo "  Total S3 storage: $(numfmt --to=iec-i --suffix=B $S3_SIZE 2>/dev/null || echo "$S3_SIZE bytes")" | tee -a "$LOG_DIR/monitor.log"
        fi

        # Check for errors
        echo "" | tee -a "$LOG_DIR/monitor.log"
        ERROR_COUNT=$(grep -c "ERROR\|error" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        echo "Errors in litestream log: $ERROR_COUNT" | tee -a "$LOG_DIR/monitor.log"

        if [ "$ERROR_COUNT" -gt 0 ]; then
            echo "Recent errors:" | tee -a "$LOG_DIR/monitor.log"
            grep "ERROR\|error" "$LOG_DIR/litestream.log" | tail -5 | tee -a "$LOG_DIR/monitor.log"
        fi

        # Check for S3-specific errors
        S3_ERROR_COUNT=$(grep -c "S3\|AWS\|403\|404\|500\|503" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        if [ "$S3_ERROR_COUNT" -gt 0 ]; then
            echo "S3-specific errors: $S3_ERROR_COUNT" | tee -a "$LOG_DIR/monitor.log"
            grep "S3\|AWS\|403\|404\|500\|503" "$LOG_DIR/litestream.log" | tail -3 | tee -a "$LOG_DIR/monitor.log"
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

        # Network/API statistics from log
        UPLOAD_COUNT=$(grep -c "uploading\|uploaded" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        echo "  Total upload operations: $UPLOAD_COUNT" | tee -a "$LOG_DIR/monitor.log"

        echo "" | tee -a "$LOG_DIR/monitor.log"
        sleep 60
    done
}

echo "Starting monitor process..."
monitor_s3_test &
MONITOR_PID=$!
echo "Monitor started with PID: $MONITOR_PID"

echo ""
echo "Initial database population..."
bin/litestream-test populate -db "$DB_PATH" -target-size 100MB -batch-size 10000 > "$LOG_DIR/populate.log" 2>&1

echo ""
echo "Starting load generator for overnight S3 test..."
echo "Configuration:"
echo "  - Duration: 8 hours"
echo "  - Write rate: 100 writes/second (higher for S3 testing)"
echo "  - Pattern: wave (simulates varying load)"
echo "  - Workers: 8"
echo ""

# Run load test for 8 hours with higher load for S3
bin/litestream-test load \
    -db "$DB_PATH" \
    -write-rate 100 \
    -duration 8h \
    -pattern wave \
    -payload-size 4096 \
    -read-ratio 0.3 \
    -workers 8 \
    > "$LOG_DIR/load.log" 2>&1 &

LOAD_PID=$!
echo "Load generator started with PID: $LOAD_PID"

echo ""
echo "================================================"
echo "Overnight S3 test is running!"
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
echo "Check S3 contents:"
echo "  aws s3 ls ${S3_PATH}/ --recursive"
echo ""
echo "The test will run for 8 hours. Press Ctrl+C to stop early."
echo ""

wait "$LOAD_PID"

echo ""
echo "Load generation completed. Testing restoration from S3..."

# Test restoration
RESTORE_DB="$TEST_DIR/restored.db"
echo "Restoring database from S3 to: $RESTORE_DB"
bin/litestream restore -o "$RESTORE_DB" "$S3_PATH" > "$LOG_DIR/restore.log" 2>&1

if [ $? -eq 0 ]; then
    echo "✓ Restoration successful!"

    # Compare row counts
    ORIGINAL_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM test_data" 2>/dev/null || echo "0")
    RESTORED_COUNT=$(sqlite3 "$RESTORE_DB" "SELECT COUNT(*) FROM test_data" 2>/dev/null || echo "0")

    echo "Original database rows: $ORIGINAL_COUNT"
    echo "Restored database rows: $RESTORED_COUNT"

    if [ "$ORIGINAL_COUNT" = "$RESTORED_COUNT" ]; then
        echo "✓ Row counts match!"
    else
        echo "✗ Row count mismatch!"
    fi
else
    echo "✗ Restoration failed! Check $LOG_DIR/restore.log"
fi
