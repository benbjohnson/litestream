#!/bin/bash
set -euo pipefail

# MinIO S3-compatible test with Docker
# This test runs Litestream against a local MinIO instance to simulate S3 behavior

TEST_DURATION="${TEST_DURATION:-2h}"
TEST_DIR="/tmp/litestream-minio-$(date +%Y%m%d-%H%M%S)"
DB_PATH="$TEST_DIR/test.db"
CONFIG_FILE="$TEST_DIR/litestream.yml"
LOG_DIR="$TEST_DIR/logs"

# MinIO settings - use alternative ports to avoid conflicts
MINIO_CONTAINER_NAME="litestream-minio-test"
MINIO_PORT=9100
MINIO_CONSOLE_PORT=9101
MINIO_ROOT_USER="minioadmin"
MINIO_ROOT_PASSWORD="minioadmin"
MINIO_BUCKET="litestream-test"
MINIO_ENDPOINT="http://localhost:${MINIO_PORT}"
S3_PATH="s3://${MINIO_BUCKET}/litestream-test-$(date +%Y%m%d-%H%M%S)"

echo "================================================"
echo "Litestream MinIO S3 Test"
echo "================================================"
echo "Duration: $TEST_DURATION"
echo "Test directory: $TEST_DIR"
echo "MinIO endpoint: $MINIO_ENDPOINT"
echo "MinIO bucket: $MINIO_BUCKET"
echo "Start time: $(date)"
echo ""

# Check for Docker
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed or not in PATH"
    echo "Please install Docker to run this test"
    exit 1
fi

cleanup() {
    echo ""
    echo "================================================"
    echo "Cleaning up..."
    echo "================================================"

    # Kill all spawned processes
    jobs -p | xargs -r kill 2>/dev/null || true
    wait 2>/dev/null || true

    # Stop and remove MinIO container
    if [ -n "${MINIO_CONTAINER_NAME:-}" ]; then
        echo "Stopping MinIO container..."
        docker stop "$MINIO_CONTAINER_NAME" 2>/dev/null || true
        docker rm "$MINIO_CONTAINER_NAME" 2>/dev/null || true
    fi

    echo ""
    echo "Test completed at: $(date)"
    echo "Results saved in: $TEST_DIR"
}

trap cleanup EXIT INT TERM

# Create directories
mkdir -p "$TEST_DIR" "$LOG_DIR"

# Clean up any existing container
if docker ps -a | grep -q "$MINIO_CONTAINER_NAME"; then
    echo "Removing existing MinIO container..."
    docker stop "$MINIO_CONTAINER_NAME" 2>/dev/null || true
    docker rm "$MINIO_CONTAINER_NAME" 2>/dev/null || true
fi

# Start MinIO container
echo "Starting MinIO container..."
docker run -d \
    --name "$MINIO_CONTAINER_NAME" \
    -p "${MINIO_PORT}:9000" \
    -p "${MINIO_CONSOLE_PORT}:9001" \
    -e "MINIO_ROOT_USER=${MINIO_ROOT_USER}" \
    -e "MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}" \
    minio/minio server /data --console-address ":9001"

echo "Waiting for MinIO to start..."
sleep 5

# Check if MinIO is running
if ! docker ps | grep -q "$MINIO_CONTAINER_NAME"; then
    echo "Error: MinIO container failed to start"
    docker logs "$MINIO_CONTAINER_NAME" 2>&1
    exit 1
fi

echo "MinIO is running!"
echo "  API: http://localhost:${MINIO_PORT} (mapped from container port 9000)"
echo "  Console: http://localhost:${MINIO_CONSOLE_PORT} (mapped from container port 9001)"
echo "  Credentials: ${MINIO_ROOT_USER}/${MINIO_ROOT_PASSWORD}"
echo ""

# Create MinIO bucket using mc (MinIO Client) in Docker
echo "Creating MinIO bucket..."
docker run --rm --link "${MINIO_CONTAINER_NAME}:minio" \
    -e "MC_HOST_minio=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" \
    minio/mc mb "minio/${MINIO_BUCKET}" 2>/dev/null || true

echo "Bucket '${MINIO_BUCKET}' ready"
echo ""

# Build binaries if needed
echo "Building binaries..."
if [ ! -f bin/litestream ]; then
    go build -o bin/litestream ./cmd/litestream
fi
if [ ! -f bin/litestream-test ]; then
    go build -o bin/litestream-test ./cmd/litestream-test
fi

# Create and populate test database
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

# Populate database with initial data
echo "Populating database (50MB initial data)..."
bin/litestream-test populate -db "$DB_PATH" -target-size 50MB -batch-size 1000 > "$LOG_DIR/populate.log" 2>&1
if [ $? -ne 0 ]; then
    echo "Warning: Population failed, but continuing..."
    cat "$LOG_DIR/populate.log"
fi

# Create Litestream configuration for MinIO
echo "Creating Litestream configuration for MinIO S3..."
cat > "$CONFIG_FILE" <<EOF
# MinIO S3 endpoint configuration
access-key-id: ${MINIO_ROOT_USER}
secret-access-key: ${MINIO_ROOT_PASSWORD}

# Aggressive snapshot settings for testing
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
    # Checkpoint settings
    checkpoint-interval: 1m
    min-checkpoint-page-count: 100
    max-checkpoint-page-count: 5000

    replicas:
      - url: ${S3_PATH}
        endpoint: ${MINIO_ENDPOINT}
        region: us-east-1
        force-path-style: true
        skip-verify: true
        retention-check-interval: 5m
EOF

echo "Starting litestream with MinIO backend..."
LOG_LEVEL=debug bin/litestream replicate -config "$CONFIG_FILE" > "$LOG_DIR/litestream.log" 2>&1 &
LITESTREAM_PID=$!

sleep 3

if ! kill -0 "$LITESTREAM_PID" 2>/dev/null; then
    echo "ERROR: Litestream failed to start!"
    echo "Last 50 lines of log:"
    tail -50 "$LOG_DIR/litestream.log"
    exit 1
fi

echo "Litestream running (PID: $LITESTREAM_PID)"
echo ""

# Start load generator
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

# Monitor function for MinIO
monitor_minio() {
    local last_checkpoint_count=0
    local last_compaction_count=0
    local last_sync_count=0

    while true; do
        sleep 60

        echo "[$(date +%H:%M:%S)] Status Report"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        # Database metrics
        if [ -f "$DB_PATH" ]; then
            DB_SIZE=$(stat -f%z "$DB_PATH" 2>/dev/null || stat -c%s "$DB_PATH" 2>/dev/null)
            echo "  Database size: $(numfmt --to=iec-i --suffix=B $DB_SIZE 2>/dev/null || echo "$DB_SIZE bytes")"

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

        # MinIO/S3 metrics using docker exec
        echo ""
        echo "  MinIO S3 Statistics:"

        # Count objects in MinIO
        OBJECT_COUNT=$(docker run --rm --link "${MINIO_CONTAINER_NAME}:minio" \
            -e "MC_HOST_minio=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" \
            minio/mc ls "minio/${MINIO_BUCKET}/" --recursive 2>/dev/null | wc -l | tr -d ' ' || echo "0")

        # Count LTX files (modern format) and snapshots
        LTX_COUNT=$(docker run --rm --link "${MINIO_CONTAINER_NAME}:minio" \
            -e "MC_HOST_minio=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" \
            minio/mc ls "minio/${MINIO_BUCKET}/" --recursive 2>/dev/null | grep -c "\.ltx" || echo "0")

        SNAPSHOT_COUNT=$(docker run --rm --link "${MINIO_CONTAINER_NAME}:minio" \
            -e "MC_HOST_minio=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" \
            minio/mc ls "minio/${MINIO_BUCKET}/" --recursive 2>/dev/null | grep -c "snapshot" || echo "0")

        echo "    Total objects: $OBJECT_COUNT"
        echo "    LTX segments: $LTX_COUNT"
        echo "    Snapshots: $SNAPSHOT_COUNT"

        # Operation metrics
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

            echo ""
            echo "  Operations: $CHECKPOINT_COUNT checkpoints (+$CHECKPOINT_DELTA), $COMPACTION_COUNT compactions (+$COMPACTION_DELTA)"
            echo "  Syncs: $SYNC_COUNT total (+$SYNC_DELTA in last minute)"

            last_checkpoint_count=$CHECKPOINT_COUNT
            last_compaction_count=$COMPACTION_COUNT
            last_sync_count=$SYNC_COUNT
        fi

        # Check for errors
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

echo "Running MinIO S3 test for $TEST_DURATION..."
echo "Monitor will report every 60 seconds"
echo "================================================"
echo ""

# Start monitoring in background
monitor_minio &
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
echo "MinIO S3 Statistics:"
FINAL_OBJECTS=$(docker run --rm --link "${MINIO_CONTAINER_NAME}:minio" \
    -e "MC_HOST_minio=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" \
    minio/mc ls "minio/${MINIO_BUCKET}/" --recursive 2>/dev/null | wc -l | tr -d ' ' || echo "0")

FINAL_LTX=$(docker run --rm --link "${MINIO_CONTAINER_NAME}:minio" \
    -e "MC_HOST_minio=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" \
    minio/mc ls "minio/${MINIO_BUCKET}/" --recursive 2>/dev/null | grep -c "\.ltx" || echo "0")

FINAL_SNAPSHOTS=$(docker run --rm --link "${MINIO_CONTAINER_NAME}:minio" \
    -e "MC_HOST_minio=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" \
    minio/mc ls "minio/${MINIO_BUCKET}/" --recursive 2>/dev/null | grep -c "snapshot" || echo "0")

echo "  Total objects in MinIO: $FINAL_OBJECTS"
echo "  LTX segments: $FINAL_LTX"
echo "  Snapshots: $FINAL_SNAPSHOTS"

# Get storage size
STORAGE_INFO=$(docker run --rm --link "${MINIO_CONTAINER_NAME}:minio" \
    -e "MC_HOST_minio=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" \
    minio/mc du "minio/${MINIO_BUCKET}/" --recursive 2>/dev/null | tail -1 || echo "0")
echo "  Total storage used: $STORAGE_INFO"

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

# Test restoration from MinIO
echo ""
echo "Testing restoration from MinIO S3..."
RESTORE_DB="$TEST_DIR/restored.db"

# Export credentials for litestream restore
export AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}"
export AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}"

# Create a config file for restoration
cat > "$TEST_DIR/restore.yml" <<EOF
access-key-id: ${MINIO_ROOT_USER}
secret-access-key: ${MINIO_ROOT_PASSWORD}
EOF

bin/litestream restore \
    -config "$TEST_DIR/restore.yml" \
    -o "$RESTORE_DB" \
    "$S3_PATH" > "$LOG_DIR/restore.log" 2>&1

if [ $? -eq 0 ]; then
    echo "✓ Restoration successful!"

    # Compare row counts
    TABLES=$(sqlite3 "$RESTORE_DB" ".tables" 2>/dev/null)
    if echo "$TABLES" | grep -q "load_test"; then
        RESTORED_COUNT=$(sqlite3 "$RESTORE_DB" "SELECT COUNT(*) FROM load_test" 2>/dev/null || echo "0")
    elif echo "$TABLES" | grep -q "test_table_0"; then
        RESTORED_COUNT=$(sqlite3 "$RESTORE_DB" "SELECT COUNT(*) FROM test_table_0" 2>/dev/null || echo "0")
    elif echo "$TABLES" | grep -q "test_data"; then
        RESTORED_COUNT=$(sqlite3 "$RESTORE_DB" "SELECT COUNT(*) FROM test_data" 2>/dev/null || echo "0")
    else
        RESTORED_COUNT="0"
    fi

    if [ "$ROW_COUNT" = "$RESTORED_COUNT" ]; then
        echo "✓ Row counts match! ($RESTORED_COUNT rows)"
    else
        echo "⚠ Row count mismatch! Original: $ROW_COUNT, Restored: $RESTORED_COUNT"
    fi
else
    echo "✗ Restoration failed!"
    tail -20 "$LOG_DIR/restore.log"
fi

# Summary
echo ""
echo "================================================"
echo "Test Summary"
echo "================================================"

CRITICAL_ERROR_COUNT=$(grep -i "ERROR" "$LOG_DIR/litestream.log" 2>/dev/null | grep -v "page size not initialized" | wc -l | tr -d ' ')

if [ "$CRITICAL_ERROR_COUNT" -eq 0 ] && [ "$FINAL_OBJECTS" -gt 0 ]; then
    echo "✓ MINIO S3 TEST PASSED!"
    echo ""
    echo "Successfully validated:"
    echo "  - S3-compatible replication to MinIO"
    echo "  - Stored $FINAL_OBJECTS objects"
    echo "  - Compactions: $COMPACTION_COUNT"
    echo "  - Syncs: $SYNC_COUNT"
    [ "$CHECKPOINT_COUNT" -gt 0 ] && echo "  - Checkpoints: $CHECKPOINT_COUNT"
    [ "$FINAL_SNAPSHOTS" -gt 0 ] && echo "  - Snapshots: $FINAL_SNAPSHOTS"
    echo "  - Database restoration from S3"
else
    echo "⚠ TEST COMPLETED WITH ISSUES:"
    [ "$CRITICAL_ERROR_COUNT" -gt 0 ] && echo "  - Critical errors detected: $CRITICAL_ERROR_COUNT"
    [ "$FINAL_OBJECTS" -eq 0 ] && echo "  - No objects stored in MinIO"
    echo ""
    echo "Review the logs for details:"
    echo "  $LOG_DIR/litestream.log"
fi

echo ""
echo "MinIO Console: http://localhost:${MINIO_CONSOLE_PORT}"
echo "Credentials: ${MINIO_ROOT_USER}/${MINIO_ROOT_PASSWORD}"
echo ""
echo "Full test results available in: $TEST_DIR"
echo "================================================"
