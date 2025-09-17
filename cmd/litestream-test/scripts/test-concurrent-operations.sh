#!/bin/bash

# Test Script: Concurrent Database Operations
#
# This test verifies Litestream's behavior under heavy concurrent load with
# multiple databases replicating simultaneously, mixed operations, and
# competing checkpoints.

set -e

echo "============================================"
echo "Concurrent Database Operations Test"
echo "============================================"
echo ""
echo "Testing Litestream with multiple concurrent databases and operations"
echo ""

# Configuration
BASE_DIR="/tmp/concurrent-test"
LITESTREAM_TEST="./bin/litestream-test"
LITESTREAM="./bin/litestream"
NUM_DBS=5
DB_SIZE="50MB"
DURATION="30s"

# Clean up any previous test
echo "[SETUP] Cleaning up previous test files..."
rm -rf "$BASE_DIR"
mkdir -p "$BASE_DIR"

# Check for required binaries
if [ ! -f "$LITESTREAM_TEST" ]; then
    echo "ERROR: litestream-test not found at $LITESTREAM_TEST"
    echo "Build with: go build -o bin/litestream-test ./cmd/litestream-test"
    exit 1
fi

if [ ! -f "$LITESTREAM" ]; then
    echo "ERROR: litestream not found at $LITESTREAM"
    echo "Build with: go build -o bin/litestream ./cmd/litestream"
    exit 1
fi

# Create configuration file for multiple databases
echo "[1] Creating Litestream configuration for $NUM_DBS databases..."
cat > "$BASE_DIR/litestream.yml" <<EOF
dbs:
EOF

for i in $(seq 1 $NUM_DBS); do
    cat >> "$BASE_DIR/litestream.yml" <<EOF
  - path: $BASE_DIR/db${i}.db
    replicas:
      - url: file://$BASE_DIR/replica${i}
        sync-interval: 1s
EOF
done

echo "  ✓ Configuration created"

# Create and populate databases
echo ""
echo "[2] Creating and populating $NUM_DBS databases..."
for i in $(seq 1 $NUM_DBS); do
    echo "  Creating database $i..."
    $LITESTREAM_TEST populate -db "$BASE_DIR/db${i}.db" -target-size "$DB_SIZE" -table-count 2 &
done
wait
echo "  ✓ All databases created"

# Start Litestream with multiple databases
echo ""
echo "[3] Starting Litestream for all databases..."
$LITESTREAM replicate -config "$BASE_DIR/litestream.yml" > "$BASE_DIR/litestream.log" 2>&1 &
LITESTREAM_PID=$!
sleep 3

if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
    echo "ERROR: Litestream failed to start"
    cat "$BASE_DIR/litestream.log"
    exit 1
fi
echo "  ✓ Litestream running (PID: $LITESTREAM_PID)"

# Start concurrent operations on all databases
echo ""
echo "[4] Starting concurrent operations on all databases..."
PIDS=()

# Different workload patterns for each database
for i in $(seq 1 $NUM_DBS); do
    case $i in
        1)
            # High-frequency writes
            echo "  DB$i: High-frequency writes (500/sec)"
            $LITESTREAM_TEST load -db "$BASE_DIR/db${i}.db" \
                -write-rate 500 -duration "$DURATION" \
                -pattern constant > "$BASE_DIR/load${i}.log" 2>&1 &
            ;;
        2)
            # Burst writes
            echo "  DB$i: Burst writes (1000/sec burst)"
            $LITESTREAM_TEST load -db "$BASE_DIR/db${i}.db" \
                -write-rate 1000 -duration "$DURATION" \
                -pattern burst > "$BASE_DIR/load${i}.log" 2>&1 &
            ;;
        3)
            # Mixed with checkpoints
            echo "  DB$i: Moderate writes with periodic checkpoints"
            (
                $LITESTREAM_TEST load -db "$BASE_DIR/db${i}.db" \
                    -write-rate 100 -duration "$DURATION" \
                    -pattern constant > "$BASE_DIR/load${i}.log" 2>&1 &
                LOAD_PID=$!

                # Periodic checkpoints
                for j in {1..6}; do
                    sleep 5
                    sqlite3 "$BASE_DIR/db${i}.db" "PRAGMA wal_checkpoint(PASSIVE);" 2>/dev/null || true
                done

                wait $LOAD_PID
            ) &
            ;;
        4)
            # Shrinking operations
            echo "  DB$i: Writes with periodic shrinking"
            (
                $LITESTREAM_TEST load -db "$BASE_DIR/db${i}.db" \
                    -write-rate 50 -duration "$DURATION" \
                    -pattern wave > "$BASE_DIR/load${i}.log" 2>&1 &
                LOAD_PID=$!

                # Periodic shrinks
                for j in {1..3}; do
                    sleep 10
                    $LITESTREAM_TEST shrink -db "$BASE_DIR/db${i}.db" \
                        -delete-percentage 30 2>/dev/null || true
                done

                wait $LOAD_PID
            ) &
            ;;
        5)
            # Large transactions
            echo "  DB$i: Large batch transactions"
            for j in {1..10}; do
                sqlite3 "$BASE_DIR/db${i}.db" <<EOF
BEGIN;
INSERT INTO test_table_0 (data)
SELECT randomblob(1000) FROM generate_series(1, 10000);
COMMIT;
EOF
                sleep 3
            done &
            ;;
    esac
    PIDS+=($!)
done

# Monitor progress
echo ""
echo "[5] Running concurrent operations for $DURATION..."
ELAPSED=0
MAX_ELAPSED=30

while [ $ELAPSED -lt $MAX_ELAPSED ]; do
    sleep 5
    ELAPSED=$((ELAPSED + 5))

    # Check Litestream health
    if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
        echo "  ERROR: Litestream crashed!"
        cat "$BASE_DIR/litestream.log" | tail -20
        exit 1
    fi

    # Check for errors
    ERROR_COUNT=$(grep -i "error\|panic" "$BASE_DIR/litestream.log" 2>/dev/null | wc -l || echo "0")
    if [ "$ERROR_COUNT" -gt 0 ]; then
        echo "  Errors detected: $ERROR_COUNT"
    fi

    echo "  Progress: ${ELAPSED}s / ${MAX_ELAPSED}s"
done

# Stop all operations
echo ""
echo "[6] Stopping operations..."
for pid in "${PIDS[@]}"; do
    kill $pid 2>/dev/null || true
done
wait

# Give Litestream time to catch up
echo "  Waiting for final sync..."
sleep 5

# Collect metrics
echo ""
echo "[7] Collecting metrics..."
for i in $(seq 1 $NUM_DBS); do
    DB_SIZE=$(stat -f%z "$BASE_DIR/db${i}.db" 2>/dev/null || stat -c%s "$BASE_DIR/db${i}.db")
    WAL_SIZE=$(stat -f%z "$BASE_DIR/db${i}.db-wal" 2>/dev/null || stat -c%s "$BASE_DIR/db${i}.db-wal" 2>/dev/null || echo "0")
    REPLICA_COUNT=$(find "$BASE_DIR/replica${i}" -type f 2>/dev/null | wc -l || echo "0")

    echo "  DB$i:"
    echo "    Database size: $((DB_SIZE / 1024 / 1024))MB"
    echo "    WAL size: $((WAL_SIZE / 1024 / 1024))MB"
    echo "    Replica files: $REPLICA_COUNT"
done

# Stop Litestream
kill $LITESTREAM_PID 2>/dev/null || true
sleep 2

# Test restoration for all databases
echo ""
echo "[8] Testing restoration of all databases..."
RESTORE_FAILED=0

for i in $(seq 1 $NUM_DBS); do
    echo "  Restoring DB$i..."
    rm -f "$BASE_DIR/restored${i}.db"

    if $LITESTREAM restore -config "$BASE_DIR/litestream.yml" \
        -o "$BASE_DIR/restored${i}.db" "$BASE_DIR/db${i}.db" > "$BASE_DIR/restore${i}.log" 2>&1; then

        # Verify integrity
        INTEGRITY=$(sqlite3 "$BASE_DIR/restored${i}.db" "PRAGMA integrity_check;" 2>/dev/null || echo "FAILED")
        if [ "$INTEGRITY" = "ok" ]; then
            echo "    ✓ DB$i restored successfully"
        else
            echo "    ✗ DB$i integrity check failed!"
            RESTORE_FAILED=$((RESTORE_FAILED + 1))
        fi
    else
        echo "    ✗ DB$i restore failed!"
        cat "$BASE_DIR/restore${i}.log"
        RESTORE_FAILED=$((RESTORE_FAILED + 1))
    fi
done

# Check for race conditions or deadlocks in logs
echo ""
echo "[9] Analyzing logs for issues..."
ISSUES_FOUND=0

# Check for deadlocks
if grep -i "deadlock" "$BASE_DIR/litestream.log" > /dev/null 2>&1; then
    echo "  ✗ Deadlock detected!"
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
fi

# Check for database locked errors
LOCKED_COUNT=$(grep -c "database is locked" "$BASE_DIR/litestream.log" 2>/dev/null || echo "0")
if [ "$LOCKED_COUNT" -gt 10 ]; then
    echo "  ⚠ High number of 'database locked' errors: $LOCKED_COUNT"
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
fi

# Check for checkpoint failures
CHECKPOINT_ERRORS=$(grep -c "checkpoint.*error\|checkpoint.*fail" "$BASE_DIR/litestream.log" 2>/dev/null || echo "0")
if [ "$CHECKPOINT_ERRORS" -gt 0 ]; then
    echo "  ⚠ Checkpoint errors detected: $CHECKPOINT_ERRORS"
fi

# Summary
echo ""
echo "============================================"
echo "Test Results Summary"
echo "============================================"
echo ""
echo "Databases tested: $NUM_DBS"
echo "Restore failures: $RESTORE_FAILED"
echo "Critical issues found: $ISSUES_FOUND"

if [ "$RESTORE_FAILED" -eq 0 ] && [ "$ISSUES_FOUND" -eq 0 ]; then
    echo ""
    echo "✅ CONCURRENT OPERATIONS TEST PASSED"
    echo ""
    echo "Litestream successfully handled:"
    echo "- $NUM_DBS databases replicating simultaneously"
    echo "- Mixed workload patterns (high-frequency, burst, batch)"
    echo "- Concurrent checkpoints and shrinking operations"
    echo "- All databases restored successfully"
else
    echo ""
    echo "❌ CONCURRENT OPERATIONS TEST FAILED"
    echo ""
    echo "Issues detected during concurrent operations"
    echo "Check logs at: $BASE_DIR/"
    exit 1
fi

# Clean up
pkill -f litestream-test 2>/dev/null || true
pkill -f "litestream replicate" 2>/dev/null || true
echo ""
echo "Test complete. Artifacts saved in: $BASE_DIR/"
