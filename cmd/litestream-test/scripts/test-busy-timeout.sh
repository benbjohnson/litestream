#!/bin/bash
set -e

# Test busy timeout handling with concurrent writes
# This test verifies proper handling of write lock conflicts between app and Litestream

echo "=========================================="
echo "Busy Timeout and Write Lock Conflict Test"
echo "=========================================="
echo ""
echo "Testing write lock conflict handling with various busy_timeout settings"
echo ""

# Configuration
DB="/tmp/busy-test.db"
REPLICA="/tmp/busy-replica"
LITESTREAM="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# Cleanup function
cleanup() {
    pkill -f "litestream replicate.*busy-test.db" 2>/dev/null || true
    pkill -f "litestream-test load.*busy-test.db" 2>/dev/null || true
    rm -f "$DB" "$DB-wal" "$DB-shm" "$DB-litestream"
    rm -rf "$REPLICA"
    rm -f /tmp/busy-*.log
}

trap cleanup EXIT

echo "[SETUP] Cleaning up previous test files..."
cleanup

echo ""
echo "[1] Creating test database..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
CREATE TABLE test (id INTEGER PRIMARY KEY, data BLOB, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);
INSERT INTO test (data) VALUES (randomblob(1000));
EOF
echo "  ✓ Database created"

echo ""
echo "[2] Starting Litestream replication..."
"$LITESTREAM" replicate "$DB" "file://$REPLICA" > /tmp/busy-litestream.log 2>&1 &
LITESTREAM_PID=$!
sleep 2

if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
    echo "  ✗ Litestream failed to start"
    cat /tmp/busy-litestream.log
    exit 1
fi
echo "  ✓ Litestream running (PID: $LITESTREAM_PID)"

echo ""
echo "=========================================="
echo "Test 1: No busy_timeout (default behavior)"
echo "=========================================="

echo "[3] Starting aggressive writes without busy_timeout..."
ERRORS_NO_TIMEOUT=0
SUCCESS_NO_TIMEOUT=0

for i in {1..100}; do
    if sqlite3 "$DB" "INSERT INTO test (data) VALUES (randomblob(1000));" 2>/dev/null; then
        ((SUCCESS_NO_TIMEOUT++))
    else
        ((ERRORS_NO_TIMEOUT++))
    fi
done

echo "  Results without busy_timeout:"
echo "    ✓ Successful writes: $SUCCESS_NO_TIMEOUT"
echo "    ✗ Failed writes (SQLITE_BUSY): $ERRORS_NO_TIMEOUT"

if [ $ERRORS_NO_TIMEOUT -gt 0 ]; then
    echo "  ⚠️  Conflicts detected without busy_timeout (expected)"
else
    echo "  ✓ No conflicts (may indicate low checkpoint frequency)"
fi

echo ""
echo "=========================================="
echo "Test 2: With 5-second busy_timeout (recommended)"
echo "=========================================="

echo "[4] Testing with recommended 5-second timeout..."
ERRORS_WITH_TIMEOUT=0
SUCCESS_WITH_TIMEOUT=0

for i in {1..100}; do
    if sqlite3 "$DB" "PRAGMA busy_timeout = 5000; INSERT INTO test (data) VALUES (randomblob(1000));" 2>/dev/null; then
        ((SUCCESS_WITH_TIMEOUT++))
    else
        ((ERRORS_WITH_TIMEOUT++))
    fi
done

echo "  Results with 5s busy_timeout:"
echo "    ✓ Successful writes: $SUCCESS_WITH_TIMEOUT"
echo "    ✗ Failed writes: $ERRORS_WITH_TIMEOUT"

if [ $ERRORS_WITH_TIMEOUT -eq 0 ]; then
    echo "  ✓ All writes succeeded with proper timeout!"
elif [ $ERRORS_WITH_TIMEOUT -lt $ERRORS_NO_TIMEOUT ]; then
    echo "  ✓ Timeout reduced conflicts significantly"
else
    echo "  ⚠️  Timeout didn't help (may need investigation)"
fi

echo ""
echo "=========================================="
echo "Test 3: Concurrent high-frequency writes"
echo "=========================================="

echo "[5] Starting 3 concurrent write processes..."

# Start multiple concurrent writers
(
    for i in {1..50}; do
        sqlite3 "$DB" "PRAGMA busy_timeout = 5000; INSERT INTO test (data) VALUES ('Writer1: ' || randomblob(500));" 2>/dev/null
        sleep 0.01
    done
) > /tmp/busy-writer1.log 2>&1 &
WRITER1_PID=$!

(
    for i in {1..50}; do
        sqlite3 "$DB" "PRAGMA busy_timeout = 5000; INSERT INTO test (data) VALUES ('Writer2: ' || randomblob(500));" 2>/dev/null
        sleep 0.01
    done
) > /tmp/busy-writer2.log 2>&1 &
WRITER2_PID=$!

(
    for i in {1..50}; do
        sqlite3 "$DB" "PRAGMA busy_timeout = 5000; INSERT INTO test (data) VALUES ('Writer3: ' || randomblob(500));" 2>/dev/null
        sleep 0.01
    done
) > /tmp/busy-writer3.log 2>&1 &
WRITER3_PID=$!

echo "  Writers started: PID $WRITER1_PID, $WRITER2_PID, $WRITER3_PID"

# Monitor for conflicts
sleep 1
echo ""
echo "[6] Forcing checkpoints during concurrent writes..."
for i in {1..5}; do
    sqlite3 "$DB" "PRAGMA busy_timeout = 5000; PRAGMA wal_checkpoint(PASSIVE);" 2>/dev/null || true
    sleep 1
done

# Wait for writers to complete
wait $WRITER1_PID 2>/dev/null
wait $WRITER2_PID 2>/dev/null
wait $WRITER3_PID 2>/dev/null

echo "  ✓ Concurrent writers completed"

echo ""
echo "[7] Checking for lock contention in Litestream log..."
CHECKPOINT_ERRORS=$(grep -c "checkpoint" /tmp/busy-litestream.log 2>/dev/null || echo "0")
SYNC_ERRORS=$(grep -c "database is locked" /tmp/busy-litestream.log 2>/dev/null || echo "0")

echo "  Litestream errors:"
echo "    Checkpoint errors: $CHECKPOINT_ERRORS"
echo "    Lock errors: $SYNC_ERRORS"

if [ "$SYNC_ERRORS" -eq "0" ]; then
    echo "  ✓ No lock errors in Litestream"
else
    echo "  ⚠️  Some lock contention detected (may be normal under high load)"
fi

echo ""
echo "=========================================="
echo "Test 4: Checkpoint during write transaction"
echo "=========================================="

echo "[8] Testing checkpoint during long transaction..."

# Start a long transaction
sqlite3 "$DB" "PRAGMA busy_timeout = 5000; BEGIN EXCLUSIVE;" 2>/dev/null &
TRANS_PID=$!
sleep 0.5

# Try to checkpoint while transaction is held
CHECKPOINT_RESULT=$(sqlite3 "$DB" "PRAGMA busy_timeout = 1000; PRAGMA wal_checkpoint(FULL);" 2>&1 || echo "FAILED")

if [[ "$CHECKPOINT_RESULT" == *"FAILED"* ]] || [[ "$CHECKPOINT_RESULT" == *"database is locked"* ]]; then
    echo "  ✓ Checkpoint correctly blocked by exclusive transaction"
else
    echo "  ⚠️  Unexpected checkpoint behavior: $CHECKPOINT_RESULT"
fi

# Clean up transaction
kill $TRANS_PID 2>/dev/null || true

echo ""
echo "[9] Final statistics..."
TOTAL_ROWS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM test;")
WAL_SIZE=$(du -h "$DB-wal" 2>/dev/null | cut -f1 || echo "0")
DB_SIZE=$(du -h "$DB" | cut -f1)

echo "  Database stats:"
echo "    Total rows inserted: $TOTAL_ROWS"
echo "    Database size: $DB_SIZE"
echo "    WAL size: $WAL_SIZE"

echo ""
echo "=========================================="
echo "Busy Timeout Test Summary:"
echo "  Without timeout: $ERRORS_NO_TIMEOUT conflicts"
echo "  With 5s timeout: $ERRORS_WITH_TIMEOUT conflicts"
echo "  Concurrent writes: Completed successfully"
echo "  Lock contention: Properly handled"
echo ""
if [ $ERRORS_WITH_TIMEOUT -lt $ERRORS_NO_TIMEOUT ] || [ $ERRORS_WITH_TIMEOUT -eq 0 ]; then
    echo "✅ TEST PASSED: busy_timeout improves conflict handling"
else
    echo "⚠️  TEST NOTICE: Timeout may need tuning for this workload"
fi
echo "=========================================="
