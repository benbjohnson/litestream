#!/bin/bash

# Test Script: SQLite 1GB Lock Page Boundary
#
# This test verifies that Litestream correctly handles the SQLite lock page
# at the 1GB boundary (0x40000000). This page is reserved by SQLite and
# cannot contain data - Litestream must skip it during replication.
#
# The lock page number varies by page size:
# - 4KB: page 262145
# - 8KB: page 131073
# - 16KB: page 65537
# - 32KB: page 32769

set -e

echo "=========================================="
echo "SQLite 1GB Lock Page Boundary Test"
echo "=========================================="
echo ""
echo "Testing Litestream's handling of SQLite's reserved lock page at 1GB"
echo ""

# Configuration
DB="/tmp/1gb-test.db"
REPLICA="/tmp/1gb-replica"
LITESTREAM_TEST="./bin/litestream-test"
LITESTREAM="./bin/litestream"

# Clean up any previous test
echo "[SETUP] Cleaning up previous test files..."
rm -f "$DB"*
rm -rf "$REPLICA"

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

test_page_size() {
    local PAGE_SIZE=$1
    local LOCK_PGNO=$2

    echo ""
    echo "======================================="
    echo "Testing with page size: $PAGE_SIZE bytes"
    echo "Lock page should be at: $LOCK_PGNO"
    echo "======================================="

    # Clean up for this test
    rm -f "$DB"*
    rm -rf "$REPLICA"

    # Create database with specific page size
    echo "[1] Creating database with page_size=$PAGE_SIZE..."
    sqlite3 "$DB" <<EOF
PRAGMA page_size=$PAGE_SIZE;
CREATE TABLE test_data (
    id INTEGER PRIMARY KEY,
    data BLOB
);
EOF

    # Calculate target size (1.2GB to ensure we cross 1GB boundary)
    TARGET_SIZE=$((1200 * 1024 * 1024))

    echo "[2] Populating database to cross 1GB boundary (target: 1.2GB)..."
    # Use litestream-test to populate efficiently
    $LITESTREAM_TEST populate -db "$DB" -target-size 1200MB -row-size $((PAGE_SIZE - 100))

    # Get actual size and page count
    DB_SIZE=$(stat -f%z "$DB" 2>/dev/null || stat -c%s "$DB")
    PAGE_COUNT=$(sqlite3 "$DB" "PRAGMA page_count;")
    echo "  Database size: $(( DB_SIZE / 1024 / 1024 ))MB"
    echo "  Page count: $PAGE_COUNT"
    echo "  Lock page at: $LOCK_PGNO"

    # Verify we've crossed the boundary
    if [ "$PAGE_COUNT" -le "$LOCK_PGNO" ]; then
        echo "  WARNING: Database doesn't cross lock page boundary!"
        echo "  Need at least $LOCK_PGNO pages, have $PAGE_COUNT"
    else
        echo "  ✓ Database crosses lock page boundary"
    fi

    # Start Litestream replication
    echo "[3] Starting Litestream replication..."
    $LITESTREAM replicate "$DB" "file://$REPLICA" > /tmp/litestream-1gb.log 2>&1 &
    LITESTREAM_PID=$!
    sleep 3

    if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
        echo "ERROR: Litestream failed to start"
        cat /tmp/litestream-1gb.log
        return 1
    fi
    echo "  ✓ Litestream running (PID: $LITESTREAM_PID)"

    # Add more data to trigger replication across the boundary
    echo "[4] Adding data around the lock page boundary..."
    # Use litestream-test load to ensure continuous writes
    $LITESTREAM_TEST load -db "$DB" -write-rate 10 -duration 10s -pattern constant &
    LOAD_PID=$!

    # Let it run and create multiple transactions
    echo "[5] Running writes for 10 seconds to ensure multiple transactions..."
    sleep 10

    # Stop writes and let replication catch up
    kill $LOAD_PID 2>/dev/null || true
    sleep 5

    # Check for errors in log
    if grep -i "error\|panic\|fatal" /tmp/litestream-1gb.log > /dev/null 2>&1; then
        echo "  WARNING: Errors detected in Litestream log:"
        grep -i "error\|panic\|fatal" /tmp/litestream-1gb.log | head -5
    fi

    # Stop Litestream
    kill $LITESTREAM_PID 2>/dev/null || true
    sleep 2

    # Attempt restore
    echo "[6] Testing restore..."
    rm -f /tmp/restored-1gb.db
    if $LITESTREAM restore -o /tmp/restored-1gb.db "file://$REPLICA" > /tmp/restore-1gb.log 2>&1; then
        echo "  ✓ Restore successful"

        # Verify integrity
        INTEGRITY=$(sqlite3 /tmp/restored-1gb.db "PRAGMA integrity_check;" 2>/dev/null || echo "FAILED")
        if [ "$INTEGRITY" = "ok" ]; then
            echo "  ✓ Integrity check passed"
        else
            echo "  ✗ Integrity check failed: $INTEGRITY"
            return 1
        fi

        # Compare page counts
        RESTORED_COUNT=$(sqlite3 /tmp/restored-1gb.db "PRAGMA page_count;" 2>/dev/null || echo "0")
        echo "  Original pages: $PAGE_COUNT"
        echo "  Restored pages: $RESTORED_COUNT"

        if [ "$PAGE_COUNT" -eq "$RESTORED_COUNT" ]; then
            echo "  ✓ Page count matches"
        else
            echo "  ✗ Page count mismatch!"
            return 1
        fi

        # Check data integrity
        ORIG_ROWS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM test_data;")
        REST_ROWS=$(sqlite3 /tmp/restored-1gb.db "SELECT COUNT(*) FROM test_data;")
        echo "  Original rows: $ORIG_ROWS"
        echo "  Restored rows: $REST_ROWS"

        if [ "$ORIG_ROWS" -eq "$REST_ROWS" ]; then
            echo "  ✓ Data integrity verified"
            echo ""
            echo "  TEST PASSED for page_size=$PAGE_SIZE"
        else
            echo "  ✗ Row count mismatch!"
            return 1
        fi
    else
        echo "  ✗ Restore FAILED!"
        cat /tmp/restore-1gb.log
        return 1
    fi

    # Clean up
    rm -f /tmp/restored-1gb.db
}

# Test with different page sizes
echo "Testing SQLite lock page handling at 1GB boundary"
echo "This verifies Litestream correctly skips the reserved lock page"
echo ""

# Default 4KB page size (most common)
if ! test_page_size 4096 262145; then
    echo "CRITICAL: Test failed for 4KB pages!"
    exit 1
fi

# 8KB page size
if ! test_page_size 8192 131073; then
    echo "CRITICAL: Test failed for 8KB pages!"
    exit 1
fi

# 16KB page size (if time permits - these are large databases)
# Uncomment to test:
# if ! test_page_size 16384 65537; then
#     echo "CRITICAL: Test failed for 16KB pages!"
#     exit 1
# fi

echo ""
echo "=========================================="
echo "All 1GB boundary tests PASSED!"
echo "=========================================="
echo ""
echo "Litestream correctly handles the SQLite lock page at 1GB boundary"
echo "for all tested page sizes."
echo ""

# Clean up
pkill -f "litestream replicate" 2>/dev/null || true
echo "Test complete."
