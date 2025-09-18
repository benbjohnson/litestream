#!/bin/bash

# Litestream v0.5.0 Critical Bug Reproduction Script
#
# This script demonstrates a CRITICAL data loss bug where restore fails
# after Litestream is interrupted and a checkpoint occurs during downtime.
#
# Requirements:
# - litestream binary (built from current main branch)
# - litestream-test binary (from PR #748 or build with: go build -o bin/litestream-test ./cmd/litestream-test)
# - SQLite3 command line tool
#
# Expected behavior: Database should restore successfully
# Actual behavior: Restore fails with "nonsequential page numbers" error

set -e

echo "============================================"
echo "Litestream v0.5.0 Critical Bug Reproduction"
echo "============================================"
echo ""
echo "This demonstrates a data loss scenario where restore fails after:"
echo "1. Litestream is killed (simulating crash)"
echo "2. Writes continue and a checkpoint occurs"
echo "3. Litestream is restarted"
echo ""

# Configuration
DB="/tmp/critical-bug-test.db"
REPLICA="/tmp/critical-bug-replica"

# Clean up any previous test
echo "[SETUP] Cleaning up previous test files..."
rm -f "$DB"*
rm -rf "$REPLICA"

# ALWAYS use local build for testing
LITESTREAM="./bin/litestream"
if [ ! -f "$LITESTREAM" ]; then
    echo "ERROR: Local litestream build not found at $LITESTREAM"
    echo "Please build first: go build -o bin/litestream ./cmd/litestream"
    exit 1
fi
echo "Using local build: $LITESTREAM"

# Check for litestream-test binary
if [ -f "./bin/litestream-test" ]; then
    LITESTREAM_TEST="./bin/litestream-test"
    echo "Using local litestream-test: $LITESTREAM_TEST"
else
    echo "ERROR: litestream-test not found. Please build with:"
    echo "  go build -o bin/litestream-test ./cmd/litestream-test"
    echo ""
    echo "Or get it from: https://github.com/benbjohnson/litestream/pull/748"
    exit 1
fi

# Show versions
echo "Versions:"
$LITESTREAM version
echo ""

# Step 1: Create and populate initial database
echo ""
echo "[STEP 1] Creating test database (50MB)..."
$LITESTREAM_TEST populate -db "$DB" -target-size 50MB -table-count 2
INITIAL_SIZE=$(ls -lh "$DB" 2>/dev/null | awk '{print $5}')
echo "✓ Database created: $INITIAL_SIZE"

# Step 2: Start Litestream replication
echo ""
echo "[STEP 2] Starting Litestream replication..."
./bin/litestream replicate "$DB" "file://$REPLICA" > /tmp/litestream.log 2>&1 &
LITESTREAM_PID=$!
sleep 3

if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
    echo "ERROR: Litestream failed to start. Check /tmp/litestream.log"
    cat /tmp/litestream.log
    exit 1
fi
echo "✓ Litestream running (PID: $LITESTREAM_PID)"

# Step 3: Start continuous writes
echo ""
echo "[STEP 3] Starting continuous writes..."
./bin/litestream-test load -db "$DB" -write-rate 100 -duration 2m -pattern constant > /tmp/writes.log 2>&1 &
WRITE_PID=$!
echo "✓ Write load started (PID: $WRITE_PID)"

# Step 4: Let it run normally for 20 seconds
echo ""
echo "[STEP 4] Running normally for 20 seconds..."
sleep 20

# Get row count before interruption
ROWS_BEFORE=$(sqlite3 "$DB" "SELECT COUNT(*) FROM load_test;" 2>/dev/null || echo "0")
echo "✓ Rows written before interruption: $ROWS_BEFORE"

# Step 5: Kill Litestream (simulate crash)
echo ""
echo "[STEP 5] Killing Litestream (simulating crash)..."
kill -9 $LITESTREAM_PID 2>/dev/null || true
echo "✓ Litestream killed"

# Step 6: Let writes continue for 15 seconds without Litestream
echo ""
echo "[STEP 6] Continuing writes for 15 seconds (Litestream is down)..."
sleep 15

# Step 7: Execute non-PASSIVE checkpoint
echo ""
echo "[STEP 7] Executing FULL checkpoint while Litestream is down..."
CHECKPOINT_RESULT=$(sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);" 2>&1)
echo "✓ Checkpoint result: $CHECKPOINT_RESULT"

ROWS_AFTER_CHECKPOINT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM load_test;")
echo "✓ Rows after checkpoint: $ROWS_AFTER_CHECKPOINT"

# Step 8: Resume Litestream
echo ""
echo "[STEP 8] Resuming Litestream..."
./bin/litestream replicate "$DB" "file://$REPLICA" >> /tmp/litestream.log 2>&1 &
NEW_LITESTREAM_PID=$!
sleep 3

if ! kill -0 $NEW_LITESTREAM_PID 2>/dev/null; then
    echo "WARNING: Litestream failed to restart"
fi
echo "✓ Litestream restarted (PID: $NEW_LITESTREAM_PID)"

# Step 9: Let Litestream catch up
echo ""
echo "[STEP 9] Letting Litestream catch up for 20 seconds..."
sleep 20

# Stop writes
kill $WRITE_PID 2>/dev/null || true
echo "✓ Writes stopped"

# Wait for final sync
sleep 5

# Get final row count
FINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM load_test;")
echo "✓ Final row count in source database: $FINAL_COUNT"

# Kill Litestream
kill $NEW_LITESTREAM_PID 2>/dev/null || true

# Step 10: Attempt to restore (THIS IS WHERE THE BUG OCCURS)
echo ""
echo "[STEP 10] Attempting to restore database..."
echo "=========================================="
echo ""

rm -f /tmp/restored.db
if ./bin/litestream restore -o /tmp/restored.db "file://$REPLICA" 2>&1 | tee /tmp/restore-output.log; then
    echo ""
    echo "✓ SUCCESS: Restore completed successfully"

    # Verify restored database
    RESTORED_COUNT=$(sqlite3 /tmp/restored.db "SELECT COUNT(*) FROM load_test;" 2>/dev/null || echo "0")
    INTEGRITY=$(sqlite3 /tmp/restored.db "PRAGMA integrity_check;" 2>/dev/null || echo "FAILED")

    echo "  - Restored row count: $RESTORED_COUNT"
    echo "  - Integrity check: $INTEGRITY"

    if [ "$RESTORED_COUNT" -eq "$FINAL_COUNT" ]; then
        echo "  - Data integrity: ✓ VERIFIED (no data loss)"
    else
        LOSS=$((FINAL_COUNT - RESTORED_COUNT))
        echo "  - Data integrity: ✗ FAILED (lost $LOSS rows)"
    fi
else
    echo ""
    echo "✗ CRITICAL BUG REPRODUCED: Restore failed!"
    echo ""
    echo "Error output:"
    echo "-------------"
    cat /tmp/restore-output.log
    echo ""
    echo "This is the critical bug. The database cannot be restored after"
    echo "Litestream was interrupted and a checkpoint occurred during downtime."
    echo ""
    echo "Original database stats:"
    echo "  - Rows before interruption: $ROWS_BEFORE"
    echo "  - Rows after checkpoint: $ROWS_AFTER_CHECKPOINT"
    echo "  - Final rows: $FINAL_COUNT"
    echo "  - DATA IS UNRECOVERABLE"
fi

echo ""
echo "=========================================="
echo "Test artifacts saved in:"
echo "  - Source database: $DB"
echo "  - Replica files: $REPLICA/"
echo "  - Litestream log: /tmp/litestream.log"
echo "  - Restore output: /tmp/restore-output.log"
echo ""

# Clean up processes
pkill -f litestream-test 2>/dev/null || true
pkill -f "litestream replicate" 2>/dev/null || true

echo "Test complete."
