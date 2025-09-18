#!/bin/bash
set -e

# Test to reproduce original #754 flag issue
# This recreates the scenario where #754 was first discovered

echo "=========================================="
echo "v0.5.0 → v0.5.0 Flag Issue Reproduction"
echo "=========================================="
echo ""
echo "Reproducing the original #754 'no flags allowed' scenario"
echo "Testing v0.5.0 backing up a database that already has v0.5.0 LTX files"
echo ""

# Configuration
DB="/tmp/flag-reproduction-test.db"
REPLICA="/tmp/flag-reproduction-replica"
LITESTREAM_V5="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# Cleanup function
cleanup() {
    pkill -f "litestream replicate.*flag-reproduction-test.db" 2>/dev/null || true
    rm -f "$DB" "$DB-wal" "$DB-shm" "$DB-litestream"
    rm -rf "$REPLICA"
    rm -f /tmp/flag-reproduction-*.log
}

trap cleanup EXIT

echo "[SETUP] Cleaning up previous test files..."
cleanup

echo ""
echo "[1] Creating large database with v0.5.0 (first run)..."
$LITESTREAM_TEST populate -db "$DB" -target-size 1200MB >/dev/null 2>&1

# Add identifiable data
sqlite3 "$DB" <<EOF
CREATE TABLE flag_test (
    id INTEGER PRIMARY KEY,
    run_number INTEGER,
    data BLOB,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO flag_test (run_number, data) VALUES (1, randomblob(5000));
EOF

DB_SIZE=$(du -h "$DB" | cut -f1)
PAGE_COUNT=$(sqlite3 "$DB" "PRAGMA page_count;")
INITIAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM flag_test;")

echo "  ✓ Database created:"
echo "    Size: $DB_SIZE"
echo "    Pages: $PAGE_COUNT"
echo "    Records: $INITIAL_COUNT"

echo ""
echo "[2] First v0.5.0 replication run..."
$LITESTREAM_V5 replicate "$DB" "file://$REPLICA" > /tmp/flag-reproduction-run1.log 2>&1 &
RUN1_PID=$!
sleep 5

if ! kill -0 $RUN1_PID 2>/dev/null; then
    echo "  ✗ First v0.5.0 run failed"
    cat /tmp/flag-reproduction-run1.log
    exit 1
fi
echo "  ✓ First v0.5.0 run started (PID: $RUN1_PID)"

# Add some data during first run
for i in {1..10}; do
    sqlite3 "$DB" "INSERT INTO flag_test (run_number, data) VALUES (1, randomblob(3000));"
done
sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);" >/dev/null 2>&1
sleep 3

RUN1_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM flag_test;")
echo "  ✓ First run data added, total: $RUN1_COUNT"

# Check first run for errors
RUN1_ERRORS=$(grep -c "ERROR" /tmp/flag-reproduction-run1.log 2>/dev/null || echo "0")
RUN1_FLAGS=$(grep -c "no flags allowed" /tmp/flag-reproduction-run1.log 2>/dev/null || echo "0")

echo "  First run status:"
echo "    Errors: $RUN1_ERRORS"
echo "    Flag errors: $RUN1_FLAGS"

if [ "$RUN1_FLAGS" -gt "0" ]; then
    echo "  ⚠️  Flag errors in first run (unexpected)"
fi

echo ""
echo "[3] Examining first run LTX files..."
if [ -d "$REPLICA" ]; then
    LTX_FILES=$(find "$REPLICA" -name "*.ltx" | wc -l)
    echo "  LTX files created: $LTX_FILES"

    # Look for files with HeaderFlagNoChecksum
    echo "  Examining LTX file headers..."
    find "$REPLICA" -name "*.ltx" | head -3 | while read ltx_file; do
        echo "    $(basename $ltx_file): $(file "$ltx_file" 2>/dev/null || echo "unknown format")"
    done
else
    echo "  ✗ No replica directory found"
    exit 1
fi

echo ""
echo "[4] Stopping first run and simulating restart..."
kill $RUN1_PID 2>/dev/null || true
wait $RUN1_PID 2>/dev/null
echo "  ✓ First run stopped"

# Add data while Litestream is down
sqlite3 "$DB" "INSERT INTO flag_test (run_number, data) VALUES (2, randomblob(4000));"
BETWEEN_RUNS_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM flag_test;")
echo "  ✓ Data added between runs, total: $BETWEEN_RUNS_COUNT"

echo ""
echo "[5] CRITICAL: Second v0.5.0 run (where #754 might occur)..."
echo "  Starting v0.5.0 against database with existing v0.5.0 LTX files..."

$LITESTREAM_V5 replicate "$DB" "file://$REPLICA" > /tmp/flag-reproduction-run2.log 2>&1 &
RUN2_PID=$!
sleep 5

if ! kill -0 $RUN2_PID 2>/dev/null; then
    echo "  ✗ Second v0.5.0 run failed to start"
    cat /tmp/flag-reproduction-run2.log
else
    echo "  ✓ Second v0.5.0 run started (PID: $RUN2_PID)"
fi

echo ""
echo "[6] Monitoring for #754 flag errors..."
sleep 10

RUN2_FLAGS=$(grep -c "no flags allowed" /tmp/flag-reproduction-run2.log 2>/dev/null || echo "0")
RUN2_VERIFICATION=$(grep -c "ltx verification failed" /tmp/flag-reproduction-run2.log 2>/dev/null || echo "0")
RUN2_SYNC_ERRORS=$(grep -c "sync error" /tmp/flag-reproduction-run2.log 2>/dev/null || echo "0")
RUN2_TOTAL_ERRORS=$(grep -c "ERROR" /tmp/flag-reproduction-run2.log 2>/dev/null || echo "0")

echo "  Second run error analysis:"
echo "    'no flags allowed' errors: $RUN2_FLAGS"
echo "    'ltx verification failed' errors: $RUN2_VERIFICATION"
echo "    'sync error' count: $RUN2_SYNC_ERRORS"
echo "    Total errors: $RUN2_TOTAL_ERRORS"

if [ "$RUN2_FLAGS" -gt "0" ] || [ "$RUN2_VERIFICATION" -gt "0" ]; then
    echo ""
    echo "  🚨 #754 FLAG ISSUE REPRODUCED!"
    echo "  This occurs when v0.5.0 reads existing v0.5.0 LTX files"
    echo "  Error details:"
    grep -A2 -B2 "no flags allowed\|ltx verification failed" /tmp/flag-reproduction-run2.log || true
    FLAG_ISSUE_REPRODUCED=true
else
    echo "  ✅ No #754 flag errors in second run"
    FLAG_ISSUE_REPRODUCED=false
fi

echo ""
echo "[7] Adding more data during second run..."
if kill -0 $RUN2_PID 2>/dev/null; then
    for i in {1..5}; do
        sqlite3 "$DB" "INSERT INTO flag_test (run_number, data) VALUES (2, randomblob(3500));" 2>/dev/null || true
    done
    FINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM flag_test;")
    echo "  ✓ Second run data added, final total: $FINAL_COUNT"

    kill $RUN2_PID 2>/dev/null || true
    wait $RUN2_PID 2>/dev/null
else
    echo "  ✗ Second run already failed, cannot add data"
    FINAL_COUNT=$BETWEEN_RUNS_COUNT
fi

echo ""
echo "[8] Final analysis..."
echo "  Checking recent errors from second run:"
if [ "$RUN2_TOTAL_ERRORS" -gt "0" ]; then
    tail -10 /tmp/flag-reproduction-run2.log | grep ERROR || echo "    No recent errors"
fi

# Count total LTX files after both runs
FINAL_LTX_FILES=$(find "$REPLICA" -name "*.ltx" 2>/dev/null | wc -l)

echo ""
echo "=========================================="
echo "Flag Issue Reproduction Results"
echo "=========================================="
echo ""
echo "Database progression:"
echo "  Initial: $INITIAL_COUNT records"
echo "  After run 1: $RUN1_COUNT records"
echo "  Between runs: $BETWEEN_RUNS_COUNT records"
echo "  Final: $FINAL_COUNT records"
echo ""
echo "Error analysis:"
echo "  Run 1 errors: $RUN1_ERRORS (flag errors: $RUN1_FLAGS)"
echo "  Run 2 errors: $RUN2_TOTAL_ERRORS (flag errors: $RUN2_FLAGS)"
echo "  LTX files created: $FINAL_LTX_FILES"
echo ""
echo "CRITICAL FINDING:"
if [ "$FLAG_ISSUE_REPRODUCED" = true ]; then
    echo "🚨 #754 FLAG ISSUE REPRODUCED!"
    echo "   Trigger: v0.5.0 restarting against existing v0.5.0 LTX files"
    echo "   Root cause: HeaderFlagNoChecksum incompatibility with LTX v0.5.0"
else
    echo "✅ Could not reproduce #754 flag issue"
    echo "   Issue may require specific conditions or database content"
fi
echo ""
echo "Implication for upgrades:"
if [ "$FLAG_ISSUE_REPRODUCED" = true ]; then
    echo "   v0.3.x → v0.5.0 upgrades should be safe (different file formats)"
    echo "   v0.5.0 → v0.5.0 restarts are the problem"
else
    echo "   Further investigation needed to identify trigger conditions"
fi
echo "=========================================="
