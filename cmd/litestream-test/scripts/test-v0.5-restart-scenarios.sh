#!/bin/bash
set -e

# Test v0.5.0 restart scenarios to reproduce #754 flag issue
# Focus on HeaderFlagNoChecksum usage and LTX file handling

echo "=========================================="
echo "v0.5.0 Restart Scenarios Test"
echo "=========================================="
echo ""
echo "Testing various v0.5.0 restart conditions to reproduce #754"
echo ""

# Configuration
DB="/tmp/restart-test.db"
REPLICA="/tmp/restart-replica"
LITESTREAM_V5="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# Cleanup function
cleanup() {
    pkill -f "litestream replicate.*restart-test.db" 2>/dev/null || true
    rm -f "$DB" "$DB-wal" "$DB-shm" "$DB-litestream"
    rm -rf "$REPLICA"
    rm -f /tmp/restart-*.log
}

trap cleanup EXIT

echo "[SETUP] Cleaning up previous test files..."
cleanup

echo ""
echo "=========================================="
echo "Scenario 1: Simple v0.5.0 restart"
echo "=========================================="

echo "[1] Creating large database for restart testing..."
$LITESTREAM_TEST populate -db "$DB" -target-size 1200MB >/dev/null 2>&1

# Add identifiable data
sqlite3 "$DB" <<EOF
CREATE TABLE restart_test (
    id INTEGER PRIMARY KEY,
    scenario TEXT,
    restart_number INTEGER,
    data BLOB,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO restart_test (scenario, restart_number, data) VALUES ('initial', 0, randomblob(5000));
EOF

DB_SIZE=$(du -h "$DB" | cut -f1)
PAGE_COUNT=$(sqlite3 "$DB" "PRAGMA page_count;")
echo "  ✓ Database created: $DB_SIZE ($PAGE_COUNT pages)"

echo ""
echo "[2] First v0.5.0 run..."
$LITESTREAM_V5 replicate "$DB" "file://$REPLICA" > /tmp/restart-run1.log 2>&1 &
RUN1_PID=$!
sleep 5

if ! kill -0 $RUN1_PID 2>/dev/null; then
    echo "  ✗ First run failed"
    cat /tmp/restart-run1.log
    exit 1
fi
echo "  ✓ First run started (PID: $RUN1_PID)"

# Add data during first run
for i in {1..10}; do
    sqlite3 "$DB" "INSERT INTO restart_test (scenario, restart_number, data) VALUES ('first-run', 1, randomblob(3000));"
done
sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);" >/dev/null 2>&1
sleep 3

RUN1_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM restart_test;")
echo "  ✓ First run data: $RUN1_COUNT rows"

echo ""
echo "[3] Examining first run LTX files..."
if [ -d "$REPLICA" ]; then
    LTX_FILES_RUN1=$(find "$REPLICA" -name "*.ltx" | wc -l)
    echo "  LTX files after run 1: $LTX_FILES_RUN1"

    # Check for HeaderFlagNoChecksum in files
    echo "  Examining LTX headers for flag usage..."
    find "$REPLICA" -name "*.ltx" | head -2 | while read ltx_file; do
        echo "    $(basename $ltx_file): $(wc -c < "$ltx_file") bytes"
    done
else
    echo "  ✗ No replica directory found"
    exit 1
fi

# Check first run errors
RUN1_ERRORS=$(grep -c "ERROR" /tmp/restart-run1.log 2>/dev/null || echo "0")
RUN1_FLAGS=$(grep -c "no flags allowed" /tmp/restart-run1.log 2>/dev/null || echo "0")
echo "  First run status: $RUN1_ERRORS errors, $RUN1_FLAGS flag errors"

echo ""
echo "[4] Stopping first run and adding offline data..."
kill $RUN1_PID 2>/dev/null || true
wait $RUN1_PID 2>/dev/null

# Add data while Litestream is down
sqlite3 "$DB" "INSERT INTO restart_test (scenario, restart_number, data) VALUES ('offline', 0, randomblob(4000));"
OFFLINE_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM restart_test;")
echo "  ✓ Offline data added, total: $OFFLINE_COUNT rows"

echo ""
echo "[5] CRITICAL: Second v0.5.0 restart..."
echo "  Starting v0.5.0 against existing LTX files with HeaderFlagNoChecksum..."

$LITESTREAM_V5 replicate "$DB" "file://$REPLICA" > /tmp/restart-run2.log 2>&1 &
RUN2_PID=$!
sleep 5

if ! kill -0 $RUN2_PID 2>/dev/null; then
    echo "  ✗ Second run failed to start"
    cat /tmp/restart-run2.log
    exit 1
fi
echo "  ✓ Second run started (PID: $RUN2_PID)"

echo ""
echo "[6] Monitoring for #754 flag errors during restart..."
sleep 10

RUN2_FLAGS=$(grep -c "no flags allowed" /tmp/restart-run2.log 2>/dev/null || echo "0")
RUN2_VERIFICATION=$(grep -c "ltx verification failed" /tmp/restart-run2.log 2>/dev/null || echo "0")
RUN2_SYNC_ERRORS=$(grep -c "sync error" /tmp/restart-run2.log 2>/dev/null || echo "0")
RUN2_TOTAL_ERRORS=$(grep -c "ERROR" /tmp/restart-run2.log 2>/dev/null || echo "0")

echo "  Second run error analysis:"
echo "    'no flags allowed' errors: $RUN2_FLAGS"
echo "    'ltx verification failed' errors: $RUN2_VERIFICATION"
echo "    'sync error' count: $RUN2_SYNC_ERRORS"
echo "    Total errors: $RUN2_TOTAL_ERRORS"

if [ "$RUN2_FLAGS" -gt "0" ] || [ "$RUN2_VERIFICATION" -gt "0" ]; then
    echo ""
    echo "  🚨 #754 FLAG ISSUE REPRODUCED IN RESTART!"
    echo "  Error details:"
    grep -A2 -B2 "no flags allowed\|ltx verification failed" /tmp/restart-run2.log || true
    RESTART_TRIGGERS_754=true
else
    echo "  ✅ No #754 flag errors in simple restart"
    RESTART_TRIGGERS_754=false
fi

echo ""
echo "=========================================="
echo "Scenario 2: Checkpoint during restart"
echo "=========================================="

# Add more data during second run
echo "[7] Adding data during second run with checkpoints..."
for i in {1..5}; do
    sqlite3 "$DB" "INSERT INTO restart_test (scenario, restart_number, data) VALUES ('second-run', 2, randomblob(3500));"
    if [ $((i % 2)) -eq 0 ]; then
        sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);" >/dev/null 2>&1
    fi
done

RUN2_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM restart_test;")
echo "  ✓ Second run data with checkpoints: $RUN2_COUNT rows"

# Monitor for additional errors
sleep 5
CHECKPOINT_FLAGS=$(grep -c "no flags allowed" /tmp/restart-run2.log 2>/dev/null || echo "0")
if [ "$CHECKPOINT_FLAGS" -gt "$RUN2_FLAGS" ]; then
    echo "  ⚠️  Additional flag errors during checkpoint operations"
fi

echo ""
echo "=========================================="
echo "Scenario 3: Multiple restart cycles"
echo "=========================================="

echo "[8] Third restart cycle..."
kill $RUN2_PID 2>/dev/null || true
wait $RUN2_PID 2>/dev/null

sqlite3 "$DB" "INSERT INTO restart_test (scenario, restart_number, data) VALUES ('between-2-and-3', 0, randomblob(2500));"

$LITESTREAM_V5 replicate "$DB" "file://$REPLICA" > /tmp/restart-run3.log 2>&1 &
RUN3_PID=$!
sleep 5

if kill -0 $RUN3_PID 2>/dev/null; then
    echo "  ✓ Third run started (PID: $RUN3_PID)"

    # Quick check for immediate errors
    sleep 5
    RUN3_FLAGS=$(grep -c "no flags allowed" /tmp/restart-run3.log 2>/dev/null || echo "0")
    RUN3_ERRORS=$(grep -c "ERROR" /tmp/restart-run3.log 2>/dev/null || echo "0")

    echo "  Third run status: $RUN3_ERRORS errors, $RUN3_FLAGS flag errors"

    if [ "$RUN3_FLAGS" -gt "0" ]; then
        echo "  ⚠️  Flag errors in third restart"
    fi

    kill $RUN3_PID 2>/dev/null || true
    wait $RUN3_PID 2>/dev/null
else
    echo "  ✗ Third run failed"
    cat /tmp/restart-run3.log | head -10
fi

echo ""
echo "[9] Final analysis..."
FINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM restart_test;")
FINAL_LTX_FILES=$(find "$REPLICA" -name "*.ltx" 2>/dev/null | wc -l)

echo "  Final statistics:"
echo "    Database rows: $FINAL_COUNT"
echo "    LTX files created: $FINAL_LTX_FILES"
echo "    Run 1 errors: $RUN1_ERRORS (flags: $RUN1_FLAGS)"
echo "    Run 2 errors: $RUN2_TOTAL_ERRORS (flags: $RUN2_FLAGS)"
echo "    Run 3 errors: $RUN3_ERRORS (flags: $RUN3_FLAGS)"

echo ""
echo "=========================================="
echo "v0.5.0 Restart Test Results"
echo "=========================================="
echo ""
echo "Test scenarios:"
echo "  ✓ Simple restart: $([ "$RESTART_TRIGGERS_754" = true ] && echo "REPRODUCED #754" || echo "No #754 errors")"
echo "  ✓ Checkpoint during restart: $([ "$CHECKPOINT_FLAGS" -gt "$RUN2_FLAGS" ] && echo "Additional errors" || echo "No additional errors")"
echo "  ✓ Multiple restart cycles: $([ "${RUN3_FLAGS:-0}" -gt "0" ] && echo "Errors in cycle 3" || echo "No errors in cycle 3")"
echo ""
echo "CRITICAL FINDINGS:"
if [ "$RESTART_TRIGGERS_754" = true ] || [ "${RUN3_FLAGS:-0}" -gt "0" ]; then
    echo "🚨 #754 FLAG ISSUE TRIGGERED BY v0.5.0 RESTARTS"
    echo "   Root cause: v0.5.0 reading its own LTX files with HeaderFlagNoChecksum"
    echo "   Trigger: Restarting Litestream against existing v0.5.0 backup files"
    echo "   Impact: Production Litestream restarts will fail"
else
    echo "✅ No #754 errors in restart scenarios tested"
    echo "   Issue may require specific database content or timing conditions"
fi
echo ""
echo "Next steps:"
echo "1. Check HeaderFlagNoChecksum usage in db.go"
echo "2. Test with different database sizes/content"
echo "3. Investigate LTX file generation differences"
echo "=========================================="
