#!/bin/bash
set -e

# Simple, direct test to reproduce #754 flag issue
# Focus on the core HeaderFlagNoChecksum problem

echo "Simple #754 Reproduction Test"
echo "=============================="
echo ""

DB="/tmp/simple754.db"
REPLICA="/tmp/simple754-replica"
LITESTREAM="./bin/litestream"

# Clean up
rm -rf "$DB"* "$REPLICA" /tmp/simple754-*.log

echo "1. Creating test database..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT);
INSERT INTO test (data) VALUES ('test data for 754 reproduction');
INSERT INTO test (data) VALUES ('more data to ensure WAL activity');
INSERT INTO test (data) VALUES ('third row to cross page boundary');
EOF

echo "   Database size: $(du -h "$DB" | cut -f1)"
echo "   WAL exists: $([ -f "$DB-wal" ] && echo "YES" || echo "NO")"

echo ""
echo "2. Starting first v0.5.0 run..."
$LITESTREAM replicate "$DB" "file://$REPLICA" > /tmp/simple754-run1.log 2>&1 &
PID1=$!

echo "   Litestream PID: $PID1"
echo "   Waiting for initial replication..."
sleep 10

# Check if it's still running
if kill -0 $PID1 2>/dev/null; then
    echo "   ✓ Litestream running"
else
    echo "   ✗ Litestream died, checking logs..."
    cat /tmp/simple754-run1.log
    exit 1
fi

# Add more data
echo ""
echo "3. Adding more data during first run..."
for i in {4..8}; do
    sqlite3 "$DB" "INSERT INTO test (data) VALUES ('Row $i added during run 1');"
done

# Force checkpoint to ensure LTX files are created
sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);"
sleep 5

echo "   Current row count: $(sqlite3 "$DB" "SELECT COUNT(*) FROM test;")"

echo ""
echo "4. Checking for LTX files..."
if [ -d "$REPLICA" ]; then
    find "$REPLICA" -name "*.ltx" | head -5
    LTX_COUNT=$(find "$REPLICA" -name "*.ltx" | wc -l)
    echo "   LTX files found: $LTX_COUNT"

    if [ "$LTX_COUNT" -eq "0" ]; then
        echo "   ⚠️  No LTX files created yet, waiting longer..."
        sleep 10
        LTX_COUNT=$(find "$REPLICA" -name "*.ltx" | wc -l)
        echo "   LTX files after wait: $LTX_COUNT"
    fi
else
    echo "   ✗ No replica directory found!"
    echo "   Litestream logs:"
    cat /tmp/simple754-run1.log
    exit 1
fi

echo ""
echo "5. Checking first run for errors..."
RUN1_ERRORS=$(grep -c "ERROR" /tmp/simple754-run1.log 2>/dev/null || echo "0")
RUN1_FLAGS=$(grep -c "no flags" /tmp/simple754-run1.log 2>/dev/null || echo "0")

echo "   Run 1 errors: $RUN1_ERRORS"
echo "   Run 1 flag errors: $RUN1_FLAGS"

if [ "$RUN1_ERRORS" -gt "0" ]; then
    echo "   Recent errors:"
    grep "ERROR" /tmp/simple754-run1.log | tail -3
fi

echo ""
echo "6. Stopping first run..."
kill $PID1 2>/dev/null
wait $PID1 2>/dev/null
echo "   ✓ First run stopped"

echo ""
echo "7. Adding offline data..."
sqlite3 "$DB" "INSERT INTO test (data) VALUES ('Offline data between runs');"
OFFLINE_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM test;")
echo "   Rows after offline addition: $OFFLINE_COUNT"

echo ""
echo "8. CRITICAL: Starting second run (potential #754 trigger)..."
echo "   This should trigger #754 if HeaderFlagNoChecksum is incompatible"

$LITESTREAM replicate "$DB" "file://$REPLICA" > /tmp/simple754-run2.log 2>&1 &
PID2=$!

echo "   Second run PID: $PID2"
sleep 5

if kill -0 $PID2 2>/dev/null; then
    echo "   ✓ Second run started"
else
    echo "   ✗ Second run failed immediately"
    cat /tmp/simple754-run2.log
    exit 1
fi

echo ""
echo "9. Monitoring for #754 errors..."
sleep 15

RUN2_FLAGS=$(grep -c "no flags allowed" /tmp/simple754-run2.log 2>/dev/null || echo "0")
RUN2_VERIFICATION=$(grep -c "ltx verification failed" /tmp/simple754-run2.log 2>/dev/null || echo "0")
RUN2_ERRORS=$(grep -c "ERROR" /tmp/simple754-run2.log 2>/dev/null || echo "0")

echo "   Second run analysis:"
echo "     Total errors: $RUN2_ERRORS"
echo "     'no flags allowed': $RUN2_FLAGS"
echo "     'ltx verification failed': $RUN2_VERIFICATION"

if [ "$RUN2_FLAGS" -gt "0" ] || [ "$RUN2_VERIFICATION" -gt "0" ]; then
    echo ""
    echo "   🚨 #754 REPRODUCED!"
    echo "   Error details:"
    grep -A1 -B1 "no flags\|ltx verification" /tmp/simple754-run2.log
    ISSUE_REPRODUCED=true
else
    echo "   ✅ No #754 errors detected"
    ISSUE_REPRODUCED=false
fi

if [ "$RUN2_ERRORS" -gt "0" ]; then
    echo ""
    echo "   All errors from second run:"
    grep "ERROR" /tmp/simple754-run2.log
fi

echo ""
echo "10. Adding final data and cleanup..."
if kill -0 $PID2 2>/dev/null; then
    sqlite3 "$DB" "INSERT INTO test (data) VALUES ('Final data from run 2');"
    FINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM test;")
    echo "    Final row count: $FINAL_COUNT"

    kill $PID2 2>/dev/null
    wait $PID2 2>/dev/null
fi

echo ""
echo "RESULTS:"
echo "========"
echo "File structure created:"
find "$REPLICA" -type f | head -10

echo ""
echo "Error summary:"
echo "  Run 1: $RUN1_ERRORS errors, $RUN1_FLAGS flag errors"
echo "  Run 2: $RUN2_ERRORS errors, $RUN2_FLAGS flag errors"

echo ""
if [ "$ISSUE_REPRODUCED" = "true" ]; then
    echo "✅ SUCCESS: #754 flag issue reproduced!"
    echo "   Trigger: v0.5.0 restart against existing LTX files"
    echo "   Root cause: HeaderFlagNoChecksum incompatible with ltx v0.5.0"
else
    echo "❌ #754 issue not reproduced in this test"
    echo "   May need different database size, content, or timing"
fi

echo ""
echo "Next steps:"
echo "- Examine HeaderFlagNoChecksum usage in db.go"
echo "- Test with different database configurations"
echo "- Verify ltx library version and flag handling"

# Cleanup
rm -rf "$DB"* "$REPLICA" /tmp/simple754-*.log
