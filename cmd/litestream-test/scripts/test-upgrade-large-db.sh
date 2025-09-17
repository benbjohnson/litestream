#!/bin/bash
set -e

# Test Litestream v0.3.x to v0.5.0 upgrade with large database (>1GB)
# Specifically testing for #754 flag issue in upgrade scenario

echo "=========================================="
echo "Large Database Upgrade Test (v0.3.x → v0.5.0)"
echo "=========================================="
echo ""
echo "Testing #754 flag issue with large database upgrade"
echo ""

# Configuration
DB="/tmp/large-upgrade-test.db"
REPLICA="/tmp/large-upgrade-replica"
LITESTREAM_V3="/opt/homebrew/bin/litestream"
LITESTREAM_V5="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# Cleanup function
cleanup() {
    pkill -f "litestream replicate.*large-upgrade-test.db" 2>/dev/null || true
    rm -f "$DB" "$DB-wal" "$DB-shm" "$DB-litestream"
    rm -rf "$REPLICA"
    rm -f /tmp/large-upgrade-*.log
}

trap cleanup EXIT

echo "[SETUP] Cleaning up previous test files..."
cleanup

echo ""
echo "[1] Creating large database with v0.3.13..."
echo "  This will take several minutes to reach >1GB..."

# Create database that will cross 1GB boundary
sqlite3 "$DB" <<EOF
PRAGMA page_size = 4096;
PRAGMA journal_mode = WAL;
CREATE TABLE large_test (
    id INTEGER PRIMARY KEY,
    phase TEXT,
    data BLOB,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
EOF

# Use our test harness to create large database quickly
$LITESTREAM_TEST populate -db "$DB" -target-size 1200MB >/dev/null 2>&1

# Add our test table after populate
sqlite3 "$DB" <<EOF
CREATE TABLE large_test (
    id INTEGER PRIMARY KEY,
    phase TEXT,
    data BLOB,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO large_test (phase, data) VALUES ('v0.3.x-large', randomblob(1000));
EOF

DB_SIZE=$(du -h "$DB" | cut -f1)
PAGE_COUNT=$(sqlite3 "$DB" "PRAGMA page_count;")
LOCK_PAGE=$((0x40000000 / 4096 + 1))

echo "  ✓ Large database created:"
echo "    Size: $DB_SIZE"
echo "    Pages: $PAGE_COUNT"
echo "    Lock page: $LOCK_PAGE"

if [ $PAGE_COUNT -gt $LOCK_PAGE ]; then
    echo "    ✓ Database crosses 1GB lock page boundary"
else
    echo "    ⚠️  Database may not cross lock page boundary"
fi

INITIAL_LARGE_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM large_test;")
echo "  ✓ Added identifiable row, total: $INITIAL_LARGE_COUNT"

echo ""
echo "[2] Starting v0.3.13 replication with large database..."
$LITESTREAM_V3 replicate "$DB" "file://$REPLICA" > /tmp/large-upgrade-v3.log 2>&1 &
V3_PID=$!
sleep 5

if ! kill -0 $V3_PID 2>/dev/null; then
    echo "  ✗ Litestream v0.3.13 failed to start with large database"
    cat /tmp/large-upgrade-v3.log
    exit 1
fi
echo "  ✓ v0.3.13 replicating large database (PID: $V3_PID)"

echo ""
echo "[3] Letting v0.3.13 complete initial replication..."
echo "  This may take several minutes for a large database..."
sleep 30

# Check if replication is working
V3_ERRORS=$(grep -c "ERROR" /tmp/large-upgrade-v3.log 2>/dev/null || echo "0")
if [ "$V3_ERRORS" -gt "0" ]; then
    echo "  ⚠️  v0.3.13 errors detected:"
    tail -5 /tmp/large-upgrade-v3.log | grep ERROR || true
fi

# Add some more data
sqlite3 "$DB" "INSERT INTO large_test (phase, data) VALUES ('v0.3.x-post-replication', randomblob(2000));"
REPLICATION_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM large_test;")
echo "  ✓ v0.3.13 replication phase complete, total rows: $REPLICATION_COUNT"

echo ""
echo "[4] Stopping v0.3.13 and upgrading to v0.5.0..."
kill $V3_PID 2>/dev/null || true
wait $V3_PID 2>/dev/null
echo "  ✓ v0.3.13 stopped"

# Add data during transition
sqlite3 "$DB" "INSERT INTO large_test (phase, data) VALUES ('upgrade-transition', randomblob(1500));"
TRANSITION_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM large_test;")
echo "  ✓ Added transition data, total: $TRANSITION_COUNT"

echo ""
echo "[5] Starting v0.5.0 with large database..."
$LITESTREAM_V5 replicate "$DB" "file://$REPLICA" > /tmp/large-upgrade-v5.log 2>&1 &
V5_PID=$!
sleep 5

if ! kill -0 $V5_PID 2>/dev/null; then
    echo "  ✗ Litestream v0.5.0 failed to start"
    cat /tmp/large-upgrade-v5.log
    exit 1
fi
echo "  ✓ v0.5.0 started with large database (PID: $V5_PID)"

echo ""
echo "[6] Critical #754 flag error check..."
sleep 5

FLAG_ERRORS=$(grep -c "no flags allowed" /tmp/large-upgrade-v5.log 2>/dev/null || echo "0")
VERIFICATION_ERRORS=$(grep -c "ltx verification failed" /tmp/large-upgrade-v5.log 2>/dev/null || echo "0")
SYNC_ERRORS=$(grep -c "sync error" /tmp/large-upgrade-v5.log 2>/dev/null || echo "0")

echo "  #754 Error Analysis:"
echo "    'no flags allowed' errors: $FLAG_ERRORS"
echo "    'ltx verification failed' errors: $VERIFICATION_ERRORS"
echo "    'sync error' count: $SYNC_ERRORS"

if [ "$FLAG_ERRORS" -gt "0" ] || [ "$VERIFICATION_ERRORS" -gt "0" ]; then
    echo ""
    echo "  🚨 #754 FLAG ISSUE DETECTED IN LARGE DB UPGRADE!"
    echo "  Error details:"
    grep -A2 -B2 "no flags allowed\|ltx verification failed" /tmp/large-upgrade-v5.log || true
    UPGRADE_TRIGGERS_754=true
else
    echo "  ✅ No #754 flag errors in large database upgrade"
    UPGRADE_TRIGGERS_754=false
fi

echo ""
echo "[7] Adding data with v0.5.0..."
sqlite3 "$DB" "INSERT INTO large_test (phase, data) VALUES ('v0.5.0-large', randomblob(3000));"
FINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM large_test;")
echo "  ✓ v0.5.0 data added, final count: $FINAL_COUNT"

echo ""
echo "[8] Stopping v0.5.0..."
kill $V5_PID 2>/dev/null || true
wait $V5_PID 2>/dev/null

# Final analysis
ALL_ERRORS=$(grep -c "ERROR" /tmp/large-upgrade-v5.log 2>/dev/null || echo "0")
echo "  ✓ v0.5.0 stopped, total errors: $ALL_ERRORS"

if [ "$ALL_ERRORS" -gt "0" ]; then
    echo "  Recent v0.5.0 errors:"
    tail -10 /tmp/large-upgrade-v5.log | grep ERROR || true
fi

echo ""
echo "=========================================="
echo "Large Database Upgrade Results"
echo "=========================================="
echo ""
echo "Database size: $DB_SIZE ($PAGE_COUNT pages)"
echo "Lock page boundary: Page $LOCK_PAGE"
echo "Data progression:"
echo "  Initial: $INITIAL_LARGE_COUNT rows"
echo "  Post-replication: $REPLICATION_COUNT rows"
echo "  Post-transition: $TRANSITION_COUNT rows"
echo "  Final: $FINAL_COUNT rows"
echo ""
echo "#754 Issue Analysis:"
if [ "$UPGRADE_TRIGGERS_754" = true ]; then
    echo "  🚨 CRITICAL: #754 flag errors occur in large DB upgrades"
    echo "  This means existing large production databases cannot upgrade to v0.5.0"
else
    echo "  ✅ #754 flag errors do NOT occur in large DB upgrades"
    echo "  Large database upgrades appear safe from this issue"
fi
echo ""
echo "Conclusion:"
if [ "$UPGRADE_TRIGGERS_754" = true ]; then
    echo "❌ Large database upgrade FAILS due to #754"
    echo "   Production impact: Existing large databases cannot upgrade"
else
    echo "✅ Large database upgrade SUCCEEDS"
    echo "   #754 issue is NOT related to v0.3.x → v0.5.0 upgrades"
fi
echo "=========================================="
