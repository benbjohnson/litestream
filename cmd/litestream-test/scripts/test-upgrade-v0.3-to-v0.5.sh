#!/bin/bash
set -e

# Test Litestream v0.3.x to v0.5.0 upgrade scenarios
# Based on conversation with Ben Johnson about upgrade behavior expectations

echo "=========================================="
echo "Litestream v0.3.x → v0.5.0 Upgrade Test"
echo "=========================================="
echo ""
echo "Testing upgrade from Litestream v0.3.13 to v0.5.0"
echo ""

# Configuration
DB="/tmp/upgrade-test.db"
REPLICA="/tmp/upgrade-replica"
RESTORED_V3="/tmp/upgrade-restored-v3.db"
RESTORED_V5="/tmp/upgrade-restored-v5.db"
LITESTREAM_V3="/opt/homebrew/bin/litestream"
LITESTREAM_V5="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# Cleanup function
cleanup() {
    pkill -f "litestream replicate.*upgrade-test.db" 2>/dev/null || true
    rm -f "$DB" "$DB-wal" "$DB-shm" "$DB-litestream"
    rm -f "$RESTORED_V3" "$RESTORED_V3-wal" "$RESTORED_V3-shm"
    rm -f "$RESTORED_V5" "$RESTORED_V5-wal" "$RESTORED_V5-shm"
    rm -rf "$REPLICA"
    rm -f /tmp/upgrade-*.log
}

trap cleanup EXIT

echo "[SETUP] Cleaning up previous test files..."
cleanup

# Verify versions
echo ""
echo "[VERSIONS] Verifying Litestream versions..."
V3_VERSION=$($LITESTREAM_V3 version 2>/dev/null || echo "NOT_FOUND")
V5_VERSION=$($LITESTREAM_V5 version 2>/dev/null || echo "NOT_FOUND")

echo "  v0.3.x (system): $V3_VERSION"
echo "  v0.5.0 (built):  $V5_VERSION"

if [ "$V3_VERSION" = "NOT_FOUND" ]; then
    echo "  ✗ System Litestream v0.3.x not found at $LITESTREAM_V3"
    exit 1
fi

if [ "$V5_VERSION" = "NOT_FOUND" ]; then
    echo "  ✗ Built Litestream v0.5.0 not found at $LITESTREAM_V5"
    exit 1
fi

echo ""
echo "=========================================="
echo "Phase 1: Create backups with v0.3.13"
echo "=========================================="

echo "[1] Creating test database..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
CREATE TABLE upgrade_test (
    id INTEGER PRIMARY KEY,
    phase TEXT,
    data BLOB,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO upgrade_test (phase, data) VALUES ('v0.3.x-initial', randomblob(1000));
INSERT INTO upgrade_test (phase, data) VALUES ('v0.3.x-initial', randomblob(2000));
EOF

INITIAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM upgrade_test;")
echo "  ✓ Database created with $INITIAL_COUNT rows"

echo ""
echo "[2] Starting Litestream v0.3.13 replication..."
$LITESTREAM_V3 replicate "$DB" "file://$REPLICA" > /tmp/upgrade-v3.log 2>&1 &
V3_PID=$!
sleep 3

if ! kill -0 $V3_PID 2>/dev/null; then
    echo "  ✗ Litestream v0.3.13 failed to start"
    cat /tmp/upgrade-v3.log
    exit 1
fi
echo "  ✓ Litestream v0.3.13 running (PID: $V3_PID)"

echo ""
echo "[3] Adding data while v0.3.13 is replicating..."
for i in {1..5}; do
    sqlite3 "$DB" "INSERT INTO upgrade_test (phase, data) VALUES ('v0.3.x-replicating', randomblob(1500));"
done
sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);" >/dev/null 2>&1
sleep 2

V3_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM upgrade_test;")
echo "  ✓ Added data, total rows: $V3_COUNT"

echo ""
echo "[4] Examining v0.3.x backup structure..."
if [ -d "$REPLICA" ]; then
    echo "  Replica directory contents:"
    find "$REPLICA" -type f | head -10 | while read file; do
        echo "    $(basename $(dirname $file))/$(basename $file)"
    done
    V3_FILES=$(find "$REPLICA" -type f | wc -l)
    echo "  ✓ v0.3.x created $V3_FILES backup files"
else
    echo "  ✗ No replica directory created"
    exit 1
fi

echo ""
echo "[5] Testing v0.3.x restore capability..."
$LITESTREAM_V3 restore -o "$RESTORED_V3" "file://$REPLICA" > /tmp/upgrade-restore-v3.log 2>&1
if [ $? -eq 0 ]; then
    RESTORED_V3_COUNT=$(sqlite3 "$RESTORED_V3" "SELECT COUNT(*) FROM upgrade_test;" 2>/dev/null || echo "0")
    echo "  ✓ v0.3.x restore successful: $RESTORED_V3_COUNT rows"
    rm -f "$RESTORED_V3" "$RESTORED_V3-wal" "$RESTORED_V3-shm"
else
    echo "  ✗ v0.3.x restore failed"
    cat /tmp/upgrade-restore-v3.log
fi

echo ""
echo "=========================================="
echo "Phase 2: Upgrade to v0.5.0"
echo "=========================================="

echo "[6] Stopping Litestream v0.3.13..."
kill $V3_PID 2>/dev/null || true
wait $V3_PID 2>/dev/null
echo "  ✓ v0.3.13 stopped"

echo ""
echo "[7] Adding data while Litestream is offline..."
for i in {1..3}; do
    sqlite3 "$DB" "INSERT INTO upgrade_test (phase, data) VALUES ('offline-transition', randomblob(1200));"
done
OFFLINE_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM upgrade_test;")
echo "  ✓ Added data during transition, total rows: $OFFLINE_COUNT"

echo ""
echo "[8] Starting Litestream v0.5.0..."
$LITESTREAM_V5 replicate "$DB" "file://$REPLICA" > /tmp/upgrade-v5.log 2>&1 &
V5_PID=$!
sleep 3

if ! kill -0 $V5_PID 2>/dev/null; then
    echo "  ✗ Litestream v0.5.0 failed to start"
    cat /tmp/upgrade-v5.log
    exit 1
fi
echo "  ✓ Litestream v0.5.0 running (PID: $V5_PID)"

echo ""
echo "[9] Checking for #754 flag errors in upgrade scenario..."
sleep 2
FLAG_ERRORS=$(grep -c "no flags allowed" /tmp/upgrade-v5.log 2>/dev/null || echo "0")
VERIFICATION_ERRORS=$(grep -c "ltx verification failed" /tmp/upgrade-v5.log 2>/dev/null || echo "0")

echo "  Flag errors: $FLAG_ERRORS"
echo "  Verification errors: $VERIFICATION_ERRORS"

if [ "$FLAG_ERRORS" -gt "0" ] || [ "$VERIFICATION_ERRORS" -gt "0" ]; then
    echo "  ⚠️  #754 flag issue detected in upgrade scenario!"
    grep "no flags allowed\|ltx verification failed" /tmp/upgrade-v5.log || true
else
    echo "  ✓ No #754 flag errors in upgrade scenario"
fi

echo ""
echo "[10] Adding data with v0.5.0..."
for i in {1..5}; do
    sqlite3 "$DB" "INSERT INTO upgrade_test (phase, data) VALUES ('v0.5.0-running', randomblob(1800));"
done
sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);" >/dev/null 2>&1
sleep 3

V5_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM upgrade_test;")
echo "  ✓ Added data with v0.5.0, total rows: $V5_COUNT"

echo ""
echo "[11] Examining backup structure after upgrade..."
echo "  Post-upgrade replica contents:"
find "$REPLICA" -type f -newer /tmp/upgrade-v3.log 2>/dev/null | head -5 | while read file; do
    echo "    NEW: $(basename $(dirname $file))/$(basename $file)"
done

V5_NEW_FILES=$(find "$REPLICA" -type f -newer /tmp/upgrade-v3.log 2>/dev/null | wc -l)
echo "  ✓ v0.5.0 created $V5_NEW_FILES new backup files"

echo ""
echo "=========================================="
echo "Phase 3: Restore compatibility testing"
echo "=========================================="

echo "[12] Testing v0.5.0 restore from mixed backup files..."
$LITESTREAM_V5 restore -o "$RESTORED_V5" "file://$REPLICA" > /tmp/upgrade-restore-v5.log 2>&1
RESTORE_EXIT=$?

if [ $RESTORE_EXIT -eq 0 ]; then
    RESTORED_V5_COUNT=$(sqlite3 "$RESTORED_V5" "SELECT COUNT(*) FROM upgrade_test;" 2>/dev/null || echo "0")
    echo "  ✓ v0.5.0 restore completed: $RESTORED_V5_COUNT rows"

    # Check which phases are present
    V3_INITIAL=$(sqlite3 "$RESTORED_V5" "SELECT COUNT(*) FROM upgrade_test WHERE phase='v0.3.x-initial';" 2>/dev/null || echo "0")
    V3_REPLICATING=$(sqlite3 "$RESTORED_V5" "SELECT COUNT(*) FROM upgrade_test WHERE phase='v0.3.x-replicating';" 2>/dev/null || echo "0")
    OFFLINE=$(sqlite3 "$RESTORED_V5" "SELECT COUNT(*) FROM upgrade_test WHERE phase='offline-transition';" 2>/dev/null || echo "0")
    V5_RUNNING=$(sqlite3 "$RESTORED_V5" "SELECT COUNT(*) FROM upgrade_test WHERE phase='v0.5.0-running';" 2>/dev/null || echo "0")

    echo "  Data breakdown:"
    echo "    v0.3.x initial: $V3_INITIAL rows"
    echo "    v0.3.x replicating: $V3_REPLICATING rows"
    echo "    Offline transition: $OFFLINE rows"
    echo "    v0.5.0 running: $V5_RUNNING rows"

    if [ "$V3_INITIAL" -eq "0" ] && [ "$V3_REPLICATING" -eq "0" ]; then
        echo "  ✓ EXPECTED: v0.5.0 ignored v0.3.x backup files"
    else
        echo "  ⚠️  UNEXPECTED: v0.5.0 restored some v0.3.x data"
    fi

    if [ "$V5_RUNNING" -gt "0" ]; then
        echo "  ✓ v0.5.0 data present in restore"
    else
        echo "  ✗ v0.5.0 data missing from restore"
    fi

else
    echo "  ✗ v0.5.0 restore failed"
    cat /tmp/upgrade-restore-v5.log
fi

echo ""
echo "[13] Stopping v0.5.0 and final analysis..."
kill $V5_PID 2>/dev/null || true
wait $V5_PID 2>/dev/null

# Final error analysis
V5_ERRORS=$(grep -c "ERROR" /tmp/upgrade-v5.log 2>/dev/null || echo "0")
V5_WARNINGS=$(grep -c "WARN" /tmp/upgrade-v5.log 2>/dev/null || echo "0")

echo "  v0.5.0 runtime analysis:"
echo "    Errors: $V5_ERRORS"
echo "    Warnings: $V5_WARNINGS"
echo "    Flag issues: $FLAG_ERRORS"

if [ "$V5_ERRORS" -gt "0" ]; then
    echo "  Recent errors:"
    tail -10 /tmp/upgrade-v5.log | grep ERROR || true
fi

echo ""
echo "=========================================="
echo "Upgrade Test Summary"
echo "=========================================="
echo ""
echo "Database progression:"
echo "  v0.3.x initial: $INITIAL_COUNT rows"
echo "  v0.3.x final: $V3_COUNT rows"
echo "  Offline: $OFFLINE_COUNT rows"
echo "  v0.5.0 final: $V5_COUNT rows"
echo ""
echo "Backup behavior:"
echo "  v0.3.x files: $V3_FILES"
echo "  v0.5.0 new files: $V5_NEW_FILES"
echo ""
echo "Restore behavior:"
echo "  v0.3.x → v0.3.x: ✓ Successful"
if [ $RESTORE_EXIT -eq 0 ]; then
    echo "  Mixed → v0.5.0: ✓ Successful ($RESTORED_V5_COUNT rows)"
    if [ "$V3_INITIAL" -eq "0" ] && [ "$V3_REPLICATING" -eq "0" ]; then
        echo "  v0.3.x compatibility: ✓ Ignored as expected"
    else
        echo "  v0.3.x compatibility: ⚠️  Unexpected behavior"
    fi
else
    echo "  Mixed → v0.5.0: ✗ Failed"
fi
echo ""
echo "Issue #754 status:"
if [ "$FLAG_ERRORS" -gt "0" ] || [ "$VERIFICATION_ERRORS" -gt "0" ]; then
    echo "  ⚠️  #754 flag errors detected in upgrade scenario"
else
    echo "  ✓ No #754 flag errors in upgrade scenario"
fi
echo ""
echo "Conclusion:"
if [ "$FLAG_ERRORS" -eq "0" ] && [ "$VERIFICATION_ERRORS" -eq "0" ] && [ $RESTORE_EXIT -eq 0 ]; then
    echo "✅ Upgrade test PASSED: v0.3.x → v0.5.0 works as expected"
else
    echo "⚠️  Upgrade test ISSUES: Some unexpected behavior detected"
fi
echo "=========================================="
