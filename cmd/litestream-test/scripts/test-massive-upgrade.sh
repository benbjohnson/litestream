#!/bin/bash
set -e

# Massive database upgrade test - extreme stress testing
# Create large DB with lots of snapshots and WAL activity to thoroughly test v0.3.x → v0.5.0

echo "=========================================="
echo "MASSIVE Database Upgrade Stress Test"
echo "=========================================="
echo ""
echo "Creating 3GB+ database with multiple snapshots and heavy WAL activity"
echo "Testing v0.3.x → v0.5.0 upgrade under extreme conditions"
echo ""

# Configuration
DB="/tmp/massive-upgrade-test.db"
REPLICA="/tmp/massive-upgrade-replica"
RESTORED="/tmp/massive-restored.db"
LITESTREAM_V3="/opt/homebrew/bin/litestream"
LITESTREAM_V5="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# Cleanup function
cleanup() {
    pkill -f "litestream replicate.*massive-upgrade-test.db" 2>/dev/null || true
    rm -f "$DB" "$DB-wal" "$DB-shm" "$DB-litestream"
    rm -f "$RESTORED" "$RESTORED-wal" "$RESTORED-shm"
    rm -rf "$REPLICA"
    rm -f /tmp/massive-*.log
}

trap cleanup EXIT

echo "[SETUP] Cleaning up previous test files..."
cleanup

echo ""
echo "[1] Creating massive database (3GB target)..."
echo "  This will take 10+ minutes to create and replicate..."

# Create initial schema
sqlite3 "$DB" <<EOF
PRAGMA page_size = 4096;
PRAGMA journal_mode = WAL;
CREATE TABLE massive_test (
    id INTEGER PRIMARY KEY,
    phase TEXT,
    batch_id INTEGER,
    data BLOB,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_phase ON massive_test(phase);
CREATE INDEX idx_batch ON massive_test(batch_id);
EOF

# Create 3GB database in stages to force multiple snapshots
echo "  Creating 3GB database in 500MB chunks..."
for chunk in {1..6}; do
    echo "    Chunk $chunk/6 (500MB each)..."
    $LITESTREAM_TEST populate -db "$DB" -target-size 500MB -table-count 1 >/dev/null 2>&1

    # Add identifiable data for this chunk
    sqlite3 "$DB" "INSERT INTO massive_test (phase, batch_id, data) VALUES ('v0.3.x-chunk-$chunk', $chunk, randomblob(5000));"

    # Force checkpoint to create multiple snapshots
    sqlite3 "$DB" "PRAGMA wal_checkpoint(TRUNCATE);" >/dev/null 2>&1

    CURRENT_SIZE=$(du -h "$DB" | cut -f1)
    CURRENT_PAGES=$(sqlite3 "$DB" "PRAGMA page_count;")
    echo "      Current size: $CURRENT_SIZE ($CURRENT_PAGES pages)"
done

FINAL_SIZE=$(du -h "$DB" | cut -f1)
FINAL_PAGES=$(sqlite3 "$DB" "PRAGMA page_count;")
LOCK_PAGE=$((0x40000000 / 4096 + 1))
MASSIVE_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM massive_test;")

echo "  ✓ Massive database created:"
echo "    Size: $FINAL_SIZE"
echo "    Pages: $FINAL_PAGES"
echo "    Lock page boundary: $LOCK_PAGE"
echo "    Custom records: $MASSIVE_COUNT"

if [ $FINAL_PAGES -gt $((LOCK_PAGE * 2)) ]; then
    echo "    ✓ Database is WELL beyond 1GB lock page boundary"
else
    echo "    ⚠️  Database may not be large enough"
fi

echo ""
echo "[2] Starting v0.3.13 with massive database..."
$LITESTREAM_V3 replicate "$DB" "file://$REPLICA" > /tmp/massive-v3.log 2>&1 &
V3_PID=$!
sleep 10

if ! kill -0 $V3_PID 2>/dev/null; then
    echo "  ✗ v0.3.13 failed to start with massive database"
    cat /tmp/massive-v3.log
    exit 1
fi
echo "  ✓ v0.3.13 replicating massive database (PID: $V3_PID)"

echo ""
echo "[3] Heavy WAL activity phase (5 minutes)..."
echo "  Generating continuous writes to create many WAL segments and snapshots..."

START_TIME=$(date +%s)
BATCH=1
while [ $(($(date +%s) - START_TIME)) -lt 300 ]; do  # Run for 5 minutes
    # Insert batch of data
    for i in {1..50}; do
        sqlite3 "$DB" "INSERT INTO massive_test (phase, batch_id, data) VALUES ('v0.3.x-wal-activity', $BATCH, randomblob(2000));" 2>/dev/null || true
    done

    # Periodic checkpoint to force snapshots
    if [ $((BATCH % 20)) -eq 0 ]; then
        sqlite3 "$DB" "PRAGMA wal_checkpoint(PASSIVE);" >/dev/null 2>&1
        CURRENT_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM massive_test;" 2>/dev/null || echo "unknown")
        echo "    Batch $BATCH complete, total records: $CURRENT_COUNT"
    fi

    BATCH=$((BATCH + 1))
    sleep 1
done

WAL_ACTIVITY_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM massive_test;")
echo "  ✓ Heavy WAL activity complete, total records: $WAL_ACTIVITY_COUNT"

echo ""
echo "[4] Examining v0.3.x backup structure..."
if [ -d "$REPLICA" ]; then
    WAL_FILES=$(find "$REPLICA" -name "*.wal.lz4" | wc -l)
    SNAPSHOT_FILES=$(find "$REPLICA" -name "*.snapshot.lz4" | wc -l)
    echo "  v0.3.x backup analysis:"
    echo "    WAL files: $WAL_FILES"
    echo "    Snapshot files: $SNAPSHOT_FILES"
    echo "    Total backup files: $((WAL_FILES + SNAPSHOT_FILES))"

    if [ $WAL_FILES -gt 50 ] && [ $SNAPSHOT_FILES -gt 3 ]; then
        echo "    ✓ Excellent: Many WAL segments and multiple snapshots created"
    else
        echo "    ⚠️  Expected more backup files for thorough testing"
    fi
else
    echo "  ✗ No replica directory found!"
    exit 1
fi

echo ""
echo "[5] Final v0.3.x operations..."
# Add final identifiable data
sqlite3 "$DB" "INSERT INTO massive_test (phase, batch_id, data) VALUES ('v0.3.x-final', 9999, randomblob(10000));"
sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);" >/dev/null 2>&1
sleep 5

V3_FINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM massive_test;")
echo "  ✓ v0.3.x phase complete, final count: $V3_FINAL_COUNT"

# Check for v0.3.x errors
V3_ERRORS=$(grep -c "ERROR" /tmp/massive-v3.log 2>/dev/null || echo "0")
if [ "$V3_ERRORS" -gt "0" ]; then
    echo "  ⚠️  v0.3.x had $V3_ERRORS errors"
    tail -5 /tmp/massive-v3.log | grep ERROR || true
fi

echo ""
echo "=========================================="
echo "UPGRADE TO v0.5.0"
echo "=========================================="

echo "[6] Stopping v0.3.13..."
kill $V3_PID 2>/dev/null || true
wait $V3_PID 2>/dev/null
echo "  ✓ v0.3.13 stopped"

echo ""
echo "[7] Adding offline transition data..."
sqlite3 "$DB" "INSERT INTO massive_test (phase, batch_id, data) VALUES ('offline-transition', 8888, randomblob(7500));"
TRANSITION_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM massive_test;")
echo "  ✓ Offline data added, count: $TRANSITION_COUNT"

echo ""
echo "[8] Starting v0.5.0 with massive database..."
$LITESTREAM_V5 replicate "$DB" "file://$REPLICA" > /tmp/massive-v5.log 2>&1 &
V5_PID=$!
sleep 10

if ! kill -0 $V5_PID 2>/dev/null; then
    echo "  ✗ v0.5.0 failed to start with massive database"
    cat /tmp/massive-v5.log
    exit 1
fi
echo "  ✓ v0.5.0 started with massive database (PID: $V5_PID)"

echo ""
echo "[9] CRITICAL: #754 error analysis with massive database..."
sleep 10

FLAG_ERRORS=$(grep -c "no flags allowed" /tmp/massive-v5.log 2>/dev/null || echo "0")
VERIFICATION_ERRORS=$(grep -c "ltx verification failed" /tmp/massive-v5.log 2>/dev/null || echo "0")
SYNC_ERRORS=$(grep -c "sync error" /tmp/massive-v5.log 2>/dev/null || echo "0")
PAGE_SIZE_ERRORS=$(grep -c "page size not initialized" /tmp/massive-v5.log 2>/dev/null || echo "0")

echo "  #754 Error Analysis (Massive Database):"
echo "    'no flags allowed' errors: $FLAG_ERRORS"
echo "    'ltx verification failed' errors: $VERIFICATION_ERRORS"
echo "    'sync error' count: $SYNC_ERRORS"
echo "    'page size not initialized' errors: $PAGE_SIZE_ERRORS"

if [ "$FLAG_ERRORS" -gt "0" ] || [ "$VERIFICATION_ERRORS" -gt "0" ]; then
    echo ""
    echo "  🚨 #754 FLAG ISSUE DETECTED WITH MASSIVE DATABASE!"
    echo "  This proves the issue CAN occur in upgrade scenarios"
    grep -A3 -B3 "no flags allowed\|ltx verification failed" /tmp/massive-v5.log || true
    MASSIVE_TRIGGERS_754=true
else
    echo "  ✅ No #754 flag errors even with massive database upgrade"
    MASSIVE_TRIGGERS_754=false
fi

echo ""
echo "[10] Adding v0.5.0 data..."
sqlite3 "$DB" "INSERT INTO massive_test (phase, batch_id, data) VALUES ('v0.5.0-massive', 7777, randomblob(8000));"
V5_FINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM massive_test;")
echo "  ✓ v0.5.0 data added, final count: $V5_FINAL_COUNT"

echo ""
echo "[11] Testing restore with massive mixed backup..."
kill $V5_PID 2>/dev/null || true
wait $V5_PID 2>/dev/null

echo "  Attempting restore from massive mixed backup files..."
$LITESTREAM_V5 restore -o "$RESTORED" "file://$REPLICA" > /tmp/massive-restore.log 2>&1
RESTORE_EXIT=$?

if [ $RESTORE_EXIT -eq 0 ]; then
    RESTORED_COUNT=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM massive_test;" 2>/dev/null || echo "0")

    # Analyze what was restored
    V3_CHUNKS=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM massive_test WHERE phase LIKE 'v0.3.x-chunk%';" 2>/dev/null || echo "0")
    V3_WAL=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM massive_test WHERE phase = 'v0.3.x-wal-activity';" 2>/dev/null || echo "0")
    V3_FINAL=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM massive_test WHERE phase = 'v0.3.x-final';" 2>/dev/null || echo "0")
    OFFLINE=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM massive_test WHERE phase = 'offline-transition';" 2>/dev/null || echo "0")
    V5_DATA=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM massive_test WHERE phase = 'v0.5.0-massive';" 2>/dev/null || echo "0")

    echo "  ✓ Massive restore successful: $RESTORED_COUNT total records"
    echo "  Detailed breakdown:"
    echo "    v0.3.x chunks: $V3_CHUNKS records"
    echo "    v0.3.x WAL activity: $V3_WAL records"
    echo "    v0.3.x final: $V3_FINAL records"
    echo "    Offline transition: $OFFLINE records"
    echo "    v0.5.0 data: $V5_DATA records"

    if [ "$V3_CHUNKS" -gt "0" ] && [ "$V3_WAL" -gt "0" ]; then
        echo "  ⚠️  MASSIVE COMPATIBILITY: v0.5.0 restored ALL v0.3.x data!"
    fi

else
    echo "  ✗ Massive restore FAILED"
    cat /tmp/massive-restore.log
fi

echo ""
echo "=========================================="
echo "MASSIVE Upgrade Test Results"
echo "=========================================="
echo ""
echo "Database statistics:"
echo "  Final size: $FINAL_SIZE ($FINAL_PAGES pages)"
echo "  Records progression:"
echo "    Initial (6 chunks): $MASSIVE_COUNT"
echo "    After WAL activity: $WAL_ACTIVITY_COUNT"
echo "    v0.3.x final: $V3_FINAL_COUNT"
echo "    After transition: $TRANSITION_COUNT"
echo "    v0.5.0 final: $V5_FINAL_COUNT"
echo ""
echo "Backup file statistics:"
echo "  v0.3.x WAL files: $WAL_FILES"
echo "  v0.3.x snapshots: $SNAPSHOT_FILES"
echo ""
echo "#754 Issue with massive database:"
if [ "$MASSIVE_TRIGGERS_754" = true ]; then
    echo "  🚨 CRITICAL: #754 errors found with massive database"
    echo "  Database size or complexity may trigger the issue"
else
    echo "  ✅ No #754 errors even with massive database (3GB+)"
    echo "  Issue may not be related to database size"
fi
echo ""
echo "Restore compatibility:"
if [ $RESTORE_EXIT -eq 0 ]; then
    echo "  ✅ Massive restore successful ($RESTORED_COUNT records)"
    if [ "$V3_CHUNKS" -gt "0" ]; then
        echo "  ⚠️  v0.5.0 CAN read v0.3.x files (contrary to expectation)"
    fi
else
    echo "  ✗ Massive restore failed"
fi
echo ""
echo "CONCLUSION:"
if [ "$MASSIVE_TRIGGERS_754" = true ]; then
    echo "❌ Massive database triggers #754 in upgrade scenario"
else
    echo "✅ Even massive databases (3GB+) upgrade successfully"
    echo "   #754 issue not triggered by large v0.3.x → v0.5.0 upgrades"
fi
echo "=========================================="
