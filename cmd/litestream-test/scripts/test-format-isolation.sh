#!/bin/bash
set -e

# Test to verify whether v0.5.0 can actually restore from PURE v0.3.x files
# Or if it's creating new v0.5.0 backups that we're actually restoring from

echo "=========================================="
echo "File Format Isolation Test"
echo "=========================================="
echo ""
echo "Testing whether v0.5.0 can restore from PURE v0.3.x files"
echo "or if it's silently creating new v0.5.0 backups"
echo ""

# Configuration
DB="/tmp/format-test.db"
REPLICA="/tmp/format-replica"
RESTORED="/tmp/format-restored.db"
LITESTREAM_V3="/opt/homebrew/bin/litestream"
LITESTREAM_V5="./bin/litestream"

# Cleanup function
cleanup() {
    pkill -f "litestream replicate.*format-test.db" 2>/dev/null || true
    rm -f "$DB" "$DB-wal" "$DB-shm" "$DB-litestream"
    rm -f "$RESTORED" "$RESTORED-wal" "$RESTORED-shm"
    rm -rf "$REPLICA"
    rm -f /tmp/format-*.log
}

trap cleanup EXIT

echo "[SETUP] Cleaning up previous test files..."
cleanup

echo ""
echo "=========================================="
echo "Phase 1: Create PURE v0.3.x backups"
echo "=========================================="

echo "[1] Creating test database..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
CREATE TABLE format_test (
    id INTEGER PRIMARY KEY,
    phase TEXT,
    data TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO format_test (phase, data) VALUES ('v0.3.x-only', 'Original v0.3.x data');
INSERT INTO format_test (phase, data) VALUES ('v0.3.x-only', 'Should only restore if v0.5.0 reads v0.3.x');
INSERT INTO format_test (phase, data) VALUES ('v0.3.x-only', 'Third row for verification');
EOF

V3_INITIAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM format_test;")
echo "  ✓ Database created with $V3_INITIAL_COUNT rows"

echo ""
echo "[2] Starting v0.3.13 replication..."
$LITESTREAM_V3 replicate "$DB" "file://$REPLICA" > /tmp/format-v3.log 2>&1 &
V3_PID=$!
sleep 3

if ! kill -0 $V3_PID 2>/dev/null; then
    echo "  ✗ v0.3.13 failed to start"
    cat /tmp/format-v3.log
    exit 1
fi
echo "  ✓ v0.3.13 replicating (PID: $V3_PID)"

echo ""
echo "[3] Adding more v0.3.x data..."
for i in {1..5}; do
    sqlite3 "$DB" "INSERT INTO format_test (phase, data) VALUES ('v0.3.x-replicated', 'Data row $i');"
done
sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);" >/dev/null 2>&1
sleep 5

V3_FINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM format_test;")
echo "  ✓ v0.3.x replication complete, total: $V3_FINAL_COUNT rows"

echo ""
echo "[4] Stopping v0.3.13 and examining PURE v0.3.x files..."
kill $V3_PID 2>/dev/null || true
wait $V3_PID 2>/dev/null

if [ -d "$REPLICA" ]; then
    echo "  v0.3.x backup structure:"
    find "$REPLICA" -type f | while read file; do
        echo "    $(basename $(dirname $file))/$(basename $file) ($(stat -f%z "$file" 2>/dev/null || stat -c%s "$file") bytes)"
    done

    V3_WAL_FILES=$(find "$REPLICA" -name "*.wal.lz4" | wc -l)
    V3_SNAPSHOT_FILES=$(find "$REPLICA" -name "*.snapshot.lz4" | wc -l)
    echo "  Summary: $V3_WAL_FILES WAL files, $V3_SNAPSHOT_FILES snapshots"
else
    echo "  ✗ No replica directory created!"
    exit 1
fi

echo ""
echo "=========================================="
echo "Phase 2: Test v0.5.0 restore from PURE v0.3.x"
echo "=========================================="

echo "[5] Attempting v0.5.0 restore from PURE v0.3.x files..."
echo "  CRITICAL: This should fail if formats are incompatible"

$LITESTREAM_V5 restore -o "$RESTORED" "file://$REPLICA" > /tmp/format-restore-pure.log 2>&1
PURE_RESTORE_EXIT=$?

if [ $PURE_RESTORE_EXIT -eq 0 ]; then
    PURE_RESTORED_COUNT=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM format_test;" 2>/dev/null || echo "0")

    if [ "$PURE_RESTORED_COUNT" -gt "0" ]; then
        echo "  🚨 UNEXPECTED: v0.5.0 CAN restore from pure v0.3.x files!"
        echo "    Restored $PURE_RESTORED_COUNT rows"

        # Check what was restored
        V3_ONLY=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM format_test WHERE phase='v0.3.x-only';" 2>/dev/null || echo "0")
        V3_REPLICATED=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM format_test WHERE phase='v0.3.x-replicated';" 2>/dev/null || echo "0")

        echo "    Breakdown:"
        echo "      v0.3.x-only: $V3_ONLY rows"
        echo "      v0.3.x-replicated: $V3_REPLICATED rows"

        PURE_V3_COMPATIBILITY=true
    else
        echo "  ✗ Restore succeeded but no data - file format issue?"
        PURE_V3_COMPATIBILITY=false
    fi
else
    echo "  ✅ EXPECTED: v0.5.0 cannot restore from pure v0.3.x files"
    echo "  Error message:"
    cat /tmp/format-restore-pure.log | head -5
    PURE_V3_COMPATIBILITY=false
fi

# Clean up restore for next test
rm -f "$RESTORED" "$RESTORED-wal" "$RESTORED-shm"

echo ""
echo "=========================================="
echo "Phase 3: Test mixed v0.3.x + v0.5.0 scenario"
echo "=========================================="

echo "[6] Starting v0.5.0 against existing v0.3.x backup..."
echo "  This simulates the upgrade scenario from our previous test"

# Delete the database but keep replica
rm -f "$DB" "$DB-wal" "$DB-shm"

# Recreate database with new data
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
CREATE TABLE format_test (
    id INTEGER PRIMARY KEY,
    phase TEXT,
    data TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO format_test (phase, data) VALUES ('v0.5.0-new', 'This is new v0.5.0 data');
INSERT INTO format_test (phase, data) VALUES ('v0.5.0-new', 'Should appear if v0.5.0 creates new backup');
EOF

echo "  ✓ Recreated database with v0.5.0 data"

# Start v0.5.0 against the replica that has v0.3.x files
$LITESTREAM_V5 replicate "$DB" "file://$REPLICA" > /tmp/format-v5.log 2>&1 &
V5_PID=$!
sleep 5

if ! kill -0 $V5_PID 2>/dev/null; then
    echo "  ✗ v0.5.0 failed to start"
    cat /tmp/format-v5.log
    exit 1
fi
echo "  ✓ v0.5.0 running against mixed replica (PID: $V5_PID)"

# Add more v0.5.0 data
for i in {1..3}; do
    sqlite3 "$DB" "INSERT INTO format_test (phase, data) VALUES ('v0.5.0-running', 'Runtime data $i');"
done
sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);" >/dev/null 2>&1
sleep 3

V5_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM format_test;")
echo "  ✓ v0.5.0 phase complete, database has: $V5_COUNT rows"

echo ""
echo "[7] Examining mixed backup structure..."
echo "  Files after v0.5.0 runs:"
find "$REPLICA" -type f | while read file; do
    echo "    $(basename $(dirname $file))/$(basename $file) ($(stat -f%z "$file" 2>/dev/null || stat -c%s "$file") bytes)"
done

# Look for new v0.5.0 files
V5_LTX_FILES=$(find "$REPLICA" -name "*.ltx" 2>/dev/null | wc -l)
echo "  New v0.5.0 LTX files: $V5_LTX_FILES"

kill $V5_PID 2>/dev/null || true
wait $V5_PID 2>/dev/null

echo ""
echo "[8] Testing restore from mixed backup..."
$LITESTREAM_V5 restore -o "$RESTORED" "file://$REPLICA" > /tmp/format-restore-mixed.log 2>&1
MIXED_RESTORE_EXIT=$?

if [ $MIXED_RESTORE_EXIT -eq 0 ]; then
    MIXED_RESTORED_COUNT=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM format_test;" 2>/dev/null || echo "0")
    echo "  ✓ Mixed restore successful: $MIXED_RESTORED_COUNT rows"

    # Analyze what was restored
    V3_ONLY_MIXED=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM format_test WHERE phase='v0.3.x-only';" 2>/dev/null || echo "0")
    V3_REPLICATED_MIXED=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM format_test WHERE phase='v0.3.x-replicated';" 2>/dev/null || echo "0")
    V5_NEW_MIXED=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM format_test WHERE phase='v0.5.0-new';" 2>/dev/null || echo "0")
    V5_RUNNING_MIXED=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM format_test WHERE phase='v0.5.0-running';" 2>/dev/null || echo "0")

    echo "  Detailed breakdown:"
    echo "    v0.3.x-only: $V3_ONLY_MIXED rows"
    echo "    v0.3.x-replicated: $V3_REPLICATED_MIXED rows"
    echo "    v0.5.0-new: $V5_NEW_MIXED rows"
    echo "    v0.5.0-running: $V5_RUNNING_MIXED rows"

    if [ "$V3_ONLY_MIXED" -gt "0" ] || [ "$V3_REPLICATED_MIXED" -gt "0" ]; then
        echo "  🚨 v0.5.0 restored v0.3.x data in mixed scenario!"
        MIXED_V3_COMPATIBILITY=true
    else
        echo "  ✅ v0.5.0 only restored its own v0.5.0 data"
        MIXED_V3_COMPATIBILITY=false
    fi
else
    echo "  ✗ Mixed restore failed"
    cat /tmp/format-restore-mixed.log
    MIXED_V3_COMPATIBILITY=false
fi

echo ""
echo "=========================================="
echo "File Format Compatibility Analysis"
echo "=========================================="
echo ""
echo "Test Results:"
echo "  Pure v0.3.x restore: $([ "$PURE_V3_COMPATIBILITY" = true ] && echo "✓ SUCCESS" || echo "✗ FAILED")"
echo "  Mixed backup restore: $([ "$MIXED_V3_COMPATIBILITY" = true ] && echo "✓ INCLUDES v0.3.x data" || echo "✗ v0.5.0 data only")"
echo ""
echo "Data counts:"
echo "  Original v0.3.x: $V3_FINAL_COUNT rows"
echo "  v0.5.0 database: $V5_COUNT rows"
if [ $PURE_RESTORE_EXIT -eq 0 ]; then
    echo "  Pure v0.3.x restore: $PURE_RESTORED_COUNT rows"
fi
if [ $MIXED_RESTORE_EXIT -eq 0 ]; then
    echo "  Mixed restore: $MIXED_RESTORED_COUNT rows"
fi
echo ""
echo "CONCLUSION:"
if [ "$PURE_V3_COMPATIBILITY" = true ]; then
    echo "🚨 CRITICAL: v0.5.0 CAN read pure v0.3.x backup files!"
    echo "   This means the formats are compatible or v0.5.0 has v0.3.x support"
    echo "   Ben's expectation that they're incompatible is incorrect"
elif [ "$MIXED_V3_COMPATIBILITY" = true ]; then
    echo "⚠️  PARTIAL: v0.5.0 cannot read pure v0.3.x files"
    echo "   BUT it can read them when mixed with v0.5.0 files"
    echo "   This suggests v0.5.0 creates new backups but can access old ones"
else
    echo "✅ EXPECTED: v0.5.0 cannot read v0.3.x files at all"
    echo "   Previous test results were misleading"
    echo "   v0.5.0 only restores its own backup data"
fi
echo "=========================================="
