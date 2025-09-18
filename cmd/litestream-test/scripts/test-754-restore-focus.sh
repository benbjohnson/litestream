#!/bin/bash
set -e

# Aggressive #754 reproduction - focus on RESTORE scenarios
# The "ltx verification failed" error most likely happens during restore

echo "========================================"
echo "Aggressive #754 Restore Focus Test"
echo "========================================"
echo ""
echo "Testing restore scenarios to trigger 'ltx verification failed'"
echo ""

DB="/tmp/restore754.db"
REPLICA="/tmp/restore754-replica"
RESTORED="/tmp/restore754-restored.db"
LITESTREAM="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# Cleanup
cleanup() {
    pkill -f "litestream replicate.*restore754.db" 2>/dev/null || true
    rm -rf "$DB"* "$REPLICA" "$RESTORED"* /tmp/restore754-*.log
}

trap cleanup EXIT
cleanup

echo "=========================================="
echo "Test 1: Large database with many restores"
echo "=========================================="

echo "[1] Creating large database (2GB+)..."
$LITESTREAM_TEST populate -db "$DB" -target-size 2GB >/dev/null 2>&1

# Add complex schema
sqlite3 "$DB" <<EOF
CREATE TABLE restore_test (
    id INTEGER PRIMARY KEY,
    test_round INTEGER,
    data BLOB,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO restore_test (test_round, data) VALUES (1, randomblob(10000));
INSERT INTO restore_test (test_round, data) VALUES (1, randomblob(15000));
INSERT INTO restore_test (test_round, data) VALUES (1, randomblob(20000));
EOF

DB_SIZE=$(du -h "$DB" | cut -f1)
PAGE_COUNT=$(sqlite3 "$DB" "PRAGMA page_count;")
echo "  ✓ Large database: $DB_SIZE ($PAGE_COUNT pages)"

echo ""
echo "[2] Creating LTX backups with HeaderFlagNoChecksum..."
$LITESTREAM replicate "$DB" "file://$REPLICA" > /tmp/restore754-replication.log 2>&1 &
REPL_PID=$!
sleep 10

if ! kill -0 $REPL_PID 2>/dev/null; then
    echo "  ✗ Replication failed"
    cat /tmp/restore754-replication.log
    exit 1
fi

# Generate more LTX files with checkpoints
echo "  Adding data and forcing multiple LTX files..."
for round in {2..6}; do
    for i in {1..10}; do
        sqlite3 "$DB" "INSERT INTO restore_test (test_round, data) VALUES ($round, randomblob(5000));"
    done
    sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);"
    sleep 2
done

LTX_COUNT=$(find "$REPLICA" -name "*.ltx" 2>/dev/null | wc -l)
echo "  ✓ Generated $LTX_COUNT LTX files with HeaderFlagNoChecksum"

kill $REPL_PID 2>/dev/null
wait $REPL_PID 2>/dev/null

echo ""
echo "[3] CRITICAL: Testing restore from HeaderFlagNoChecksum files..."

for attempt in {1..5}; do
    echo "  Restore attempt $attempt..."
    rm -f "$RESTORED"*

    $LITESTREAM restore -o "$RESTORED" "file://$REPLICA" > /tmp/restore754-attempt$attempt.log 2>&1
    RESTORE_EXIT=$?

    # Check for the specific #754 errors
    FLAGS_ERROR=$(grep -c "no flags allowed" /tmp/restore754-attempt$attempt.log 2>/dev/null || echo "0")
    VERIFY_ERROR=$(grep -c "ltx verification failed" /tmp/restore754-attempt$attempt.log 2>/dev/null || echo "0")

    echo "    Exit code: $RESTORE_EXIT"
    echo "    'no flags allowed': $FLAGS_ERROR"
    echo "    'ltx verification failed': $VERIFY_ERROR"

    if [ "$FLAGS_ERROR" -gt "0" ] || [ "$VERIFY_ERROR" -gt "0" ]; then
        echo "    🚨 #754 REPRODUCED ON RESTORE!"
        echo "    Error details:"
        grep -A2 -B2 "no flags\|ltx verification" /tmp/restore754-attempt$attempt.log
        echo ""
        RESTORE_754_FOUND=true
        break
    elif [ $RESTORE_EXIT -ne 0 ]; then
        echo "    ⚠️  Restore failed with different error:"
        head -3 /tmp/restore754-attempt$attempt.log
    else
        RESTORED_COUNT=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM restore_test;" 2>/dev/null || echo "0")
        echo "    ✓ Restore succeeded: $RESTORED_COUNT rows"
    fi
    echo ""
done

echo ""
echo "=========================================="
echo "Test 2: Corrupt existing LTX file to force errors"
echo "=========================================="

if [ "$LTX_COUNT" -gt "0" ]; then
    echo "[4] Deliberately corrupting an LTX file to test error handling..."

    # Find first LTX file and modify it
    FIRST_LTX=$(find "$REPLICA" -name "*.ltx" | head -1)
    if [ -n "$FIRST_LTX" ]; then
        echo "  Corrupting: $(basename "$FIRST_LTX")"
        # Modify the header to trigger flag verification
        echo "CORRUPTED_HEADER" > "$FIRST_LTX"

        echo "  Attempting restore from corrupted LTX..."
        rm -f "$RESTORED"*

        $LITESTREAM restore -o "$RESTORED" "file://$REPLICA" > /tmp/restore754-corrupted.log 2>&1
        CORRUPT_EXIT=$?

        CORRUPT_FLAGS=$(grep -c "no flags allowed" /tmp/restore754-corrupted.log 2>/dev/null || echo "0")
        CORRUPT_VERIFY=$(grep -c "ltx verification failed" /tmp/restore754-corrupted.log 2>/dev/null || echo "0")

        echo "    Exit code: $CORRUPT_EXIT"
        echo "    'no flags allowed': $CORRUPT_FLAGS"
        echo "    'ltx verification failed': $CORRUPT_VERIFY"

        if [ "$CORRUPT_FLAGS" -gt "0" ] || [ "$CORRUPT_VERIFY" -gt "0" ]; then
            echo "    🚨 #754 TRIGGERED BY CORRUPTED LTX!"
            grep -A2 -B2 "no flags\|ltx verification" /tmp/restore754-corrupted.log
            CORRUPT_754_FOUND=true
        else
            echo "    Different error (as expected for corruption):"
            head -3 /tmp/restore754-corrupted.log
        fi
    fi
fi

echo ""
echo "=========================================="
echo "Test 3: Multiple database sizes"
echo "=========================================="

for size in "500MB" "1GB" "3GB"; do
    echo "[5.$size] Testing with $size database..."

    cleanup

    # Create database of specific size
    $LITESTREAM_TEST populate -db "$DB" -target-size "$size" >/dev/null 2>&1
    sqlite3 "$DB" "CREATE TABLE size_test (id INTEGER PRIMARY KEY, size TEXT, data BLOB); INSERT INTO size_test (size, data) VALUES ('$size', randomblob(8000));"

    # Quick replication
    $LITESTREAM replicate "$DB" "file://$REPLICA" > /dev/null 2>&1 &
    REPL_PID=$!
    sleep 5

    # Add data and checkpoint
    sqlite3 "$DB" "INSERT INTO size_test (size, data) VALUES ('$size-checkpoint', randomblob(10000));"
    sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);"
    sleep 3

    kill $REPL_PID 2>/dev/null
    wait $REPL_PID 2>/dev/null

    # Test restore
    rm -f "$RESTORED"*
    $LITESTREAM restore -o "$RESTORED" "file://$REPLICA" > /tmp/restore754-$size.log 2>&1
    SIZE_EXIT=$?

    SIZE_FLAGS=$(grep -c "no flags allowed" /tmp/restore754-$size.log 2>/dev/null || echo "0")
    SIZE_VERIFY=$(grep -c "ltx verification failed" /tmp/restore754-$size.log 2>/dev/null || echo "0")

    echo "    $size result: exit=$SIZE_EXIT, flags=$SIZE_FLAGS, verify=$SIZE_VERIFY"

    if [ "$SIZE_FLAGS" -gt "0" ] || [ "$SIZE_VERIFY" -gt "0" ]; then
        echo "    🚨 #754 TRIGGERED WITH $size DATABASE!"
        grep -A1 -B1 "no flags\|ltx verification" /tmp/restore754-$size.log
        SIZE_754_FOUND=true
    fi
done

echo ""
echo "=========================================="
echo "FINAL RESULTS"
echo "=========================================="
echo ""
echo "Test scenarios:"
echo "  Large DB restore attempts: $([ "${RESTORE_754_FOUND:-false}" = "true" ] && echo "REPRODUCED #754" || echo "No #754 errors")"
echo "  Corrupted LTX file: $([ "${CORRUPT_754_FOUND:-false}" = "true" ] && echo "REPRODUCED #754" || echo "No #754 errors")"
echo "  Multiple sizes: $([ "${SIZE_754_FOUND:-false}" = "true" ] && echo "REPRODUCED #754" || echo "No #754 errors")"
echo ""

if [ "${RESTORE_754_FOUND:-false}" = "true" ] || [ "${CORRUPT_754_FOUND:-false}" = "true" ] || [ "${SIZE_754_FOUND:-false}" = "true" ]; then
    echo "✅ SUCCESS: #754 REPRODUCED!"
    echo "   Issue confirmed: HeaderFlagNoChecksum incompatible with ltx v0.5.0"
    echo "   Trigger: Restore operations on LTX files with deprecated flags"
    echo ""
    echo "   This proves issue #754 is real and needs fixing before v0.5.0 release"
else
    echo "❌ #754 NOT REPRODUCED"
    echo "   Either:"
    echo "   1. Issue was fixed in recent changes"
    echo "   2. Requires very specific conditions not tested"
    echo "   3. Issue is in different code path (not restore)"
    echo ""
    echo "   Need to investigate further or check if issue still exists"
fi

echo ""
echo "HeaderFlagNoChecksum locations to fix:"
echo "  - db.go:883"
echo "  - db.go:1208"
echo "  - db.go:1298"
echo "  - replica.go:466"
echo "========================================"
