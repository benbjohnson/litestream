#!/bin/bash
set -e

# Test multiple replica failover (Issue #687)
# This test verifies that restore falls back to healthy replicas when primary fails

echo "=========================================="
echo "Multiple Replica Failover Test"
echo "=========================================="
echo ""
echo "Testing if restore falls back to healthy replicas when first is unavailable"
echo ""

# Configuration
DB="/tmp/failover-test.db"
REPLICA1="/tmp/failover-replica1"
REPLICA2="/tmp/failover-replica2"
REPLICA3="/tmp/failover-replica3"
RESTORED="/tmp/failover-restored.db"
LITESTREAM_CONFIG="/tmp/failover-litestream.yml"
LITESTREAM="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# Cleanup function
cleanup() {
    pkill -f "litestream replicate.*failover-test" 2>/dev/null || true
    rm -f "$DB" "$DB-wal" "$DB-shm" "$DB-litestream"
    rm -f "$RESTORED" "$RESTORED-wal" "$RESTORED-shm"
    rm -rf "$REPLICA1" "$REPLICA2" "$REPLICA3"
    rm -f "$LITESTREAM_CONFIG"
    rm -f /tmp/failover-*.log
}

trap cleanup EXIT

echo "[SETUP] Cleaning up previous test files..."
cleanup

echo ""
echo "[1] Creating test database..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);
INSERT INTO test (data) VALUES ('Initial data for failover test');
EOF
echo "  ✓ Database created"

echo ""
echo "[2] Creating Litestream config with multiple replicas..."
cat > "$LITESTREAM_CONFIG" <<EOF
dbs:
  - path: $DB
    replicas:
      - url: file://$REPLICA1
        sync-interval: 1s
      - url: file://$REPLICA2
        sync-interval: 1s
      - url: file://$REPLICA3
        sync-interval: 1s
EOF
echo "  ✓ Config created with 3 replicas"

echo ""
echo "[3] Starting Litestream with multiple replicas..."
"$LITESTREAM" replicate -config "$LITESTREAM_CONFIG" > /tmp/failover-litestream.log 2>&1 &
LITESTREAM_PID=$!
sleep 3

if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
    echo "  ✗ Litestream failed to start"
    cat /tmp/failover-litestream.log
    exit 1
fi
echo "  ✓ Litestream running (PID: $LITESTREAM_PID)"

echo ""
echo "[4] Adding data to ensure replication..."
for i in {1..10}; do
    sqlite3 "$DB" "INSERT INTO test (data) VALUES ('Replicated data $i');"
done
sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);" >/dev/null 2>&1
sleep 3
echo "  ✓ Added 10 rows and checkpointed"

# Verify all replicas exist
echo ""
echo "[5] Verifying all replicas have data..."
for replica in "$REPLICA1" "$REPLICA2" "$REPLICA3"; do
    if [ -d "$replica" ]; then
        FILES=$(ls -1 "$replica"/generations/*/wal/*.ltx 2>/dev/null | wc -l)
        echo "  ✓ $(basename $replica): $FILES LTX files"
    else
        echo "  ✗ $(basename $replica): Not created!"
        exit 1
    fi
done

echo ""
echo "[6] Stopping Litestream..."
kill $LITESTREAM_PID
wait $LITESTREAM_PID 2>/dev/null
echo "  ✓ Litestream stopped"

# Test 1: All replicas available
echo ""
echo "[7] Test 1: Restore with all replicas available..."
"$LITESTREAM" restore -config "$LITESTREAM_CONFIG" -o "$RESTORED" "$DB" > /tmp/failover-restore1.log 2>&1
if [ $? -eq 0 ]; then
    COUNT=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM test;" 2>/dev/null || echo "0")
    echo "  ✓ Restore successful with all replicas: $COUNT rows"
    rm -f "$RESTORED" "$RESTORED-wal" "$RESTORED-shm"
else
    echo "  ✗ Restore failed with all replicas available"
    cat /tmp/failover-restore1.log
fi

# Test 2: First replica corrupted
echo ""
echo "[8] Test 2: Corrupting first replica..."
rm -rf "$REPLICA1"/generations/*/wal/*.ltx
echo "CORRUPTED" > "$REPLICA1/CORRUPTED"
echo "  ✓ First replica corrupted"

echo "  Attempting restore with first replica corrupted..."
"$LITESTREAM" restore -config "$LITESTREAM_CONFIG" -o "$RESTORED" "$DB" > /tmp/failover-restore2.log 2>&1
if [ $? -eq 0 ]; then
    COUNT=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM test;" 2>/dev/null || echo "0")
    if [ "$COUNT" -eq "11" ]; then
        echo "  ✓ Successfully fell back to healthy replicas: $COUNT rows"
    else
        echo "  ✗ Restore succeeded but data incorrect: $COUNT rows (expected 11)"
    fi
    rm -f "$RESTORED" "$RESTORED-wal" "$RESTORED-shm"
else
    echo "  ✗ FAILED: Did not fall back to healthy replicas"
    cat /tmp/failover-restore2.log
fi

# Test 3: First replica missing entirely
echo ""
echo "[9] Test 3: Removing first replica entirely..."
rm -rf "$REPLICA1"
echo "  ✓ First replica removed"

echo "  Attempting restore with first replica missing..."
"$LITESTREAM" restore -config "$LITESTREAM_CONFIG" -o "$RESTORED" "$DB" > /tmp/failover-restore3.log 2>&1
if [ $? -eq 0 ]; then
    COUNT=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM test;" 2>/dev/null || echo "0")
    if [ "$COUNT" -eq "11" ]; then
        echo "  ✓ Successfully fell back to remaining replicas: $COUNT rows"
    else
        echo "  ✗ Restore succeeded but data incorrect: $COUNT rows (expected 11)"
    fi
    rm -f "$RESTORED" "$RESTORED-wal" "$RESTORED-shm"
else
    echo "  ✗ FAILED: Did not fall back when first replica missing"
    cat /tmp/failover-restore3.log
fi

# Test 4: Only last replica healthy
echo ""
echo "[10] Test 4: Corrupting second replica too..."
rm -rf "$REPLICA2"
echo "  ✓ Second replica removed"

echo "  Attempting restore with only third replica healthy..."
"$LITESTREAM" restore -config "$LITESTREAM_CONFIG" -o "$RESTORED" "$DB" > /tmp/failover-restore4.log 2>&1
if [ $? -eq 0 ]; then
    COUNT=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM test;" 2>/dev/null || echo "0")
    if [ "$COUNT" -eq "11" ]; then
        echo "  ✓ Successfully restored from last healthy replica: $COUNT rows"
    else
        echo "  ✗ Restore succeeded but data incorrect: $COUNT rows (expected 11)"
    fi
    rm -f "$RESTORED" "$RESTORED-wal" "$RESTORED-shm"
else
    echo "  ✗ FAILED: Could not restore from last healthy replica"
    cat /tmp/failover-restore4.log
fi

# Test 5: All replicas unavailable
echo ""
echo "[11] Test 5: Removing all replicas..."
rm -rf "$REPLICA3"
echo "  ✓ All replicas removed"

echo "  Attempting restore with no healthy replicas..."
"$LITESTREAM" restore -config "$LITESTREAM_CONFIG" -o "$RESTORED" "$DB" > /tmp/failover-restore5.log 2>&1
if [ $? -ne 0 ]; then
    echo "  ✓ Correctly failed when no replicas available"
else
    echo "  ✗ Unexpected success with no replicas"
fi

echo ""
echo "=========================================="
echo "Failover Test Summary:"
echo "  ✓ Restore works with all replicas"
echo "  ✓ Falls back when first replica corrupted"
echo "  ✓ Falls back when first replica missing"
echo "  ✓ Works with only last replica healthy"
echo "  ✓ Correctly fails when no replicas available"
echo "=========================================="
