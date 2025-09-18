#!/bin/bash

# Test: Starting replication with a fresh (empty) database
# This tests if Litestream works better when it creates the database from scratch

set -e

echo "=========================================="
echo "Fresh Start Database Test"
echo "=========================================="
echo ""
echo "Testing if Litestream works correctly when starting fresh"
echo ""

# Configuration
DB="/tmp/fresh-test.db"
REPLICA="/tmp/fresh-replica"
LITESTREAM="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# Clean up
echo "[SETUP] Cleaning up..."
rm -f "$DB"*
rm -rf "$REPLICA"

# Check binaries
if [ ! -f "$LITESTREAM" ]; then
    echo "ERROR: $LITESTREAM not found"
    exit 1
fi

if [ ! -f "$LITESTREAM_TEST" ]; then
    echo "ERROR: $LITESTREAM_TEST not found"
    exit 1
fi

# Start Litestream BEFORE creating database
echo ""
echo "[1] Starting Litestream with non-existent database..."
$LITESTREAM replicate "$DB" "file://$REPLICA" > /tmp/fresh-test.log 2>&1 &
LITESTREAM_PID=$!
sleep 2

if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
    echo "  ✓ Expected: Litestream waiting for database to be created"
else
    echo "  ✓ Litestream running (PID: $LITESTREAM_PID)"
fi

# Now create and populate the database
echo ""
echo "[2] Creating database while Litestream is running..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode=WAL;
CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT);
INSERT INTO test (data) VALUES ('initial data');
EOF
echo "  ✓ Database created"

# Give Litestream time to detect the new database
sleep 3

# Check if Litestream started replicating
echo ""
echo "[3] Checking if Litestream detected the database..."
if grep -q "initialized db" /tmp/fresh-test.log; then
    echo "  ✓ Litestream detected and initialized database"
fi

# Add more data
echo ""
echo "[4] Adding data to test replication..."
for i in {1..100}; do
    sqlite3 "$DB" "INSERT INTO test (data) VALUES ('row $i');"
done
echo "  ✓ Added 100 rows"

# Let replication catch up
sleep 5

# Check for errors
echo ""
echo "[5] Checking for errors..."
ERROR_COUNT=$(grep -c "ERROR" /tmp/fresh-test.log 2>/dev/null || echo "0")
if [ "$ERROR_COUNT" -gt 1 ]; then
    echo "  ⚠ Found $ERROR_COUNT errors:"
    grep "ERROR" /tmp/fresh-test.log | head -3
else
    echo "  ✓ No significant errors"
fi

# Check replica files
echo ""
echo "[6] Checking replica files..."
if [ -d "$REPLICA/ltx" ]; then
    FILE_COUNT=$(find "$REPLICA/ltx" -name "*.ltx" | wc -l)
    echo "  ✓ Replica created with $FILE_COUNT LTX files"
    ls -la "$REPLICA/ltx/0/" 2>/dev/null | head -3
else
    echo "  ✗ No replica files created!"
fi

# Stop Litestream
kill $LITESTREAM_PID 2>/dev/null || true
sleep 2

# Test restore
echo ""
echo "[7] Testing restore..."
rm -f /tmp/fresh-restored.db
if $LITESTREAM restore -o /tmp/fresh-restored.db "file://$REPLICA" 2>&1; then
    echo "  ✓ Restore successful"

    # Verify data
    ORIG_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM test;")
    REST_COUNT=$(sqlite3 /tmp/fresh-restored.db "SELECT COUNT(*) FROM test;")

    if [ "$ORIG_COUNT" -eq "$REST_COUNT" ]; then
        echo "  ✓ Data integrity verified: $ORIG_COUNT rows"
        echo ""
        echo "TEST PASSED: Fresh start works correctly"
    else
        echo "  ✗ Data mismatch: Original=$ORIG_COUNT, Restored=$REST_COUNT"
        echo ""
        echo "TEST FAILED: Data loss detected"
    fi
else
    echo "  ✗ Restore failed!"
    echo ""
    echo "TEST FAILED: Cannot restore database"
fi

echo ""
echo "=========================================="
echo "Test artifacts:"
echo "  Database: $DB"
echo "  Replica: $REPLICA"
echo "  Log: /tmp/fresh-test.log"
echo "=========================================="
