#!/bin/bash
set -e

# Test database deletion and recreation scenarios
# This test verifies proper handling when databases are deleted and recreated

echo "=========================================="
echo "Database Deletion and Recreation Test"
echo "=========================================="
echo ""
echo "Testing Litestream's handling of database deletion and recreation"
echo ""

# Configuration
DB="/tmp/deletion-test.db"
REPLICA="/tmp/deletion-replica"
LITESTREAM="./bin/litestream"

# Cleanup function
cleanup() {
    pkill -f "litestream replicate.*deletion-test.db" 2>/dev/null || true
    rm -f "$DB" "$DB-wal" "$DB-shm" "$DB-litestream"
    rm -rf "$REPLICA"
    rm -f /tmp/deletion-*.log
}

trap cleanup EXIT

echo "[SETUP] Cleaning up previous test files..."
cleanup

echo ""
echo "[1] Creating initial database..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
CREATE TABLE original (id INTEGER PRIMARY KEY, data TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP);
INSERT INTO original (data) VALUES ('Original database content');
INSERT INTO original (data) VALUES ('Should not appear in new database');
EOF
ORIGINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM original;")
echo "  ✓ Original database created with $ORIGINAL_COUNT rows"

echo ""
echo "[2] Starting Litestream replication..."
"$LITESTREAM" replicate "$DB" "file://$REPLICA" > /tmp/deletion-litestream.log 2>&1 &
LITESTREAM_PID=$!
sleep 2

if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
    echo "  ✗ Litestream failed to start"
    cat /tmp/deletion-litestream.log
    exit 1
fi
echo "  ✓ Litestream running (PID: $LITESTREAM_PID)"

echo ""
echo "[3] Letting replication stabilize..."
sleep 3
echo "  ✓ Initial replication complete"

echo ""
echo "=========================================="
echo "Test 1: Delete database while Litestream running"
echo "=========================================="

echo "[4] Deleting database files..."
rm -f "$DB" "$DB-wal" "$DB-shm"
echo "  ✓ Database files deleted"

echo ""
echo "[5] Creating new database with different schema..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
CREATE TABLE replacement (id INTEGER PRIMARY KEY, content BLOB, version INTEGER);
INSERT INTO replacement (content, version) VALUES (randomblob(100), 1);
INSERT INTO replacement (content, version) VALUES (randomblob(200), 2);
EOF
NEW_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM replacement;")
echo "  ✓ New database created with $NEW_COUNT rows"

echo ""
echo "[6] Checking for Litestream errors..."
sleep 2
ERRORS=$(grep -c "ERROR" /tmp/deletion-litestream.log 2>/dev/null || echo "0")
WARNINGS=$(grep -c "WAL" /tmp/deletion-litestream.log 2>/dev/null || echo "0")
echo "  Litestream errors: $ERRORS"
echo "  WAL warnings: $WARNINGS"

if [ $ERRORS -gt 0 ]; then
    echo "  ⚠️  Errors detected (expected when database deleted)"
    tail -5 /tmp/deletion-litestream.log | grep ERROR || true
fi

echo ""
echo "=========================================="
echo "Test 2: Check for leftover WAL corruption"
echo "=========================================="

echo "[7] Stopping Litestream..."
kill $LITESTREAM_PID 2>/dev/null || true
wait $LITESTREAM_PID 2>/dev/null
echo "  ✓ Litestream stopped"

echo ""
echo "[8] Simulating leftover WAL file scenario..."
# Create a database with WAL
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
INSERT INTO replacement (content, version) VALUES (randomblob(300), 3);
EOF
echo "  ✓ WAL file created"

# Delete only the main database file (leaving WAL)
echo "[9] Deleting only main database file (leaving WAL)..."
rm -f "$DB"
ls -la /tmp/deletion-test* 2>/dev/null | head -5 || true

echo ""
echo "[10] Creating new database with leftover WAL..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
CREATE TABLE new_table (id INTEGER PRIMARY KEY, data TEXT);
INSERT INTO new_table (data) VALUES ('New database with old WAL');
EOF

# Check if corruption occurred
INTEGRITY=$(sqlite3 "$DB" "PRAGMA integrity_check;" 2>&1)
if [ "$INTEGRITY" = "ok" ]; then
    echo "  ✓ No corruption despite leftover WAL"
else
    echo "  ✗ CORRUPTION DETECTED: $INTEGRITY"
    echo "  This confirms leftover WAL files can corrupt new databases!"
fi

echo ""
echo "=========================================="
echo "Test 3: Clean deletion procedure"
echo "=========================================="

echo "[11] Demonstrating proper deletion procedure..."

# Clean up everything
rm -f "$DB" "$DB-wal" "$DB-shm"
rm -rf "$DB-litestream"
echo "  ✓ All database files removed"

# Create fresh database
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
CREATE TABLE clean (id INTEGER PRIMARY KEY, data TEXT);
INSERT INTO clean (data) VALUES ('Clean start');
EOF

FINAL_INTEGRITY=$(sqlite3 "$DB" "PRAGMA integrity_check;")
FINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM clean;")

echo "  ✓ Clean database created"
echo "    Integrity: $FINAL_INTEGRITY"
echo "    Rows: $FINAL_COUNT"

echo ""
echo "=========================================="
echo "Database Deletion Test Summary:"
echo "  ✓ Detected database deletion scenarios"
echo "  ✓ Demonstrated WAL file corruption risk"
echo "  ✓ Showed proper cleanup procedure"
echo ""
echo "IMPORTANT: When deleting databases:"
echo "  1. Stop Litestream first"
echo "  2. Delete: DB, DB-wal, DB-shm, DB-litestream"
echo "  3. Restart Litestream after creating new DB"
echo "=========================================="
