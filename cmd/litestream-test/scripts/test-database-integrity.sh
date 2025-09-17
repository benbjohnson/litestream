#!/bin/bash
set -e

# Test database integrity after restore (Issue #582)
# This test creates complex data patterns, replicates, and verifies integrity after restore

echo "=========================================="
echo "Database Integrity After Restore Test"
echo "=========================================="
echo ""
echo "Testing if restored databases pass integrity checks"
echo ""

# Configuration
DB="/tmp/integrity-test.db"
REPLICA="/tmp/integrity-replica"
RESTORED="/tmp/integrity-restored.db"
LITESTREAM_CONFIG="/tmp/integrity-litestream.yml"
LITESTREAM="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# Cleanup function
cleanup() {
    pkill -f "litestream replicate.*integrity-test.db" 2>/dev/null || true
    rm -f "$DB" "$DB-wal" "$DB-shm" "$DB-litestream"
    rm -f "$RESTORED" "$RESTORED-wal" "$RESTORED-shm"
    rm -rf "$REPLICA"
    rm -f "$LITESTREAM_CONFIG"
    rm -f /tmp/integrity-*.log
}

trap cleanup EXIT

echo "[SETUP] Cleaning up previous test files..."
cleanup

echo ""
echo "[1] Creating database with complex data patterns..."
# Create database with various data types and constraints
sqlite3 "$DB" <<EOF
PRAGMA page_size = 4096;
PRAGMA journal_mode = WAL;

-- Table with primary key and foreign key constraints
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Table with indexes
CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    content BLOB,
    score REAL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX idx_posts_user ON posts(user_id);
CREATE INDEX idx_posts_score ON posts(score);

-- Table with check constraints
CREATE TABLE transactions (
    id INTEGER PRIMARY KEY,
    amount REAL NOT NULL CHECK (amount != 0),
    type TEXT CHECK (type IN ('credit', 'debit')),
    balance REAL
);

-- Add initial data
INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@test.com'),
    ('Bob', 'bob@test.com'),
    ('Charlie', 'charlie@test.com');

-- Add posts with various data types
INSERT INTO posts (user_id, title, content, score) VALUES
    (1, 'First Post', randomblob(1000), 4.5),
    (2, 'Second Post', randomblob(2000), 3.8),
    (3, 'Third Post', NULL, 4.9);

-- Add transactions
INSERT INTO transactions (amount, type, balance) VALUES
    (100.50, 'credit', 100.50),
    (-25.75, 'debit', 74.75),
    (50.00, 'credit', 124.75);
EOF

echo "  ✓ Database created with complex schema"

# Add more data manually to preserve schema
echo ""
echo "[2] Adding bulk data..."
for i in {1..100}; do
    sqlite3 "$DB" "INSERT INTO posts (user_id, title, content, score) VALUES ((ABS(RANDOM()) % 3) + 1, 'Post $i', randomblob(5000), RANDOM() % 5);" 2>/dev/null
    sqlite3 "$DB" "INSERT INTO transactions (amount, type, balance) VALUES (ABS(RANDOM() % 1000) + 0.01, CASE WHEN RANDOM() % 2 = 0 THEN 'credit' ELSE 'debit' END, ABS(RANDOM() % 10000));" 2>/dev/null
done
INITIAL_SIZE=$(du -h "$DB" | cut -f1)
echo "  ✓ Database populated: $INITIAL_SIZE"

echo ""
echo "[3] Running initial integrity check..."
INITIAL_INTEGRITY=$(sqlite3 "$DB" "PRAGMA integrity_check;")
if [ "$INITIAL_INTEGRITY" != "ok" ]; then
    echo "  ✗ Initial database has integrity issues: $INITIAL_INTEGRITY"
    exit 1
fi
echo "  ✓ Initial integrity check: $INITIAL_INTEGRITY"

# Get checksums for verification
USERS_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM users;")
POSTS_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM posts;")
TRANS_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM transactions;")
TABLE_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM posts;" 2>/dev/null || echo "0")

echo ""
echo "[4] Starting Litestream replication..."
"$LITESTREAM" replicate "$DB" "file://$REPLICA" > /tmp/integrity-litestream.log 2>&1 &
LITESTREAM_PID=$!
sleep 3

if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
    echo "  ✗ Litestream failed to start"
    cat /tmp/integrity-litestream.log
    exit 1
fi
echo "  ✓ Litestream running (PID: $LITESTREAM_PID)"

echo ""
echo "[5] Making changes while replicating..."
# Add more data and modify existing
sqlite3 "$DB" <<EOF
-- Update existing data
UPDATE users SET name = 'Alice Updated' WHERE id = 1;
DELETE FROM posts WHERE id = 2;

-- Add new data with edge cases
INSERT INTO users (name, email) VALUES ('Dave', 'dave@test.com');
INSERT INTO posts (user_id, title, content, score) VALUES
    (4, 'Edge Case Post', randomblob(5000), 0.0),
    (4, 'Another Post', randomblob(100), -1.5);

-- Trigger constraint checks
INSERT INTO transactions (amount, type, balance) VALUES
    (1000.00, 'credit', 1124.75),
    (-500.00, 'debit', 624.75);
EOF

# Force checkpoint
sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);" >/dev/null 2>&1
sleep 2

echo "  ✓ Changes made and checkpoint executed"

echo ""
echo "[6] Stopping Litestream and attempting restore..."
kill $LITESTREAM_PID
wait $LITESTREAM_PID 2>/dev/null

# Attempt restore
"$LITESTREAM" restore -o "$RESTORED" "file://$REPLICA" > /tmp/integrity-restore.log 2>&1
RESTORE_EXIT=$?

if [ $RESTORE_EXIT -ne 0 ]; then
    echo "  ✗ Restore failed with exit code: $RESTORE_EXIT"
    cat /tmp/integrity-restore.log
    exit 1
fi
echo "  ✓ Restore completed"

echo ""
echo "[7] Running integrity check on restored database..."
RESTORED_INTEGRITY=$(sqlite3 "$RESTORED" "PRAGMA integrity_check;" 2>&1)

if [ "$RESTORED_INTEGRITY" != "ok" ]; then
    echo "  ✗ CRITICAL: Restored database FAILED integrity check!"
    echo "    Result: $RESTORED_INTEGRITY"

    # Try to get more info
    echo ""
    echo "  Attempting detailed analysis:"
    sqlite3 "$RESTORED" "PRAGMA foreign_key_check;" 2>/dev/null || echo "    Foreign key check failed"
    sqlite3 "$RESTORED" "SELECT COUNT(*) FROM sqlite_master;" 2>/dev/null || echo "    Cannot read schema"

    exit 1
else
    echo "  ✓ Integrity check PASSED: $RESTORED_INTEGRITY"
fi

echo ""
echo "[8] Verifying data consistency..."
# Check row counts
RESTORED_USERS=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM users;" 2>/dev/null || echo "ERROR")
RESTORED_POSTS=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM posts;" 2>/dev/null || echo "ERROR")
RESTORED_TRANS=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM transactions;" 2>/dev/null || echo "ERROR")
RESTORED_TABLE=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM posts;" 2>/dev/null || echo "0")

# Expected counts after changes
EXPECTED_USERS=4  # 3 original + 1 added
EXPECTED_POSTS=104  # 3 original + 100 bulk - 1 deleted + 2 added
EXPECTED_TRANS=105  # 3 original + 100 bulk + 2 added

echo "  Data verification:"
echo "    Users: $RESTORED_USERS (expected: $EXPECTED_USERS)"
echo "    Posts: $RESTORED_POSTS (expected: $EXPECTED_POSTS)"
echo "    Transactions: $RESTORED_TRANS (expected: $EXPECTED_TRANS)"
echo "    Test Table: $RESTORED_TABLE (expected: $TABLE_COUNT)"

DATA_INTACT=true
if [ "$RESTORED_USERS" != "$EXPECTED_USERS" ]; then
    echo "  ✗ User count mismatch!"
    DATA_INTACT=false
fi
if [ "$RESTORED_POSTS" != "$EXPECTED_POSTS" ]; then
    echo "  ✗ Post count mismatch!"
    DATA_INTACT=false
fi
if [ "$RESTORED_TRANS" != "$EXPECTED_TRANS" ]; then
    echo "  ✗ Transaction count mismatch!"
    DATA_INTACT=false
fi

echo ""
echo "[9] Testing constraint enforcement..."
# Test that constraints still work
CONSTRAINT_TEST=$(sqlite3 "$RESTORED" "INSERT INTO transactions (amount, type) VALUES (0, 'credit');" 2>&1 || echo "CONSTRAINT_OK")
if [[ "$CONSTRAINT_TEST" == *"CONSTRAINT_OK"* ]] || [[ "$CONSTRAINT_TEST" == *"CHECK constraint failed"* ]]; then
    echo "  ✓ Check constraints working"
else
    echo "  ✗ Check constraints not enforced!"
    DATA_INTACT=false
fi

# Test foreign keys
FK_TEST=$(sqlite3 "$RESTORED" "PRAGMA foreign_keys=ON; INSERT INTO posts (user_id, title) VALUES (999, 'Bad FK');" 2>&1 || echo "FK_OK")
if [[ "$FK_TEST" == *"FK_OK"* ]] || [[ "$FK_TEST" == *"FOREIGN KEY constraint failed"* ]]; then
    echo "  ✓ Foreign key constraints working"
else
    echo "  ✗ Foreign key constraints not enforced!"
    DATA_INTACT=false
fi

echo ""
if [ "$DATA_INTACT" = true ] && [ "$RESTORED_INTEGRITY" = "ok" ]; then
    echo "✅ TEST PASSED: Database integrity preserved after restore"
else
    echo "❌ TEST FAILED: Database integrity issues detected"
    exit 1
fi

echo ""
echo "=========================================="
echo "Summary:"
echo "  Integrity Check: $RESTORED_INTEGRITY"
echo "  Data Consistency: $DATA_INTACT"
echo "  Constraints: Working"
echo "=========================================="
