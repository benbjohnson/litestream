#!/bin/bash
set -e

# Quick test: Can v0.5.0 restore from PURE v0.3.x files?

echo "Quick Format Compatibility Test"
echo "================================"

DB="/tmp/quick-test.db"
REPLICA="/tmp/quick-replica"
RESTORED="/tmp/quick-restored.db"

# Cleanup
rm -rf "$DB"* "$REPLICA" "$RESTORED"*

# 1. Create database and backup with v0.3.13 ONLY
echo "1. Creating v0.3.x backup..."
sqlite3 "$DB" "PRAGMA journal_mode=WAL; CREATE TABLE test(id INTEGER, data TEXT); INSERT INTO test VALUES(1,'v0.3.x data');"

/opt/homebrew/bin/litestream replicate "$DB" "file://$REPLICA" &
PID=$!
sleep 3
sqlite3 "$DB" "INSERT INTO test VALUES(2,'more v0.3.x data');"
sleep 2
kill $PID
wait $PID 2>/dev/null

echo "2. v0.3.x files created:"
find "$REPLICA" -type f

# 2. Delete database completely
rm -f "$DB"*

# 3. Try to restore with v0.5.0 from PURE v0.3.x files
echo "3. Testing v0.5.0 restore from pure v0.3.x..."
./bin/litestream restore -o "$RESTORED" "file://$REPLICA" 2>&1
RESULT=$?

if [ $RESULT -eq 0 ]; then
    COUNT=$(sqlite3 "$RESTORED" "SELECT COUNT(*) FROM test;" 2>/dev/null || echo "0")
    echo "SUCCESS: v0.5.0 restored $COUNT rows from pure v0.3.x files"
    sqlite3 "$RESTORED" "SELECT * FROM test;" 2>/dev/null || echo "No data"
else
    echo "FAILED: v0.5.0 cannot restore from pure v0.3.x files (expected)"
fi

# Cleanup
rm -rf "$DB"* "$REPLICA" "$RESTORED"*
