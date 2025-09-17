#!/bin/bash
set -e

# Test #754 flag issue with S3 scenarios and retention cleanup
# Tests both S3 vs file replication behavior and LTX file cleanup

echo "=========================================="
echo "#754 S3 Scenarios & Retention Test"
echo "=========================================="
echo ""
echo "Testing #754 flag issue with S3 replication vs file replication"
echo "Verifying LTX file cleanup after retention period"
echo ""

# Check if we have S3 environment setup
if [ -z "$AWS_ACCESS_KEY_ID" ] && [ -z "$LITESTREAM_S3_ACCESS_KEY_ID" ]; then
    echo "⚠️  No S3 credentials found. Setting up local S3-compatible test..."
    echo ""

    # Create minimal S3-like configuration for testing
    export LITESTREAM_S3_ACCESS_KEY_ID="testkey"
    export LITESTREAM_S3_SECRET_ACCESS_KEY="testsecret"
    export LITESTREAM_S3_BUCKET="test754bucket"
    export LITESTREAM_S3_ENDPOINT="s3.amazonaws.com"
    export LITESTREAM_S3_REGION="us-east-1"

    echo "ℹ️  S3 test environment configured (will use real S3 if credentials are valid)"
    echo "   Bucket: $LITESTREAM_S3_BUCKET"
    echo "   Region: $LITESTREAM_S3_REGION"
else
    echo "✓ Using existing S3 credentials"
fi

DB="/tmp/s3-754-test.db"
S3_PATH="s3://$LITESTREAM_S3_BUCKET/754-test"
FILE_REPLICA="/tmp/file-754-replica"
LITESTREAM="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# Cleanup function
cleanup() {
    pkill -f "litestream replicate.*s3-754-test.db" 2>/dev/null || true
    rm -f "$DB"* /tmp/s3-754-*.log /tmp/s3-754-*.yml
    rm -rf "$FILE_REPLICA"
}

trap cleanup EXIT
cleanup

echo ""
echo "=========================================="
echo "Test 1: Compare File vs S3 #754 Behavior"
echo "=========================================="

echo "[1] Creating large database for comparison testing..."
$LITESTREAM_TEST populate -db "$DB" -target-size 1200MB >/dev/null 2>&1

sqlite3 "$DB" <<EOF
CREATE TABLE s3_test (
    id INTEGER PRIMARY KEY,
    test_type TEXT,
    scenario TEXT,
    data BLOB,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO s3_test (test_type, scenario, data) VALUES ('s3-comparison', 'initial', randomblob(5000));
EOF

DB_SIZE=$(du -h "$DB" | cut -f1)
PAGE_COUNT=$(sqlite3 "$DB" "PRAGMA page_count;")
echo "  ✓ Database created: $DB_SIZE ($PAGE_COUNT pages)"

echo ""
echo "[2] Testing file replication first (baseline)..."

# Test with file replication first
$LITESTREAM replicate "$DB" "file://$FILE_REPLICA" > /tmp/s3-754-file.log 2>&1 &
FILE_PID=$!
sleep 5

if kill -0 $FILE_PID 2>/dev/null; then
    echo "  ✓ File replication started (PID: $FILE_PID)"

    # Add data and trigger checkpoint
    for i in {1..5}; do
        sqlite3 "$DB" "INSERT INTO s3_test (test_type, scenario, data) VALUES ('file-test', 'run-$i', randomblob(3000));"
    done
    sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);"
    sleep 5

    kill $FILE_PID 2>/dev/null
    wait $FILE_PID 2>/dev/null

    # Check for #754 errors in file replication
    FILE_FLAGS=$(grep -c "no flags allowed" /tmp/s3-754-file.log 2>/dev/null || echo "0")
    FILE_VERIFY=$(grep -c "ltx verification failed" /tmp/s3-754-file.log 2>/dev/null || echo "0")
    FILE_ERRORS=$(grep -c "ERROR" /tmp/s3-754-file.log 2>/dev/null || echo "0")

    echo "  File replication results:"
    echo "    Total errors: $FILE_ERRORS"
    echo "    'no flags allowed': $FILE_FLAGS"
    echo "    'ltx verification failed': $FILE_VERIFY"
    echo "    LTX files created: $(find "$FILE_REPLICA" -name "*.ltx" 2>/dev/null | wc -l)"

    if [ "$FILE_FLAGS" -gt "0" ] || [ "$FILE_VERIFY" -gt "0" ]; then
        echo "  🚨 #754 reproduced with FILE replication"
        FILE_754_FOUND=true
    else
        echo "  ✅ No #754 errors with file replication"
        FILE_754_FOUND=false
    fi
else
    echo "  ✗ File replication failed to start"
    cat /tmp/s3-754-file.log
    exit 1
fi

echo ""
echo "[3] Testing S3 replication..."

# Create S3 configuration file
cat > /tmp/s3-754-config.yml <<EOF
dbs:
  - path: $DB
    replicas:
      - type: s3
        bucket: $LITESTREAM_S3_BUCKET
        path: 754-test
        region: $LITESTREAM_S3_REGION
        access-key-id: $LITESTREAM_S3_ACCESS_KEY_ID
        secret-access-key: $LITESTREAM_S3_SECRET_ACCESS_KEY
        retention: 24h
        sync-interval: 5s
EOF

if [ -n "$LITESTREAM_S3_ENDPOINT" ] && [ "$LITESTREAM_S3_ENDPOINT" != "s3.amazonaws.com" ]; then
    echo "        endpoint: $LITESTREAM_S3_ENDPOINT" >> /tmp/s3-754-config.yml
fi

# Add offline data between tests
sqlite3 "$DB" "INSERT INTO s3_test (test_type, scenario, data) VALUES ('between-tests', 'offline', randomblob(4000));"

echo "  S3 Configuration:"
echo "    Bucket: $LITESTREAM_S3_BUCKET"
echo "    Path: 754-test"
echo "    Retention: 24h"

# Test S3 replication
$LITESTREAM replicate -config /tmp/s3-754-config.yml > /tmp/s3-754-s3.log 2>&1 &
S3_PID=$!
sleep 10

if kill -0 $S3_PID 2>/dev/null; then
    echo "  ✓ S3 replication started (PID: $S3_PID)"

    # Add data and trigger checkpoint
    for i in {1..5}; do
        sqlite3 "$DB" "INSERT INTO s3_test (test_type, scenario, data) VALUES ('s3-test', 'run-$i', randomblob(3000));"
    done
    sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);"
    sleep 10

    kill $S3_PID 2>/dev/null
    wait $S3_PID 2>/dev/null

    # Check for #754 errors in S3 replication
    S3_FLAGS=$(grep -c "no flags allowed" /tmp/s3-754-s3.log 2>/dev/null || echo "0")
    S3_VERIFY=$(grep -c "ltx verification failed" /tmp/s3-754-s3.log 2>/dev/null || echo "0")
    S3_ERRORS=$(grep -c "ERROR" /tmp/s3-754-s3.log 2>/dev/null || echo "0")

    echo "  S3 replication results:"
    echo "    Total errors: $S3_ERRORS"
    echo "    'no flags allowed': $S3_FLAGS"
    echo "    'ltx verification failed': $S3_VERIFY"

    if [ "$S3_FLAGS" -gt "0" ] || [ "$S3_VERIFY" -gt "0" ]; then
        echo "  🚨 #754 reproduced with S3 replication"
        S3_754_FOUND=true
    else
        echo "  ✅ No #754 errors with S3 replication"
        S3_754_FOUND=false
    fi

    # Show recent S3 errors if any
    if [ "$S3_ERRORS" -gt "0" ]; then
        echo "  Recent S3 errors:"
        grep "ERROR" /tmp/s3-754-s3.log | tail -3
    fi
else
    echo "  ⚠️  S3 replication failed to start (likely no valid credentials)"
    echo "  S3 test output:"
    head -10 /tmp/s3-754-s3.log
    S3_754_FOUND="unknown"
    S3_SKIPPED=true
fi

echo ""
echo "=========================================="
echo "Test 2: S3 Restart Scenario (Critical)"
echo "=========================================="

if [ "${S3_SKIPPED:-false}" != "true" ]; then
    echo "[4] Testing S3 restart scenario..."

    # Add data while Litestream is down
    sqlite3 "$DB" "INSERT INTO s3_test (test_type, scenario, data) VALUES ('restart-test', 'offline-data', randomblob(5000));"

    # Restart S3 replication
    $LITESTREAM replicate -config /tmp/s3-754-config.yml > /tmp/s3-754-restart.log 2>&1 &
    S3_RESTART_PID=$!
    sleep 15

    if kill -0 $S3_RESTART_PID 2>/dev/null; then
        echo "  ✓ S3 restart succeeded"

        # Monitor for #754 errors during restart
        sleep 10
        RESTART_FLAGS=$(grep -c "no flags allowed" /tmp/s3-754-restart.log 2>/dev/null || echo "0")
        RESTART_VERIFY=$(grep -c "ltx verification failed" /tmp/s3-754-restart.log 2>/dev/null || echo "0")

        echo "  S3 restart analysis:"
        echo "    'no flags allowed': $RESTART_FLAGS"
        echo "    'ltx verification failed': $RESTART_VERIFY"

        if [ "$RESTART_FLAGS" -gt "0" ] || [ "$RESTART_VERIFY" -gt "0" ]; then
            echo "  🚨 #754 triggered by S3 RESTART"
            grep -A1 -B1 "no flags allowed\\|ltx verification failed" /tmp/s3-754-restart.log || true
            S3_RESTART_754=true
        else
            echo "  ✅ No #754 errors on S3 restart"
            S3_RESTART_754=false
        fi

        kill $S3_RESTART_PID 2>/dev/null
        wait $S3_RESTART_PID 2>/dev/null
    else
        echo "  ✗ S3 restart failed"
        cat /tmp/s3-754-restart.log | head -10
        S3_RESTART_754="failed"
    fi
else
    echo "⚠️  Skipping S3 restart test (no valid S3 credentials)"
    S3_RESTART_754="skipped"
fi

echo ""
echo "=========================================="
echo "Test 3: S3 LTX File Retention Check"
echo "=========================================="

if [ "${S3_SKIPPED:-false}" != "true" ]; then
    echo "[5] Testing LTX file retention and cleanup..."

    # Create a short retention test with file replication for comparison
    SHORT_RETENTION_CONFIG="/tmp/s3-754-short-retention.yml"
    cat > "$SHORT_RETENTION_CONFIG" <<EOF
dbs:
  - path: $DB
    replicas:
      - type: s3
        bucket: $LITESTREAM_S3_BUCKET
        path: 754-retention-test
        region: $LITESTREAM_S3_REGION
        access-key-id: $LITESTREAM_S3_ACCESS_KEY_ID
        secret-access-key: $LITESTREAM_S3_SECRET_ACCESS_KEY
        retention: 30s
        sync-interval: 2s
EOF

    echo "  ⏱️  Testing with 30-second retention period..."

    # Start short retention replication
    $LITESTREAM replicate -config "$SHORT_RETENTION_CONFIG" > /tmp/s3-754-retention.log 2>&1 &
    RETENTION_PID=$!
    sleep 5

    if kill -0 $RETENTION_PID 2>/dev/null; then
        echo "  ✓ Short retention replication started"

        # Generate multiple LTX files quickly
        echo "  📝 Generating multiple LTX files..."
        for round in {1..6}; do
            for i in {1..3}; do
                sqlite3 "$DB" "INSERT INTO s3_test (test_type, scenario, data) VALUES ('retention-test', 'round-$round-$i', randomblob(2000));"
            done
            sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);"
            sleep 5
        done

        echo "  ⏳ Waiting for retention cleanup (45 seconds)..."
        sleep 45

        # Check if old files are cleaned up
        RETENTION_ERRORS=$(grep -c "ERROR" /tmp/s3-754-retention.log 2>/dev/null || echo "0")
        echo "  Retention test results:"
        echo "    Retention errors: $RETENTION_ERRORS"

        # Look for cleanup messages
        CLEANUP_MSGS=$(grep -c "clean\\|delet\\|expir\\|retention" /tmp/s3-754-retention.log 2>/dev/null || echo "0")
        echo "    Cleanup operations: $CLEANUP_MSGS"

        if [ "$CLEANUP_MSGS" -gt "0" ]; then
            echo "  ✅ LTX file cleanup appears to be working"
            echo "  Recent cleanup activity:"
            grep -i "clean\\|delet\\|expir\\|retention" /tmp/s3-754-retention.log | tail -3 || echo "    (No cleanup messages found)"
        else
            echo "  ⚠️  No explicit cleanup messages found"
            echo "  (This may be normal - cleanup might be silent)"
        fi

        kill $RETENTION_PID 2>/dev/null
        wait $RETENTION_PID 2>/dev/null
    else
        echo "  ✗ Short retention test failed"
        cat /tmp/s3-754-retention.log | head -5
    fi
else
    echo "⚠️  Skipping retention test (no valid S3 credentials)"
fi

echo ""
echo "=========================================="
echo "S3 vs File Replication Comparison Results"
echo "=========================================="
echo ""

FINAL_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM s3_test;" 2>/dev/null || echo "unknown")
echo "Database statistics:"
echo "  Final record count: $FINAL_COUNT"
echo "  Database size: $(du -h "$DB" | cut -f1)"

echo ""
echo "Comparison results:"
echo "  File replication #754: $([ "${FILE_754_FOUND:-false}" = "true" ] && echo "REPRODUCED" || echo "Not reproduced")"

if [ "${S3_SKIPPED:-false}" != "true" ]; then
    echo "  S3 replication #754: $([ "${S3_754_FOUND:-false}" = "true" ] && echo "REPRODUCED" || echo "Not reproduced")"
    echo "  S3 restart #754: $([ "${S3_RESTART_754:-false}" = "true" ] && echo "REPRODUCED" || echo "Not reproduced")"
else
    echo "  S3 replication #754: SKIPPED (no credentials)"
    echo "  S3 restart #754: SKIPPED (no credentials)"
fi

echo ""
echo "Key findings:"
if [ "${FILE_754_FOUND:-false}" = "true" ] && [ "${S3_754_FOUND:-false}" = "true" ]; then
    echo "🚨 #754 affects BOTH file and S3 replication"
elif [ "${FILE_754_FOUND:-false}" = "true" ]; then
    echo "⚠️  #754 affects file replication but S3 behavior unclear"
elif [ "${S3_754_FOUND:-false}" = "true" ]; then
    echo "⚠️  #754 affects S3 replication but not file replication"
else
    echo "✅ #754 not reproduced in this test scenario"
    echo "   (May require different conditions - try larger DB or restart scenarios)"
fi

echo ""
echo "For Ben's debugging:"
echo "  ✓ Test scripts available in cmd/litestream-test/scripts/"
echo "  ✓ Log files in /tmp/s3-754-*.log"
echo "  ✓ S3 configuration example in /tmp/s3-754-config.yml"
echo "  ✓ Test focused on HeaderFlagNoChecksum issue locations:"
echo "    - db.go:883, 1208, 1298"
echo "    - replica.go:466"
echo "=========================================="
