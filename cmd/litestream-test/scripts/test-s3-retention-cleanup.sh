#!/bin/bash
set -e

# Test S3 LTX file retention and cleanup behavior
# This script helps verify that old LTX files are properly cleaned up

echo "=========================================="
echo "S3 LTX File Retention Cleanup Test"
echo "=========================================="
echo ""
echo "Testing that old LTX files are cleaned up after retention period"
echo ""

# Check for required tools
if ! command -v aws &> /dev/null; then
    echo "⚠️  AWS CLI not found. Install with: brew install awscli"
    echo "   This test can still run but S3 bucket inspection will be limited"
    AWS_AVAILABLE=false
else
    AWS_AVAILABLE=true
fi

# Configuration
DB="/tmp/retention-test.db"
LITESTREAM="./bin/litestream"
LITESTREAM_TEST="./bin/litestream-test"

# S3 Configuration (modify these for your bucket)
S3_BUCKET="${LITESTREAM_S3_BUCKET:-your-test-bucket}"
S3_PREFIX="${LITESTREAM_S3_PREFIX:-litestream-retention-test}"
S3_REGION="${LITESTREAM_S3_REGION:-us-east-1}"

if [ "$S3_BUCKET" = "your-test-bucket" ]; then
    echo "⚠️  Please set S3 environment variables:"
    echo "   export LITESTREAM_S3_BUCKET=your-bucket-name"
    echo "   export LITESTREAM_S3_ACCESS_KEY_ID=your-key"
    echo "   export LITESTREAM_S3_SECRET_ACCESS_KEY=your-secret"
    echo "   export LITESTREAM_S3_REGION=your-region"
    echo ""
    echo "Or update the script with your S3 bucket details"
    echo ""
    read -p "Continue with example bucket name? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 0
    fi
fi

echo "S3 Configuration:"
echo "  Bucket: $S3_BUCKET"
echo "  Prefix: $S3_PREFIX"
echo "  Region: $S3_REGION"
echo ""

# Cleanup function
cleanup() {
    pkill -f "litestream replicate.*retention-test.db" 2>/dev/null || true
    rm -f "$DB"* /tmp/retention-*.log /tmp/retention-*.yml
}

trap cleanup EXIT
cleanup

echo "=========================================="
echo "Test 1: Short Retention Period (2 minutes)"
echo "=========================================="

echo "[1] Creating test database..."
sqlite3 "$DB" <<EOF
PRAGMA journal_mode = WAL;
CREATE TABLE retention_test (
    id INTEGER PRIMARY KEY,
    batch INTEGER,
    data BLOB,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO retention_test (batch, data) VALUES (0, randomblob(1000));
EOF

echo "  ✓ Database created with initial data"

# Create retention config with short retention period
cat > /tmp/retention-config.yml <<EOF
dbs:
  - path: $DB
    replicas:
      - type: s3
        bucket: $S3_BUCKET
        path: $S3_PREFIX
        region: $S3_REGION
        retention: 2m
        sync-interval: 10s
EOF

echo ""
echo "[2] Starting replication with 2-minute retention..."
$LITESTREAM replicate -config /tmp/retention-config.yml > /tmp/retention-test.log 2>&1 &
REPL_PID=$!
sleep 5

if ! kill -0 $REPL_PID 2>/dev/null; then
    echo "  ✗ Replication failed to start"
    cat /tmp/retention-test.log
    exit 1
fi

echo "  ✓ Replication started (PID: $REPL_PID)"

echo ""
echo "[3] Generating LTX files over time..."

# Generate files in batches to create multiple LTX files
for batch in {1..6}; do
    echo "  Batch $batch: Adding data and checkpointing..."

    # Add data
    for i in {1..5}; do
        sqlite3 "$DB" "INSERT INTO retention_test (batch, data) VALUES ($batch, randomblob(2000));"
    done

    # Force checkpoint to create LTX files
    sqlite3 "$DB" "PRAGMA wal_checkpoint(FULL);"

    # Show current record count
    RECORD_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM retention_test;")
    echo "    Records: $RECORD_COUNT"

    # Wait between batches
    sleep 20
done

echo ""
echo "[4] Waiting for retention cleanup (4 minutes total)..."
echo "  Files should start being cleaned up after 2 minutes..."

# Monitor for 4 minutes to see cleanup
for minute in {1..4}; do
    echo "  Minute $minute/4..."
    sleep 60

    # Check for cleanup activity in logs
    CLEANUP_ACTIVITY=$(grep -i "clean\\|delet\\|expir\\|retention\\|removed" /tmp/retention-test.log 2>/dev/null | wc -l)
    echo "    Cleanup log entries: $CLEANUP_ACTIVITY"
done

echo ""
echo "[5] Stopping replication and analyzing results..."
kill $REPL_PID 2>/dev/null
wait $REPL_PID 2>/dev/null

# Analyze logs for retention behavior
echo ""
echo "Retention Analysis:"
echo "=================="

TOTAL_ERRORS=$(grep -c "ERROR" /tmp/retention-test.log 2>/dev/null || echo "0")
CLEANUP_MSGS=$(grep -c -i "clean\\|delet\\|expir\\|retention\\|removed" /tmp/retention-test.log 2>/dev/null || echo "0")
SYNC_COUNT=$(grep -c "sync" /tmp/retention-test.log 2>/dev/null || echo "0")

echo "Log summary:"
echo "  Total errors: $TOTAL_ERRORS"
echo "  Cleanup messages: $CLEANUP_MSGS"
echo "  Sync operations: $SYNC_COUNT"

if [ "$CLEANUP_MSGS" -gt "0" ]; then
    echo ""
    echo "Cleanup activity found:"
    grep -i "clean\\|delet\\|expir\\|retention\\|removed" /tmp/retention-test.log | head -10
else
    echo ""
    echo "⚠️  No explicit cleanup messages found"
    echo "   Note: Litestream may perform silent cleanup"
fi

if [ "$TOTAL_ERRORS" -gt "0" ]; then
    echo ""
    echo "Errors encountered:"
    grep "ERROR" /tmp/retention-test.log | tail -5
fi

echo ""
echo "=========================================="
echo "Test 2: S3 Bucket Inspection (if available)"
echo "=========================================="

if [ "$AWS_AVAILABLE" = true ] && [ "$S3_BUCKET" != "your-test-bucket" ]; then
    echo "[6] Inspecting S3 bucket contents..."

    # Try to list S3 objects
    if aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/" --recursive 2>/dev/null; then
        echo ""
        echo "S3 object analysis:"
        TOTAL_OBJECTS=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/" --recursive 2>/dev/null | wc -l)
        LTX_OBJECTS=$(aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/" --recursive 2>/dev/null | grep -c "\.ltx" || echo "0")

        echo "  Total objects: $TOTAL_OBJECTS"
        echo "  LTX files: $LTX_OBJECTS"

        if [ "$LTX_OBJECTS" -gt "0" ]; then
            echo ""
            echo "Recent LTX files:"
            aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/" --recursive 2>/dev/null | grep "\.ltx" | tail -5
        fi

        echo ""
        echo "File age analysis:"
        aws s3 ls "s3://$S3_BUCKET/$S3_PREFIX/" --recursive 2>/dev/null | \
        awk '{print $1" "$2" "$4}' | sort

    else
        echo "  ⚠️  Unable to access S3 bucket (check credentials/permissions)"
    fi
else
    echo "⚠️  S3 inspection skipped (AWS CLI not available or bucket not configured)"
fi

echo ""
echo "=========================================="
echo "Manual S3 Inspection Commands"
echo "=========================================="
echo ""
echo "To manually check S3 bucket contents, use:"
echo ""
echo "# List all objects in the prefix"
echo "aws s3 ls s3://$S3_BUCKET/$S3_PREFIX/ --recursive"
echo ""
echo "# Count LTX files"
echo "aws s3 ls s3://$S3_BUCKET/$S3_PREFIX/ --recursive | grep -c '\.ltx'"
echo ""
echo "# Show file ages"
echo "aws s3 ls s3://$S3_BUCKET/$S3_PREFIX/ --recursive | sort"
echo ""
echo "# Clean up test files"
echo "aws s3 rm s3://$S3_BUCKET/$S3_PREFIX/ --recursive"
echo ""

FINAL_RECORDS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM retention_test;" 2>/dev/null || echo "unknown")
echo "Final Results:"
echo "=============="
echo "Database records: $FINAL_RECORDS"
echo "Test duration: ~6 minutes"
echo "Expected behavior: Old LTX files (>2min) should be cleaned up"
echo ""
echo "Key files for debugging:"
echo "  - Replication log: /tmp/retention-test.log"
echo "  - Config file: /tmp/retention-config.yml"
echo "  - S3 path: s3://$S3_BUCKET/$S3_PREFIX/"
echo ""
echo "If no cleanup was observed:"
echo "  1. Check if retention period is working correctly"
echo "  2. Verify S3 bucket policy allows DELETE operations"
echo "  3. Increase logging verbosity in Litestream"
echo "  4. Use longer test duration for larger retention periods"
echo "=========================================="
