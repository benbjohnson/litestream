#!/bin/bash
set -e

# Test script for measuring Litestream idle CPU usage with S3 replication

DURATION=${1:-300}  # Default 5 minutes
CONFIG_FILE="litestream-test-polling.yml"
MODE_DESC="Polling mode (1s interval)"

echo "========================================="
echo "Litestream CPU Usage Test"
echo "========================================="
echo "Mode: $MODE_DESC"
echo "Config: $CONFIG_FILE"
echo "Duration: ${DURATION}s"
echo "========================================="

# Create test database
echo "Creating test database..."
rm -f /tmp/test.db /tmp/test.db-wal /tmp/test.db-shm
sqlite3 /tmp/test.db "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT);"
sqlite3 /tmp/test.db "INSERT INTO test (data) VALUES ('test');"

# Start Litestream in background
echo "Starting Litestream..."
# Get script directory and repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$REPO_ROOT/.envrc"
"$REPO_ROOT/bin/litestream" replicate -config "$SCRIPT_DIR/$CONFIG_FILE" &
LITESTREAM_PID=$!

echo "Litestream PID: $LITESTREAM_PID"
echo ""
echo "Monitoring CPU usage for ${DURATION}s..."
echo "Press Ctrl+C to stop early"
echo ""

# Monitor CPU usage
echo "Time,CPU%,VSZ,RSS" > /tmp/litestream-cpu-log.csv
for i in $(seq 1 $DURATION); do
    if ! kill -0 $LITESTREAM_PID 2>/dev/null; then
        echo "ERROR: Litestream process died!"
        exit 1
    fi

    # Get CPU and memory stats
    CPU=$(ps -p $LITESTREAM_PID -o %cpu= | xargs)
    VSZ=$(ps -p $LITESTREAM_PID -o vsz= | xargs)
    RSS=$(ps -p $LITESTREAM_PID -o rss= | xargs)

    echo "$i,$CPU,$VSZ,$RSS" >> /tmp/litestream-cpu-log.csv

    # Display every 10 seconds
    if [ $((i % 10)) -eq 0 ]; then
        echo "[$i/${DURATION}s] CPU: ${CPU}%  VSZ: ${VSZ}KB  RSS: ${RSS}KB"
    fi

    sleep 1
done

# Stop Litestream
echo ""
echo "Stopping Litestream..."
kill $LITESTREAM_PID
wait $LITESTREAM_PID 2>/dev/null || true

# Calculate average CPU
echo ""
echo "========================================="
echo "Results"
echo "========================================="
AVG_CPU=$(awk -F',' 'NR>1 {sum+=$2; count++} END {if(count>0) print sum/count; else print 0}' /tmp/litestream-cpu-log.csv)
echo "Average CPU: ${AVG_CPU}%"
echo "Detailed log: /tmp/litestream-cpu-log.csv"
echo ""

# Show sample of S3 uploads
echo "S3 Bucket Contents:"
aws s3 ls s3://sprite-litestream-debugging/test-db-${CONFIG_MODE}/ --recursive | head -10
