#!/usr/bin/env bash
# check-replication.sh - Check Litestream replication status
# Usage: ./check-replication.sh [database-path]

set -e

DB_PATH="${1:-}"
CONFIG_PATH="${CONFIG_PATH:-/etc/litestream.yml}"

echo "=== Litestream Replication Status Check ==="
echo

# Check if Litestream is running
echo "1. Checking Litestream process..."
if pgrep -x litestream > /dev/null; then
    PID=$(pgrep -x litestream)
    echo "   Litestream is RUNNING (PID: $PID)"
else
    echo "   Litestream is NOT RUNNING"
    echo "   Start with: litestream replicate -config $CONFIG_PATH"
fi

# Check if Litestream command is available
if ! command -v litestream &> /dev/null; then
    echo
    echo "ERROR: litestream command not found in PATH"
    exit 1
fi

# Show status
echo
echo "2. Database status..."
if [ -n "$DB_PATH" ]; then
    litestream status -config "$CONFIG_PATH" "$DB_PATH" 2>/dev/null || echo "   Unable to get status"
else
    litestream status -config "$CONFIG_PATH" 2>/dev/null || echo "   Unable to get status"
fi

# List configured databases
echo
echo "3. Configured databases..."
litestream databases -config "$CONFIG_PATH" 2>/dev/null || echo "   Unable to list databases"

# Check WAL files
echo
echo "4. Checking WAL files..."
if [ -n "$DB_PATH" ]; then
    DBS="$DB_PATH"
else
    # Get database paths from config
    DBS=$(litestream databases -config "$CONFIG_PATH" 2>/dev/null | tail -n +2 | cut -f1)
fi

for db in $DBS; do
    if [ -f "$db" ]; then
        echo "   Database: $db"

        # Check WAL file
        WAL_PATH="${db}-wal"
        if [ -f "$WAL_PATH" ]; then
            WAL_SIZE=$(ls -lh "$WAL_PATH" | awk '{print $5}')
            echo "      WAL file: $WAL_SIZE"
        else
            echo "      WAL file: Not present"
        fi

        # Check database size
        DB_SIZE=$(ls -lh "$db" | awk '{print $5}')
        echo "      DB size: $DB_SIZE"

        # Check metadata directory
        META_DIR=$(dirname "$db")/.litestream
        if [ -d "$META_DIR" ]; then
            L0_COUNT=$(find "$META_DIR" -name "*.ltx" -path "*/0/*" 2>/dev/null | wc -l)
            echo "      Local L0 files: $L0_COUNT"
        else
            echo "      Metadata dir: Not found"
        fi
    else
        echo "   Database: $db (NOT FOUND)"
    fi
done

# Check LTX files (if database specified)
if [ -n "$DB_PATH" ] && [ -f "$DB_PATH" ]; then
    echo
    echo "5. Recent LTX files..."
    litestream ltx -config "$CONFIG_PATH" "$DB_PATH" 2>/dev/null | head -11 || echo "   Unable to list LTX files"
fi

# Check system resources
echo
echo "6. System resources..."
echo "   Disk space:"
df -h $(dirname "${DB_PATH:-/var/lib}") 2>/dev/null | tail -1 | awk '{print "      Used: " $3 " / " $2 " (" $5 ")"}'

if pgrep -x litestream > /dev/null; then
    PID=$(pgrep -x litestream)
    echo "   Litestream memory:"
    ps -p "$PID" -o rss= 2>/dev/null | awk '{printf "      RSS: %.1f MB\n", $1/1024}'
fi

# Check metrics endpoint if configured
echo
echo "7. Metrics endpoint..."
METRICS_ADDR=$(grep -E "^addr:" "$CONFIG_PATH" 2>/dev/null | head -1 | awk '{print $2}' | tr -d '"')
if [ -n "$METRICS_ADDR" ]; then
    # Handle :port format
    if [[ "$METRICS_ADDR" == :* ]]; then
        METRICS_URL="http://localhost${METRICS_ADDR}/metrics"
    else
        METRICS_URL="http://${METRICS_ADDR}/metrics"
    fi

    if curl -s --max-time 2 "$METRICS_URL" > /dev/null 2>&1; then
        echo "   Metrics available at: $METRICS_URL"
        echo "   Sample metrics:"
        curl -s --max-time 2 "$METRICS_URL" 2>/dev/null | grep -E "^litestream_" | head -5 | sed 's/^/      /'
    else
        echo "   Metrics endpoint configured but not responding"
    fi
else
    echo "   No metrics endpoint configured"
fi

# Summary
echo
echo "=== Summary ==="
if pgrep -x litestream > /dev/null; then
    echo "Status: Litestream is running"
else
    echo "Status: Litestream is NOT running"
    echo "Action: Start with 'litestream replicate'"
fi

echo
echo "=== Check Complete ==="
