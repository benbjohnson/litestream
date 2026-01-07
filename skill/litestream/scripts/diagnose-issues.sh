#!/usr/bin/env bash
# diagnose-issues.sh - Diagnose common Litestream issues
# Usage: ./diagnose-issues.sh [database-path]

set -e

DB_PATH="${1:-}"
CONFIG_PATH="${CONFIG_PATH:-/etc/litestream.yml}"

echo "=== Litestream Diagnostics ==="
echo

ISSUES_FOUND=0

# Helper function
check_issue() {
    local status="$1"
    local message="$2"
    if [ "$status" = "FAIL" ]; then
        echo "   [FAIL] $message"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    elif [ "$status" = "WARN" ]; then
        echo "   [WARN] $message"
    else
        echo "   [OK]   $message"
    fi
}

# 1. Check Litestream installation
echo "1. Litestream Installation"
if command -v litestream &> /dev/null; then
    VERSION=$(litestream version 2>&1 || echo "unknown")
    check_issue "OK" "Litestream installed: $VERSION"
else
    check_issue "FAIL" "Litestream not found in PATH"
fi

# 2. Check configuration
echo
echo "2. Configuration"
if [ -f "$CONFIG_PATH" ]; then
    check_issue "OK" "Config file exists: $CONFIG_PATH"

    # Check syntax
    if command -v yq &> /dev/null; then
        if yq eval '.' "$CONFIG_PATH" > /dev/null 2>&1; then
            check_issue "OK" "Config syntax valid"
        else
            check_issue "FAIL" "Config syntax invalid"
        fi
    fi

    # Check for deprecated settings
    if grep -q "wal:" "$CONFIG_PATH" 2>/dev/null; then
        check_issue "WARN" "'wal:' is deprecated, use 'ltx:'"
    fi

    if grep -q "age:" "$CONFIG_PATH" 2>/dev/null; then
        check_issue "FAIL" "Age encryption not supported in this version"
    fi
else
    check_issue "FAIL" "Config file not found: $CONFIG_PATH"
fi

# 3. Check database
echo
echo "3. Database Status"

if [ -n "$DB_PATH" ]; then
    DBS="$DB_PATH"
elif command -v litestream &> /dev/null && [ -f "$CONFIG_PATH" ]; then
    DBS=$(litestream databases -config "$CONFIG_PATH" 2>/dev/null | tail -n +2 | cut -f1 || echo "")
fi

for db in $DBS; do
    echo "   Checking: $db"

    if [ ! -f "$db" ]; then
        check_issue "WARN" "Database file not found"
        continue
    fi

    # Check if in WAL mode
    if command -v sqlite3 &> /dev/null; then
        JOURNAL_MODE=$(sqlite3 "$db" "PRAGMA journal_mode;" 2>/dev/null || echo "error")
        if [ "$JOURNAL_MODE" = "wal" ]; then
            check_issue "OK" "Database is in WAL mode"
        elif [ "$JOURNAL_MODE" = "error" ]; then
            check_issue "WARN" "Could not read journal mode"
        else
            check_issue "FAIL" "Database not in WAL mode (current: $JOURNAL_MODE)"
            echo "         Fix: Run 'sqlite3 $db \"PRAGMA journal_mode=WAL;\"'"
        fi

        # Check integrity
        INTEGRITY=$(sqlite3 "$db" "PRAGMA quick_check;" 2>/dev/null || echo "error")
        if [ "$INTEGRITY" = "ok" ]; then
            check_issue "OK" "Database integrity check passed"
        else
            check_issue "FAIL" "Database integrity check failed: $INTEGRITY"
        fi
    fi

    # Check WAL size
    WAL_PATH="${db}-wal"
    if [ -f "$WAL_PATH" ]; then
        WAL_SIZE=$(stat -f%z "$WAL_PATH" 2>/dev/null || stat -c%s "$WAL_PATH" 2>/dev/null || echo "0")
        WAL_SIZE_MB=$((WAL_SIZE / 1024 / 1024))
        if [ "$WAL_SIZE_MB" -gt 100 ]; then
            check_issue "WARN" "WAL file is large: ${WAL_SIZE_MB}MB (may need checkpoint)"
        else
            check_issue "OK" "WAL file size: ${WAL_SIZE_MB}MB"
        fi
    else
        check_issue "OK" "No WAL file (database may be idle)"
    fi

    # Check file permissions
    if [ -w "$db" ]; then
        check_issue "OK" "Database is writable"
    else
        check_issue "FAIL" "Database is not writable"
    fi

    # Check for lock page issue (>1GB databases)
    DB_SIZE=$(stat -f%z "$db" 2>/dev/null || stat -c%s "$db" 2>/dev/null || echo "0")
    if [ "$DB_SIZE" -gt 1073741824 ]; then
        check_issue "OK" "Database >1GB - lock page handling applies"
    fi
done

# 4. Check process
echo
echo "4. Process Status"
if pgrep -x litestream > /dev/null; then
    PID=$(pgrep -x litestream)
    check_issue "OK" "Litestream is running (PID: $PID)"

    # Check resource usage
    if command -v ps &> /dev/null; then
        CPU=$(ps -p "$PID" -o %cpu= 2>/dev/null | tr -d ' ')
        MEM=$(ps -p "$PID" -o %mem= 2>/dev/null | tr -d ' ')
        RSS=$(ps -p "$PID" -o rss= 2>/dev/null | tr -d ' ')
        RSS_MB=$((RSS / 1024))

        if [ "${CPU%.*}" -gt 50 ]; then
            check_issue "WARN" "High CPU usage: ${CPU}%"
        else
            check_issue "OK" "CPU usage: ${CPU}%"
        fi

        check_issue "OK" "Memory: ${RSS_MB}MB (${MEM}%)"
    fi
else
    check_issue "FAIL" "Litestream is not running"
fi

# 5. Check connectivity
echo
echo "5. Storage Connectivity"
# Try to list LTX files to verify connectivity
if [ -n "$DBS" ] && command -v litestream &> /dev/null; then
    for db in $DBS; do
        if [ -f "$db" ]; then
            if litestream ltx -config "$CONFIG_PATH" "$db" > /dev/null 2>&1; then
                check_issue "OK" "Can connect to replica for $db"
            else
                check_issue "WARN" "Cannot verify replica connectivity for $db"
            fi
        fi
    done
fi

# 6. Check disk space
echo
echo "6. Disk Space"
if [ -n "$DBS" ]; then
    for db in $DBS; do
        if [ -f "$db" ]; then
            DISK_USAGE=$(df "$(dirname "$db")" 2>/dev/null | tail -1 | awk '{print $5}' | tr -d '%')
            if [ -n "$DISK_USAGE" ]; then
                if [ "$DISK_USAGE" -gt 90 ]; then
                    check_issue "FAIL" "Disk space critical: ${DISK_USAGE}% used"
                elif [ "$DISK_USAGE" -gt 80 ]; then
                    check_issue "WARN" "Disk space low: ${DISK_USAGE}% used"
                else
                    check_issue "OK" "Disk space: ${DISK_USAGE}% used"
                fi
            fi
            break
        fi
    done
fi

# 7. Check recent logs
echo
echo "7. Recent Errors"
if command -v journalctl &> /dev/null; then
    ERRORS=$(journalctl -u litestream --since "1 hour ago" -p err 2>/dev/null | grep -v "^--" | tail -5)
    if [ -n "$ERRORS" ]; then
        check_issue "WARN" "Recent errors found in logs:"
        echo "$ERRORS" | sed 's/^/         /'
    else
        check_issue "OK" "No recent errors in systemd logs"
    fi
else
    check_issue "OK" "Systemd logs not available"
fi

# Summary
echo
echo "=== Diagnostics Summary ==="
if [ "$ISSUES_FOUND" -gt 0 ]; then
    echo "Found $ISSUES_FOUND issue(s) that may need attention"
    echo
    echo "Common fixes:"
    echo "  - Enable WAL mode: sqlite3 db.sqlite 'PRAGMA journal_mode=WAL;'"
    echo "  - Start Litestream: litestream replicate -config $CONFIG_PATH"
    echo "  - Check credentials: verify AWS_ACCESS_KEY_ID, etc."
    echo "  - View logs: journalctl -u litestream -f"
else
    echo "No issues found"
fi

echo
echo "=== Diagnostics Complete ==="
exit $ISSUES_FOUND
