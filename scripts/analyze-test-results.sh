#!/bin/bash
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <test-directory>"
    echo ""
    echo "Analyzes overnight test results from the specified test directory."
    echo ""
    echo "Example:"
    echo "  $0 /tmp/litestream-overnight-20240924-120000"
    exit 1
fi

TEST_DIR="$1"

if [ ! -d "$TEST_DIR" ]; then
    echo "Error: Test directory does not exist: $TEST_DIR"
    exit 1
fi

LOG_DIR="$TEST_DIR/logs"
ANALYSIS_REPORT="$TEST_DIR/analysis-report.txt"

echo "================================================"
echo "Litestream Test Analysis Report"
echo "================================================"
echo "Test directory: $TEST_DIR"
echo "Analysis time: $(date)"
echo ""

{
    echo "================================================"
    echo "Litestream Test Analysis Report"
    echo "================================================"
    echo "Test directory: $TEST_DIR"
    echo "Analysis time: $(date)"
    echo ""

    echo "1. TEST DURATION AND TIMELINE"
    echo "=============================="
    if [ -f "$LOG_DIR/litestream.log" ]; then
        START_TIME=$(head -1 "$LOG_DIR/litestream.log" 2>/dev/null | grep -oE '[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' | head -1 || echo "Unknown")
        END_TIME=$(tail -1 "$LOG_DIR/litestream.log" 2>/dev/null | grep -oE '[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' | head -1 || echo "Unknown")
        echo "Start time: $START_TIME"
        echo "End time: $END_TIME"

        # Calculate duration if possible
        if command -v python3 >/dev/null 2>&1; then
            DURATION=$(python3 -c "
from datetime import datetime
try:
    start = datetime.strptime('$START_TIME', '%Y/%m/%d %H:%M:%S')
    end = datetime.strptime('$END_TIME', '%Y/%m/%d %H:%M:%S')
    duration = end - start
    hours = duration.total_seconds() / 3600
    print(f'Duration: {hours:.2f} hours')
except:
    print('Duration: Unable to calculate')
" 2>/dev/null || echo "Duration: Unable to calculate")
            echo "$DURATION"
        fi
    fi
    echo ""

    echo "2. DATABASE STATISTICS"
    echo "======================"
    if [ -f "$TEST_DIR/test.db" ]; then
        DB_SIZE=$(stat -f%z "$TEST_DIR/test.db" 2>/dev/null || stat -c%s "$TEST_DIR/test.db" 2>/dev/null || echo "0")
        echo "Final database size: $(numfmt --to=iec-i --suffix=B $DB_SIZE 2>/dev/null || echo "$DB_SIZE bytes")"

        # Get row count if database is accessible
        ROW_COUNT=$(sqlite3 "$TEST_DIR/test.db" "SELECT COUNT(*) FROM test_data" 2>/dev/null || echo "Unknown")
        echo "Total rows inserted: $ROW_COUNT"

        # Get page statistics
        PAGE_COUNT=$(sqlite3 "$TEST_DIR/test.db" "PRAGMA page_count" 2>/dev/null || echo "Unknown")
        PAGE_SIZE=$(sqlite3 "$TEST_DIR/test.db" "PRAGMA page_size" 2>/dev/null || echo "Unknown")
        echo "Database pages: $PAGE_COUNT (page size: $PAGE_SIZE bytes)"
    fi
    echo ""

    echo "3. REPLICATION STATISTICS"
    echo "========================="
    if [ -d "$TEST_DIR/replica" ]; then
        SNAPSHOT_COUNT=$(find "$TEST_DIR/replica" -name "*.snapshot.lz4" 2>/dev/null | wc -l | tr -d ' ')
        WAL_COUNT=$(find "$TEST_DIR/replica" -name "*.wal.lz4" 2>/dev/null | wc -l | tr -d ' ')
        REPLICA_SIZE=$(du -sh "$TEST_DIR/replica" 2>/dev/null | cut -f1)

        echo "Snapshots created: $SNAPSHOT_COUNT"
        echo "WAL segments created: $WAL_COUNT"
        echo "Total replica size: $REPLICA_SIZE"

        # Analyze snapshot intervals
        if [ "$SNAPSHOT_COUNT" -gt 1 ]; then
            echo ""
            echo "Snapshot creation times:"
            find "$TEST_DIR/replica" -name "*.snapshot.lz4" -exec stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" {} \; 2>/dev/null | sort || \
            find "$TEST_DIR/replica" -name "*.snapshot.lz4" -exec stat -c "%y" {} \; 2>/dev/null | cut -d. -f1 | sort || echo "Unable to get timestamps"
        fi
    fi
    echo ""

    echo "4. COMPACTION ANALYSIS"
    echo "======================"
    if [ -f "$LOG_DIR/litestream.log" ]; then
        COMPACTION_COUNT=$(grep -c "compacting" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        echo "Total compaction operations: $COMPACTION_COUNT"

        # Count compactions by level
        echo ""
        echo "Compactions by retention level:"
        grep "compacting" "$LOG_DIR/litestream.log" 2>/dev/null | grep -oE "retention=[0-9]+[hms]+" | sort | uniq -c | sort -rn || echo "No compaction data found"

        # Show compaction timing patterns
        echo ""
        echo "Compaction frequency (last 10):"
        grep "compacting" "$LOG_DIR/litestream.log" 2>/dev/null | tail -10 | grep -oE "[0-9]{2}:[0-9]{2}:[0-9]{2}" || echo "No timing data"
    fi
    echo ""

    echo "5. LOAD GENERATOR PERFORMANCE"
    echo "============================="
    if [ -f "$LOG_DIR/load.log" ]; then
        # Extract final statistics
        FINAL_STATS=$(tail -20 "$LOG_DIR/load.log" | grep "Load generation complete" -A 10 || echo "")
        if [ -n "$FINAL_STATS" ]; then
            echo "$FINAL_STATS"
        else
            # Try to get statistics from progress logs
            echo "Load generator statistics:"
            grep "Load statistics" "$LOG_DIR/load.log" | tail -5 || echo "No statistics found"
        fi
    fi
    echo ""

    echo "6. ERROR ANALYSIS"
    echo "================="
    ERROR_COUNT=0
    WARNING_COUNT=0

    if [ -f "$LOG_DIR/litestream.log" ]; then
        ERROR_COUNT=$(grep -ic "ERROR\|error" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        WARNING_COUNT=$(grep -ic "WARN\|warning" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")

        echo "Total errors: $ERROR_COUNT"
        echo "Total warnings: $WARNING_COUNT"

        if [ "$ERROR_COUNT" -gt 0 ]; then
            echo ""
            echo "Error types:"
            grep -i "ERROR\|error" "$LOG_DIR/litestream.log" | sed 's/.*ERROR[: ]*//' | cut -d' ' -f1-5 | sort | uniq -c | sort -rn | head -10
        fi

        # Check for specific issues
        echo ""
        echo "Specific issues detected:"
        BUSY_ERRORS=$(grep -c "database is locked\|SQLITE_BUSY" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        TIMEOUT_ERRORS=$(grep -c "timeout\|timed out" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        S3_ERRORS=$(grep -c "S3\|AWS\|403\|404\|500\|503" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")

        [ "$BUSY_ERRORS" -gt 0 ] && echo "  - Database busy/locked errors: $BUSY_ERRORS"
        [ "$TIMEOUT_ERRORS" -gt 0 ] && echo "  - Timeout errors: $TIMEOUT_ERRORS"
        [ "$S3_ERRORS" -gt 0 ] && echo "  - S3/AWS errors: $S3_ERRORS"
    fi
    echo ""

    echo "7. CHECKPOINT ANALYSIS"
    echo "======================"
    if [ -f "$LOG_DIR/litestream.log" ]; then
        CHECKPOINT_COUNT=$(grep -c "checkpoint" "$LOG_DIR/litestream.log" 2>/dev/null || echo "0")
        echo "Total checkpoint operations: $CHECKPOINT_COUNT"

        # Analyze checkpoint performance
        echo ""
        echo "Checkpoint timing (last 10):"
        grep "checkpoint" "$LOG_DIR/litestream.log" 2>/dev/null | tail -10 | grep -oE "[0-9]{2}:[0-9]{2}:[0-9]{2}" || echo "No checkpoint data"
    fi
    echo ""

    echo "8. VALIDATION RESULTS"
    echo "===================="
    if [ -f "$LOG_DIR/validate.log" ]; then
        echo "Validation output:"
        cat "$LOG_DIR/validate.log"
    elif [ -f "$LOG_DIR/restore.log" ]; then
        echo "Restoration test results:"
        tail -20 "$LOG_DIR/restore.log"
    else
        echo "No validation/restoration data found"
    fi
    echo ""

    echo "9. RESOURCE USAGE"
    echo "================"
    if [ -f "$LOG_DIR/monitor.log" ]; then
        echo "Peak values from monitoring:"

        # Extract peak database size
        MAX_DB_SIZE=$(grep "Database size:" "$LOG_DIR/monitor.log" | grep -oE "[0-9]+[KMG]?i?B" | sort -h | tail -1 || echo "Unknown")
        echo "  Peak database size: $MAX_DB_SIZE"

        # Extract peak WAL size
        MAX_WAL_SIZE=$(grep "WAL size:" "$LOG_DIR/monitor.log" | grep -oE "[0-9]+[KMG]?i?B" | sort -h | tail -1 || echo "Unknown")
        echo "  Peak WAL size: $MAX_WAL_SIZE"

        # Extract max WAL segments
        MAX_WAL_SEGS=$(grep "WAL segments (total):" "$LOG_DIR/monitor.log" | grep -oE "[0-9]+" | sort -n | tail -1 || echo "Unknown")
        echo "  Max WAL segments: $MAX_WAL_SEGS"
    fi
    echo ""

    echo "10. SUMMARY AND RECOMMENDATIONS"
    echo "==============================="

    # Analyze test success
    TEST_SUCCESS=true
    ISSUES=()

    if [ "$ERROR_COUNT" -gt 100 ]; then
        TEST_SUCCESS=false
        ISSUES+=("High error count ($ERROR_COUNT errors)")
    fi

    if [ -f "$LOG_DIR/validate.log" ] && grep -q "failed\|error" "$LOG_DIR/validate.log" 2>/dev/null; then
        TEST_SUCCESS=false
        ISSUES+=("Validation failed")
    fi

    if [ -f "$LOG_DIR/litestream.log" ] && ! grep -q "compacting" "$LOG_DIR/litestream.log" 2>/dev/null; then
        ISSUES+=("No compaction operations detected")
    fi

    if [ "$TEST_SUCCESS" = true ] && [ ${#ISSUES[@]} -eq 0 ]; then
        echo "✓ Test completed successfully!"
        echo ""
        echo "Key achievements:"
        echo "  - Ran for intended duration"
        echo "  - Successfully created $SNAPSHOT_COUNT snapshots"
        echo "  - Performed $COMPACTION_COUNT compaction operations"
        echo "  - Processed $ROW_COUNT database rows"
    else
        echo "⚠ Test completed with issues:"
        for issue in "${ISSUES[@]}"; do
            echo "  - $issue"
        done
    fi

    echo ""
    echo "Recommendations:"
    if [ "$ERROR_COUNT" -gt 50 ]; then
        echo "  - Investigate error patterns, particularly around resource contention"
    fi

    if [ "$COMPACTION_COUNT" -lt 10 ]; then
        echo "  - Verify compaction configuration is working as expected"
    fi

    if [ "$BUSY_ERRORS" -gt 10 ]; then
        echo "  - Consider adjusting checkpoint intervals or busy timeout settings"
    fi

    echo ""
    echo "Test artifacts location: $TEST_DIR"

} | tee "$ANALYSIS_REPORT"

echo ""
echo "================================================"
echo "Analysis complete!"
echo "Report saved to: $ANALYSIS_REPORT"
echo "================================================"
