#!/usr/bin/env bash
set -euo pipefail

# Local nightly soak test runner for Litestream.
# Usage:
#   ./scripts/local-soak.sh              # 30m per profile (default)
#   ./scripts/local-soak.sh 2h           # 2h per profile
#   ./scripts/local-soak.sh 30m high-volume  # single profile

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DURATION="${1:-30m}"
PROFILE_FILTER="${2:-}"
LOG_DIR="$REPO_DIR/tmp/soak-logs"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="$LOG_DIR/soak-$TIMESTAMP.log"

mkdir -p "$LOG_DIR"

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

log "Litestream local soak test"
log "Duration per profile: $DURATION"
log "Log: $LOG_FILE"
log ""

cd "$REPO_DIR"

# Clean up old test artifacts so results are unambiguous.
# Every error in the run is guaranteed to be from THIS run.
log "Cleaning old test artifacts..."
rm -rf /tmp/litestream-ltx-behavior-* /tmp/litestream-minio-soak-* /tmp/litestream-comprehensive-soak-*
go clean -testcache
log "Clean complete."
log ""

# Build binaries
log "Building binaries..."
go build -o bin/litestream ./cmd/litestream
go build -o bin/litestream-test ./cmd/litestream-test
log "Build complete."
log ""

# Determine test filter
RUN_FILTER='TestLTXBehavior$'
if [[ -n "$PROFILE_FILTER" ]]; then
    RUN_FILTER="TestLTXBehavior\$/$PROFILE_FILTER"
    log "Running single profile: $PROFILE_FILTER"
else
    log "Running all profiles: low-volume, high-volume, burst-volume"
fi

# Disable errexit around test commands so we always reach the summary.
set +e

# Run LTX behavioral tests
log "Starting LTX behavioral tests..."
SOAK_DURATION="$DURATION" go test -tags 'integration,soak' \
    -run "$RUN_FILTER" \
    -v -timeout 8h \
    ./tests/integration/ 2>&1 | tee -a "$LOG_FILE"

LTX_EXIT=${PIPESTATUS[0]}

# Run snapshot regression test
log ""
log "Starting snapshot regression test..."
go test -tags 'integration,soak' \
    -run 'TestLTXBehavior_NoExcessiveSnapshots' \
    -v -timeout 20m \
    ./tests/integration/ 2>&1 | tee -a "$LOG_FILE"

SNAP_EXIT=${PIPESTATUS[0]}

set -e

# Summary
log ""
log "================================================"
log "Soak Test Summary"
log "================================================"
log "LTX Behavioral: $([ $LTX_EXIT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
log "Snapshot Regression: $([ $SNAP_EXIT -eq 0 ] && echo 'PASS' || echo 'FAIL')"
log "Full log: $LOG_FILE"
log "Artifacts: /tmp/litestream-ltx-behavior-*/"

if [[ $LTX_EXIT -ne 0 || $SNAP_EXIT -ne 0 ]]; then
    log "FAILURES DETECTED — review log for details"
    exit 1
fi

log "All tests passed."
