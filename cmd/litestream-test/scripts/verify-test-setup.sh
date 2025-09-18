#!/bin/bash

# Script to verify test environment is set up correctly
# Ensures we're using local builds, not system-installed versions

echo "=========================================="
echo "Litestream Test Environment Verification"
echo "=========================================="
echo ""

# Check for local Litestream build
echo "Checking for local Litestream build..."
if [ -f "./bin/litestream" ]; then
    echo "✓ Local litestream found: ./bin/litestream"
    echo "  Version: $($./bin/litestream version)"
    echo "  Size: $(ls -lh ./bin/litestream | awk '{print $5}')"
    echo "  Modified: $(ls -la ./bin/litestream | awk '{print $6, $7, $8}')"
else
    echo "✗ Local litestream NOT found at ./bin/litestream"
    echo "  Please build: go build -o bin/litestream ./cmd/litestream"
    exit 1
fi

# Check for system Litestream (should NOT be used)
echo ""
echo "Checking for system Litestream..."
if command -v litestream &> /dev/null; then
    SYSTEM_LITESTREAM=$(which litestream)
    echo "⚠ System litestream found at: $SYSTEM_LITESTREAM"
    echo "  Version: $(litestream version 2>&1 || echo "unknown")"
    echo "  WARNING: Tests should NOT use this version!"
    echo "  All test scripts use ./bin/litestream explicitly"
else
    echo "✓ No system litestream found (good - avoids confusion)"
fi

# Check for litestream-test binary
echo ""
echo "Checking for litestream-test binary..."
if [ -f "./bin/litestream-test" ]; then
    echo "✓ Local litestream-test found: ./bin/litestream-test"
    echo "  Size: $(ls -lh ./bin/litestream-test | awk '{print $5}')"
    echo "  Modified: $(ls -la ./bin/litestream-test | awk '{print $6, $7, $8}')"
else
    echo "✗ litestream-test NOT found at ./bin/litestream-test"
    echo "  Please build: go build -o bin/litestream-test ./cmd/litestream-test"
    exit 1
fi

# Verify test scripts use local builds
echo ""
echo "Verifying test scripts use local builds..."
SCRIPTS=(
    "reproduce-critical-bug.sh"
    "test-1gb-boundary.sh"
    "test-concurrent-operations.sh"
)

ALL_GOOD=true
for script in "${SCRIPTS[@]}"; do
    if [ -f "$script" ]; then
        if grep -q 'LITESTREAM="./bin/litestream"' "$script"; then
            echo "✓ $script uses local build"
        else
            echo "✗ $script may not use local build!"
            grep "LITESTREAM=" "$script" | head -2
            ALL_GOOD=false
        fi
    else
        echo "- $script not found (optional)"
    fi
done

# Check current git branch
echo ""
echo "Git status:"
BRANCH=$(git branch --show-current 2>/dev/null || echo "unknown")
echo "  Current branch: $BRANCH"
if [ "$BRANCH" = "main" ]; then
    echo "  ⚠ On main branch - be careful with commits!"
fi

# Summary
echo ""
echo "=========================================="
if [ "$ALL_GOOD" = true ] && [ -f "./bin/litestream" ] && [ -f "./bin/litestream-test" ]; then
    echo "✅ Test environment is properly configured!"
    echo ""
    echo "You can run tests with:"
    echo "  ./reproduce-critical-bug.sh"
    echo "  ./test-1gb-boundary.sh"
    echo "  ./test-concurrent-operations.sh"
else
    echo "❌ Test environment needs setup"
    echo ""
    echo "Required steps:"
    [ ! -f "./bin/litestream" ] && echo "  1. Build litestream: go build -o bin/litestream ./cmd/litestream"
    [ ! -f "./bin/litestream-test" ] && echo "  2. Build test harness: go build -o bin/litestream-test ./cmd/litestream-test"
    exit 1
fi
echo "=========================================="
