#!/bin/sh
# validate-setup.sh - Verify Litestream development environment
# POSIX-compatible script for validating build tools and project health.

set -e

PASS=0
FAIL=0
WARN=0

pass() {
    PASS=$((PASS + 1))
    printf "  [PASS] %s\n" "$1"
}

fail() {
    FAIL=$((FAIL + 1))
    printf "  [FAIL] %s\n" "$1"
}

warn() {
    WARN=$((WARN + 1))
    printf "  [WARN] %s\n" "$1"
}

printf "Litestream Development Environment Validation\n"
printf "==============================================\n\n"

# Check Go installation
printf "Checking Go...\n"
if command -v go >/dev/null 2>&1; then
    GO_VERSION=$(go version | sed 's/.*go\([0-9]*\.[0-9]*\).*/\1/')
    GO_MAJOR=$(echo "$GO_VERSION" | cut -d. -f1)
    GO_MINOR=$(echo "$GO_VERSION" | cut -d. -f2)
    if [ "$GO_MAJOR" -ge 1 ] && [ "$GO_MINOR" -ge 24 ]; then
        pass "Go $(go version | sed 's/.*go/go/' | cut -d' ' -f1) installed (>= 1.24 required)"
    else
        fail "Go $GO_VERSION found but >= 1.24 required"
    fi
else
    fail "Go not found in PATH"
fi

# Check project builds
printf "\nChecking build...\n"
if go build -o /dev/null ./cmd/litestream 2>/dev/null; then
    pass "Project builds successfully"
else
    fail "Project build failed (go build -o /dev/null ./cmd/litestream)"
fi

# Check go vet
printf "\nChecking go vet...\n"
if go vet ./... 2>/dev/null; then
    pass "go vet passes"
else
    fail "go vet reports issues (run: go vet ./...)"
fi

# Quick smoke test (run a fast subset of tests)
printf "\nChecking tests (smoke test)...\n"
if go test -race -short -count=1 ./... >/dev/null 2>&1; then
    pass "Tests pass with race detector (-short mode)"
else
    # Try without race detector in case of environment issues
    if go test -short -count=1 ./... >/dev/null 2>&1; then
        warn "Tests pass but race detector may not be supported on this platform"
    else
        fail "Tests failing (run: go test -race -short ./...)"
    fi
fi

# Check pre-commit (optional)
printf "\nChecking optional tools...\n"
if command -v pre-commit >/dev/null 2>&1; then
    pass "pre-commit installed"
else
    warn "pre-commit not found (optional; install: pip install pre-commit)"
fi

# Check git
if command -v git >/dev/null 2>&1; then
    pass "git installed"
else
    fail "git not found in PATH"
fi

# Summary
printf "\n==============================================\n"
printf "Results: %d passed, %d failed, %d warnings\n" "$PASS" "$FAIL" "$WARN"

if [ "$FAIL" -gt 0 ]; then
    printf "\nEnvironment has issues that must be resolved.\n"
    exit 1
else
    printf "\nEnvironment is ready for Litestream development.\n"
    exit 0
fi
