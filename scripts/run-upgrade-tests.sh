#!/bin/bash
set -euo pipefail

# Run the v0.3.x → v0.5.x upgrade integration tests locally.
# Downloads the v0.3.13 binary if not already cached, builds the current
# binary, and runs the Go integration test.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CACHE_DIR="$ROOT_DIR/.cache/litestream-v3"
V3_BIN="$CACHE_DIR/litestream"

# Detect platform.
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64)  ARCH="amd64" ;;
    aarch64) ARCH="arm64" ;;
    arm64)   ARCH="arm64" ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

# Download v0.3.13 binary if not cached.
if [ ! -x "$V3_BIN" ]; then
    echo "Downloading Litestream v0.3.13 (${OS}-${ARCH})..."
    mkdir -p "$CACHE_DIR"

    case "$OS" in
        darwin)
            ASSET="litestream-v0.3.13-darwin-${ARCH}.zip"
            gh release download v0.3.13 --repo benbjohnson/litestream --pattern "$ASSET" --dir "$CACHE_DIR" --clobber
            unzip -o "$CACHE_DIR/$ASSET" -d "$CACHE_DIR"
            rm -f "$CACHE_DIR/$ASSET"
            ;;
        linux)
            ASSET="litestream-v0.3.13-linux-${ARCH}.tar.gz"
            gh release download v0.3.13 --repo benbjohnson/litestream --pattern "$ASSET" --dir "$CACHE_DIR" --clobber
            tar -xzf "$CACHE_DIR/$ASSET" -C "$CACHE_DIR"
            rm -f "$CACHE_DIR/$ASSET"
            ;;
        *)
            echo "Unsupported OS: $OS"
            exit 1
            ;;
    esac

    chmod +x "$V3_BIN"
    echo "Cached v0.3.13 binary at $V3_BIN"
else
    echo "Using cached v0.3.13 binary at $V3_BIN"
fi

echo "v0.3.13 version: $("$V3_BIN" version)"

# Build current binaries.
echo "Building current binaries..."
cd "$ROOT_DIR"
go build -o bin/litestream ./cmd/litestream
go build -o bin/litestream-test ./cmd/litestream-test
echo "Current version: $(./bin/litestream version)"

# Run upgrade tests.
echo ""
echo "Running upgrade integration tests..."
LITESTREAM_V3_BIN="$V3_BIN" CGO_ENABLED=1 \
    go test -v -tags=integration -timeout=10m ./tests/integration/... -run=TestUpgrade "$@"
