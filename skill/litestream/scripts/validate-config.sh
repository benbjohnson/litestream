#!/usr/bin/env bash
# validate-config.sh - Validate Litestream configuration file
# Usage: ./validate-config.sh [config-path]

set -e

CONFIG_PATH="${1:-/etc/litestream.yml}"

echo "=== Litestream Configuration Validator ==="
echo

# Check if config file exists
if [ ! -f "$CONFIG_PATH" ]; then
    echo "ERROR: Configuration file not found: $CONFIG_PATH"
    echo
    echo "Usage: $0 [config-path]"
    echo "Default: /etc/litestream.yml"
    exit 1
fi

echo "Validating: $CONFIG_PATH"
echo

# Check YAML syntax
echo "1. Checking YAML syntax..."
if command -v yq &> /dev/null; then
    if yq eval '.' "$CONFIG_PATH" > /dev/null 2>&1; then
        echo "   YAML syntax: OK"
    else
        echo "   YAML syntax: INVALID"
        yq eval '.' "$CONFIG_PATH" 2>&1 | head -5
        exit 1
    fi
elif command -v python3 &> /dev/null; then
    if python3 -c "import yaml; yaml.safe_load(open('$CONFIG_PATH'))" 2>/dev/null; then
        echo "   YAML syntax: OK"
    else
        echo "   YAML syntax: INVALID"
        python3 -c "import yaml; yaml.safe_load(open('$CONFIG_PATH'))" 2>&1 | head -5
        exit 1
    fi
else
    echo "   YAML syntax: SKIPPED (install yq or python3 for validation)"
fi

# Check for required sections
echo
echo "2. Checking configuration sections..."

if grep -q "^dbs:" "$CONFIG_PATH" || grep -q "^dbs:" "$CONFIG_PATH"; then
    echo "   Database config (dbs:): FOUND"
else
    echo "   Database config (dbs:): MISSING"
    echo "   ERROR: At least one database must be configured"
    exit 1
fi

# Check for deprecated settings
echo
echo "3. Checking for deprecated settings..."

if grep -q "wal:" "$CONFIG_PATH"; then
    echo "   WARNING: 'wal:' is deprecated, use 'ltx:' instead"
fi

if grep -q "replicas:" "$CONFIG_PATH"; then
    echo "   WARNING: 'replicas:' array is deprecated, use single 'replica:' instead"
fi

if grep "name:" "$CONFIG_PATH" | grep -qv "account-name"; then
    echo "   NOTE: Replica 'name:' field is deprecated"
fi

# Check for age encryption (not supported)
if grep -q "age:" "$CONFIG_PATH"; then
    echo "   ERROR: Age encryption is not currently supported"
    echo "          Remove 'age:' section or use Litestream v0.3.x"
    exit 1
fi

# Check for common issues
echo
echo "4. Checking for common configuration issues..."

# Check for both path and dir
if grep -A5 "^  - path:" "$CONFIG_PATH" | grep -q "dir:"; then
    echo "   WARNING: Cannot specify both 'path' and 'dir' for a database"
fi

# Check for dir without pattern
if grep -q "dir:" "$CONFIG_PATH"; then
    if ! grep -A3 "dir:" "$CONFIG_PATH" | grep -q "pattern:"; then
        echo "   WARNING: 'pattern:' is required when using 'dir:'"
    fi
fi

# Check for environment variables
echo
echo "5. Checking for environment variables..."
ENV_VARS=$(grep -oE '\$\{[^}]+\}|\$[A-Z_]+' "$CONFIG_PATH" 2>/dev/null || true)
if [ -n "$ENV_VARS" ]; then
    echo "   Environment variables found:"
    echo "$ENV_VARS" | sort -u | while read -r var; do
        var_name=$(echo "$var" | sed 's/\${\?\([^}]*\)}\?/\1/')
        if [ -n "${!var_name:-}" ]; then
            echo "      $var: SET"
        else
            echo "      $var: NOT SET (will need to be set at runtime)"
        fi
    done
else
    echo "   No environment variables found"
fi

# Check database paths
echo
echo "6. Checking database paths..."
grep -E "^\s+path:" "$CONFIG_PATH" | while read -r line; do
    path=$(echo "$line" | sed 's/.*path:\s*//' | tr -d '"' | tr -d "'")
    # Skip if it looks like an env var
    if echo "$path" | grep -q '\$'; then
        echo "   $path: USES ENVIRONMENT VARIABLE"
    elif [ -f "$path" ]; then
        echo "   $path: EXISTS"
    else
        echo "   $path: NOT FOUND (will be created or restored)"
    fi
done

# Check replica URLs
echo
echo "7. Checking replica URLs..."
grep -E "^\s+url:" "$CONFIG_PATH" | while read -r line; do
    url=$(echo "$line" | sed 's/.*url:\s*//' | tr -d '"' | tr -d "'")
    scheme=$(echo "$url" | cut -d: -f1)
    case "$scheme" in
        s3)
            echo "   $url: S3/S3-compatible"
            ;;
        gs)
            echo "   $url: Google Cloud Storage"
            ;;
        abs)
            echo "   $url: Azure Blob Storage"
            ;;
        sftp)
            echo "   $url: SFTP"
            ;;
        nats)
            echo "   $url: NATS JetStream"
            ;;
        file|/*)
            echo "   $url: Local file"
            ;;
        *)
            if echo "$url" | grep -q '\$'; then
                echo "   $url: USES ENVIRONMENT VARIABLE"
            else
                echo "   $url: UNKNOWN SCHEME"
            fi
            ;;
    esac
done

# Try to validate with Litestream itself
echo
echo "8. Attempting Litestream validation..."
if command -v litestream &> /dev/null; then
    if litestream databases -config "$CONFIG_PATH" > /dev/null 2>&1; then
        echo "   Litestream validation: OK"
        echo
        echo "   Configured databases:"
        litestream databases -config "$CONFIG_PATH" 2>/dev/null | head -20
    else
        echo "   Litestream validation: FAILED"
        litestream databases -config "$CONFIG_PATH" 2>&1 | head -10
    fi
else
    echo "   SKIPPED (litestream not in PATH)"
fi

echo
echo "=== Validation Complete ==="
