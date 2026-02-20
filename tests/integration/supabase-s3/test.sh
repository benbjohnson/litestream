#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
ENDPOINT="http://localhost:5555/s3"
STORAGE_API="http://localhost:5555"
ACCESS_KEY="supabase-s3-access-key"
SECRET_KEY="supabase-s3-secret-key-that-is-long-enough"
SERVICE_KEY="eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJpc3MiOiAic3VwYWJhc2UiLCAicm9sZSI6ICJzZXJ2aWNlX3JvbGUifQ.lRaC0LUy-3mILAj_17hVWOBnaft3QPpK-pqAs8h2MRI"
BUCKET="litestream-test"
REGION="us-east-1"
TMPDIR_BASE=$(mktemp -d)

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    rm -rf "$TMPDIR_BASE"
    cd "$SCRIPT_DIR"
    docker compose down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

echo "================================================"
echo "Supabase S3 Integration Test for Litestream"
echo "================================================"
echo ""

# Step 1: Build litestream
echo "[1/6] Building litestream..."
cd "$PROJECT_ROOT"
go build -o "$TMPDIR_BASE/litestream" ./cmd/litestream
echo "  OK: Binary built at $TMPDIR_BASE/litestream"
echo ""

# Step 2: Start Supabase stack
echo "[2/6] Starting Supabase storage stack..."
cd "$SCRIPT_DIR"
docker compose up -d --wait --wait-timeout 90
echo "  OK: Supabase storage running at $ENDPOINT"
echo ""

# Step 3: Create bucket via Supabase REST API
echo "[3/6] Creating bucket '$BUCKET' via Supabase REST API..."
BUCKET_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$STORAGE_API/bucket" \
    -H "Authorization: Bearer $SERVICE_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"$BUCKET\", \"public\": true}")
BUCKET_HTTP_CODE=$(echo "$BUCKET_RESPONSE" | tail -1)
BUCKET_BODY=$(echo "$BUCKET_RESPONSE" | sed '$d')
if [ "$BUCKET_HTTP_CODE" = "200" ] || [ "$BUCKET_HTTP_CODE" = "201" ]; then
    echo "  OK: Bucket created (HTTP $BUCKET_HTTP_CODE)"
elif echo "$BUCKET_BODY" | grep -q "already exists"; then
    echo "  OK: Bucket already exists"
else
    echo "  FAIL: Bucket creation failed (HTTP $BUCKET_HTTP_CODE): $BUCKET_BODY"
    exit 1
fi
echo ""

# Step 4: Create and populate test database
echo "[4/6] Creating test SQLite database..."
DB_PATH="$TMPDIR_BASE/test.db"
sqlite3 "$DB_PATH" <<'SQL'
PRAGMA journal_mode=WAL;
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, created_at TEXT);
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', datetime('now'));
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', datetime('now'));
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', datetime('now'));
SQL
ROW_COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM users;")
echo "  OK: Database created with $ROW_COUNT rows"
echo ""

# Step 5: Replicate to Supabase S3
echo "[5/6] Replicating database to Supabase S3..."
REPLICA_URL="s3://${BUCKET}/testdb?endpoint=${ENDPOINT}&region=${REGION}"
echo "  URL: $REPLICA_URL"
echo "  (Auto-detection should apply force-path-style=true and sign-payload=true)"

AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
"$TMPDIR_BASE/litestream" replicate "$DB_PATH" "$REPLICA_URL" &
LITESTREAM_PID=$!

# Wait for initial sync, then write more data
sleep 5
sqlite3 "$DB_PATH" <<'SQL'
INSERT INTO users VALUES (4, 'Diana', 'diana@example.com', datetime('now'));
INSERT INTO users VALUES (5, 'Eve', 'eve@example.com', datetime('now'));
SQL
echo "  OK: Wrote 2 additional rows during replication"

# Give time for WAL sync
sleep 10
kill "$LITESTREAM_PID" 2>/dev/null || true
wait "$LITESTREAM_PID" 2>/dev/null || true
echo "  OK: Replication completed"
echo ""

# Step 6: Restore and verify
echo "[6/6] Restoring database from Supabase S3..."
RESTORE_PATH="$TMPDIR_BASE/restored.db"
AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
"$TMPDIR_BASE/litestream" restore -o "$RESTORE_PATH" "$REPLICA_URL"

RESTORED_COUNT=$(sqlite3 "$RESTORE_PATH" "SELECT COUNT(*) FROM users;")
echo "  Restored row count: $RESTORED_COUNT"

if [ "$RESTORED_COUNT" -ge 3 ]; then
    echo ""
    echo "================================================"
    echo "SUCCESS: Supabase S3 integration test passed!"
    echo "  - Auto-detection: Working (no manual force-path-style/sign-payload needed)"
    echo "  - Replication: Working"
    echo "  - Restore: Working ($RESTORED_COUNT rows recovered)"
    echo "================================================"
else
    echo ""
    echo "FAIL: Expected at least 3 rows, got $RESTORED_COUNT"
    exit 1
fi
