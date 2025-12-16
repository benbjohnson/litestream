#!/bin/bash
set -e

# Script to run S3 integration tests against a local MinIO container.
# This provides a more realistic test environment than moto for testing
# S3-compatible provider compatibility.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "Starting MinIO container..."
docker compose -f docker-compose.test.yml up -d

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
for i in {1..30}; do
    if docker compose -f docker-compose.test.yml exec -T minio mc ready local 2>/dev/null; then
        echo "MinIO is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "MinIO failed to start"
        docker compose -f docker-compose.test.yml logs minio
        docker compose -f docker-compose.test.yml down
        exit 1
    fi
    sleep 1
done

# Create test bucket using mc client inside the container
echo "Creating test bucket..."
docker compose -f docker-compose.test.yml exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker compose -f docker-compose.test.yml exec -T minio mc mb local/test-bucket --ignore-existing

# Set up cleanup trap
cleanup() {
    echo "Cleaning up..."
    docker compose -f docker-compose.test.yml down
}
trap cleanup EXIT

# Export environment variables for the S3 integration tests
export LITESTREAM_S3_ACCESS_KEY_ID=minioadmin
export LITESTREAM_S3_SECRET_ACCESS_KEY=minioadmin
export LITESTREAM_S3_BUCKET=test-bucket
export LITESTREAM_S3_ENDPOINT=http://localhost:9000
export LITESTREAM_S3_FORCE_PATH_STYLE=true
export LITESTREAM_S3_REGION=us-east-1

echo "Running S3 integration tests against MinIO..."
go test -v ./replica_client_test.go -integration -replica-clients=s3 "$@"

echo "Tests completed successfully!"
