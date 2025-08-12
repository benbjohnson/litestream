# Integration Testing Guide

## Manual Integration Test Execution

Repository maintainers can manually trigger integration tests for pull requests before merging to main.

### Two Methods Available

#### Method 1: Via Modified commit.yml Workflow

The main commit workflow now supports manual triggering with selective integration tests.

1. Go to the [Actions tab](../../actions/workflows/commit.yml)
2. Click "Run workflow"
3. Select which integration tests to run:
   - Run S3 Integration Tests
   - Run GCP Integration Tests
   - Run Azure Blob Storage Integration Tests
4. Click "Run workflow" button

#### Method 2: Via Dedicated Manual Integration Test Workflow

A dedicated workflow for running integration tests on any PR or branch.

1. Go to the [Actions tab](../../actions/workflows/integration-tests-manual.yml)
2. Click "Run workflow"
3. Fill in:
   - **PR Number** (optional): Enter PR number to test that specific PR
   - **Run S3**: Check to run S3 integration tests (default: true)
   - **Run GCP**: Check to run GCP integration tests
   - **Run ABS**: Check to run Azure Blob Storage integration tests
4. Click "Run workflow" button

### Environment Protection

All integration test jobs use the `integration-tests` environment. To add approval requirements:

1. Go to Settings → Environments
2. Create/Configure `integration-tests` environment
3. Add protection rules:
   - Required reviewers (specify maintainers who can approve)
   - Deployment branches (restrict to specific branches if needed)

### Running Integration Tests Locally

```bash
# S3 Integration Tests
export LITESTREAM_S3_ACCESS_KEY_ID="your-key"
export LITESTREAM_S3_SECRET_ACCESS_KEY="your-secret"
export LITESTREAM_S3_REGION="us-east-1"
export LITESTREAM_S3_BUCKET="your-bucket"
go test -v ./replica_client_test.go -integration s3

# GCP Integration Tests
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/credentials.json"
export LITESTREAM_GCS_BUCKET="your-bucket"
go test -v ./replica_client_test.go -integration gcs

# Azure Blob Storage Integration Tests
export LITESTREAM_ABS_ACCOUNT_NAME="your-account"
export LITESTREAM_ABS_ACCOUNT_KEY="your-key"
export LITESTREAM_ABS_BUCKET="your-container"
go test -v ./replica_client_test.go -integration abs
```

### Mock S3 Tests (No Credentials Required)

```bash
# Install moto
pip install moto[s3,server]

# Run mock S3 tests
./etc/s3_mock.py go test -v ./replica_client_test.go -integration s3
```

## GitHub Secrets Configuration

The following secrets must be configured in the repository settings:

### S3

- `LITESTREAM_S3_ACCESS_KEY_ID`
- `LITESTREAM_S3_SECRET_ACCESS_KEY`

### GCP

- `GOOGLE_APPLICATION_CREDENTIALS` (JSON service account key)

### Azure Blob Storage

- `LITESTREAM_ABS_ACCOUNT_NAME`
- `LITESTREAM_ABS_ACCOUNT_KEY`
