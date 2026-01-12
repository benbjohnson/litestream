# CPU Usage Testing

This directory contains test scripts and configurations for measuring Litestream's idle CPU usage, particularly for validating the fixes in issue #992.

## Files

- `test-cpu-usage.sh` - Automated CPU monitoring script
- `litestream-test-polling.yml` - Config for testing with S3 replication

## Prerequisites

1. Build Litestream binary:
   ```bash
   cd ../..
   go build -o bin/litestream ./cmd/litestream
   ```

2. Set up AWS credentials in `.envrc` at repo root:
   ```bash
   export AWS_ACCESS_KEY_ID="your-key-id"
   export AWS_SECRET_ACCESS_KEY="your-secret-key"
   export AWS_REGION="us-east-2"
   export S3_BUCKET="your-test-bucket"
   ```

3. Have `sqlite3` CLI installed

## Usage

From this directory, run:

```bash
# Test for 60 seconds
./test-cpu-usage.sh 60

# Longer test (5 minutes)
./test-cpu-usage.sh 300
```

## What It Tests

The script:
1. Creates a test SQLite database at `/tmp/test.db`
2. Starts Litestream with S3 replication
3. Monitors CPU usage every second using `ps`
4. Calculates average CPU usage
5. Verifies S3 replication is working
6. Outputs results and detailed CSV log

## Expected Results

Based on testing for PR #993:

- **With S3 transport fix:** ~0.0067% CPU (99% improvement)
- **Original (v0.5.6):** ~0.7% CPU

The S3 transport fix achieves near-zero idle CPU usage, validating the fix.

## Output

Results are printed to stdout and detailed logs are saved to:
- `/tmp/litestream-cpu-log.csv` - Per-second CPU measurements

## Notes

- Tests require real S3 credentials and will upload data to your bucket
- Test database is created at `/tmp/test.db` and cleaned up on each run
- CPU measurements are instantaneous snapshots, not averages over intervals
- Longer test durations (5-10 minutes) provide more stable averages
