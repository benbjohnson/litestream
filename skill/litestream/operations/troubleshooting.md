# Troubleshooting Guide

Common issues and their solutions.

## Diagnostic Commands

```bash
# Check database status
litestream status /path/to/db.sqlite

# List LTX files (verify backups exist)
litestream ltx /path/to/db.sqlite

# Check database integrity
sqlite3 /path/to/db.sqlite "PRAGMA integrity_check;"

# Test restoration
litestream restore -o /tmp/test.db s3://bucket/backup
```

## Common Issues

### Issue: WAL Growing Too Large

**Symptoms:**
- WAL file grows to gigabytes
- Disk space running low
- Database performance degrading

**Causes:**
1. Long-running read transactions blocking checkpoint
2. Checkpoint interval too long
3. Application not closing connections

**Solutions:**

1. **Check for long-running transactions:**
   ```sql
   -- SQLite doesn't track this directly
   -- Check application code for open transactions
   ```

2. **Adjust checkpoint settings:**
   ```yaml
   dbs:
     - path: /data/app.db
       min-checkpoint-page-count: 500   # Checkpoint more often
       truncate-page-n: 5000            # Force truncate at threshold
   ```

3. **Monitor WAL size:**
   ```bash
   ls -la /data/app.db-wal
   ```

### Issue: Replication Not Starting

**Symptoms:**
- No LTX files created
- `litestream status` shows "not initialized"

**Causes:**
1. Database not in WAL mode
2. No writes have occurred
3. Configuration error

**Solutions:**

1. **Enable WAL mode:**
   ```sql
   PRAGMA journal_mode = WAL;
   ```

2. **Make a test write:**
   ```sql
   -- Any write will trigger replication
   CREATE TABLE IF NOT EXISTS _litestream_test (id INTEGER);
   INSERT INTO _litestream_test VALUES (1);
   DROP TABLE _litestream_test;
   ```

3. **Verify configuration:**
   ```bash
   litestream databases
   # Should show your database
   ```

### Issue: "Access Denied" Errors

**Symptoms:**
- Sync errors in logs
- "AccessDenied" or "Forbidden" messages

**Causes:**
1. Invalid credentials
2. Insufficient permissions
3. Bucket/container doesn't exist

**Solutions:**

1. **Verify credentials:**
   ```bash
   # For S3
   aws s3 ls s3://your-bucket/

   # For GCS
   gsutil ls gs://your-bucket/
   ```

2. **Check IAM permissions:**
   - S3: GetObject, PutObject, DeleteObject, ListBucket
   - GCS: storage.objects.get, create, delete, list

3. **Create bucket if missing:**
   ```bash
   aws s3 mb s3://your-bucket
   ```

### Issue: Restore Fails

**Symptoms:**
- "no matching backup files available"
- Empty or corrupt restored database

**Causes:**
1. No backups exist
2. Wrong replica URL
3. Network connectivity issues

**Solutions:**

1. **Verify backups exist:**
   ```bash
   litestream ltx s3://bucket/backup
   # Should show list of LTX files
   ```

2. **Check URL format:**
   ```bash
   # Correct
   litestream restore -o /tmp/db s3://bucket/path

   # Wrong (missing path)
   litestream restore -o /tmp/db s3://bucket
   ```

3. **Test connectivity:**
   ```bash
   # For S3
   aws s3 ls s3://bucket/path/
   ```

### Issue: Database Corruption After Restore

**Symptoms:**
- PRAGMA integrity_check fails
- Application errors reading data

**Causes:**
1. Incomplete restore (interrupted)
2. Lock page not properly skipped (unlikely in current versions)
3. Source database was corrupted

**Solutions:**

1. **Verify source integrity:**
   ```bash
   # Restore to temp location
   litestream restore -o /tmp/test.db s3://bucket/backup
   sqlite3 /tmp/test.db "PRAGMA integrity_check;"
   ```

2. **Try point-in-time recovery:**
   ```bash
   # Restore to earlier state
   litestream restore -timestamp 2024-01-14T00:00:00Z -o /tmp/test.db s3://bucket/backup
   ```

### Issue: High Replication Lag

**Symptoms:**
- Remote TXID far behind local
- Long recovery point objective (RPO)

**Causes:**
1. Slow network connection
2. Sync interval too long
3. Storage backend throttling

**Solutions:**

1. **Reduce sync interval:**
   ```yaml
   dbs:
     - path: /data/app.db
       replica:
         url: s3://bucket/backup
         sync-interval: 100ms  # More frequent sync
   ```

2. **Check network:**
   ```bash
   # Time a test upload
   time aws s3 cp /tmp/testfile s3://bucket/
   ```

3. **Monitor metrics:**
   ```bash
   curl localhost:9090/metrics | grep litestream
   ```

### Issue: Lock Page Errors (>1GB Databases)

**Symptoms:**
- Errors with databases larger than 1GB
- Corruption around 1GB mark

**Causes:**
- SQLite reserves page at 0x40000000 for locking
- This page must be skipped during replication

**Note:** Current Litestream versions handle this automatically. If you see issues:

1. **Ensure latest version:**
   ```bash
   litestream version
   ```

2. **Test with large database:**
   ```bash
   # Create test database > 1GB and verify replication
   ```

### Issue: Race Conditions / Data Races

**Symptoms:**
- Sporadic errors
- Inconsistent behavior
- Crashes under load

**Solutions:**

1. **Run tests with race detector:**
   ```bash
   go test -race -v ./...
   ```

2. **Ensure single writer:**
   - Only one process should write to the database
   - Multiple readers are okay

### Issue: Slow Compaction

**Symptoms:**
- Many small LTX files accumulating
- Storage costs increasing

**Causes:**
1. Compaction intervals too long
2. Very high write volume
3. Storage backend latency

**Solutions:**

1. **Adjust compaction levels:**
   ```yaml
   levels:
     - interval: 15s    # Faster L1 compaction
     - interval: 2m     # Faster L2 compaction
     - interval: 30m    # Faster L3 compaction
   ```

2. **Monitor compaction:**
   ```bash
   # Count LTX files
   litestream ltx /path/to/db | wc -l
   ```

## Getting Help

1. **Check logs:**
   ```bash
   journalctl -u litestream -f
   ```

2. **Enable debug logging:**
   ```yaml
   logging:
     level: debug
   ```

3. **Gather diagnostics:**
   ```bash
   litestream version
   litestream databases
   litestream status
   ls -la /path/to/db*
   ```

4. **Report issues:**
   - GitHub: https://github.com/benbjohnson/litestream/issues
   - Include: version, config (redacted), error messages, steps to reproduce
