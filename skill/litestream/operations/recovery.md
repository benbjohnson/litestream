# Recovery Procedures

Point-in-time recovery and disaster recovery procedures.

## Recovery Types

### Latest State Recovery

Restore to the most recent backup:

```bash
litestream restore -o /data/app.db s3://bucket/backup
```

### Point-in-Time Recovery

Restore to a specific timestamp:

```bash
litestream restore -timestamp 2024-01-15T10:30:00Z -o /data/app.db s3://bucket/backup
```

### Transaction-Based Recovery

Restore to a specific transaction ID:

```bash
litestream restore -txid 1000 -o /data/app.db s3://bucket/backup
```

## Pre-Recovery Steps

### 1. Stop the Application

```bash
# Stop application that uses the database
systemctl stop myapp

# Stop Litestream (if running)
systemctl stop litestream
```

### 2. Backup Current State

```bash
# Even if corrupted, backup current state
mv /data/app.db /data/app.db.corrupted
mv /data/app.db-wal /data/app.db-wal.corrupted
mv /data/app.db-shm /data/app.db-shm.corrupted
```

### 3. Verify Backup Availability

```bash
# List available backups
litestream ltx s3://bucket/backup

# Output shows TXID ranges and timestamps
min_txid              max_txid              size     created
0000000000000001      0000000000001000      102400   2024-01-15T00:00:00Z
```

## Recovery Procedures

### Scenario 1: Complete Database Loss

Database file was deleted or server failed.

```bash
# 1. Ensure directory exists
mkdir -p /data

# 2. Restore latest backup
litestream restore -o /data/app.db s3://bucket/backup

# 3. Verify integrity
sqlite3 /data/app.db "PRAGMA integrity_check;"

# 4. Restart services
systemctl start litestream
systemctl start myapp
```

### Scenario 2: Corruption Recovery

Database became corrupted (e.g., disk error).

```bash
# 1. Stop services
systemctl stop myapp litestream

# 2. Move corrupted files
mv /data/app.db /data/app.db.corrupted

# 3. Restore from before corruption
# Find timestamp before corruption occurred
litestream ltx s3://bucket/backup | less

# 4. Restore to that time
litestream restore -timestamp 2024-01-14T23:00:00Z -o /data/app.db s3://bucket/backup

# 5. Verify and restart
sqlite3 /data/app.db "PRAGMA integrity_check;"
systemctl start litestream
systemctl start myapp
```

### Scenario 3: Accidental Data Deletion

User accidentally deleted important data.

```bash
# 1. Identify when deletion occurred
# (from application logs or user reports)

# 2. Find backup from before deletion
litestream ltx s3://bucket/backup | grep "2024-01-15T09"

# 3. Restore to recovery database (don't overwrite production!)
litestream restore -timestamp 2024-01-15T09:00:00Z -o /tmp/recovery.db s3://bucket/backup

# 4. Extract needed data
sqlite3 /tmp/recovery.db ".dump table_name" > recovery.sql

# 5. Apply to production (carefully!)
sqlite3 /data/app.db < recovery.sql
```

### Scenario 4: Server Migration

Moving database to new server.

```bash
# On new server:

# 1. Install Litestream
brew install litestream  # or appropriate method

# 2. Copy configuration
scp oldserver:/etc/litestream.yml /etc/litestream.yml
# Update paths if needed

# 3. Restore database
litestream restore -o /data/app.db s3://bucket/backup

# 4. Start replication (will continue from current point)
systemctl start litestream
```

### Scenario 5: Development Database Refresh

Get production data for development.

```bash
# Create development copy from production backup
litestream restore -o /tmp/dev.db s3://bucket/production-backup

# Optionally sanitize data
sqlite3 /tmp/dev.db << EOF
UPDATE users SET email = 'test' || id || '@example.com';
UPDATE users SET password_hash = 'invalidated';
EOF
```

## Recovery Verification

### Integrity Check

```bash
sqlite3 /data/app.db "PRAGMA integrity_check;"
# Should output: ok
```

### Quick Check

```bash
sqlite3 /data/app.db "PRAGMA quick_check;"
# Faster but less thorough
```

### Data Verification

```bash
# Check row counts match expectations
sqlite3 /data/app.db "SELECT COUNT(*) FROM important_table;"

# Check recent data exists
sqlite3 /data/app.db "SELECT MAX(created_at) FROM transactions;"
```

## Automated Recovery

### Docker Entrypoint

```bash
#!/bin/bash
set -e

# Auto-restore if database doesn't exist
if [ ! -f /data/app.db ]; then
    echo "Database not found, restoring from backup..."
    litestream restore -if-replica-exists -o /data/app.db "$REPLICA_URL"
fi

# Start Litestream with application
exec litestream replicate -exec "$@"
```

### Kubernetes Init Container

```yaml
initContainers:
  - name: restore
    image: litestream/litestream
    command:
      - litestream
      - restore
      - -if-db-not-exists
      - -o
      - /data/app.db
      - s3://bucket/backup
    volumeMounts:
      - name: data
        mountPath: /data
```

### Systemd Pre-Start

```ini
[Service]
ExecStartPre=/usr/bin/litestream restore -if-db-not-exists -o /data/app.db s3://bucket/backup
ExecStart=/usr/bin/litestream replicate
```

## Disaster Recovery Planning

### RPO (Recovery Point Objective)

How much data can you afford to lose?

| Sync Interval | Typical RPO |
|---------------|-------------|
| 100ms | ~100ms |
| 1s (default) | ~1s |
| 10s | ~10s |
| 1m | ~1m |

### RTO (Recovery Time Objective)

How fast must you recover?

| Database Size | Restore Time (estimate) |
|---------------|------------------------|
| < 100MB | < 1 minute |
| 100MB - 1GB | 1-5 minutes |
| 1GB - 10GB | 5-30 minutes |
| > 10GB | Depends on network |

### Backup Strategy

1. **Primary**: Continuous replication to cloud storage
2. **Secondary**: Periodic snapshots for long-term retention
3. **Tertiary**: Geographic replication (different region)

### Testing Recovery

Regularly test your recovery process:

```bash
# Monthly recovery test
#!/bin/bash
DATE=$(date +%Y%m%d)
litestream restore -o /tmp/recovery-test-$DATE.db s3://bucket/backup
sqlite3 /tmp/recovery-test-$DATE.db "PRAGMA integrity_check;"
rm /tmp/recovery-test-$DATE.db
echo "Recovery test passed: $DATE"
```

## See Also

- [restore Command](../commands/restore.md)
- [Troubleshooting](troubleshooting.md)
- [Monitoring](monitoring.md)
