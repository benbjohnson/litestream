# restore Command

Restore a SQLite database from a backup.

## Synopsis

```bash
# Restore from replica URL
litestream restore -o OUTPUT_PATH REPLICA_URL

# Restore from configured database
litestream restore [-config PATH] [-o OUTPUT_PATH] DB_PATH
```

## Description

The `restore` command recovers a SQLite database from LTX backup files. It can:

- Restore to the latest available state
- Restore to a specific point in time
- Restore to a specific transaction ID
- Skip restoration if database already exists

## Flags

| Flag | Description |
|------|-------------|
| `-o PATH` | Output path for restored database (required with URL) |
| `-config PATH` | Configuration file path |
| `-no-expand-env` | Disable environment variable expansion |
| `-timestamp TIME` | Restore to specific timestamp (RFC3339 format) |
| `-txid TXID` | Restore to specific transaction ID |
| `-parallelism N` | Number of parallel downloads (default: runtime.NumCPU) |
| `-if-db-not-exists` | Skip if output file already exists |
| `-if-replica-exists` | Skip if no backup files found |

## Examples

### Basic Restore

```bash
# From replica URL
litestream restore -o /data/app.db s3://bucket/app-backup

# From configured database
litestream restore /data/app.db
```

### Point-in-Time Recovery

Restore to a specific timestamp:

```bash
litestream restore -timestamp 2024-01-15T10:30:00Z -o /data/app.db s3://bucket/backup
```

Timestamp must be in RFC3339 format (ISO 8601 with timezone).

### Restore to Transaction ID

```bash
litestream restore -txid 1000 -o /data/app.db s3://bucket/backup
```

### Conditional Restore

```bash
# Only restore if database doesn't exist
litestream restore -if-db-not-exists -o /data/app.db s3://bucket/backup

# Skip silently if no backups available
litestream restore -if-replica-exists -o /data/app.db s3://bucket/backup
```

### Parallel Downloads

Speed up large restores:

```bash
litestream restore -parallelism 8 -o /data/app.db s3://bucket/backup
```

## Restore Process

1. **Connect** to storage backend
2. **List** available LTX files
3. **Find** target transaction (latest, timestamp, or TXID)
4. **Download** required LTX files in parallel
5. **Apply** pages in transaction order
6. **Write** restored database to output path

## Output Path Behavior

### With Replica URL

Output path (`-o`) is required:

```bash
litestream restore -o /data/app.db s3://bucket/backup
```

### With Config File

If `-o` not specified, restores to the configured database path:

```bash
# Restores to /data/app.db (from config)
litestream restore /data/app.db
```

Override with `-o`:

```bash
# Restore to different location
litestream restore -o /tmp/test.db /data/app.db
```

## Finding Available Backups

Before restoring, check available backups:

```bash
# List LTX files
litestream ltx s3://bucket/backup

# Output:
# min_txid        max_txid        size    created
# 0000000000000001 0000000000000100 4096    2024-01-15T10:00:00Z
# 0000000000000101 0000000000000200 8192    2024-01-15T10:30:00Z
```

## Common Issues

### "no matching backup files available"
- Verify replica URL is correct
- Check storage credentials
- Use `litestream ltx` to see available backups
- Add `-if-replica-exists` to skip silently

### "output path required"
- Add `-o PATH` when using replica URL directly

### "database not found in config"
- Verify database path matches config exactly
- Check config file is loaded correctly

### "invalid -timestamp"
- Use RFC3339 format: `2024-01-15T10:30:00Z`
- Include timezone (Z for UTC)

### Slow restore
- Increase `-parallelism` for large databases
- Check network connectivity to storage backend

## Automation

### Startup Script

```bash
#!/bin/bash
DB_PATH=/data/app.db
REPLICA_URL=s3://bucket/backup

# Restore if database doesn't exist
if [ ! -f "$DB_PATH" ]; then
    litestream restore -o "$DB_PATH" "$REPLICA_URL"
fi

# Start application
exec myapp serve
```

### Docker Entrypoint

```bash
#!/bin/bash
# Restore database if not present
litestream restore -if-db-not-exists -o /data/app.db s3://bucket/backup

# Start Litestream with application
exec litestream replicate -exec "myapp serve"
```

## See Also

- [replicate Command](replicate.md)
- [ltx Command](ltx.md)
- [Recovery Operations](../operations/recovery.md)
