# ltx Command

List LTX files available for a database.

## Synopsis

```bash
# From configured database
litestream ltx [-config PATH] DB_PATH

# From replica URL
litestream ltx REPLICA_URL
```

## Description

The `ltx` command lists all LTX (Log Transaction) files available for a database. This is useful for:

- Verifying backups exist
- Finding available restore points
- Debugging replication issues
- Understanding the backup timeline

## Flags

| Flag | Description |
|------|-------------|
| `-config PATH` | Configuration file path (default: `/etc/litestream.yml`) |
| `-no-expand-env` | Disable environment variable expansion |

## Output Columns

| Column | Description |
|--------|-------------|
| `min_txid` | Starting transaction ID |
| `max_txid` | Ending transaction ID |
| `size` | File size in bytes |
| `created` | Creation timestamp (RFC3339) |

## Examples

### List from Config

```bash
$ litestream ltx /data/app.db

min_txid              max_txid              size     created
0000000000000001      0000000000000100      4096     2024-01-15T10:00:00Z
0000000000000101      0000000000000200      8192     2024-01-15T10:30:00Z
0000000000000201      0000000000000300      4096     2024-01-15T11:00:00Z
```

### List from Replica URL

```bash
$ litestream ltx s3://mybucket/app-backup

min_txid              max_txid              size     created
0000000000000001      0000000000001000      102400   2024-01-15T00:00:00Z
```

### Check Specific Storage

```bash
# S3
litestream ltx s3://bucket/path

# GCS
litestream ltx gs://bucket/path

# Azure
litestream ltx abs://container@account/path

# SFTP
litestream ltx sftp://user@host/path
```

## Understanding Output

### Transaction ID Range

Each LTX file covers a range of transactions:
- `min_txid`: First transaction in file
- `max_txid`: Last transaction in file

Files should be contiguous for complete recovery.

### Compaction Levels

LTX files exist at different compaction levels:
- **L0**: Raw files from WAL changes (many small files)
- **L1+**: Compacted files (fewer, larger files)

The `ltx` command shows L0 files. Compacted files may have wider TXID ranges.

### File Sizes

- Small files (~4KB): Few pages changed
- Large files: Many pages changed or compacted
- Consistent sizes: Regular write patterns

## Use Cases

### Verify Backup Exists

```bash
# Check that backups are being created
litestream ltx s3://bucket/backup | head -5
```

### Find Restore Point

```bash
# Find transaction around a specific time
litestream ltx /data/app.db | grep "2024-01-15T10"
```

### Diagnose Gaps

```bash
# Look for TXID gaps (missing transactions)
litestream ltx /data/app.db
# Check that max_txid of one file matches min_txid-1 of next
```

### Check Replication Lag

```bash
# Compare local vs remote TXIDs
# Local TXID
litestream status /data/app.db

# Remote TXID (latest max_txid)
litestream ltx s3://bucket/backup | tail -1
```

## Common Issues

### "database not found in config"
- Verify database path matches config exactly
- Check config file is being loaded

### "database has no replica"
- Add replica configuration for the database
- Check config file syntax

### Empty output
- No LTX files have been created yet
- Start `litestream replicate` first
- Make a write to the database to trigger replication

### Connection errors
- Verify storage credentials
- Check network connectivity
- Review endpoint configuration

## Note: Deprecated Command

The `litestream wal` command has been deprecated and replaced by `litestream ltx`. Use `ltx` for listing backup files.

## See Also

- [restore Command](restore.md)
- [status Command](status.md)
- [LTX Format](../concepts/ltx-format.md)
