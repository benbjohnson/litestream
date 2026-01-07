# status Command

Display replication status for configured databases.

## Synopsis

```bash
litestream status [-config PATH] [DATABASE_PATH]
```

## Description

The `status` command shows the current replication state of databases in your configuration file. It displays:

- Database path
- Current status
- Local transaction ID
- WAL file size

## Flags

| Flag | Description |
|------|-------------|
| `-config PATH` | Configuration file path (default: `/etc/litestream.yml`) |
| `-no-expand-env` | Disable environment variable expansion |

## Output Columns

| Column | Description |
|--------|-------------|
| `database` | Path to the SQLite database file |
| `status` | Current state: `ok`, `not initialized`, `no database`, `error` |
| `local txid` | Latest local transaction ID |
| `wal size` | Current WAL file size |

## Status Values

| Status | Description |
|--------|-------------|
| `ok` | Database is operational and has been replicated |
| `not initialized` | Database exists but hasn't been replicated yet |
| `no database` | Database file doesn't exist at configured path |
| `error` | Unable to read database status |
| `unknown` | Status could not be determined |

## Examples

### View All Databases

```bash
$ litestream status

database           status            local txid            wal size
/data/app.db       ok                0000000000001234      1.2 MB
/data/users.db     ok                0000000000000567      256 KB
/data/logs.db      not initialized   -                     0 B
```

### View Specific Database

```bash
$ litestream status /data/app.db

database           status            local txid            wal size
/data/app.db       ok                0000000000001234      1.2 MB
```

### With Custom Config

```bash
$ litestream status -config /etc/litestream-prod.yml
```

## Understanding the Output

### Transaction ID (TXID)

The local TXID shows the latest transaction that has been:
1. Written to the WAL
2. Converted to LTX format
3. Stored locally

This is the local state, not necessarily the replicated state.

### WAL Size

The WAL size indicates how much data is pending:
- Small size: WAL is being checkpointed regularly
- Large size: Checkpoint may be blocked or writes are very frequent
- `0 B`: No pending changes or WAL doesn't exist

## Limitations

The `status` command shows **local state only**. It does not:
- Connect to storage backends
- Show replica TXID or sync status
- Display replication lag

For replica-side status, use:
- `litestream ltx` to list remote LTX files
- Check Prometheus metrics during `replicate`
- Review Litestream logs

## Common Issues

### "no database"
- Database file doesn't exist at configured path
- Check path in config file
- Run `litestream restore` if needed

### "not initialized"
- Database exists but Litestream hasn't created LTX files yet
- Start `litestream replicate` to initialize
- Make a write to trigger replication

### "error"
- Unable to read database metadata
- Check file permissions
- Verify database isn't corrupted

## Monitoring

For ongoing monitoring during replication:

```bash
# Watch status
watch -n 5 litestream status

# Or use Prometheus metrics
curl http://localhost:9090/metrics | grep litestream
```

## See Also

- [replicate Command](replicate.md)
- [ltx Command](ltx.md)
- [Monitoring](../operations/monitoring.md)
