# replicate Command

Continuously replicate SQLite databases to cloud storage.

## Synopsis

```bash
# With config file
litestream replicate [-config PATH]

# With command line arguments
litestream replicate [flags] DB_PATH REPLICA_URL [REPLICA_URL...]

# Run application alongside replication
litestream replicate -exec "myapp serve"
```

## Description

The `replicate` command is the main Litestream daemon. It:

1. Opens configured SQLite databases
2. Monitors the WAL for changes
3. Converts changes to LTX format
4. Uploads to configured storage backends
5. Manages checkpoints and compaction

## Flags

| Flag | Description |
|------|-------------|
| `-config PATH` | Configuration file path (default: `/etc/litestream.yml`) |
| `-no-expand-env` | Disable environment variable expansion |
| `-exec COMMAND` | Run command as subprocess (shutdown when it exits) |
| `-restore-if-db-not-exists` | Restore from replica if database doesn't exist |
| `-once` | Replicate once and exit |
| `-force-snapshot` | Force snapshot creation (requires `-once`) |
| `-enforce-retention` | Enforce snapshot retention (requires `-once`) |

## Examples

### Using Config File

```bash
# Use default config location
litestream replicate

# Specify config file
litestream replicate -config /path/to/litestream.yml
```

### Command Line Arguments

```bash
# Single replica
litestream replicate /data/app.db s3://bucket/backup

# Multiple replicas (deprecated: use config file)
litestream replicate /data/app.db s3://bucket/backup sftp://user@host/backup
```

### Run Application Subprocess

```bash
# Via command line
litestream replicate -exec "myapp serve --port 8080"

# Via config file
# exec: "myapp serve --port 8080"
```

Litestream will:
1. Start replication
2. Execute the command
3. Shut down gracefully when command exits

### Auto-Restore on Startup

```bash
litestream replicate -restore-if-db-not-exists
```

If the database doesn't exist, restore it from the replica before starting replication.

### One-Shot Replication

```bash
# Replicate once and exit
litestream replicate -once

# Force snapshot during one-shot
litestream replicate -once -force-snapshot

# Enforce retention during one-shot
litestream replicate -once -enforce-retention
```

Useful for:
- Manual backup triggers
- CI/CD pipelines
- Periodic snapshots via cron

## Behavior

### Startup Sequence

1. Load and validate configuration
2. Open each configured database
3. Optionally restore missing databases
4. Start WAL monitoring
5. Begin replication to backends
6. Optionally execute subprocess
7. Start metrics server (if configured)

### Monitoring Intervals

- **WAL Check**: Every `monitor-interval` (default: 10ms)
- **Sync**: Every `sync-interval` (default: 1s)
- **Checkpoint**: When page threshold reached or `checkpoint-interval` elapsed
- **Compaction**: Per configured level intervals

### Graceful Shutdown

On SIGTERM or SIGINT:
1. Stop accepting new connections
2. Wait for in-flight syncs (up to `shutdown-sync-timeout`)
3. Perform final sync
4. Close databases
5. Exit

## Metrics

When `addr` is configured, Prometheus metrics available at `/metrics`:

```
litestream_db_size_bytes{db="/path/to/db"}
litestream_wal_size_bytes{db="/path/to/db"}
litestream_sync_count_total{db="/path/to/db"}
litestream_checkpoint_count_total{db="/path/to/db"}
```

## Common Issues

### "database not found in config"
- Verify database path matches exactly
- Check path expansion (~ vs absolute)

### "cannot specify -once with -exec"
- These flags are mutually exclusive
- Use one or the other

### "flag must be positioned before arguments"
- Put all flags before DB_PATH and REPLICA_URL
- Example: `litestream replicate -once /data/app.db s3://bucket`

### Database not syncing
- Check sync-interval is appropriate
- Verify storage backend credentials
- Review logs for errors

## See Also

- [Configuration Overview](../configuration/overview.md)
- [restore Command](restore.md)
- [status Command](status.md)
