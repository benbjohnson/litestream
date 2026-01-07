# Litestream Configuration Overview

Litestream can be configured via command-line arguments (simple cases) or YAML configuration file (recommended for production).

## Configuration File Location

Default locations checked:
1. `-config` flag path
2. `/etc/litestream.yml`

## Basic Structure

```yaml
# Global settings (optional)
addr: ":9090"                    # Prometheus metrics address

# Database configurations
dbs:
  - path: /data/app.db           # Database to replicate
    replica:
      url: s3://bucket/path      # Destination
```

## Database Configuration

### Single Database

```yaml
dbs:
  - path: /data/app.db
    monitor-interval: 10ms        # WAL check frequency (default: 10ms)
    checkpoint-interval: 1m       # Time-based checkpoint (default: 1m)
    min-checkpoint-page-count: 1000  # Page threshold for checkpoint
    busy-timeout: 5s              # SQLite busy timeout
    replica:
      url: s3://bucket/app-backup
```

### Directory of Databases

```yaml
dbs:
  - dir: /data/databases         # Directory to scan
    pattern: "*.db"              # File pattern to match
    recursive: true              # Include subdirectories
    watch: true                  # Monitor for new databases
    replica:
      url: s3://bucket/db-backups
```

## Replica Configuration

### Using URL (Recommended)

```yaml
replica:
  url: s3://bucket/path
  sync-interval: 1s              # How often to sync (default: 1s)
```

### Using Type + Path

```yaml
replica:
  type: s3
  bucket: my-bucket
  path: backups/app
  region: us-east-1
```

## Global Defaults

Set defaults for all replicas at the top level:

```yaml
# These apply to all replicas unless overridden
access-key-id: ${AWS_ACCESS_KEY_ID}
secret-access-key: ${AWS_SECRET_ACCESS_KEY}
region: us-east-1
sync-interval: 1s

dbs:
  - path: /data/app1.db
    replica:
      url: s3://bucket/app1      # Uses global AWS credentials

  - path: /data/app2.db
    replica:
      url: s3://bucket/app2      # Uses global AWS credentials
```

## Environment Variables

Environment variables are expanded in configuration:

```yaml
dbs:
  - path: ${DATABASE_PATH}
    replica:
      url: ${REPLICA_URL}
      access-key-id: ${AWS_ACCESS_KEY_ID}
      secret-access-key: ${AWS_SECRET_ACCESS_KEY}
```

Disable expansion with `-no-expand-env` flag.

## Compaction Levels

Configure multi-level compaction for storage efficiency:

```yaml
levels:
  - interval: 30s     # L1: Compact every 30 seconds
  - interval: 5m      # L2: Compact every 5 minutes
  - interval: 1h      # L3: Compact hourly
```

Default levels: 30s, 5m, 1h

## Snapshot Configuration

```yaml
snapshot:
  interval: 24h       # Create snapshots daily
  retention: 24h      # Keep snapshots for 24 hours
```

## L0 Retention

```yaml
l0-retention: 1h                    # Keep L0 files for 1 hour
l0-retention-check-interval: 5m     # Check retention every 5 minutes
```

## Logging

```yaml
logging:
  level: info         # debug, info, warn, error
  type: text          # text or json
  stderr: false       # Output to stderr instead of stdout
```

Or via environment: `LOG_LEVEL=debug`

## Heartbeat

Send periodic heartbeat to external URL:

```yaml
heartbeat-url: https://healthchecks.io/ping/xxx
heartbeat-interval: 5m
```

## Shutdown Settings

```yaml
shutdown-sync-timeout: 30s      # Max time to wait for final sync
shutdown-sync-interval: 100ms   # Retry interval during shutdown
```

## Subcommand Execution

Run a subcommand alongside replication:

```yaml
exec: "myapp serve"
```

Litestream will:
1. Start replication
2. Execute the subcommand
3. Shut down when subcommand exits

## Metrics Server

```yaml
addr: ":9090"         # Prometheus metrics endpoint
```

Access metrics at `http://localhost:9090/metrics`

## Database Settings Reference

| Setting | Default | Description |
|---------|---------|-------------|
| `path` | required | Database file path |
| `dir` | - | Directory to scan for databases |
| `pattern` | - | File pattern (required with `dir`) |
| `recursive` | false | Scan subdirectories |
| `watch` | false | Monitor directory for changes |
| `meta-path` | - | Custom metadata directory |
| `monitor-interval` | 10ms | WAL check frequency |
| `checkpoint-interval` | 1m | Time-based checkpoint interval |
| `busy-timeout` | 1s | SQLite busy timeout |
| `min-checkpoint-page-count` | 1000 | Page threshold for passive checkpoint |
| `truncate-page-n` | 10000 | Emergency truncate threshold |
| `restore-if-db-not-exists` | false | Auto-restore on startup |

## Replica Settings Reference

| Setting | Default | Description |
|---------|---------|-------------|
| `url` | - | Replica URL (recommended) |
| `type` | - | Replica type (file, s3, gs, abs, sftp, nats, webdav, oss) |
| `path` | - | Replica path |
| `sync-interval` | 1s | Sync frequency |
| `validation-interval` | - | Validation frequency |

## Validation Errors

Common configuration errors:

- `cannot specify both 'path' and 'dir'` - Use one or the other
- `'pattern' is required when using 'dir'` - Specify file pattern
- `cannot specify url & path for replica` - Use URL or type+path
- `age encryption is not currently supported` - Use v0.3.x for encryption
