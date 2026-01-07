---
name: litestream
description: SQLite disaster recovery and streaming replication to cloud storage (S3, GCS, Azure, SFTP, NATS). Use this skill for configuring Litestream, deploying to cloud platforms, troubleshooting WAL replication issues, and implementing point-in-time recovery for SQLite databases.
version: 1.0.0
---

# Litestream

Litestream is a disaster recovery tool for SQLite that runs as a background process, continuously replicating database changes to cloud storage. It monitors the SQLite Write-Ahead Log (WAL), converts changes to immutable LTX files, and uploads them to your chosen storage backend.

## Key Concepts

- **WAL Monitoring**: Watches SQLite Write-Ahead Log for changes at configurable intervals
- **LTX Files**: Immutable files containing database page changes, never modified after creation
- **Generations**: Transaction ID (TXID) ranges enabling point-in-time recovery
- **Compaction**: Merges LTX files at configurable intervals to reduce storage overhead

## Quick Start

### Installation

```bash
# macOS
brew install litestream

# Linux (Debian/Ubuntu)
wget https://github.com/benbjohnson/litestream/releases/download/v0.3.13/litestream-v0.3.13-linux-amd64.deb
sudo dpkg -i litestream-v0.3.13-linux-amd64.deb

# Docker
docker pull litestream/litestream
```

### Basic Replication

```bash
# Command line (single database)
litestream replicate /path/to/db.sqlite s3://bucket/db

# With config file
litestream replicate -config /etc/litestream.yml
```

### Restore Database

```bash
# Restore latest backup
litestream restore -o /path/to/restored.db s3://bucket/db

# Point-in-time recovery
litestream restore -timestamp 2024-01-15T10:30:00Z -o /tmp/db s3://bucket/db

# Restore to specific transaction
litestream restore -txid 1000 -o /tmp/db s3://bucket/db
```

### Check Status

```bash
# View all databases and their replication status
litestream status

# List available LTX files
litestream ltx /path/to/db.sqlite
```

## Critical Rules

These rules are essential for correct Litestream operation:

1. **Lock Page at 1GB**: SQLite reserves the page at offset 0x40000000 (1GB). This page must always be skipped during replication and compaction. The page number varies by page size:
   - 4KB pages: page 262145
   - 8KB pages: page 131073
   - 16KB pages: page 65537

2. **LTX Files are Immutable**: Once created, LTX files are never modified. New changes create new files.

3. **Single Replica per Database**: Each database can only replicate to one destination. Use multiple Litestream instances for multiple destinations.

4. **WAL Mode Required**: SQLite must be in WAL mode (`PRAGMA journal_mode=WAL;`)

5. **Use `litestream ltx`**: The `litestream wal` command is deprecated.

## Storage Backends

| Backend | URL Scheme | Example |
|---------|-----------|---------|
| AWS S3 | `s3://` | `s3://bucket/path` |
| S3-Compatible (R2, Tigris, MinIO) | `s3://` | `s3://bucket/path?endpoint=host:port` |
| Google Cloud Storage | `gs://` | `gs://bucket/path` |
| Azure Blob Storage | `abs://` | `abs://container@account/path` |
| SFTP | `sftp://` | `sftp://user@host:22/path` |
| NATS JetStream | `nats://` | `nats://host:4222/bucket` |
| WebDAV | `webdav://` | `webdav://host/path` |
| Alibaba OSS | `oss://` | `oss://bucket/path` |
| Local File | (path) | `/var/backups/db` |

## Configuration File Example

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: s3://my-bucket/app-backup
      access-key-id: ${AWS_ACCESS_KEY_ID}
      secret-access-key: ${AWS_SECRET_ACCESS_KEY}
      region: us-east-1
      sync-interval: 1s
```

## When to Use This Skill

Use Litestream skill when:
- Configuring SQLite database replication to cloud storage
- Setting up disaster recovery for SQLite applications
- Troubleshooting replication issues (WAL growth, sync failures)
- Implementing point-in-time recovery procedures
- Deploying Litestream with Docker, Fly.io, Kubernetes, or systemd
- Understanding WAL-based replication concepts

## Skill Contents

- `concepts/` - Architecture, replication mechanics, LTX format, SQLite WAL
- `configuration/` - Storage backend configurations and examples
- `commands/` - CLI command reference (replicate, restore, status, ltx)
- `operations/` - Monitoring, troubleshooting, and recovery procedures
- `deployment/` - Docker, Fly.io, Kubernetes, and systemd deployment guides
- `scripts/` - Validation and diagnostic helper scripts

## Common Issues

### WAL Growing Too Large
- Check sync interval (default 1s may be too slow for write-heavy workloads)
- Verify storage backend connectivity
- Check for checkpoint blocking (long-running transactions)

### Replication Lag
- Monitor with `litestream status`
- Check network connectivity to storage backend
- Review sync interval configuration

### Restore Failures
- Verify backup exists: `litestream ltx /path/to/db`
- Check storage backend credentials
- Ensure target directory is writable

## Links

- [Official Documentation](https://litestream.io)
- [GitHub Repository](https://github.com/benbjohnson/litestream)
- [Getting Started Guide](https://litestream.io/getting-started/)
