# Local File Backend Configuration

Replicate SQLite databases to local filesystem or mounted network storage.

## Quick Start

```yaml
dbs:
  - path: /data/app.db
    replica:
      path: /mnt/backups/app
```

## Configuration

### Using Path

```yaml
dbs:
  - path: /data/app.db
    replica:
      path: /var/backups/litestream/app
```

### Using URL

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: file:///var/backups/litestream/app
```

## Use Cases

### Local Backup

Simple local backup before cloud replication:

```yaml
dbs:
  - path: /data/app.db
    replica:
      path: /var/backups/app
```

### Network Attached Storage (NAS)

Mount NAS and replicate:

```yaml
dbs:
  - path: /data/app.db
    replica:
      path: /mnt/nas/backups/app
```

### External Drive

Replicate to mounted external drive:

```yaml
dbs:
  - path: /data/app.db
    replica:
      path: /mnt/external/backups/app
```

### Shared Network Drive (NFS/SMB)

```yaml
dbs:
  - path: /data/app.db
    replica:
      path: /mnt/shared/backups/app
```

## Directory Structure

Litestream creates this structure in the replica path:

```
/var/backups/app/
├── generations/
│   ├── 0000000000000001/     # Generation directory
│   │   ├── 0/                # Level 0 (raw)
│   │   │   ├── 00000001-00000010.ltx
│   │   │   └── 00000011-00000020.ltx
│   │   ├── 1/                # Level 1 (compacted)
│   │   │   └── 00000001-00000100.ltx
│   │   └── 2/                # Level 2 (more compacted)
│   │       └── 00000001-00001000.ltx
│   └── 0000000000000002/     # Next generation
│       └── ...
└── snapshots/                # Database snapshots
    └── ...
```

## Permissions

Ensure Litestream has write access:

```bash
# Create backup directory
sudo mkdir -p /var/backups/litestream
sudo chown litestream:litestream /var/backups/litestream

# Or adjust permissions
sudo chmod 755 /var/backups/litestream
```

## Examples

### Development Setup

Local backup for development:

```yaml
dbs:
  - path: ~/projects/myapp/data.db
    replica:
      path: ~/backups/myapp
```

### Production with NFS

```yaml
dbs:
  - path: /data/production.db
    replica:
      path: /mnt/nfs/litestream/production
      sync-interval: 1s
```

### Multiple Databases to Same Location

```yaml
dbs:
  - path: /data/app1.db
    replica:
      path: /var/backups/app1

  - path: /data/app2.db
    replica:
      path: /var/backups/app2
```

## Performance Considerations

### Local SSD
- Best performance
- Fastest sync times
- Good for development

### HDD/NAS
- Slower than SSD
- Consider increasing sync-interval
- Good for secondary backups

### Network Mounts (NFS/SMB)
- Depends on network speed
- May have higher latency
- Ensure mount is reliable

## Troubleshooting

### "Permission denied"
- Check directory ownership
- Verify write permissions
- Ensure parent directories exist

### "No space left on device"
- Free disk space
- Configure retention to remove old files
- Use compaction to reduce storage

### "Mount point not available"
- Verify network mount is connected
- Check mount configuration
- Add mount to fstab for auto-mount

### Files not appearing
- Check the full path including generations/
- Verify sync-interval has elapsed
- Look for .ltx files in subdirectories

## Combining with Cloud Backup

Use file backend as primary, cloud as secondary:

```yaml
dbs:
  - path: /data/app.db
    # Primary: local for fast recovery
    replica:
      path: /var/backups/app

# Note: Litestream supports only one replica per database.
# For multiple destinations, run multiple Litestream instances
# or use a tool like rclone to sync the file replica to cloud.
```
