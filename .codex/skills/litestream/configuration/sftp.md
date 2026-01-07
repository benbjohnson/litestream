# SFTP Configuration

Replicate SQLite databases to SFTP servers.

## Quick Start

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: sftp://user@hostname:22/path/to/backup
      key-path: /etc/litestream/sftp_key
```

## URL Format

```
sftp://USER@HOST:PORT/PATH
```

Where:
- `USER` - SSH username
- `HOST` - SFTP server hostname
- `PORT` - SSH port (default: 22)
- `PATH` - Absolute path on server

## Authentication

### SSH Key (Recommended)

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: sftp://backupuser@backup.example.com:22/backups/app
      key-path: /etc/litestream/id_ed25519
```

Generate a key:
```bash
ssh-keygen -t ed25519 -f /etc/litestream/id_ed25519 -N ""
# Copy public key to server
ssh-copy-id -i /etc/litestream/id_ed25519.pub backupuser@backup.example.com
```

### Password Authentication

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: sftp://backupuser@backup.example.com/backups/app
      password: ${SFTP_PASSWORD}
```

**Note**: Key-based authentication is strongly recommended for security.

## Host Key Verification

**Important**: Always specify the host key for production deployments.

### Get Host Key

```bash
ssh-keyscan backup.example.com
```

Output example:
```
backup.example.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIMvvyp...
```

### Configure Host Key

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: sftp://backupuser@backup.example.com/backups/app
      key-path: /etc/litestream/id_ed25519
      host-key: ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIMvvyp...
```

Without host-key verification, you're vulnerable to MITM attacks.

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `user` | - | SSH username |
| `password` | - | SSH password |
| `key-path` | - | Path to SSH private key |
| `host-key` | - | Server's SSH host key |
| `host` | - | SFTP hostname |
| `concurrent-writes` | false | Enable concurrent uploads |

## Examples

### Basic SFTP Backup

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: sftp://backup@192.168.1.100:22/var/backups/litestream/app
      key-path: ~/.ssh/backup_key
      host-key: ssh-ed25519 AAAAC3...
```

### NAS/Network Storage

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: sftp://admin@nas.local/volume1/backups/app
      key-path: /etc/litestream/nas_key
      host-key: ssh-rsa AAAAB3Nza...
```

### Multiple Databases to Same Server

```yaml
dbs:
  - path: /data/app1.db
    replica:
      url: sftp://backup@server.example.com/backups/app1
      key-path: /etc/litestream/sftp_key
      host-key: ssh-ed25519 AAAAC3...

  - path: /data/app2.db
    replica:
      url: sftp://backup@server.example.com/backups/app2
      key-path: /etc/litestream/sftp_key
      host-key: ssh-ed25519 AAAAC3...
```

## Server Setup

### Create Backup User

```bash
# Create user with restricted shell
sudo useradd -m -s /bin/false backupuser

# Create backup directory
sudo mkdir -p /var/backups/litestream
sudo chown backupuser:backupuser /var/backups/litestream

# Add SSH key
sudo mkdir -p /home/backupuser/.ssh
sudo cp /path/to/public_key.pub /home/backupuser/.ssh/authorized_keys
sudo chown -R backupuser:backupuser /home/backupuser/.ssh
sudo chmod 700 /home/backupuser/.ssh
sudo chmod 600 /home/backupuser/.ssh/authorized_keys
```

### Restrict to SFTP Only

Add to `/etc/ssh/sshd_config`:

```
Match User backupuser
    ForceCommand internal-sftp
    ChrootDirectory /var/backups
    AllowTcpForwarding no
    X11Forwarding no
```

## Troubleshooting

### "Connection refused"
- Verify SSH service is running on server
- Check port is correct (default 22)
- Verify firewall allows SSH connections

### "Permission denied"
- Verify username is correct
- Check SSH key permissions: `chmod 600 /path/to/key`
- Ensure public key is in server's authorized_keys
- Verify password is correct (if using password auth)

### "Host key verification failed"
- Get correct host key with `ssh-keyscan hostname`
- Update `host-key` configuration
- Or remove host-key to skip verification (not recommended)

### "No such file or directory"
- Ensure target directory exists on server
- Verify user has write permissions to directory

### Slow transfers
- Enable `concurrent-writes: true` for parallel uploads
- Check network latency to SFTP server
- Consider using a server geographically closer
