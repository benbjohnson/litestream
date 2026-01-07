# Systemd Deployment

Run Litestream as a systemd service on Linux.

## Installation

### Debian/Ubuntu

```bash
wget https://github.com/benbjohnson/litestream/releases/download/v0.3.13/litestream-v0.3.13-linux-amd64.deb
sudo dpkg -i litestream-v0.3.13-linux-amd64.deb
```

### Other Linux

```bash
wget https://github.com/benbjohnson/litestream/releases/download/v0.3.13/litestream-v0.3.13-linux-amd64.tar.gz
tar -xzf litestream-v0.3.13-linux-amd64.tar.gz
sudo mv litestream /usr/local/bin/
```

## Basic Service Unit

### /etc/systemd/system/litestream.service

```ini
[Unit]
Description=Litestream SQLite Replication
After=network.target

[Service]
Type=simple
User=litestream
Group=litestream
ExecStart=/usr/local/bin/litestream replicate
Restart=on-failure
RestartSec=5

# Environment
EnvironmentFile=/etc/litestream/env

[Install]
WantedBy=multi-user.target
```

## Configuration

### /etc/litestream.yml

```yaml
dbs:
  - path: /var/lib/myapp/app.db
    replica:
      url: s3://my-bucket/app-backup
      access-key-id: ${AWS_ACCESS_KEY_ID}
      secret-access-key: ${AWS_SECRET_ACCESS_KEY}
      region: us-east-1
```

### /etc/litestream/env

```bash
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=xxx
```

Secure the environment file:

```bash
sudo chmod 600 /etc/litestream/env
sudo chown litestream:litestream /etc/litestream/env
```

## User Setup

Create dedicated user:

```bash
sudo useradd --system --no-create-home --shell /bin/false litestream
```

Grant access to database directory:

```bash
sudo chown -R litestream:litestream /var/lib/myapp
# Or add to application group
sudo usermod -a -G myapp litestream
```

## Service Management

```bash
# Enable on boot
sudo systemctl enable litestream

# Start service
sudo systemctl start litestream

# Check status
sudo systemctl status litestream

# View logs
sudo journalctl -u litestream -f

# Restart
sudo systemctl restart litestream

# Stop
sudo systemctl stop litestream
```

## With Application

### Application Service

```ini
# /etc/systemd/system/myapp.service
[Unit]
Description=My Application
After=network.target litestream.service
Requires=litestream.service

[Service]
Type=simple
User=myapp
ExecStart=/usr/local/bin/myapp serve
Restart=always

[Install]
WantedBy=multi-user.target
```

### Litestream Managing Application

Alternative: Litestream runs and manages the app:

```ini
# /etc/systemd/system/litestream-app.service
[Unit]
Description=Litestream with Application
After=network.target

[Service]
Type=simple
User=myapp
ExecStartPre=/usr/local/bin/litestream restore -if-db-not-exists -o /var/lib/myapp/app.db s3://bucket/backup
ExecStart=/usr/local/bin/litestream replicate -exec "/usr/local/bin/myapp serve"
Restart=always
EnvironmentFile=/etc/litestream/env

[Install]
WantedBy=multi-user.target
```

## Auto-Restore on Start

```ini
[Service]
ExecStartPre=/usr/local/bin/litestream restore -if-db-not-exists -if-replica-exists -o /var/lib/myapp/app.db s3://bucket/backup
ExecStart=/usr/local/bin/litestream replicate
```

## Hardening

### Security Options

```ini
[Service]
# Restrict capabilities
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true

# Allow write only to data directory
ReadWritePaths=/var/lib/myapp

# Restrict network
RestrictAddressFamilies=AF_INET AF_INET6

# Restrict system calls
SystemCallFilter=@system-service
SystemCallErrorNumber=EPERM
```

### Full Hardened Unit

```ini
[Unit]
Description=Litestream SQLite Replication
After=network.target

[Service]
Type=simple
User=litestream
Group=litestream

ExecStart=/usr/local/bin/litestream replicate

Restart=on-failure
RestartSec=5

# Security
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
ReadWritePaths=/var/lib/myapp
EnvironmentFile=/etc/litestream/env

[Install]
WantedBy=multi-user.target
```

## Logging

### Journal Integration

View logs:

```bash
# Follow logs
sudo journalctl -u litestream -f

# View recent logs
sudo journalctl -u litestream --since "1 hour ago"

# View errors only
sudo journalctl -u litestream -p err
```

### Log Level

In litestream.yml:

```yaml
logging:
  level: info  # debug, info, warn, error
```

Or via environment:

```bash
# /etc/litestream/env
LOG_LEVEL=debug
```

## Health Monitoring

### Watchdog

```ini
[Service]
WatchdogSec=60
Restart=on-failure
```

### Heartbeat

```yaml
# litestream.yml
heartbeat-url: https://healthchecks.io/ping/xxx
heartbeat-interval: 5m
```

### Custom Health Check

```bash
#!/bin/bash
# /usr/local/bin/litestream-health

if ! systemctl is-active --quiet litestream; then
    echo "CRITICAL: Litestream not running"
    exit 2
fi

if ! litestream databases > /dev/null 2>&1; then
    echo "WARNING: Cannot query databases"
    exit 1
fi

echo "OK: Litestream running"
exit 0
```

## Multiple Databases

### Separate Services

For different users/permissions:

```bash
# /etc/systemd/system/litestream-app1.service
# /etc/systemd/system/litestream-app2.service
```

### Single Service Multiple DBs

```yaml
# /etc/litestream.yml
dbs:
  - path: /var/lib/app1/db.sqlite
    replica:
      url: s3://bucket/app1

  - path: /var/lib/app2/db.sqlite
    replica:
      url: s3://bucket/app2
```

## Troubleshooting

### Service won't start

```bash
# Check syntax
sudo systemctl daemon-reload
sudo systemctl start litestream

# View detailed errors
sudo journalctl -u litestream -n 50 --no-pager
```

### Permission denied

```bash
# Check file ownership
ls -la /var/lib/myapp/

# Check user groups
id litestream

# Fix permissions
sudo chown -R litestream:litestream /var/lib/myapp
```

### Database locked

- Ensure only one Litestream instance
- Check for other processes using database

```bash
fuser /var/lib/myapp/app.db
```

## See Also

- [Docker Deployment](docker.md)
- [Configuration Overview](../configuration/overview.md)
- [Monitoring](../operations/monitoring.md)
