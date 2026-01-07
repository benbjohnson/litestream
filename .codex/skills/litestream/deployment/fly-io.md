# Fly.io Deployment

Deploy SQLite applications with Litestream on Fly.io.

## Overview

Fly.io + Litestream is excellent for:
- Single-node SQLite applications
- Global edge deployment
- Simple, cost-effective hosting

## Tigris Storage (Recommended)

Fly.io's Tigris is S3-compatible storage optimized for Fly:

```bash
# Create Tigris bucket
fly storage create

# Note the bucket name and credentials
```

### Configuration

```yaml
# litestream.yml
dbs:
  - path: /data/app.db
    replica:
      url: s3://your-bucket/app-backup?endpoint=fly.storage.tigris.dev&region=auto
```

Tigris automatically configures:
- Signing settings
- Content-MD5 handling
- Regional optimization

## Project Structure

```
myapp/
├── Dockerfile
├── fly.toml
├── litestream.yml
├── entrypoint.sh
└── ... (application files)
```

## Dockerfile

```dockerfile
FROM alpine:latest

# Install dependencies
RUN apk add --no-cache ca-certificates

# Install Litestream
ADD https://github.com/benbjohnson/litestream/releases/download/v0.3.13/litestream-v0.3.13-linux-amd64.tar.gz /tmp/
RUN tar -xzf /tmp/litestream-*.tar.gz -C /usr/local/bin && rm /tmp/litestream-*.tar.gz

# Copy application
COPY myapp /usr/local/bin/
COPY litestream.yml /etc/litestream.yml
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh
RUN mkdir -p /data

ENTRYPOINT ["/entrypoint.sh"]
```

## Entrypoint Script

```bash
#!/bin/sh
# entrypoint.sh
set -e

# Restore database if it doesn't exist
if [ ! -f /data/app.db ]; then
    echo "No database found. Attempting restore..."
    litestream restore -if-replica-exists -o /data/app.db "${REPLICA_URL}"
    if [ -f /data/app.db ]; then
        echo "Database restored successfully."
    else
        echo "No backup found. Starting fresh."
    fi
fi

# Start Litestream and application
exec litestream replicate -exec "myapp serve"
```

## fly.toml

```toml
app = "myapp"
primary_region = "iad"

[build]
  dockerfile = "Dockerfile"

[env]
  REPLICA_URL = "s3://your-bucket/app-backup?endpoint=fly.storage.tigris.dev&region=auto"

[mounts]
  source = "data"
  destination = "/data"

[[services]]
  internal_port = 8080
  protocol = "tcp"

  [[services.ports]]
    handlers = ["http"]
    port = 80

  [[services.ports]]
    handlers = ["tls", "http"]
    port = 443

  [services.concurrency]
    type = "connections"
    hard_limit = 25
    soft_limit = 20
```

## Setting Secrets

```bash
# For Tigris (credentials from fly storage create)
fly secrets set AWS_ACCESS_KEY_ID=xxx
fly secrets set AWS_SECRET_ACCESS_KEY=xxx

# Or for external S3
fly secrets set AWS_ACCESS_KEY_ID=AKIA...
fly secrets set AWS_SECRET_ACCESS_KEY=xxx
fly secrets set AWS_REGION=us-east-1
```

## Litestream Configuration

```yaml
# litestream.yml
dbs:
  - path: /data/app.db
    replica:
      url: ${REPLICA_URL}
      access-key-id: ${AWS_ACCESS_KEY_ID}
      secret-access-key: ${AWS_SECRET_ACCESS_KEY}
      sync-interval: 1s
```

## Volume Setup

Create persistent volume:

```bash
# Create volume in your region
fly volumes create data --region iad --size 1

# View volumes
fly volumes list
```

## Deployment

```bash
# Deploy
fly deploy

# View logs
fly logs

# SSH into container
fly ssh console
```

## Scaling Considerations

### Single Instance (Recommended)

SQLite works best with single writer:

```bash
# Set to exactly 1 instance
fly scale count 1
```

### Regional Deployment

Deploy to a single region for best SQLite performance:

```toml
primary_region = "iad"
```

### Read Replicas (Advanced)

For read-heavy workloads, consider:
- LiteFS for distributed reads
- Multiple regions with read replicas

## Health Checks

```toml
# fly.toml
[[services]]
  [services.http_checks]
    interval = 10000
    timeout = 2000
    path = "/health"
    method = "get"
```

## Monitoring

### Fly Metrics

```bash
fly status
fly logs
```

### Litestream Metrics

Expose metrics endpoint:

```yaml
# litestream.yml
addr: ":9091"
```

```toml
# fly.toml (internal service for metrics)
[[services]]
  internal_port = 9091
  protocol = "tcp"
```

## Troubleshooting

### Database Not Persisting

- Verify volume is mounted: `fly ssh console` then `ls -la /data`
- Check volume exists: `fly volumes list`

### Restore Failing

- Check Tigris credentials: `fly secrets list`
- Verify bucket exists and has data
- Check logs: `fly logs`

### Application Crashes on Start

- Ensure entrypoint script has execute permission
- Check Litestream configuration syntax
- Verify database path matches volume mount

### Slow Performance

- Ensure single instance
- Use volume in same region as app
- Check Tigris latency (should be low within Fly)

## Example: Go Application

Complete example for a Go web app:

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o myapp

FROM alpine:latest
RUN apk add --no-cache ca-certificates
ADD https://github.com/benbjohnson/litestream/releases/download/v0.3.13/litestream-v0.3.13-linux-amd64.tar.gz /tmp/
RUN tar -xzf /tmp/litestream-*.tar.gz -C /usr/local/bin

COPY --from=builder /app/myapp /usr/local/bin/
COPY litestream.yml /etc/litestream.yml
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && mkdir -p /data

ENTRYPOINT ["/entrypoint.sh"]
```

## See Also

- [Docker Deployment](docker.md)
- [S3 Configuration](../configuration/s3.md)
- [Recovery Operations](../operations/recovery.md)
