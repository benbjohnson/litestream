# Docker Deployment

Run Litestream in Docker containers.

## Official Image

```bash
docker pull litestream/litestream
```

## Basic Usage

### Standalone Replication

```bash
docker run -d \
  -v /path/to/data:/data \
  -v /path/to/litestream.yml:/etc/litestream.yml \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  litestream/litestream replicate
```

### One-Shot Restore

```bash
docker run --rm \
  -v /path/to/data:/data \
  -e AWS_ACCESS_KEY_ID=xxx \
  -e AWS_SECRET_ACCESS_KEY=xxx \
  litestream/litestream restore -o /data/app.db s3://bucket/backup
```

## Multi-Container with Docker Compose

### Application + Litestream Sidecar

```yaml
# docker-compose.yml
version: '3.8'

services:
  app:
    image: myapp:latest
    volumes:
      - db-data:/data
    depends_on:
      - litestream

  litestream:
    image: litestream/litestream
    volumes:
      - db-data:/data
      - ./litestream.yml:/etc/litestream.yml
    environment:
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
    command: replicate

volumes:
  db-data:
```

### Litestream Configuration

```yaml
# litestream.yml
dbs:
  - path: /data/app.db
    replica:
      url: s3://my-bucket/app-backup
```

## Single Container Pattern

Run both application and Litestream in one container:

### Dockerfile

```dockerfile
FROM alpine:latest

# Install Litestream
ADD https://github.com/benbjohnson/litestream/releases/download/v0.3.13/litestream-v0.3.13-linux-amd64.tar.gz /tmp/
RUN tar -xzf /tmp/litestream-*.tar.gz -C /usr/local/bin

# Install your application
COPY myapp /usr/local/bin/
COPY litestream.yml /etc/litestream.yml

# Create data directory
RUN mkdir -p /data

# Use Litestream as entrypoint to wrap your app
ENTRYPOINT ["/usr/local/bin/litestream", "replicate", "-exec"]
CMD ["myapp", "serve"]
```

### With Auto-Restore

```dockerfile
FROM alpine:latest

ADD https://github.com/benbjohnson/litestream/releases/download/v0.3.13/litestream-v0.3.13-linux-amd64.tar.gz /tmp/
RUN tar -xzf /tmp/litestream-*.tar.gz -C /usr/local/bin

COPY myapp /usr/local/bin/
COPY litestream.yml /etc/litestream.yml
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh
RUN mkdir -p /data

ENTRYPOINT ["/entrypoint.sh"]
CMD ["myapp", "serve"]
```

```bash
#!/bin/sh
# entrypoint.sh

# Restore database if it doesn't exist
if [ ! -f /data/app.db ]; then
    echo "Restoring database..."
    litestream restore -if-replica-exists -o /data/app.db "${REPLICA_URL}"
fi

# Start Litestream with the application
exec litestream replicate -exec "$@"
```

## Health Checks

```yaml
# docker-compose.yml
services:
  litestream:
    image: litestream/litestream
    volumes:
      - db-data:/data
      - ./litestream.yml:/etc/litestream.yml
    healthcheck:
      test: ["CMD", "litestream", "databases"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
```

## Metrics Exposure

```yaml
# docker-compose.yml
services:
  litestream:
    image: litestream/litestream
    ports:
      - "9090:9090"
    volumes:
      - db-data:/data
      - ./litestream.yml:/etc/litestream.yml
```

```yaml
# litestream.yml
addr: ":9090"

dbs:
  - path: /data/app.db
    replica:
      url: s3://bucket/backup
```

## Environment Variables

Pass credentials via environment:

```yaml
# docker-compose.yml
services:
  litestream:
    image: litestream/litestream
    environment:
      - AWS_ACCESS_KEY_ID
      - AWS_SECRET_ACCESS_KEY
      - AWS_REGION=us-east-1
```

```yaml
# litestream.yml (with env expansion)
dbs:
  - path: /data/app.db
    replica:
      url: s3://bucket/backup
      access-key-id: ${AWS_ACCESS_KEY_ID}
      secret-access-key: ${AWS_SECRET_ACCESS_KEY}
```

## Volume Considerations

### Persistent Volume

```yaml
volumes:
  db-data:
    driver: local
```

### tmpfs for Ephemeral

```yaml
services:
  app:
    tmpfs:
      - /data
```

Use with auto-restore for ephemeral containers.

## Multi-Stage Build

```dockerfile
# Build stage
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o myapp

# Runtime stage
FROM alpine:latest

# Install Litestream
COPY --from=litestream/litestream:latest /usr/local/bin/litestream /usr/local/bin/

# Copy application
COPY --from=builder /app/myapp /usr/local/bin/
COPY litestream.yml /etc/litestream.yml

ENTRYPOINT ["litestream", "replicate", "-exec"]
CMD ["myapp"]
```

## Troubleshooting

### Container exits immediately
- Check Litestream logs: `docker logs <container>`
- Verify configuration file exists
- Check database path is correct

### Permission denied
- Ensure volumes have correct permissions
- Use `user:` directive if needed
- Check file ownership in container

### Database not found
- Verify volume mount paths
- Check auto-restore is configured
- Ensure replica has backups

## See Also

- [Fly.io Deployment](fly-io.md)
- [Kubernetes Deployment](kubernetes.md)
- [Configuration Overview](../configuration/overview.md)
