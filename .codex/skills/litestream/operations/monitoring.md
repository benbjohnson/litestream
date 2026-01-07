# Monitoring Litestream

Monitor replication status, health, and performance.

## Prometheus Metrics

Enable metrics by setting the `addr` in configuration:

```yaml
addr: ":9090"
```

Access metrics at `http://localhost:9090/metrics`.

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `litestream_db_size_bytes` | Gauge | Database file size |
| `litestream_wal_size_bytes` | Gauge | WAL file size |
| `litestream_total_wal_bytes` | Counter | Total bytes written to WAL |
| `litestream_checkpoint_count_total` | Counter | Total checkpoints performed |
| `litestream_sync_count_total` | Counter | Total syncs to replica |
| `litestream_sync_error_count_total` | Counter | Total sync errors |

### Labels

Metrics include labels for filtering:

- `db`: Database file path

Example query:
```promql
litestream_wal_size_bytes{db="/data/app.db"}
```

## Prometheus Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'litestream'
    static_configs:
      - targets: ['localhost:9090']
```

## Grafana Dashboard

### Basic Dashboard Panels

**Database Size**
```promql
litestream_db_size_bytes
```

**WAL Size**
```promql
litestream_wal_size_bytes
```

**Sync Rate**
```promql
rate(litestream_sync_count_total[5m])
```

**Error Rate**
```promql
rate(litestream_sync_error_count_total[5m])
```

**Checkpoint Rate**
```promql
rate(litestream_checkpoint_count_total[5m])
```

## Alerting

### Prometheus Alert Rules

```yaml
# alerts.yml
groups:
  - name: litestream
    rules:
      - alert: LitestreamSyncErrors
        expr: rate(litestream_sync_error_count_total[5m]) > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Litestream sync errors detected"
          description: "Database {{ $labels.db }} has sync errors"

      - alert: LitestreamWALTooLarge
        expr: litestream_wal_size_bytes > 100000000  # 100MB
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "WAL file too large"
          description: "WAL for {{ $labels.db }} is {{ $value | humanizeBytes }}"

      - alert: LitestreamNoSyncs
        expr: rate(litestream_sync_count_total[10m]) == 0
        for: 15m
        labels:
          severity: critical
        annotations:
          summary: "No Litestream syncs"
          description: "No syncs for {{ $labels.db }} in 15 minutes"
```

## Health Checks

### Heartbeat URL

Configure Litestream to ping a health check service:

```yaml
heartbeat-url: https://healthchecks.io/ping/your-uuid
heartbeat-interval: 5m
```

Compatible services:
- [Healthchecks.io](https://healthchecks.io)
- [Cronitor](https://cronitor.io)
- [Better Uptime](https://betteruptime.com)

### Systemd Watchdog

```ini
# /etc/systemd/system/litestream.service
[Service]
WatchdogSec=30
Restart=on-failure
RestartSec=5
```

### Custom Health Check Script

```bash
#!/bin/bash
# health-check.sh

# Check if Litestream is running
if ! pgrep -x litestream > /dev/null; then
    echo "CRITICAL: Litestream not running"
    exit 2
fi

# Check recent sync activity
LAST_SYNC=$(stat -c %Y /data/.litestream/*/l0/*.ltx 2>/dev/null | sort -rn | head -1)
NOW=$(date +%s)
AGE=$((NOW - LAST_SYNC))

if [ "$AGE" -gt 300 ]; then
    echo "WARNING: No sync in $AGE seconds"
    exit 1
fi

echo "OK: Last sync $AGE seconds ago"
exit 0
```

## Log Monitoring

### Logging Configuration

```yaml
logging:
  level: info    # debug, info, warn, error
  type: text     # text or json
  stderr: false  # Output to stderr instead of stdout
```

### JSON Logging

For log aggregation (ELK, Loki, etc.):

```yaml
logging:
  type: json
```

### Log Levels

| Level | Description |
|-------|-------------|
| `debug` | Verbose debugging information |
| `info` | Normal operation messages |
| `warn` | Warning conditions |
| `error` | Error conditions |

### Example Log Output

```
INFO  database opened path=/data/app.db
INFO  replica started type=s3
INFO  sync completed txid=1234 duration=45ms
WARN  checkpoint delayed reason="readers present"
ERROR sync failed error="connection timeout"
```

## Status Commands

### Quick Status Check

```bash
# View all databases
litestream status

# Output:
# database           status   local txid            wal size
# /data/app.db       ok       0000000000001234      1.2 MB
```

### Verify Backups Exist

```bash
# List LTX files
litestream ltx /data/app.db

# Count backup files
litestream ltx /data/app.db | wc -l
```

### Watch Status

```bash
# Continuous monitoring
watch -n 5 litestream status
```

## Docker Monitoring

### Docker Health Check

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD ["litestream", "databases"] || exit 1
```

### Docker Compose with Metrics

```yaml
services:
  app:
    image: myapp
    volumes:
      - ./data:/data

  litestream:
    image: litestream/litestream
    volumes:
      - ./data:/data
      - ./litestream.yml:/etc/litestream.yml
    ports:
      - "9090:9090"
    command: replicate

  prometheus:
    image: prom/prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9091:9090"
```

## Monitoring Checklist

### Daily Checks
- [ ] Verify Litestream is running
- [ ] Check for sync errors in logs
- [ ] Verify WAL size is reasonable

### Weekly Checks
- [ ] Review backup file counts
- [ ] Test restore to verify backup integrity
- [ ] Check storage usage trends

### Monthly Checks
- [ ] Full disaster recovery test
- [ ] Review and update alerting thresholds
- [ ] Audit access to backup storage

## See Also

- [Troubleshooting](troubleshooting.md)
- [Recovery](recovery.md)
- [status Command](../commands/status.md)
