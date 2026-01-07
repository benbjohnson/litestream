# NATS JetStream Configuration

Replicate SQLite databases to NATS JetStream object storage.

## Quick Start

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://nats.example.com:4222
      bucket: litestream-backups
```

## URL Format

```
nats://HOST:PORT
```

The bucket is specified separately in configuration.

## Configuration

### Basic Setup

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://localhost:4222
      bucket: app-backups
```

### With Authentication

#### Username/Password

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://nats.example.com:4222
      bucket: backups
      username: litestream
      password: ${NATS_PASSWORD}
```

#### Token

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://nats.example.com:4222
      bucket: backups
      token: ${NATS_TOKEN}
```

#### NKey

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://nats.example.com:4222
      bucket: backups
      nkey: SUACS34K...
```

#### JWT + Seed (Decentralized Auth)

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://nats.example.com:4222
      bucket: backups
      jwt: eyJ0eXAiOiJKV1QiLC...
      seed: SUACS34K...
```

#### Credentials File

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://nats.example.com:4222
      bucket: backups
      creds: /etc/litestream/nats.creds
```

## TLS Configuration

### Basic TLS

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://nats.example.com:4222
      bucket: backups
      tls: true
```

### TLS with Custom CA

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://nats.example.com:4222
      bucket: backups
      tls: true
      root-cas:
        - /etc/litestream/ca.pem
```

### Mutual TLS (mTLS)

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://nats.example.com:4222
      bucket: backups
      tls: true
      root-cas:
        - /etc/litestream/ca.pem
      client-cert: /etc/litestream/client.pem
      client-key: /etc/litestream/client.key
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `url` | - | NATS server URL |
| `bucket` | - | JetStream object store bucket |
| `username` | - | Username for auth |
| `password` | - | Password for auth |
| `token` | - | Token for auth |
| `nkey` | - | NKey seed for auth |
| `jwt` | - | JWT for decentralized auth |
| `seed` | - | Seed for JWT auth |
| `creds` | - | Path to credentials file |
| `tls` | false | Enable TLS |
| `root-cas` | - | List of CA certificate paths |
| `client-cert` | - | Client certificate path |
| `client-key` | - | Client key path |
| `max-reconnects` | 60 | Max reconnection attempts |
| `reconnect-wait` | 2s | Wait between reconnects |
| `timeout` | 10s | Connection timeout |

## NATS Server Setup

### Enable JetStream

In `nats-server.conf`:

```
jetstream {
    store_dir: /data/nats
    max_mem: 1G
    max_file: 10G
}
```

### Create Object Store Bucket

Using NATS CLI:

```bash
nats object add litestream-backups --storage file --replicas 3
```

### User Permissions

For decentralized auth, the user needs:

```
publish: ["$JS.API.OBJECT.>"]
subscribe: ["$JS.API.OBJECT.>", "_INBOX.>"]
```

## Examples

### Local Development

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://localhost:4222
      bucket: dev-backups
```

### Clustered NATS

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://nats1.example.com:4222,nats://nats2.example.com:4222,nats://nats3.example.com:4222
      bucket: backups
      username: litestream
      password: ${NATS_PASSWORD}
```

### Synadia NGS (NATS Global Service)

```yaml
dbs:
  - path: /data/app.db
    replica:
      type: nats
      url: nats://connect.ngs.global
      bucket: backups
      creds: /etc/litestream/ngs.creds
      tls: true
```

## Troubleshooting

### "Connection refused"
- Verify NATS server is running
- Check firewall allows port 4222
- Verify URL is correct

### "Authorization violation"
- Check username/password or credentials
- Verify user has permissions for JetStream operations

### "Bucket not found"
- Create the object store bucket first
- Verify bucket name is correct

### "TLS handshake failed"
- Verify CA certificate is correct
- Check client cert/key match
- Ensure server certificate is valid

### Reconnection issues
- Increase `max-reconnects` for unstable networks
- Adjust `reconnect-wait` for your environment
