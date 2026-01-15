# IPC Control Commands

Litestream provides runtime control commands for dynamically managing database replication without restarting the daemon. This is useful for scenarios where you need to selectively enable/disable replication based on runtime conditions such as external locks, failover states, or dynamic provisioning.

## Table of Contents
- [Overview](#overview)
- [Configuration](#configuration)
- [Security](#security)
- [Commands](#commands)
  - [start](#start)
  - [stop](#stop)
  - [sync](#sync)
  - [status](#status)
  - [list](#list)
  - [info](#info)
- [Usage Examples](#usage-examples)
- [HTTP API](#http-api)
- [Use Cases](#use-cases)

## Overview

The IPC control system consists of two components:

1. **Control Server**: Runs alongside the `litestream replicate` daemon, accepting commands via Unix socket or HTTP
2. **CLI Commands**: Send commands to the control server to manage database replication at runtime

The control server is opt-in: configure a socket path to enable it. You can also access the control API via HTTP if the metrics server is enabled.

## Configuration

### Unix Socket (Opt-In)

The Unix socket must be explicitly enabled. This is because users often run multiple Litestream instances, and a default socket path would cause conflicts.

```yaml
# litestream.yml
socket:
  path: /var/run/litestream.sock  # Required to enable control socket
  permissions: 0600                # Owner-only access (default)
  persist-to-config: false         # Persist enabled state to config (optional)
```

When running multiple Litestream instances, use unique socket paths:

```yaml
# Instance 1
socket:
  path: /var/run/litestream-app1.sock

# Instance 2
socket:
  path: /var/run/litestream-app2.sock
```

### HTTP API

The control API is automatically available via HTTP if the metrics server is enabled:

```yaml
addr: "localhost:9090"  # Enables both /metrics and /control/ endpoints
```

The HTTP API shares the same server as Prometheus metrics, reducing operational complexity.

## Security

### Unix Socket Security

The Unix socket is **safe by default**:

- **0600 permissions**: Only the socket owner can read/write
- **Local IPC only**: Not exposed to the network
- **Root directory**: `/var/run` is root-owned on most systems
- **Can be disabled**: Set `socket: ""` if not needed

Unlike exposing a Docker socket (which grants full system access), Litestream's control socket only manages database replication - a much narrower security surface.

### HTTP API Security

The HTTP API currently has no built-in authentication. Security recommendations:

- **Bind to localhost**: Use `addr: "localhost:9090"` for local-only access
- **Private network**: If remote access is needed, bind to a private network address
- **Use firewall rules**: Restrict access at the network level
- **Reverse proxy**: Put behind a reverse proxy with authentication if exposing publicly

## Commands

### start

Start replication for a database.

```bash
litestream start [OPTIONS] DB_PATH
```

**Options:**
- `-config PATH` - Path to config file with replica settings
- `-wait` - Block until replication has started
- `-timeout SECONDS` - Maximum wait time (default: 30)
- `-socket PATH` - Control socket path (required)

**Examples:**

```bash
# Start replication (async)
litestream start -socket /var/run/litestream.sock /data/app.db

# Start with explicit config and wait for confirmation
litestream start -socket /var/run/litestream.sock -config app-replica.yml -wait /data/app.db

# Start with custom timeout
litestream start -socket /var/run/litestream.sock -wait -timeout 60 /data/app.db
```

**Errors:**
- `database not found` - Database not registered with the daemon
- `database already enabled` - Database is already replicating
- `database has no replica configured` - Database has no replica configured
- `timeout waiting for start` - Database didn't start within timeout period

### stop

Stop replication for a database with graceful shutdown.

```bash
litestream stop [OPTIONS] DB_PATH
```

**Options:**
- `-timeout SECONDS` - Maximum wait time (default: 30)
- `-socket PATH` - Control socket path (required)

The stop command always waits for a graceful shutdown, flushing any pending WAL changes to the replica before stopping.

**Examples:**

```bash
# Stop replication
litestream stop -socket /var/run/litestream.sock /data/app.db

# Stop with a custom timeout
litestream stop -socket /var/run/litestream.sock -timeout 60 /data/app.db
```

**Errors:**
- `database not found` - Database not registered with the daemon
- `database already disabled` - Database is not currently replicating

### sync

Force an immediate sync of pending changes.

```bash
litestream sync [OPTIONS] DB_PATH
```

**Options:**
- `-wait` - Block until sync completes
- `-timeout SECONDS` - Maximum wait time (default: 30)
- `-socket PATH` - Control socket path (required)

**Examples:**

```bash
# Trigger sync (async)
litestream sync -socket /var/run/litestream.sock /data/app.db

# Trigger sync and wait for completion
litestream sync -socket /var/run/litestream.sock -wait /data/app.db
```

**Errors:**
- `database not found` - Database not registered with the daemon
- `database has no replica` - Database has no replica configured
- `sync failed` - Sync operation failed

### status

Query the replication status of a database.

```bash
litestream status [OPTIONS] DB_PATH
```

**Options:**
- `-socket PATH` - Control socket path (required)

**Output:**

```json
{
  "path": "/data/app.db",
  "status": "open"
}
```

Status values:
- `open` - Database is open and replicating
- `closed` - Database is registered but not replicating

### list

List all databases managed by the daemon.

```bash
litestream list [OPTIONS]
```

**Options:**
- `-socket PATH` - Control socket path (required)

**Note:** The HTTP/IPC method name `databases` is supported as an alias for `list`.

**Output:**

```json
[
  {
    "path": "/data/app.db",
    "status": "open"
  },
  {
    "path": "/data/tenant1.db",
    "status": "closed"
  }
]
```

### info

Show daemon information.

```bash
litestream info [OPTIONS]
```

**Options:**
- `-socket PATH` - Control socket path (required)

**Output:**

```json
{
  "version": "v0.4.0",
  "pid": 1234,
  "config_path": "/etc/litestream.yml",
  "socket_path": "/var/run/litestream.sock",
  "http_addr": "localhost:9090",
  "persist_to_config": false,
  "databases": 2,
  "open_databases": 1,
  "started_at": "2024-01-01T00:00:00Z"
}
```

## Usage Examples

### Dynamic Multi-Tenant Scenario

Managing per-tenant databases with runtime control:

```bash
# Start the daemon with socket enabled
litestream replicate -config /etc/litestream.yml

# Dynamically add a new tenant database
litestream start -socket /var/run/litestream.sock -config tenant-replica.yml -wait /data/tenant-abc.db

# Check status
litestream status -socket /var/run/litestream.sock /data/tenant-abc.db

# Force a sync before maintenance
litestream sync -socket /var/run/litestream.sock -wait /data/tenant-abc.db

# Stop replication when tenant is inactive
litestream stop -socket /var/run/litestream.sock /data/tenant-abc.db
```

### Failover with External Locks

Using control commands with distributed locks:

```bash
#!/bin/bash
SOCKET=/var/run/litestream.sock

# Acquire distributed lock for this database
if acquire_lock "db-primary-lock"; then
    # We have the lock - start replication
    litestream start -socket $SOCKET -wait /data/app.db

    # Run application
    ./my-app

    # Clean shutdown - stop replication
    litestream stop -socket $SOCKET /data/app.db

    # Release lock
    release_lock "db-primary-lock"
fi
```

### Pre-Shutdown Sync

Ensure all changes are synced before stopping:

```bash
SOCKET=/var/run/litestream.sock

# Force final sync before stopping daemon
litestream sync -socket $SOCKET -wait -timeout 60 /data/app.db

# Stop the database
litestream stop -socket $SOCKET /data/app.db

# Now safe to stop daemon
kill $LITESTREAM_PID
```

## HTTP API

The control API is available via HTTP when the metrics server is enabled:

**Endpoint:** `POST http://localhost:9090/control/`

**Request Format (JSON-RPC 2.0):**

```json
{
  "jsonrpc": "2.0",
  "method": "start",
  "params": {
    "path": "/data/app.db",
    "wait": true,
    "timeout": 30
  },
  "id": 1
}
```

**Response Format:**

```json
{
  "jsonrpc": "2.0",
  "result": {
    "status": "started",
    "path": "/data/app.db"
  },
  "id": 1
}
```

**Error Response:**

```json
{
  "jsonrpc": "2.0",
  "error": {
    "code": -32001,
    "message": "database not found: /data/app.db"
  },
  "id": 1
}
```

**Available Methods:**
- `start` - Start replication
- `stop` - Stop replication
- `sync` - Force sync
- `status` - Query status
- `list` - List databases
- `info` - Show daemon information
- `databases` - Alias for `list`

**Example with curl:**

```bash
curl -X POST http://localhost:9090/control/ \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "list",
    "id": 1
  }'
```

## Use Cases

### 1. Multi-Tenant SaaS

Dynamically start/stop replication for tenant databases based on activity:

```yaml
# Main config - no databases configured
dbs: []
socket:
  path: /var/run/litestream.sock
```

```bash
SOCKET=/var/run/litestream.sock

# When tenant signs up
litestream start -socket $SOCKET -config tenant-config.yml /data/tenant-${ID}.db

# When tenant becomes inactive
litestream stop -socket $SOCKET /data/tenant-${ID}.db
```

### 2. Active-Passive Failover

Use external coordination (etcd, Consul) to determine which node replicates:

```bash
SOCKET=/var/run/litestream.sock

# On primary node
if is_primary; then
    litestream start -socket $SOCKET -wait /data/app.db
else
    litestream stop -socket $SOCKET /data/app.db
fi
```

### 3. Maintenance Windows

Stop replication during maintenance, resume after:

```bash
SOCKET=/var/run/litestream.sock

# Before maintenance
litestream stop -socket $SOCKET /data/app.db

# Perform maintenance
./maintenance-script.sh

# Resume replication
litestream start -socket $SOCKET -wait /data/app.db
```

### 4. Cost Optimization

Disable replication during off-hours for cost savings:

```bash
# Cron: Stop replication at night
0 22 * * * litestream stop -socket /var/run/litestream.sock /data/app.db

# Cron: Resume in the morning
0 6 * * * litestream start -socket /var/run/litestream.sock -wait /data/app.db
```

## Troubleshooting

### Socket Path Required

```
Error: socket path required; use -socket flag
```

**Solution:**
The `-socket` flag is required for all control commands. Specify the socket path:
```bash
litestream start -socket /var/run/litestream.sock /data/app.db
```

### Socket Connection Failed

```
Error: failed to connect to control socket: dial unix /var/run/litestream.sock: connect: no such file or directory
```

**Solutions:**
1. Check that `litestream replicate` is running
2. Verify socket is enabled in config with `socket.path` set
3. Check socket path matches config
4. Verify permissions on `/var/run` directory

### Permission Denied

```
Error: failed to connect to control socket: dial unix /var/run/litestream.sock: connect: permission denied
```

**Solutions:**
1. Run command as same user as daemon
2. Check socket permissions with `ls -la /var/run/litestream.sock`
3. Adjust `socket-permissions` in config if needed

### Database Not Found

```
Error: database not found: /data/app.db
```

The database must be registered with the daemon first. Options:

1. Add to config file and restart daemon
2. Use control API to register dynamically (future feature)

### Timeout Waiting for Start

```
Error: timeout waiting for start
```

The database is taking longer than expected to open. Check:

1. Database file is accessible
2. Replica configuration is valid
3. Network connectivity to replica storage
4. Increase timeout: `--timeout 60`
