---
description: Debug IPC Unix socket issues
---

# Debug IPC Command

Diagnose issues with the Litestream IPC control socket (`server.go`).

## 1. Socket Configuration Check

Verify the socket is enabled and correctly configured:

```yaml
# litestream.yml
socket:
  enabled: true                          # Default: false
  path: /var/run/litestream.sock         # Default path
  permissions: 0600                      # Default permissions
```

Source: `SocketConfig` struct in `server.go:17-21`, defaults in `DefaultSocketConfig()`.

## 2. Endpoint Reference

All endpoints are HTTP over Unix socket (`server.go:74-87`):

| Method | Path | Request Body | Response |
|--------|------|-------------|----------|
| `GET` | `/info` | — | `{version, pid, uptime_seconds, started_at, database_count}` |
| `GET` | `/list` | — | `{databases: [{path, status, last_sync_at}]}` |
| `GET` | `/txid?path=` | — | `{txid}` |
| `POST` | `/register` | `{path, replica_url}` | `{status, path}` |
| `POST` | `/unregister` | `{path, timeout?}` | `{status, path}` |
| `POST` | `/start` | `{path, timeout?}` | `{status, path}` |
| `POST` | `/stop` | `{path, timeout?}` | `{status, path}` |
| `GET` | `/debug/pprof/` | — | Standard Go pprof index |
| `GET` | `/debug/pprof/profile` | — | CPU profile |
| `GET` | `/debug/pprof/trace` | — | Execution trace |

## 3. Testing with curl

```bash
# Server info (version, PID, uptime)
curl --unix-socket /var/run/litestream.sock http://localhost/info

# List all managed databases
curl --unix-socket /var/run/litestream.sock http://localhost/list

# Get transaction ID for a specific database
curl --unix-socket /var/run/litestream.sock "http://localhost/txid?path=/path/to/db"

# Register a new database at runtime
curl --unix-socket /var/run/litestream.sock -X POST \
  -H "Content-Type: application/json" \
  -d '{"path":"/path/to/db","replica_url":"s3://bucket/path"}' \
  http://localhost/register

# Unregister (stop replicating) a database
curl --unix-socket /var/run/litestream.sock -X POST \
  -H "Content-Type: application/json" \
  -d '{"path":"/path/to/db"}' \
  http://localhost/unregister

# CPU profile (30 seconds by default)
curl --unix-socket /var/run/litestream.sock http://localhost/debug/pprof/profile > cpu.prof
go tool pprof cpu.prof

# Heap profile
curl --unix-socket /var/run/litestream.sock http://localhost/debug/pprof/heap > heap.prof
go tool pprof heap.prof
```

## 4. Common Issues

### Socket not enabled
**Symptom**: `curl: (7) Couldn't connect to server`
**Fix**: Set `socket.enabled: true` in config and restart litestream.

### Permission denied
**Symptom**: `curl: (7) Permission denied`
**Fix**: Check `socket.permissions` (default `0600`). The connecting user must match the litestream process owner.

### Stale socket file after crash
**Symptom**: `bind: address already in use` in logs on startup
**Fix**: Remove the stale socket file: `rm /var/run/litestream.sock`

### Database not found
**Symptom**: `{"error":"database not found"}` from `/txid` or `/start`
**Fix**: Verify the path matches the expanded path in config. Use `/list` to see registered databases. Note that `$PID` and env vars are expanded in config paths.

### Register fails with "already exists"
**Symptom**: `{"error":"database already registered"}`
**Fix**: The database path is already being replicated. Use `/unregister` first, then `/register` with new settings.

## 5. pprof Debugging

Available pprof endpoints on the IPC socket:

```bash
# Interactive CPU profile analysis
curl -s --unix-socket /var/run/litestream.sock \
  http://localhost/debug/pprof/profile?seconds=30 > cpu.prof
go tool pprof -http=:8080 cpu.prof

# Goroutine dump (useful for deadlock investigation)
curl --unix-socket /var/run/litestream.sock \
  http://localhost/debug/pprof/goroutine?debug=2

# Memory allocation profile
curl -s --unix-socket /var/run/litestream.sock \
  http://localhost/debug/pprof/heap > heap.prof
go tool pprof -http=:8080 heap.prof

# Execution trace (captures scheduler, GC, goroutine events)
curl -s --unix-socket /var/run/litestream.sock \
  http://localhost/debug/pprof/trace?seconds=5 > trace.out
go tool trace trace.out
```
