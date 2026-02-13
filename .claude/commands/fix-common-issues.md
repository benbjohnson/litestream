---
description: Fix common Litestream issues
---

# Fix Common Issues Command

Diagnose and fix common issues in Litestream deployments.

## Issue 1: Lock Page Not Being Skipped

**Symptom**: Errors or corruption with databases >1GB

**Check**:
```bash
# Find lock page references
grep -r "LockPgno" --include="*.go"
```

**Fix**:
```go
// Ensure all page iterations skip lock page
lockPgno := ltx.LockPgno(pageSize)
if pgno == lockPgno {
    continue
}
```

## Issue 2: Race Condition in Replica Position

**Symptom**: Data races detected, inconsistent position tracking

**Check**:
```bash
go test -race -v -run TestReplica_Sync ./...
```

**Fix**:
```go
// Change from RLock to Lock for writes
func (r *Replica) SetPos(pos ltx.Pos) {
    r.mu.Lock() // NOT RLock!
    defer r.mu.Unlock()
    r.pos = pos
}
```

## Issue 3: Eventual Consistency Issues

**Symptom**: Compaction failures, partial file reads

**Check**:
```bash
# Look for remote reads during compaction
grep -r "OpenLTXFile" db.go | grep -v "os.Open"
```

**Fix**:
```go
// Always try local first
f, err := os.Open(db.LTXPath(info.Level, info.MinTXID, info.MaxTXID))
if err == nil {
    return f, nil
}
// Only fall back to remote if local doesn't exist
return replica.Client.OpenLTXFile(...)
```

## Issue 4: CreatedAt Timestamp Loss

**Symptom**: Point-in-time recovery lacks accurate timestamps

**Check**:
```go
info, err := client.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
if err != nil {
    t.Fatal(err)
}
if info.CreatedAt.IsZero() {
    t.Fatal("CreatedAt not set")
}
```

**Fix**:
```go
// Ensure storage metadata is copied into the returned FileInfo
modTime := resp.LastModified
info.CreatedAt = modTime
```

## Issue 5: Non-Atomic File Writes

**Symptom**: Partial files, corruption on crash

**Check**:
```bash
# Find direct writes without temp files
grep -r "os.Create\|os.WriteFile" --include="*.go"
```

**Fix**:
```go
// Write to temp, then rename
tmpPath := path + ".tmp"
if err := os.WriteFile(tmpPath, data, 0644); err != nil {
    return err
}
return os.Rename(tmpPath, path)
```

## Issue 6: WAL Checkpoint Blocking

**Symptom**: WAL grows indefinitely, database locks

**Check**:
```sql
-- Check WAL size
PRAGMA wal_checkpoint(PASSIVE);
SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size();
```

**Fix**:
```go
// Release read transaction periodically
db.rtx.Rollback()
db.rtx = nil
// Checkpoint
db.db.Exec("PRAGMA wal_checkpoint(RESTART)")
// Restart read transaction
db.initReadTx()
```

## Issue 7: Memory Leaks

**Symptom**: Growing memory usage over time

**Check**:
```bash
# Generate heap profile
go test -memprofile=mem.prof -run=XXX -bench=.
go tool pprof -top mem.prof
```

**Fix**:
```go
// Use sync.Pool for buffers
var pagePool = sync.Pool{
    New: func() interface{} {
        b := make([]byte, pageSize)
        return &b
    },
}

// Close resources properly
defer func() {
    if f != nil {
        f.Close()
    }
}()
```

## Issue 8: Corrupted or Missing LTX Files

**Symptom**: Sync failures with errors like "ltx validation failed", "nonsequential page numbers",
"non-contiguous transaction files", or persistent sync retry backoff loops after unclean shutdowns.

**Check**:
```bash
# Look for LTXError messages in logs
rg -i "ltx.*error|reset.*local|auto.recover" /var/log/litestream.log

# Check if meta directory has corrupted state (note dot prefix)
ls -la /path/to/.database.db-litestream/ltx/
```

**Fix - Manual Reset**:
```bash
# Clears local LTX state, forces fresh snapshot on next sync
# Database file is NOT modified
litestream reset /path/to/database.db

# With explicit config file
litestream reset -config /etc/litestream.yml /path/to/database.db
```

**Fix - Automatic Recovery**:
```yaml
# Add to replica config in litestream.yml
dbs:
  - path: /path/to/database.db
    replicas:
      - url: s3://bucket/path
        auto-recover: true  # Automatically resets on LTX errors
```

**When to use which**: Use `auto-recover` for unattended deployments where automatic recovery
is preferred over manual intervention. Use manual `reset` when you want to investigate the
corruption first. `auto-recover` is disabled by default because resetting discards local LTX
history, which may reduce point-in-time restore granularity.

**Reference**: `cmd/litestream/reset.go`, `replica.go` (auto-recover logic), `db.go` (`ResetLocalState`)

## Issue 9: IPC Socket Connection Failures

**Symptom**: `connection refused` or `no such file or directory` when using the control socket

**Check**:
```bash
# Verify socket exists and has correct permissions
ls -la /var/run/litestream.sock

# Verify socket is enabled in config
rg -A3 'socket:' /etc/litestream.yml

# Check if litestream process is running
pgrep -a litestream
```

**Fix**:
```yaml
# Enable socket in litestream.yml
socket:
  enabled: true
  path: /var/run/litestream.sock
  permissions: 0600
```

**Stale socket**: If litestream crashed, the socket file may still exist. The process creates a new socket on startup and will fail if the stale file exists. Remove it manually:
```bash
rm /var/run/litestream.sock
```

**Reference**: `server.go` (`SocketConfig`, `Server.Start`)

## Issue 10: Backup Files Accumulating (Retention Disabled)

**Symptom**: Storage usage growing unbounded, old LTX files never deleted

**Check**:
```bash
# Look for retention warning in logs
rg "retention disabled" /var/log/litestream.log
```

**Fix**:
```yaml
# Option 1: Re-enable Litestream retention (default)
retention:
  enabled: true

# Option 2: Keep disabled, but configure cloud lifecycle policies
# Example: S3 lifecycle rule to expire objects in ltx/ prefix after 30 days
```

**Reference**: `store.go` (`SetRetentionEnabled`), `compactor.go` (`RetentionEnabled`), `cmd/litestream/replicate.go:295`

## Issue 11: v0.3.x Restore Not Finding Backups

**Symptom**: Restore fails with "no snapshots available" but v0.3.x backups exist in storage

**Check**:
```bash
# Verify v0.3.x backup structure exists
aws s3 ls s3://bucket/path/generations/ --recursive | head -20
```

**Fix**: Ensure the backend implements `ReplicaClientV3`. The S3 backend supports this automatically. The restore process checks for v0.3.x backups when no v0.4.x+ backup is found.

**Reference**: `v3.go` (`ReplicaClientV3` interface), `s3/replica_client.go`

## Diagnostic Commands

```bash
# Check database integrity
sqlite3 database.db "PRAGMA integrity_check;"

# List replicated LTX files
litestream ltx /path/to/db.sqlite

# Check replication status
litestream databases

# Reset corrupted local state
litestream reset /path/to/database.db

# Test restoration
litestream restore -o test.db [replica-url]

# IPC socket diagnostics (requires socket.enabled: true)
curl --unix-socket /var/run/litestream.sock http://localhost/info
curl --unix-socket /var/run/litestream.sock http://localhost/list
curl --unix-socket /var/run/litestream.sock "http://localhost/txid?path=/path/to/db"
```

## Prevention Checklist

- [ ] Always test with databases >1GB
- [ ] Run with race detector in CI
- [ ] Test all page sizes (4KB, 8KB, 16KB, 32KB)
- [ ] Verify eventual consistency handling
- [ ] Check for proper locking (Lock vs RLock)
- [ ] Ensure atomic file operations
- [ ] Preserve timestamps in compaction
