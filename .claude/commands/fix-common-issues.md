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

## Diagnostic Commands

```bash
# Check database integrity
sqlite3 database.db "PRAGMA integrity_check;"

# List replicated LTX files
litestream ltx /path/to/db.sqlite

# Check replication status
litestream databases

# Test restoration
litestream restore -o test.db [replica-url]
```

## Prevention Checklist

- [ ] Always test with databases >1GB
- [ ] Run with race detector in CI
- [ ] Test all page sizes (4KB, 8KB, 16KB, 32KB)
- [ ] Verify eventual consistency handling
- [ ] Check for proper locking (Lock vs RLock)
- [ ] Ensure atomic file operations
- [ ] Preserve timestamps in compaction
