# Litestream Code Patterns Reference

Condensed code patterns and anti-patterns for agents writing Litestream code.

## Architectural Boundaries

```
DB Layer (db.go)           → Database state, restoration, monitoring
Replica Layer (replica.go) → Replication mechanics only
Storage Layer              → ReplicaClient implementations
```

### DO: Handle database state in DB layer

```go
func (db *DB) init() error {
    if db.needsRestore() {
        if err := db.restore(); err != nil {
            return err
        }
    }
    return db.replica.Start() // Replica focuses only on replication
}
```

### DON'T: Put database state logic in Replica layer

```go
// WRONG
func (r *Replica) Start() error {
    if needsRestore() {  // Wrong layer!
        restoreDatabase()
    }
}
```

## Atomic File Operations

### DO: Write to temp file, then rename

```go
func writeFileAtomic(path string, data []byte) error {
    dir := filepath.Dir(path)
    tmpFile, err := os.CreateTemp(dir, ".tmp-*")
    if err != nil {
        return fmt.Errorf("create temp file: %w", err)
    }
    tmpPath := tmpFile.Name()

    defer func() {
        if tmpFile != nil {
            tmpFile.Close()
            os.Remove(tmpPath)
        }
    }()

    if _, err := tmpFile.Write(data); err != nil {
        return fmt.Errorf("write temp file: %w", err)
    }
    if err := tmpFile.Sync(); err != nil {
        return fmt.Errorf("sync temp file: %w", err)
    }
    if err := tmpFile.Close(); err != nil {
        return fmt.Errorf("close temp file: %w", err)
    }
    tmpFile = nil

    if err := os.Rename(tmpPath, path); err != nil {
        os.Remove(tmpPath)
        return fmt.Errorf("rename to final path: %w", err)
    }
    return nil
}
```

### DON'T: Write directly to final location

```go
// WRONG - Can leave partial files on failure
os.WriteFile(path, data, 0644)
```

## Error Handling

### DO: Return errors immediately

```go
func (db *DB) validatePosition() error {
    dpos, err := db.Pos()
    if err != nil {
        return err
    }
    rpos := replica.Pos()
    if dpos.TXID < rpos.TXID {
        return fmt.Errorf("database position (%v) behind replica (%v)", dpos, rpos)
    }
    return nil
}
```

### DON'T: Log and continue on critical errors

```go
// WRONG
if err := processFile(file); err != nil {
    log.Printf("error: %v", err) // Just logging!
    // Continuing is dangerous
}
```

### DO: Return errors from loops

```go
func (db *DB) processFiles() error {
    for _, file := range files {
        if err := processFile(file); err != nil {
            return fmt.Errorf("process file %s: %w", file, err)
        }
    }
    return nil
}
```

## Locking Patterns

### DO: Use Lock() for writes

```go
r.mu.Lock()
defer r.mu.Unlock()
r.pos = pos
```

### DON'T: Use RLock for write operations

```go
// WRONG - Race condition
r.mu.RLock()
defer r.mu.RUnlock()
r.pos = pos // Writing with RLock!
```

### Lock Ordering

Always acquire in order: Store.mu → DB.mu → DB.chkMu → Replica.mu

## Compaction Patterns

### DO: Read from local when available

```go
f, err := os.Open(db.LTXPath(info.Level, info.MinTXID, info.MaxTXID))
if err == nil {
    return f, nil // Local copy is complete and consistent
}
return replica.Client.OpenLTXFile(...) // Fall back to remote
```

### DON'T: Read from remote during compaction

```go
// WRONG - Can get partial/corrupt data from eventually consistent storage
f, err := client.OpenLTXFile(ctx, level, minTXID, maxTXID, 0, 0)
```

### DO: Preserve earliest timestamp

```go
info.CreatedAt = oldestSourceFile.CreatedAt
```

### DON'T: Use current time during compaction

```go
// WRONG - Loses timestamp granularity for point-in-time restores
info := &ltx.FileInfo{CreatedAt: time.Now()}
```

## Lock Page Handling

The lock page at 1 GB (0x40000000) must always be skipped:

```go
lockPgno := ltx.LockPgno(pageSize)
if pgno == lockPgno {
    continue
}
```

| Page Size | Lock Page Number |
|-----------|------------------|
| 4 KB      | 262145           |
| 8 KB      | 131073           |
| 16 KB     | 65537            |
| 32 KB     | 32769            |

## Common Pitfalls

1. **Mixing architectural concerns**: DB state logic in Replica layer
2. **Recreating existing functionality**: Use `db.verify()` for snapshots
3. **Ignoring lock page**: Must skip during replication and compaction
4. **Generic error types**: Return `os.ErrNotExist` for missing files
5. **Blocking iterators**: Use lazy pagination for `LTXFiles`
6. **Ignoring context**: Check `ctx.Done()` in long operations
