Debug WAL monitoring issues in Litestream. This command helps diagnose problems with WAL change detection, checkpointing, and replication triggers.

First, understand the symptoms:
- Is replication not triggering on changes?
- Are checkpoints failing or not happening?
- Is the WAL growing unbounded?

Then debug the monitoring system:

1. **Check monitor goroutine** (db.go:1499):
```go
// Verify monitor is running
func (db *DB) monitor() {
    ticker := time.NewTicker(db.MonitorInterval) // Default: 1s
    // Check if ticker is firing
    // Verify checkWAL() is being called
}
```

2. **Verify WAL change detection**:
```go
// Check if WAL changes are detected
func (db *DB) checkWAL() (bool, error) {
    // Get WAL size and checksum
    // Compare with previous values
    // Should return true if changed
}
```

3. **Debug checkpoint triggers**:
```go
// Check checkpoint thresholds
MinCheckpointPageN int // Default: 1000 pages
TruncatePageN int     // Default: 121359 pages

// Verify WAL page count
walPageCount := db.WALPageCount()
if walPageCount > db.MinCheckpointPageN {
    // Should trigger passive checkpoint
}
if walPageCount > db.TruncatePageN {
    // Should trigger truncate checkpoint (emergency brake)
}
```

4. **Check long-running read transaction**:
```go
// Ensure rtx is maintained
if db.rtx == nil {
    // Read transaction lost - replication may fail
}
```

5. **Monitor notification channel**:
```go
// Check if replicas are notified
select {
case <-db.notify:
    // WAL change detected
default:
    // No changes
}
```

Common issues to check:
- MonitorInterval too long (default 1s)
- Checkpoint failing due to active transactions
- Read transaction preventing checkpoint
- Notify channel not triggering replicas
- WAL file permissions issues

Debug commands:
```sql
-- Check WAL status
PRAGMA wal_checkpoint;
PRAGMA journal_mode;
PRAGMA page_count;
PRAGMA wal_autocheckpoint;

-- Check for locks
SELECT * FROM pragma_lock_status();
```

Testing WAL monitoring:
```go
func TestDB_WALMonitoring(t *testing.T) {
    db := setupTestDB(t)

    // Set fast monitoring for test
    db.MonitorInterval = 10 * time.Millisecond

    // Write data
    writeTestData(t, db, 100)

    // Wait for notification
    select {
    case <-db.notify:
        // Success
    case <-time.After(1 * time.Second):
        t.Error("WAL change not detected")
    }
}
```

Monitor with logging:
```go
slog.Debug("wal check",
    "size", walInfo.Size,
    "checksum", walInfo.Checksum,
    "pages", walInfo.PageCount)
```
