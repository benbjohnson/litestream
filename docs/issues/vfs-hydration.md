# feat(vfs): Add background database hydration for improved read performance

## Description

Add support for background hydration of VFS databases to a local file. While hydration is in progress, reads continue using the existing page cache and remote LTX fetch mechanism. Once hydration completes, all reads are served from the local database file, eliminating remote fetch latency.

## Motivation

Currently, the VFS fetches pages on-demand from remote storage with LRU caching. While this works well for sparse access patterns, workloads that eventually touch most pages incur cumulative fetch latency. Background hydration allows the VFS to proactively restore the entire database locally while serving immediate requests from cache/remote, then seamlessly transition to local reads once complete.

## Proposed Behavior

### Read Mode
1. VFS opens and initializes page index as normal (`vfs.go:546-616`)
2. A background goroutine uses `CalcRestorePlan` to get LTX files and restores to a local database file using `ltx.Compactor` + `ltx.Decoder.DecodeDatabaseTo`
3. During hydration, `ReadAt` serves reads from:
   - LRU cache (if page is cached)
   - Remote LTX files via `FetchPage` (if not cached)
4. Once hydration completes, `ReadAt` serves all reads from the local database file
5. Polling continues to detect new LTX files; when new transactions arrive:
   - Apply the new pages to the local database file (fetch and write individual pages)
   - Update the page index as normal

### Write Mode
1. Same hydration process as read mode
2. After successful `syncToRemote()`, apply the synced pages to the local database file
3. The local file always reflects the latest successfully synced state

### VFS Close
1. Discard the local database file (delete it)
2. On next open, hydration starts fresh

## Configuration

```go
type VFS struct {
    // Existing fields...

    // Hydration configuration
    HydrationEnabled bool     // Enable background hydration (default: false)
    HydrationPath    string   // Path to local database file (required if enabled)
}
```

## Implementation Specification

### 1. New VFSFile Fields (`vfs.go`)

```go
type VFSFile struct {
    // Existing fields...

    // Hydration state
    hydrationEnabled  bool
    hydrationPath     string
    hydrationFile     *os.File
    hydrationComplete atomic.Bool      // True when restore completes
    hydrationTXID     ltx.TXID         // TXID the hydrated file is at
    hydrationMu       sync.Mutex       // Protects hydration file writes
    hydrationErr      error            // Stores fatal hydration error
}
```

### 2. Hydration Initialization (`Open`)

After building the initial index (`buildIndex`):

```go
func (f *VFSFile) initHydration(infos []*ltx.FileInfo) error {
    // Create local database file
    file, err := os.OpenFile(f.hydrationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
    if err != nil {
        return fmt.Errorf("create hydration file: %w", err)
    }
    f.hydrationFile = file

    // Start background restore
    f.wg.Add(1)
    go f.runHydration(infos)

    return nil
}
```

### 3. Background Hydration Using CalcRestorePlan

```go
func (f *VFSFile) runHydration(infos []*ltx.FileInfo) {
    defer f.wg.Done()

    if err := f.restoreToHydrationFile(infos); err != nil {
        f.hydrationErr = err
        f.logger.Error("hydration failed", slog.Any("error", err))
        return
    }

    f.hydrationComplete.Store(true)
    f.logger.Info("hydration complete",
        slog.String("path", f.hydrationPath),
        slog.String("txid", f.hydrationTXID.String()))
}

func (f *VFSFile) restoreToHydrationFile(infos []*ltx.FileInfo) error {
    // Open all LTX files as readers
    rdrs := make([]io.Reader, 0, len(infos))
    defer func() {
        for _, rd := range rdrs {
            if closer, ok := rd.(io.Closer); ok {
                _ = closer.Close()
            }
        }
    }()

    for _, info := range infos {
        f.logger.Debug("opening ltx file for hydration",
            "level", info.Level,
            "min", info.MinTXID,
            "max", info.MaxTXID)

        rc, err := f.client.OpenLTXFile(f.ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
        if err != nil {
            return fmt.Errorf("open ltx file: %w", err)
        }
        rdrs = append(rdrs, rc)
    }

    if len(rdrs) == 0 {
        return fmt.Errorf("no ltx files for hydration")
    }

    // Compact all LTX files into a single stream
    pr, pw := io.Pipe()

    go func() {
        c, err := ltx.NewCompactor(pw, rdrs)
        if err != nil {
            pw.CloseWithError(fmt.Errorf("new ltx compactor: %w", err))
            return
        }
        c.HeaderFlags = ltx.HeaderFlagNoChecksum
        _ = pw.CloseWithError(c.Compact(f.ctx))
    }()

    // Decode compacted LTX stream directly to database file
    f.hydrationMu.Lock()
    defer f.hydrationMu.Unlock()

    dec := ltx.NewDecoder(pr)
    if err := dec.DecodeDatabaseTo(f.hydrationFile); err != nil {
        return fmt.Errorf("decode database: %w", err)
    }

    if err := f.hydrationFile.Sync(); err != nil {
        return fmt.Errorf("sync hydration file: %w", err)
    }

    // Record the TXID we hydrated to
    f.hydrationTXID = infos[len(infos)-1].MaxTXID

    return nil
}
```

### 4. Modified ReadAt

```go
func (f *VFSFile) ReadAt(p []byte, off int64) (int, error) {
    f.mu.Lock()
    defer f.mu.Unlock()

    // Handle special cases (header modification for journal mode, etc.)
    // ... existing code for dirty pages in write mode ...

    // If hydration complete, read from local file
    if f.hydrationEnabled && f.hydrationComplete.Load() {
        return f.readFromHydratedFile(p, off)
    }

    // Otherwise, use existing cache/remote fetch logic
    // ... existing code ...
}

func (f *VFSFile) readFromHydratedFile(p []byte, off int64) (int, error) {
    f.hydrationMu.Lock()
    defer f.hydrationMu.Unlock()

    n, err := f.hydrationFile.ReadAt(p, off)
    if err != nil && err != io.EOF {
        return n, fmt.Errorf("read hydrated file: %w", err)
    }
    return n, nil
}
```

### 5. Incremental Updates (Polling)

When `pollReplicaClient` detects new LTX files after hydration is complete:

```go
func (f *VFSFile) applyUpdatesToHydratedFile(updates map[uint32]ltx.PageIndexElem) error {
    if !f.hydrationComplete.Load() {
        return nil // Hydration not complete, skip
    }

    f.hydrationMu.Lock()
    defer f.hydrationMu.Unlock()

    for pgno, elem := range updates {
        // Fetch page from remote
        buf := make([]byte, f.pageSize)
        if _, err := f.fetchPageFromRemote(pgno, elem, buf); err != nil {
            return fmt.Errorf("fetch updated page %d: %w", pgno, err)
        }

        // Write to local file
        off := int64(pgno-1) * int64(f.pageSize)
        if _, err := f.hydrationFile.WriteAt(buf, off); err != nil {
            return fmt.Errorf("write updated page %d: %w", pgno, err)
        }
    }

    // Update hydration TXID
    f.hydrationTXID = f.pos.TXID

    return nil
}
```

### 6. Write Mode Integration

After successful `syncToRemote`:

```go
func (f *VFSFile) syncToRemote(ctx context.Context) error {
    // ... existing sync logic ...

    // After successful upload, apply synced pages to hydrated file
    if f.hydrationEnabled && f.hydrationComplete.Load() {
        f.hydrationMu.Lock()
        for pgno := range syncedPages {
            buf, ok := f.cache.Get(pgno) // Pages were cached during sync
            if ok {
                off := int64(pgno-1) * int64(f.pageSize)
                if _, err := f.hydrationFile.WriteAt(buf, off); err != nil {
                    f.hydrationMu.Unlock()
                    return fmt.Errorf("apply synced page %d to hydrated file: %w", pgno, err)
                }
            }
        }
        f.hydrationTXID = f.pendingTXID - 1 // TXID we just synced
        f.hydrationMu.Unlock()
    }

    // ... rest of cleanup ...
}
```

### 7. File Size Changes (Truncate)

```go
func (f *VFSFile) Truncate(size int64) error {
    // ... existing logic ...

    // Update hydrated file size if hydration is complete
    if f.hydrationEnabled && f.hydrationComplete.Load() {
        f.hydrationMu.Lock()
        if err := f.hydrationFile.Truncate(size); err != nil {
            f.hydrationMu.Unlock()
            return fmt.Errorf("truncate hydrated file: %w", err)
        }
        f.hydrationMu.Unlock()
    }

    return nil
}
```

### 8. Close and Cleanup

```go
func (f *VFSFile) Close() error {
    // ... existing close logic (cancel context, wait for goroutines) ...

    // Close and remove hydration file
    if f.hydrationFile != nil {
        f.hydrationFile.Close()
        if err := os.Remove(f.hydrationPath); err != nil && !os.IsNotExist(err) {
            f.logger.Warn("failed to remove hydration file", slog.Any("error", err))
        }
    }

    return nil
}
```

## Edge Cases

### 1. New Transactions Arrive During Hydration
- **Scenario**: Remote has new LTX files while initial restore is in progress
- **Solution**:
  - Hydration uses the `infos` from `waitForRestorePlan` at open time
  - Polling continues independently, updating `f.index` and `f.pending`
  - When hydration completes at TXID X, immediately check if `f.pos.TXID > X`
  - If so, apply incremental updates from TXID X+1 to current

```go
func (f *VFSFile) runHydration(infos []*ltx.FileInfo) {
    defer f.wg.Done()

    if err := f.restoreToHydrationFile(infos); err != nil {
        f.hydrationErr = err
        return
    }

    // Check if we need to catch up with polling
    f.mu.Lock()
    currentTXID := f.pos.TXID
    hydrationTXID := f.hydrationTXID
    f.mu.Unlock()

    if currentTXID > hydrationTXID {
        if err := f.catchUpHydration(hydrationTXID, currentTXID); err != nil {
            f.hydrationErr = err
            return
        }
    }

    f.hydrationComplete.Store(true)
}
```

### 2. Hydration Failure
- **Scenario**: Network error during LTX file download or decode
- **Solution**:
  - Log error but don't fail the VFS
  - Reads continue via cache/remote mechanism
  - `hydrationComplete` remains false
  - Expose error via pragma for observability

### 3. VFS Closed During Hydration
- **Scenario**: User closes database before hydration completes
- **Solution**:
  - Context cancellation stops the compactor goroutine
  - `wg.Wait()` ensures clean shutdown
  - Partial file is deleted in `Close()`

### 4. Time Travel Mode
- **Scenario**: User sets target time, changing the effective database state
- **Solution**:
  - On `SetTargetTime`: set `hydrationComplete = false`
  - Restart hydration with new restore plan for target time
  - Or: disable hydration during time travel (simpler)

### 5. Write Conflict During Hydration
- **Scenario**: In write mode, remote has conflicting changes
- **Solution**: Existing `ErrConflict` handling applies; hydration state is independent

### 6. Large Database / Disk Space
- **Scenario**: Database is too large for available disk space
- **Solution**:
  - `os.Create` will fail, error logged
  - VFS continues without hydration (graceful degradation)
  - Consider: check available space upfront and warn

### 7. Concurrent Reads During Hydration
- **Scenario**: Multiple goroutines reading while restore writes
- **Solution**:
  - `hydrationComplete` is atomic - clean transition point
  - Before complete: reads use existing cache/remote path (no contention)
  - After complete: reads use `hydrationMu` for synchronization with incremental updates

### 8. Index Replacement (Snapshot Compaction)
- **Scenario**: Polling detects a new snapshot that replaces the entire index (`pendingReplace = true`)
- **Solution**:
  - If hydration complete: need to re-apply all pages in new index
  - Simpler: mark `hydrationComplete = false` and restart hydration
  - This is rare (only happens when compaction produces a new snapshot)

### 9. Hydration Path Collision
- **Scenario**: Multiple VFS instances use the same hydration path
- **Solution**:
  - Document that paths must be unique per instance
  - Consider: auto-generate unique paths using temp files

## Testing Approach

### Unit Tests

1. **Basic Hydration Flow**
   ```go
   func TestVFSFile_Hydration_Basic(t *testing.T) {
       // Create mock replica client with test LTX files
       // Open VFS with hydration enabled
       // Wait for hydrationComplete
       // Read various pages, verify served from local file
       // Close VFS, verify file deleted
   }
   ```

2. **Reads During Hydration**
   ```go
   func TestVFSFile_Hydration_ReadsDuringHydration(t *testing.T) {
       // Create large database (slow restore)
       // Open VFS with hydration
       // Immediately read pages before hydration completes
       // Verify reads succeed (from cache/remote)
       // Wait for hydration
       // Read again, verify from local file
   }
   ```

3. **Incremental Updates After Hydration**
   ```go
   func TestVFSFile_Hydration_IncrementalUpdates(t *testing.T) {
       // Open VFS, wait for hydration
       // Add new LTX files to mock replica (simulating new commits)
       // Trigger poll
       // Verify new pages applied to hydrated file
       // Read updated pages, verify correct data
   }
   ```

4. **Catch-Up After Hydration**
   ```go
   func TestVFSFile_Hydration_CatchUp(t *testing.T) {
       // Create mock with initial LTX files
       // Open VFS (hydration starts)
       // Add more LTX files during hydration
       // Wait for hydration complete
       // Verify hydrated file includes all updates
   }
   ```

5. **Write Mode Integration**
   ```go
   func TestVFSFile_Hydration_WriteMode(t *testing.T) {
       // Open VFS in write mode with hydration
       // Wait for hydration
       // Write data via SQLite
       // Sync to remote
       // Verify hydrated file updated with synced pages
   }
   ```

6. **Hydration Failure Recovery**
   ```go
   func TestVFSFile_Hydration_FailureRecovery(t *testing.T) {
       // Mock replica client to fail during OpenLTXFile
       // Open VFS
       // Verify reads still work via cache/remote
       // Verify hydrationComplete is false
       // Verify hydrationErr is set
   }
   ```

7. **Close During Hydration**
   ```go
   func TestVFSFile_Hydration_CloseEarly(t *testing.T) {
       // Create large database (slow restore)
       // Open VFS
       // Immediately close (before hydration completes)
       // Verify no panic, goroutines cleaned up
       // Verify partial file deleted
   }
   ```

8. **Time Travel Invalidation**
   ```go
   func TestVFSFile_Hydration_TimeTravel(t *testing.T) {
       // Open VFS, wait for hydration
       // Set target time to past
       // Verify hydrationComplete becomes false
       // Verify reads use cache/remote again
   }
   ```

### Integration Tests

1. **S3 Backend**
   ```go
   func TestVFSFile_Hydration_S3Integration(t *testing.T) {
       // Use real S3 (or MinIO)
       // Create database with multiple LTX files at different levels
       // Open VFS with hydration
       // Verify complete restore and read path
   }
   ```

2. **Concurrent Access**
   ```go
   func TestVFSFile_Hydration_ConcurrentReads(t *testing.T) {
       // Open VFS with hydration
       // Spawn multiple goroutines reading random pages
       // Some reads during hydration, some after
       // Verify no races (run with -race)
       // Verify correct data
   }
   ```

3. **Large Database Performance**
   ```go
   func TestVFSFile_Hydration_LargeDatabase(t *testing.T) {
       // Create 100MB+ database
       // Open VFS with hydration
       // Measure hydration time
       // Compare read latency before/after hydration
   }
   ```

4. **Multi-Level LTX Files**
   ```go
   func TestVFSFile_Hydration_MultiLevel(t *testing.T) {
       // Create replica with snapshot + L1 + L0 files
       // Open VFS with hydration
       // Verify all levels compacted correctly
       // Verify final database state matches expected
   }
   ```

### Benchmark Tests

```go
func BenchmarkVFSFile_ReadHydrated(b *testing.B) {
    // Compare read latency: cache vs remote fetch vs hydrated file
}

func BenchmarkVFSFile_HydrationThroughput(b *testing.B) {
    // Measure MB/s restore throughput
}

func BenchmarkVFSFile_IncrementalUpdate(b *testing.B) {
    // Measure time to apply incremental updates
}
```

## Observability

Add metrics/logging for:
- Hydration start/complete events with duration
- Hydration failure with error details
- Read source tracking (cache hit / remote fetch / hydrated file)
- Current hydration TXID vs live TXID (lag indicator)

```go
// Pragma for hydration status
// PRAGMA litestream_hydration() returns:
//   "complete:TXID"        - hydration complete at given TXID
//   "in_progress"          - hydration running
//   "disabled"             - hydration not enabled
//   "error:message"        - hydration failed
```

## Future Enhancements

1. **Persistent Hydration**: Option to retain hydrated file across restarts (skip re-download)
2. **Hydration Priority**: Start with frequently-accessed pages (requires access tracking)
3. **Parallel Downloads**: Download multiple LTX files concurrently for faster initial restore
4. **Checksum Verification**: Verify hydrated database checksum matches LTX checksums
