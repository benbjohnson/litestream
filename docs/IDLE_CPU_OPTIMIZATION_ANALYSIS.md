# Idle CPU Usage Optimization Analysis

This document analyzes potential optimization points for reducing CPU usage during idle periods in Litestream. Current observed idle CPU usage is ~0.7%.

## Executive Summary

Litestream's idle CPU usage stems primarily from multiple concurrent polling loops that run at 1-second intervals. While the current implementation is already well-optimized with cheap change detection, there are several opportunities to further reduce CPU overhead during extended idle periods.

## Current Architecture Overview

### Active Background Goroutines (Per Database)

| Component | Interval | Purpose | File:Line |
|-----------|----------|---------|-----------|
| DB Monitor | 1s | WAL change detection + sync | `db.go:2019-2123` |
| Replica Monitor | 1s | Replicate to storage | `replica.go:308-385` |

### Store-Level Background Goroutines

| Component | Interval | Purpose | File:Line |
|-----------|----------|---------|-----------|
| L1 Compaction | 30s | Compact L0 → L1 | `store.go:321-384` |
| L2 Compaction | 5m | Compact L1 → L2 | `store.go:321-384` |
| L3 Compaction | 1h | Compact L2 → L3 | `store.go:321-384` |
| Snapshot Monitor | 24h | Create snapshots | `store.go:149-154` |
| L0 Retention | 15s | Cleanup old L0 files | `store.go:386-409` |
| Heartbeat | 15s | Health check pings | `store.go:446-462` |

### VFS (When Enabled)

| Component | Interval | Purpose | File:Line |
|-----------|----------|---------|-----------|
| Poll for updates | 1s | Check for new LTX files | `vfs.go:2217-2232` |
| Sync ticker | 1s | Sync dirty pages | `vfs.go:490` |

---

## Detailed Analysis of Idle CPU Sources

### 1. DB Monitor Loop (`db.go:2019-2123`)

**Current Behavior:**
```go
func (db *DB) monitor() {
    ticker := time.NewTicker(db.MonitorInterval) // Default: 1 second
    defer ticker.Stop()

    for {
        select {
        case <-db.ctx.Done():
            return
        case <-ticker.C:
        }

        // Check WAL file stats
        fi, err := os.Stat(walPath)           // System call
        walHeader, err := readWALHeader(path) // File open + 32-byte read + close

        // Compare with cached values
        if walSize == lastWALSize && bytes.Equal(walHeader, lastWALHeader) {
            continue // Skip sync when unchanged
        }
        // ... perform sync
    }
}
```

**Per-Iteration Cost (when idle):**
- 1x `os.Stat()` system call (~1-5µs)
- 1x `os.Open()` + `io.ReadFull(32 bytes)` + `f.Close()` (~10-50µs)
- 1x `bytes.Equal()` comparison (~nanoseconds)
- Total: ~15-60µs per iteration

**Optimization Opportunities:**

1. **Use inotify/kqueue for WAL changes** (Platform-specific)
   - Replace polling with event-driven file watching
   - Eliminate all idle I/O operations
   - Fallback to polling on unsupported platforms
   - **Impact:** High - eliminates 1s ticker entirely

2. **Keep WAL file descriptor open**
   - Cache the file descriptor between reads
   - Use `pread()` instead of open/read/close
   - **Impact:** Medium - reduces syscall overhead by ~70%

3. **Use memory-mapped WAL header**
   - `mmap` the first 32 bytes of the WAL file
   - Direct memory comparison instead of file I/O
   - **Impact:** High - near-zero I/O cost per check

4. **Adaptive polling interval**
   - Start at 1s, increase to 5s/10s after sustained idle
   - Reset to 1s on detected activity
   - **Impact:** Medium - reduces wakeups by 5-10x during idle

---

### 2. Replica Monitor Loop (`replica.go:308-385`)

**Current Behavior:**
```go
func (r *Replica) monitor(ctx context.Context) {
    ticker := time.NewTicker(r.SyncInterval) // Default: 1 second

    for initial := true; ; initial = false {
        if !initial {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C: // Wakes every second
            }
        }

        // Wait for database changes
        select {
        case <-ctx.Done():
            return
        case <-notify: // Event-driven! Already optimized
        }

        notify = r.db.Notify() // Get new notification channel
        // ... perform sync
    }
}
```

**Analysis:**
The replica monitor is already well-optimized:
- Uses event-driven notification via `db.Notify()` channel
- Only syncs when DB signals changes
- Ticker primarily enforces minimum sync interval

**Optimization Opportunities:**

1. **Remove redundant ticker when using Notify()**
   - The ticker wakes goroutine even when immediately blocking on Notify()
   - Could use single blocking select with timeout
   - **Impact:** Low - small context switch reduction

2. **Merge with DB monitor**
   - Single goroutine for both DB and Replica monitoring
   - Reduces goroutine count per database from 2 to 1
   - **Impact:** Low - reduces context switching overhead

---

### 3. readWALHeader Function (`litestream.go:80-90`)

**Current Implementation:**
```go
func readWALHeader(filename string) ([]byte, error) {
    f, err := os.Open(filename)    // Syscall: open
    if err != nil {
        return nil, err
    }
    defer f.Close()                // Syscall: close

    buf := make([]byte, WALHeaderSize) // Allocation: 32 bytes
    n, err := io.ReadFull(f, buf)      // Syscall: read
    return buf[:n], err
}
```

**Issues:**
1. Opens/closes file on every call (every 1s per DB)
2. Allocates new buffer on every call
3. Three syscalls per invocation

**Optimization Opportunities:**

1. **Cached file descriptor per database**
   ```go
   type DB struct {
       walHeaderFd *os.File
       walHeaderBuf []byte // Pre-allocated 32-byte buffer
   }

   func (db *DB) readWALHeaderCached() ([]byte, error) {
       _, err := db.walHeaderFd.ReadAt(db.walHeaderBuf, 0)
       return db.walHeaderBuf, err
   }
   ```
   - **Impact:** High - reduces syscalls from 3 to 1

2. **Memory-mapped header region**
   ```go
   // One-time setup
   db.walMmap, _ = mmap.Open(walPath, 0, WALHeaderSize, mmap.RDONLY)

   // Per-check (no syscall needed)
   if !bytes.Equal(db.walMmap[:WALHeaderSize], lastWALHeader) {
       // WAL changed
   }
   ```
   - **Impact:** Very High - zero syscalls per check

3. **Sync.Pool for buffer reuse**
   ```go
   var walHeaderPool = sync.Pool{
       New: func() interface{} {
           return make([]byte, WALHeaderSize)
       },
   }
   ```
   - **Impact:** Low - reduces GC pressure

---

### 4. Store-Level Monitors (`store.go`)

**L0 Retention Monitor (`store.go:386-409`):**
```go
func (s *Store) monitorL0Retention(ctx context.Context) {
    ticker := time.NewTicker(s.L0RetentionCheckInterval) // Default: 15s
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
        }
        for _, db := range s.DBs() {
            db.EnforceL0RetentionByTime(ctx) // File I/O operations
        }
    }
}
```

**Optimization Opportunities:**

1. **Increase L0 retention check interval**
   - Current: 15 seconds
   - Suggested: 60 seconds (retention is typically 5 minutes)
   - **Impact:** Low - 4x fewer wakeups

2. **Lazy evaluation**
   - Skip check if no L0 files were created since last check
   - Track L0 file creation events
   - **Impact:** Medium - avoids work when nothing to do

---

### 5. Compaction Level Monitors (`store.go:321-384`)

**Current Behavior:**
```go
func (s *Store) monitorCompactionLevel(ctx context.Context, lvl *CompactionLevel) {
    timer := time.NewTimer(time.Until(lvl.NextCompactionAt(time.Now())))
    for {
        select {
        case <-ctx.Done():
            return
        case <-timer.C:
        }
        // ... perform compaction work
        timer.Reset(nextDelay) // Sleep until next scheduled time
    }
}
```

**Analysis:**
Already well-optimized:
- Uses timer (not ticker) with calculated next-fire time
- Only wakes when compaction is actually due
- No busy-waiting

**Minor Optimization:**
- Consider single compaction orchestrator goroutine instead of one per level
- **Impact:** Very Low - just reduces goroutine count

---

## Quantified CPU Impact Analysis

### Syscall Overhead Per Second (Single Database, Idle)

| Source | Syscalls/sec | Estimated CPU Time |
|--------|--------------|-------------------|
| DB Monitor stat() | 1 | ~5µs |
| DB Monitor open/read/close | 3 | ~30µs |
| Ticker wakeup (DB) | 1 | ~2µs |
| Ticker wakeup (Replica) | 1 | ~2µs |
| **Total per DB** | **6** | **~40µs/s** |

For 10 databases: ~400µs/s = 0.04% CPU on a single core

### Additional Periodic Work

| Source | Interval | Syscalls/wake | CPU per wake |
|--------|----------|---------------|--------------|
| L0 Retention | 15s | N×3 (per DB) | ~50µs |
| Heartbeat | 15s | ~2 | ~10µs |
| L1 Compaction | 30s | N×5 | ~100µs |

---

## Recommended Optimizations (Priority Order)

### High Impact

1. **Implement fsnotify-based WAL watching** (replaces polling)
   - Use `github.com/fsnotify/fsnotify`
   - Fall back to current polling on unsupported platforms
   - Expected reduction: 50-70% of idle CPU

2. **Cache WAL file descriptor**
   - Keep fd open, use `pread()` or `ReadAt()`
   - Eliminates open/close syscalls per check
   - Expected reduction: 20-30% of remaining CPU

### Medium Impact

3. **Adaptive polling intervals**
   - Increase MonitorInterval after 60s of no changes
   - Reset on detected activity
   - Simple implementation, significant idle reduction

4. **Buffer pooling for WAL header reads**
   - Use `sync.Pool` for the 32-byte buffer
   - Reduces GC pressure

### Low Impact / Future Consideration

5. **Merge DB and Replica monitors**
   - Single goroutine per database
   - Reduces context switches

6. **Increase L0 retention check interval**
   - 15s → 60s (still well within 5min retention window)

---

## Implementation Recommendations

### Phase 1: Quick Wins
- Implement cached file descriptor for WAL header reads
- Add buffer pooling via sync.Pool
- Configurable L0 retention check interval

### Phase 2: Event-Driven Architecture
- Add fsnotify integration for WAL file changes
- Implement fallback mechanism for unsupported platforms
- Add metrics to track polling vs event-driven mode

### Phase 3: Adaptive Behavior
- Implement adaptive polling intervals
- Add idle detection heuristics
- Consider consolidating monitor goroutines

---

## Appendix: Profiling Commands

```bash
# CPU profiling during idle
go test -cpuprofile=cpu.prof -bench=BenchmarkIdle -benchtime=60s
go tool pprof -top cpu.prof

# Goroutine analysis
curl http://localhost:9090/debug/pprof/goroutine?debug=2

# Trace analysis for scheduling overhead
go test -trace=trace.out -bench=BenchmarkIdle
go tool trace trace.out
```

## References

- `db.go:2019-2123` - DB monitor implementation
- `replica.go:308-385` - Replica monitor implementation
- `store.go:321-409` - Store-level monitors
- `litestream.go:80-90` - readWALHeader function
- `vfs.go:2217-2232` - VFS polling loop
