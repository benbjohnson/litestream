# SQLite WAL for Litestream

Understanding SQLite's Write-Ahead Log (WAL) is essential for working with Litestream.

## WAL Basics

WAL is SQLite's method for atomic commits:

1. Changes are **first written to WAL** (not main database)
2. Main database file stays **unchanged until checkpoint**
3. Readers see **consistent view** by merging WAL + main database

```
Write Transaction
      ↓
   WAL File  ←──── Litestream monitors this
      ↓
 Checkpoint
      ↓
 Main Database
```

## SQLite File Structure

A SQLite database in WAL mode consists of:

```
database.db         # Main database file (pages)
database.db-wal     # Write-ahead log (uncommitted changes)
database.db-shm     # Shared memory (coordination between processes)
```

## WAL File Structure

```
+------------------+
| WAL Header       | 32 bytes - magic, page size, salt
+------------------+
| Frame 1 Header   | 24 bytes - page number, checksums
| Frame 1 Data     | Page size bytes (4KB typical)
+------------------+
| Frame 2 Header   | 24 bytes
| Frame 2 Data     | Page size bytes
+------------------+
| ... more frames  |
+------------------+
```

### WAL Header (32 bytes)

- Magic number: `0x377f0682` or `0x377f0683`
- File format version
- Database page size
- Checkpoint sequence number
- Salt values for checksums

### Frame Header (24 bytes)

- Page number in database
- Database size in pages (at commit)
- Salt values (must match header)
- Cumulative checksums

## The 1GB Lock Page

**Critical**: SQLite reserves a page at exactly 1GB (0x40000000) for locking.

### Lock Page Numbers by Page Size

| Page Size | Lock Page Number |
|-----------|-----------------|
| 4KB | 262145 |
| 8KB | 131073 |
| 16KB | 65537 |
| 32KB | 32769 |
| 64KB | 16385 |

### Why It Matters

1. **Never contains user data** - SQLite skips this page
2. **Must be skipped** - During replication and compaction
3. **Only affects large databases** - Databases > 1GB
4. **Page number varies** - Depends on page size

### Calculating Lock Page

```
Lock Page Number = (1073741824 / page_size) + 1
```

Where 1073741824 = 0x40000000 = 1GB

## Checkpoint Modes

Checkpoints move WAL changes to the main database:

### PASSIVE (Default)
```sql
PRAGMA wal_checkpoint(PASSIVE);
```
- Attempts checkpoint without blocking
- Fails silently if readers present
- Non-blocking

### FULL
```sql
PRAGMA wal_checkpoint(FULL);
```
- Waits for readers to finish
- Blocks new readers
- Ensures complete checkpoint

### RESTART
```sql
PRAGMA wal_checkpoint(RESTART);
```
- Like FULL, plus resets WAL
- Next writer starts at WAL beginning
- Litestream avoids this mode (can block writes)

### TRUNCATE
```sql
PRAGMA wal_checkpoint(TRUNCATE);
```
- Like RESTART, plus truncates WAL file
- Releases disk space
- Used for emergency WAL control

## Litestream's Checkpoint Strategy

Litestream manages checkpoints based on:

1. **Page count threshold** (`MinCheckpointPageN`)
   - Triggers PASSIVE checkpoint when WAL reaches threshold

2. **Time interval** (`CheckpointInterval`)
   - Periodic PASSIVE checkpoint regardless of size

3. **Emergency threshold** (`TruncatePageN`)
   - Triggers TRUNCATE when WAL grows too large

## Required SQLite Configuration

For Litestream to work correctly:

```sql
-- Enable WAL mode (required)
PRAGMA journal_mode = WAL;

-- Set busy timeout (prevents lock errors)
PRAGMA busy_timeout = 5000;

-- Recommended sync mode with WAL
PRAGMA synchronous = NORMAL;

-- Disable auto-checkpoint (Litestream manages this)
PRAGMA wal_autocheckpoint = 0;
```

## WAL Monitoring

Litestream monitors WAL by:

1. Checking WAL file size at `MonitorInterval` (default 10ms)
2. Comparing with previous size
3. Reading new frames when changes detected
4. Converting frames to LTX format
5. Notifying replicas of changes

## Common WAL Issues

### WAL Growing Indefinitely
**Cause**: Checkpoint blocked by long-running readers
**Solution**:
- Close long-running transactions
- Check for stuck read connections
- Review `TruncatePageN` threshold

### Checkpoint Not Running
**Cause**: Application holding read transaction
**Solution**:
- Ensure transactions are committed/rolled back
- Check for connection leaks

### WAL File Missing
**Cause**: Database not in WAL mode or just created
**Solution**:
- Run `PRAGMA journal_mode=WAL;`
- Ensure at least one write has occurred

## Key Takeaways

1. **WAL is temporary** - Gets merged back to main database via checkpoint
2. **Lock page is sacred** - Never write data at 1GB mark
3. **Page size matters** - Affects lock page number and performance
4. **Checkpoints are critical** - Balance WAL size vs. performance
5. **Monitor WAL size** - Growing WAL indicates checkpoint issues
