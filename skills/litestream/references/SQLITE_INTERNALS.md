# SQLite Internals Reference

Condensed reference for agents working with SQLite internals in Litestream.

## SQLite File Structure

```
database.db       Main database file (pages)
database.db-wal   Write-ahead log
database.db-shm   Shared memory file (coordination)
```

## Write-Ahead Log (WAL)

WAL is SQLite's method for atomic commits:
- Changes written to WAL first, database unchanged until checkpoint
- Readers merge WAL + database for consistent view
- Litestream monitors WAL and converts frames to LTX format

### WAL File Structure

```
+------------------+
| WAL Header       |  32 bytes
+------------------+
| Frame 1 Header   |  24 bytes
| Frame 1 Data     |  PageSize bytes
+------------------+
| Frame 2 Header   |  24 bytes
| Frame 2 Data     |  PageSize bytes
+------------------+
| ...              |
```

### WAL Header (32 bytes)

```
Magic (4B) | FileFormat (4B) | PageSize (4B) | Checkpoint (4B) |
Salt1 (4B) | Salt2 (4B) | Checksum1 (4B) | Checksum2 (4B)
```

Magic: `0x377f0682` or `0x377f0683`

### WAL Frame Header (24 bytes)

```
PageNumber (4B) | DbSize (4B) | Salt1 (4B) | Salt2 (4B) |
Checksum1 (4B) | Checksum2 (4B)
```

## The 1 GB Lock Page

SQLite reserves a page at exactly 0x40000000 bytes (1 GB) for locking. This is
the single most important SQLite detail for Litestream development.

```go
const PENDING_BYTE = 0x40000000

func LockPgno(pageSize int) uint32 {
    return uint32(PENDING_BYTE/pageSize) + 1
}
```

| Page Size | Lock Page Number |
|-----------|------------------|
| 4 KB      | 262145           |
| 8 KB      | 131073           |
| 16 KB     | 65537            |
| 32 KB     | 32769            |
| 64 KB     | 16385            |

Rules:
- Cannot contain data; SQLite never writes user data here
- Must be skipped during replication and compaction
- Only affects databases > 1 GB
- Page number changes with page size

Implementation in Litestream:

```go
for pgno := uint32(1); pgno <= maxPgno; pgno++ {
    if pgno == ltx.LockPgno(db.pageSize) {
        continue
    }
    processPage(pgno)
}
```

## Page Structure

SQLite divides databases into fixed-size pages (typically 4096 bytes):
- Page numbers are 1-based
- Types: B-tree interior, B-tree leaf, overflow, freelist, lock byte page

## Transaction Types

1. **Deferred** (default): Lock acquired on first use
2. **Immediate**: RESERVED lock acquired immediately
3. **Exclusive**: EXCLUSIVE lock acquired immediately

### Lock Hierarchy

UNLOCKED → SHARED → RESERVED → PENDING → EXCLUSIVE

- SHARED: Multiple readers allowed
- RESERVED: Signals intent to write
- PENDING: Blocks new SHARED locks
- EXCLUSIVE: Single writer, no readers

## Long-Running Read Transaction

Litestream maintains a read transaction for consistency:

```go
func (db *DB) initReadTx() error {
    tx, err := db.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
    if err != nil {
        return err
    }
    var dummy string
    err = tx.QueryRow("SELECT ''").Scan(&dummy)
    if err != nil {
        tx.Rollback()
        return err
    }
    db.rtx = tx
    return nil
}
```

Purpose:
- Prevents checkpoint past our read point
- Ensures consistent database view
- Allows reading historical pages from WAL

## Checkpoint Modes

| Mode     | Behavior                                      |
|----------|-----------------------------------------------|
| PASSIVE  | Non-blocking; fails if readers present         |
| FULL     | Waits for readers; blocks new readers          |
| RESTART  | Like FULL + resets WAL start (removed in #724) |
| TRUNCATE | Like RESTART + truncates WAL to zero           |

### Litestream Checkpoint Strategy

```
WAL pages > TruncatePageN    → TRUNCATE (emergency)
WAL pages > MinCheckpointPageN → PASSIVE
CheckpointInterval elapsed    → PASSIVE
```

Note: RESTART mode permanently removed due to issue #724 (write-blocking).

## Important SQLite Pragmas

```sql
PRAGMA journal_mode = WAL;         -- Required for Litestream
PRAGMA page_size;                  -- Get page size
PRAGMA page_count;                 -- Total pages in database
PRAGMA freelist_count;             -- Free pages
PRAGMA wal_checkpoint(PASSIVE);    -- Non-blocking checkpoint
PRAGMA wal_autocheckpoint = 10000; -- High threshold (prevent interference)
PRAGMA busy_timeout = 5000;        -- Wait 5s for locks
PRAGMA synchronous = NORMAL;       -- Safe with WAL
PRAGMA cache_size = -64000;        -- 64 MB cache
PRAGMA integrity_check;            -- Verify database integrity
```

## Critical SQLite Behaviors

1. **Automatic checkpoint**: Default at 1000 WAL pages; set high threshold to
   prevent interference with Litestream's control
2. **Busy timeout**: Default is 0 (immediate failure); set a reasonable timeout
3. **Synchronous mode**: NORMAL is safe with WAL mode
4. **Page cache**: In-memory cache; configure with negative KB or positive pages

## WAL to LTX Conversion

Litestream converts WAL frames to LTX:

1. Read WAL header and validate magic number
2. Iterate frames, reading page number and data
3. Skip lock page (`pgno == LockPgno(pageSize)`)
4. Calculate checksums
5. Write as immutable LTX file with page index
