# LTX File Format

LTX (Log Transaction) is Litestream's immutable file format for storing database changes.

## Overview

LTX files contain:
- Database page changes from transactions
- Metadata for recovery (TXID range, timestamps)
- Checksums for integrity verification

Key properties:
- **Immutable**: Never modified after creation
- **Self-contained**: All data needed for that TXID range
- **Efficient**: Binary format, page-level granularity

## File Structure

```
+------------------+
|     Header       |  Fixed-size metadata
+------------------+
|                  |
|   Page Frames    |  Variable: page headers + data
|                  |
+------------------+
|   Page Index     |  Binary search index
+------------------+
|     Trailer      |  Checksums and summary
+------------------+
```

## Header

The header contains file metadata:

| Field | Size | Description |
|-------|------|-------------|
| Magic | 4 bytes | "LTX\x00" |
| Version | 4 bytes | Format version |
| PageSize | 4 bytes | Database page size |
| MinTXID | 8 bytes | Starting transaction ID |
| MaxTXID | 8 bytes | Ending transaction ID |
| Timestamp | 8 bytes | Creation timestamp |
| Checksum | 8 bytes | Header checksum (CRC-64) |

## Page Frames

Each page frame contains:

### Page Header

| Field | Size | Description |
|-------|------|-------------|
| PageNo | 4 bytes | Page number in database |
| Size | 4 bytes | Size of page data |
| Checksum | 8 bytes | Page data checksum |

### Page Data

Raw page data follows the header:
- Size equals database page size (typically 4KB)
- Contains the exact bytes for that database page

## Page Index

Binary index for efficient page lookup:

| Field | Size | Description |
|-------|------|-------------|
| PageNo | 4 bytes | Page number |
| Offset | 8 bytes | Offset in file |
| Size | 4 bytes | Size of page frame |

The index enables:
- O(log n) page lookup by number
- Random access to specific pages
- Efficient restoration

## Trailer

File summary and checksums:

| Field | Size | Description |
|-------|------|-------------|
| PageIndexOffset | 8 bytes | Offset to page index |
| PageIndexSize | 8 bytes | Size of page index |
| PageCount | 4 bytes | Total pages in file |
| Checksum | 8 bytes | Full file checksum |

## Checksums

LTX uses CRC-64 ECMA for integrity:
- Header checksum: Verifies header integrity
- Page checksums: Verifies individual pages
- File checksum: Verifies entire file

## File Naming

Files are named by their TXID range:

```
{MinTXID}-{MaxTXID}.ltx
```

Examples:
```
0000000000000001-0000000000000100.ltx
0000000000000101-0000000000000200.ltx
```

TXID is displayed as 16-character hexadecimal.

## Lock Page Handling

**Critical**: The SQLite lock page at 1GB must be skipped.

| Page Size | Lock Page Number |
|-----------|-----------------|
| 4KB | 262145 |
| 8KB | 131073 |
| 16KB | 65537 |

LTX files never contain the lock page:
- Skipped during file creation
- Skipped during restoration
- Ensures database compatibility

## Reading LTX Files

### Header Reading

1. Read first 52 bytes
2. Verify magic is "LTX\x00"
3. Validate header checksum
4. Extract TXID range and page size

### Page Reading

1. Read trailer to get index offset
2. Read page index
3. Binary search for page number
4. Seek to offset and read frame

### Full File Reading

1. Read and verify header
2. Iterate page frames
3. For each frame:
   - Read page header
   - Read page data
   - Verify page checksum
4. Verify file checksum in trailer

## Creating LTX Files

### From WAL Frames

1. Create header with TXID range
2. For each WAL frame:
   - Skip if lock page
   - Write page header
   - Write page data
3. Build page index
4. Write index and trailer
5. Calculate checksums

### During Compaction

1. Read source LTX files
2. Merge pages (keep latest)
3. Create new file with merged data
4. Preserve earliest timestamp

## Compaction Merging

When pages appear in multiple files:

```
File 1: Page 5 (TXID 1-100)
File 2: Page 5 (TXID 101-200)

Merged: Page 5 (TXID 101-200 version only)
```

The latest version (highest TXID) is kept.

## Size Characteristics

### Small LTX Files
- Few pages changed
- Typical for low-activity databases
- Size: ~4KB per page + overhead

### Large LTX Files
- Many pages changed
- Result of compaction
- Can be megabytes

### Compacted Files
- Eliminate duplicate pages
- Larger TXID ranges
- More efficient storage

## Validation

LTX files can be validated:

```bash
# List files (validates during read)
litestream ltx /path/to/db

# Restore validates during application
litestream restore -o /tmp/test.db s3://bucket/backup
```

## Use in Recovery

During restoration:
1. List all LTX files
2. Sort by TXID order
3. Read pages from each file
4. Apply to output database
5. Skip lock page
6. Verify checksums

## See Also

- [Replication](replication.md)
- [Generations](generations.md)
- [SQLite WAL](sqlite-wal.md)
