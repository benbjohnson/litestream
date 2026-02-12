# LTX Format Reference

Condensed reference for agents working with Litestream's LTX file format.

## Overview

LTX (Log Transaction) is Litestream's custom format for storing database changes.
LTX files are:
- **Immutable**: Once written, never modified
- **Self-contained**: Each file is independent
- **Indexed**: Contains page index for efficient seeks
- **Checksummed**: CRC-64 ECMA integrity verification

## File Structure

```
+---------------------+
|      Header         |  100 bytes
+---------------------+
|    Page Frames      |  Variable (4-byte pgno + pageSize data per page)
+---------------------+
|    Page Index       |  Binary search index
+---------------------+
|      Trailer        |  16 bytes
+---------------------+

FileSize = HeaderSize + (PageCount * (4 + PageSize)) + PageIndexSize + TrailerSize
```

## Header (100 bytes)

```
Offset  Size  Field
0       4     Magic ("LTX1")
4       4     Flags (bit 1 = NoChecksum)
8       4     PageSize
12      4     Commit (page count after applying)
16      8     MinTXID
24      8     MaxTXID
32      8     Timestamp (ms since Unix epoch)
40      8     PreApplyChecksum
48      8     WALOffset (0 for snapshots)
56      8     WALSize (0 for snapshots)
64      4     WALSalt1
68      4     WALSalt2
72      8     NodeID
80     20     Reserved (zeros)
```

The version is implied by the magic string. `"LTX1"` → `ltx.Version == 2`.

## Page Frames

Each frame contains one database page:

```
Offset  Size      Field
0       4         Page Number (1-based)
4       PageSize  Page Data
```

Constraints:
- Pages written in sequential order during creation
- Lock page at 1 GB boundary is never included
- In compacted files, only the latest version of each page

## Page Index

Binary search index for efficient random access to pages. Use
`ltx.DecodePageIndex()` to parse into a map of page number to
`ltx.PageIndexElem`:

```go
type PageIndexElem struct {
    Level   int
    MinTXID TXID
    MaxTXID TXID
    Offset  int64  // Byte offset of encoded payload
    Size    int64  // Bytes occupied by encoded payload
}
```

## Trailer (16 bytes)

```
Offset  Size  Field
0       8     PostApplyChecksum (database checksum after applying file)
8       8     FileChecksum (CRC-64 of entire file)
```

The trailer is always at the end of the file. Read it by seeking
`-TrailerSize` from `io.SeekEnd`.

## File Naming Convention

```
Format:  MMMMMMMMMMMMMMMM-NNNNNNNNNNNNNNNN.ltx
         MinTXID (16 hex)  MaxTXID (16 hex)

Example: 0000000000000001-0000000000000064.ltx  (TXID 1-100)
         0000000000000065-00000000000000c8.ltx  (TXID 101-200)
```

Parse with `ltx.ParseFilename()`, format with `ltx.FormatFilename()`.

## Compaction Levels

LTX files are organized in levels for efficient storage:

```
Level 0: /ltx/0000/  Raw LTX files (no compaction)
Level 1: /ltx/0001/  Compacted (default: every 30s)
Level 2: /ltx/0002/  Compacted (default: every 5min)
Level 3: /ltx/0003/  Compacted (default: every 1h)
Snapshots:           Full database state (daily)
```

### Compaction Process

1. Enumerate level L-1 files
2. Build page map (newer pages overwrite older)
3. Write merged file skipping lock page
4. Preserve earliest `CreatedAt` from source files
5. Delete old L0 files when promoting to L1

```go
// Page deduplication: latest version wins
for _, file := range files {
    for _, page := range file.Pages {
        pageMap[page.Number] = page
    }
}
```

## Checksums

LTX uses CRC-64 ECMA checksums (`hash/crc64` with `crc64.ECMA` table):

- **PreApplyChecksum**: Database state before applying this file
- **PostApplyChecksum**: Database state after applying this file
- **FileChecksum**: Integrity of the entire LTX file

## Reading LTX Files

```go
dec := ltx.NewDecoder(reader)
header, err := dec.Header()
for {
    var hdr ltx.PageHeader
    data := make([]byte, header.PageSize)
    if err := dec.DecodePage(&hdr, data); err == io.EOF {
        break
    }
    // Process hdr.Pgno and data
}
trailer := dec.Trailer()
```

## Writing LTX Files

```go
enc := ltx.NewEncoder(writer)
enc.EncodeHeader(header)
for _, page := range pages {
    if page.Number == ltx.LockPgno(pageSize) {
        continue // Skip lock page
    }
    enc.EncodePage(pageHeader, pageData)
}
enc.EncodePageIndex(index)
enc.EncodeTrailer()
enc.Close()
```

## CLI Inspection

```bash
litestream ltx /path/to/db.sqlite
litestream ltx s3://bucket/db
```

For low-level inspection, use the Go API with `ltx.NewDecoder`.

## WAL to LTX Conversion

SQLite WAL frames are converted to LTX format during replication:
1. Read WAL frames from the WAL file
2. Skip the lock page
3. Add checksums and build page index
4. Write as immutable LTX file
5. Upload to storage backend
