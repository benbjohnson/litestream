# LTX Format Specification

LTX (Log Transaction) is Litestream's custom format for storing database changes in an immutable, append-only manner.

## Table of Contents
- [Overview](#overview)
- [File Structure](#file-structure)
- [Header Format](#header-format)
- [Page Frames](#page-frames)
- [Page Index](#page-index)
- [Trailer Format](#trailer-format)
- [File Naming Convention](#file-naming-convention)
- [Checksum Calculation](#checksum-calculation)
- [Compaction and Levels](#compaction-and-levels)
- [Reading LTX Files](#reading-ltx-files)
- [Writing LTX Files](#writing-ltx-files)
- [Relationship to SQLite WAL](#relationship-to-sqlite-wal)

## Overview

LTX files are immutable snapshots of database changes:
- **Immutable**: Once written, never modified
- **Append-only**: New changes create new files
- **Self-contained**: Each file is independent
- **Indexed**: Contains page index for efficient seeks
- **Checksummed**: Integrity verification built-in

```mermaid
graph LR
    WAL[SQLite WAL] -->|Convert| LTX[LTX File]
    LTX -->|Upload| Storage[Cloud Storage]
    Storage -->|Download| Restore[Restored DB]
```

## File Structure

```
┌─────────────────────┐
│      Header         │ Fixed size (varies by version)
├─────────────────────┤
│                     │
│    Page Frames      │ Variable number of pages
│                     │
├─────────────────────┤
│    Page Index       │ Binary search tree
├─────────────────────┤
│      Trailer        │ Fixed size metadata
└─────────────────────┘
```

### Size Calculation

```go
FileSize = HeaderSize +
           (PageCount * (PageHeaderSize + PageSize)) +
           PageIndexSize +
           TrailerSize
```

## Header Format

The LTX header contains metadata about the file:

```go
// From github.com/superfly/ltx
type Header struct {
    Version          int      // Derived from the magic string ("LTX1")
    Flags            uint32   // Reserved flag bits
    PageSize         uint32   // Database page size
    Commit           uint32   // Page count after applying file
    MinTXID          TXID
    MaxTXID          TXID
    Timestamp        int64    // Milliseconds since Unix epoch
    PreApplyChecksum Checksum // Database checksum before apply
    WALOffset        int64    // Offset within source WAL (0 for snapshots)
    WALSize          int64    // WAL byte length (0 for snapshots)
    WALSalt1         uint32
    WALSalt2         uint32
    NodeID           uint64
}

const HeaderFlagNoChecksum = uint32(1 << 1)
```

> Note: the version is implied by the magic string. Present files use
> `Magic == "LTX1"`, which corresponds to `ltx.Version == 2`.

### Binary Layout (Header)

```
Offset  Size  Field
0       4     Magic ("LTX1")
4       4     Flags
8       4     PageSize
12      4     Commit
16      8     MinTXID
24      8     MaxTXID
32      8     Timestamp
40      8     PreApplyChecksum
48      8     WALOffset
56      8     WALSize
64      4     WALSalt1
68      4     WALSalt2
72      8     NodeID
80     20     Reserved (zeros)
Total: 100 bytes
```

## Page Frames

Each page frame contains a database page with metadata:

```go
type PageFrame struct {
    Header PageHeader
    Data   []byte  // Size = PageSize from LTX header
}

type PageHeader struct {
    Pgno uint32  // Database page number (1-based)
}
```

### Binary Layout (Page Frame)

```
Offset  Size     Field
0       4        Page Number (Pgno)
4       PageSize Page Data
```

### Page Frame Constraints

1. **Sequential Writing**: Pages written in order during creation
2. **Random Access**: Can seek to any page using index
3. **Lock Page Skipping**: Page at 1GB boundary never included
4. **Deduplication**: In compacted files, only latest version of each page

## Page Index

The page index enables efficient random access to pages:

```go
type PageIndexElem struct {
    Level   int
    MinTXID TXID
    MaxTXID TXID
    Offset  int64 // Byte offset of encoded payload
    Size    int64 // Bytes occupied by encoded payload
}
```

### Binary Layout (Page Index)

```
Rather than parsing raw bytes, call `ltx.DecodePageIndex` which returns a
map of page number to `ltx.PageIndexElem` for you.
```

### Index Usage

```go
// Finding a page using the index
func findPage(index []PageIndexElem, targetPageNo uint32) (offset int64, found bool) {
    // Binary search
    idx := sort.Search(len(index), func(i int) bool {
        return index[i].PageNo >= targetPageNo
    })

    if idx < len(index) && index[idx].PageNo == targetPageNo {
        return index[idx].Offset, true
    }
    return 0, false
}
```

## Trailer Format

The trailer contains metadata and pointers:

```go
type Trailer struct {
    PostApplyChecksum Checksum // Database checksum after apply
    FileChecksum      Checksum // CRC-64 checksum of entire file
}
```

### Binary Layout (Trailer)

```
Offset  Size  Field
0       8     PostApplyChecksum
8       8     FileChecksum
Total: 16 bytes
```

### Reading Trailer

The trailer is always at the end of the file:

```go
func readTrailer(f *os.File) (*Trailer, error) {
    // Seek to trailer position
    _, err := f.Seek(-TrailerSize, io.SeekEnd)
    if err != nil {
        return nil, err
    }

    var trailer Trailer
    err = binary.Read(f, binary.BigEndian, &trailer)
    return &trailer, err
}
```

## File Naming Convention

LTX files follow a strict naming pattern:

```
Format: MMMMMMMMMMMMMMMM-NNNNNNNNNNNNNNNN.ltx
Where:
  M = MinTXID (16 hex digits, zero-padded)
  N = MaxTXID (16 hex digits, zero-padded)

Examples:
  0000000000000001-0000000000000064.ltx  (TXID 1-100)
  0000000000000065-00000000000000c8.ltx  (TXID 101-200)
```

### Parsing Filenames

```go
// From github.com/superfly/ltx
func ParseFilename(name string) (minTXID, maxTXID TXID, err error) {
    // Remove extension
    name = strings.TrimSuffix(name, ".ltx")

    // Split on hyphen
    parts := strings.Split(name, "-")
    if len(parts) != 2 {
        return 0, 0, errors.New("invalid format")
    }

    // Parse hex values
    min, err := strconv.ParseUint(parts[0], 16, 64)
    max, err := strconv.ParseUint(parts[1], 16, 64)

    return TXID(min), TXID(max), nil
}

func FormatFilename(minTXID, maxTXID TXID) string {
    return fmt.Sprintf("%016x-%016x.ltx", minTXID, maxTXID)
}
```

## Checksum Calculation

LTX uses CRC-64 ECMA checksums:

```go
import "hash/crc64"

var crcTable = crc64.MakeTable(crc64.ECMA)

func calculateChecksum(data []byte) uint64 {
    return crc64.Checksum(data, crcTable)
}

// Cumulative checksum for multiple pages
func cumulativeChecksum(pages [][]byte) uint64 {
    h := crc64.New(crcTable)
    for _, page := range pages {
        h.Write(page)
    }
    return h.Sum64()
}
```

### Verification During Read

```go
func verifyPage(header PageHeader, data []byte) error {
    if header.Checksum == 0 {
        return nil // Checksums disabled
    }

    calculated := calculateChecksum(data)
    if calculated != header.Checksum {
        return fmt.Errorf("checksum mismatch: expected %x, got %x",
            header.Checksum, calculated)
    }
    return nil
}
```

## Compaction and Levels

LTX files are organized in levels for efficient compaction:

```
Level 0: Raw files (no compaction)
         /ltx/0000/0000000000000001-0000000000000064.ltx
         /ltx/0000/0000000000000065-00000000000000c8.ltx

Level 1: Hourly compaction
         /ltx/0001/0000000000000001-0000000000000fff.ltx

Level 2: Daily compaction
         /ltx/0002/0000000000000001-000000000000ffff.ltx

Snapshots: Full database state
          /snapshots/20240101120000.ltx
```

### Compaction Process

```go
func compactLTXFiles(files []*LTXFile) (*LTXFile, error) {
    // Create page map (newer overwrites older)
    pageMap := make(map[uint32]Page)

    for _, file := range files {
        for _, page := range file.Pages {
            pageMap[page.Number] = page
        }
    }

    // Create new LTX with merged pages
    merged := &LTXFile{
        MinTXID: files[0].MinTXID,
        MaxTXID: files[len(files)-1].MaxTXID,
    }

    // Add pages in order (skip lock page)
    for pgno := uint32(1); pgno <= maxPgno; pgno++ {
        if pgno == LockPageNumber(pageSize) {
            continue // Skip 1GB lock page
        }
        if page, ok := pageMap[pgno]; ok {
            merged.Pages = append(merged.Pages, page)
        }
    }

    return merged, nil
}
```

## Reading LTX Files

### Complete File Read

```go
func ReadLTXFile(path string) (*LTXFile, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer f.Close()

    dec := ltx.NewDecoder(f)

    // Read and verify header
    header, err := dec.Header()
    if err != nil {
        return nil, err
    }

    // Read all pages
    var pages []Page
    for {
        var pageHeader ltx.PageHeader
        pageData := make([]byte, header.PageSize)

        err := dec.DecodePage(&pageHeader, pageData)
        if err == io.EOF {
            break
        }
        if err != nil {
            return nil, err
        }

        pages = append(pages, Page{
            Number: pageHeader.PageNo,
            Data:   pageData,
        })
    }

    return &LTXFile{
        Header: header,
        Pages:  pages,
    }, nil
}
```

### Partial Read Using Index

```go
func ReadPage(path string, pageNo uint32) ([]byte, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer f.Close()

    // Read trailer to find index
    trailer, err := readTrailer(f)
    if err != nil {
        return nil, err
    }

    // Read page index
    f.Seek(trailer.PageIndexOffset, io.SeekStart)
    indexData := make([]byte, trailer.PageIndexSize)
    f.Read(indexData)

    index := parsePageIndex(indexData)

    // Find page in index
    offset, found := findPage(index, pageNo)
    if !found {
        return nil, errors.New("page not found")
    }

    // Read page at offset
    f.Seek(offset, io.SeekStart)

    var pageHeader PageHeader
    binary.Read(f, binary.BigEndian, &pageHeader)

    pageData := make([]byte, pageSize)
    f.Read(pageData)

    return pageData, nil
}
```

## Writing LTX Files

### Creating New LTX File

```go
func WriteLTXFile(path string, pages []Page) error {
    f, err := os.Create(path)
    if err != nil {
        return err
    }
    defer f.Close()

    enc := ltx.NewEncoder(f)

    // Write header
    header := ltx.Header{
        Version:   ltx.Version,
        Flags:     0,
        PageSize:  4096,
        PageCount: uint32(len(pages)),
        MinTXID:   minTXID,
        MaxTXID:   maxTXID,
    }

    if err := enc.EncodeHeader(header); err != nil {
        return err
    }

    // Write pages and build index
    var index []PageIndexElem
    for _, page := range pages {
        offset := enc.Offset()

        // Skip lock page
        if page.Number == LockPageNumber(header.PageSize) {
            continue
        }

        pageHeader := ltx.PageHeader{
            PageNo:   page.Number,
            Checksum: calculateChecksum(page.Data),
        }

        if err := enc.EncodePage(pageHeader, page.Data); err != nil {
            return err
        }

        index = append(index, PageIndexElem{
            PageNo: page.Number,
            Offset: offset,
        })
    }

    // Write page index
    if err := enc.EncodePageIndex(index); err != nil {
        return err
    }

    // Write trailer
    if err := enc.EncodeTrailer(); err != nil {
        return err
    }

    return enc.Close()
}
```

## Relationship to SQLite WAL

### WAL to LTX Conversion

```mermaid
sequenceDiagram
    participant SQLite
    participant WAL
    participant Litestream
    participant LTX

    SQLite->>WAL: Write transaction
    WAL->>WAL: Append frames

    Litestream->>WAL: Monitor changes
    WAL-->>Litestream: Read frames

    Litestream->>Litestream: Convert frames
    Note over Litestream: - Skip lock page<br/>- Add checksums<br/>- Build index

    Litestream->>LTX: Write LTX file
    LTX->>Storage: Upload
```

### Key Differences

| Aspect | SQLite WAL | LTX Format |
|--------|------------|------------|
| Purpose | Temporary changes | Permanent archive |
| Mutability | Mutable (checkpoint) | Immutable |
| Structure | Sequential frames | Indexed pages |
| Checksum | Per-frame | Per-page + cumulative |
| Lock Page | Contains lock bytes | Always skipped |
| Naming | Fixed (-wal suffix) | TXID range |
| Lifetime | Until checkpoint | Forever |
| Size | Grows until checkpoint | Fixed at creation |

### Transaction ID (TXID)

```go
type TXID uint64

// TXID represents a logical transaction boundary
// Not directly from SQLite, but derived from:
// 1. WAL checkpoint sequence
// 2. Frame count
// 3. Logical grouping of changes

func (db *DB) nextTXID() TXID {
    // Increment from last known TXID
    return db.lastTXID + 1
}
```

## Best Practices

### 1. Always Skip Lock Page

```go
const PENDING_BYTE = 0x40000000

func shouldSkipPage(pageNo uint32, pageSize int) bool {
    lockPage := uint32(PENDING_BYTE/pageSize) + 1
    return pageNo == lockPage
}
```

### 2. Preserve Timestamps During Compaction

```go
// Keep earliest CreatedAt from source files
func compactWithTimestamp(files []*FileInfo) *FileInfo {
    earliest := files[0].CreatedAt
    for _, f := range files[1:] {
        if f.CreatedAt.Before(earliest) {
            earliest = f.CreatedAt
        }
    }

    return &FileInfo{
        CreatedAt: earliest, // Preserve for point-in-time recovery
    }
}
```

### 3. Verify Checksums on Read

```go
func safeReadLTX(path string) (*LTXFile, error) {
    file, err := ReadLTXFile(path)
    if err != nil {
        return nil, err
    }

    // Verify all checksums
    for _, page := range file.Pages {
        if err := verifyPage(page); err != nil {
            return nil, fmt.Errorf("corrupted page %d: %w",
                page.Number, err)
        }
    }

    return file, nil
}
```

### 4. Handle Partial Files

```go
// For eventually consistent storage
func readWithRetry(client ReplicaClient, info *FileInfo) ([]byte, error) {
    for attempts := 0; attempts < 5; attempts++ {
        data, err := client.OpenLTXFile(...)
        if err == nil {
            // Verify we got complete file
            if int64(len(data)) == info.Size {
                return data, nil
            }
        }

        time.Sleep(time.Second * time.Duration(attempts+1))
    }

    return nil, errors.New("incomplete file after retries")
}
```

## Debugging LTX Files

### Inspect LTX Files

The Litestream CLI currently exposes a single helper for listing LTX files:

```bash
litestream ltx /path/to/db.sqlite
litestream ltx s3://bucket/db
```

For low-level inspection (page payloads, checksums, etc.), use the Go API:

```go
f, err := os.Open("0000000000000001-0000000000000064.ltx")
if err != nil {
    log.Fatal(err)
}
defer f.Close()

dec := ltx.NewDecoder(f)
if err := dec.DecodeHeader(); err != nil {
    log.Fatal(err)
}
for {
    var hdr ltx.PageHeader
    data := make([]byte, dec.Header().PageSize)
    if err := dec.DecodePage(&hdr, data); err == io.EOF {
        break
    } else if err != nil {
        log.Fatal(err)
    }
    // Inspect hdr.Pgno or data here.
}
if err := dec.Close(); err != nil {
    log.Fatal(err)
}
fmt.Println("post-apply checksum:", dec.Trailer().PostApplyChecksum)
```

## Summary

LTX format provides:
1. **Immutable history** - Every change preserved
2. **Efficient storage** - Indexed, compressed via compaction
3. **Data integrity** - Checksums at multiple levels
4. **Point-in-time recovery** - Via TXID ranges
5. **Cloud-optimized** - Designed for object storage

Understanding LTX is essential for:
- Implementing replica clients
- Debugging replication issues
- Optimizing compaction
- Ensuring data integrity
- Building recovery tools
