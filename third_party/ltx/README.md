Lite Transaction File (LTX)
=================================

The LTX file format provides a way to store SQLite transactional data in
a way that can be encrypted and compacted and is optimized for performance.

## File Format

An LTX file is composed of several sections:

1. Header
2. Page block
3. Trailer

The header contains metadata about the file, the page block contains page
frames, and the trailer contains checksums of the file and the database end state.


#### Header

The header provides information about the number of page frames as well as
database information such as the page size and database size. LTX files
can be compacted together so each file contains the transaction ID (TXID) range
that it represents. A timestamp provides users with a rough approximation of
the time the transaction occurred and the checksum provides a basic integrity
check.

| Offset | Size | Description                             |
| -------| ---- | --------------------------------------- |
| 0      | 4    | Magic number. Always "LTX1".            |
| 4      | 4    | Flags. Reserved. Always 0.              |
| 8      | 4    | Page size, in bytes.                    |
| 12     | 4    | Size of DB after transaction, in pages. |
| 16     | 4    | Database ID.                            |
| 20     | 8    | Minimum transaction ID.                 |
| 28     | 8    | Maximum transaction ID.                 |
| 36     | 8    | Timestamp (Milliseconds since epoch)    |
| 44     | 8    | Pre-apply DB checksum (CRC-ISO-64)      |
| 52     | 48   | Reserved.                               |


#### Page block

This block stores a series of page headers and page data.

| Offset | Size | Description                 |
| -------| ---- | --------------------------- |
| 0      | 4    | Page number.                |
| 4      | N    | Page data.                  |


#### Trailer

The trailer provides checksum for the LTX file data, a rolling checksum of the
database state after the LTX file is applied, and the checksum of the trailer
itself.

| Offset | Size | Description                             |
| -------| ---- | --------------------------------------- |
| 0      | 8    | Post-apply DB checksum (CRC-ISO-64)     |
| 8      | 8    | File checksum (CRC-ISO-64)              |
