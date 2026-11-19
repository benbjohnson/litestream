# LTX format and Litestream integration

The format authority is the `github.com/superfly/ltx` version pinned in `go.mod`. `Version` is 3, `Magic` is `LTX1`, `HeaderSize` is 100 bytes, `PageHeaderSize` is 6 bytes, and `TrailerSize` is 16 bytes. Use the dependency's encoder and decoder rather than hand-parsing copied layouts.

## File contents

An LTX file has a header, encoded pages, page-index metadata, and a trailer. Pages may be compressed; a fixed `pageCount * pageSize` formula does not describe encoded size. `ltx.PageHeader` contains `Pgno` and `Flags`; the size flag introduces the encoded payload length. The page index maps page numbers to encoded offsets and sizes, with a length footer. It is not a binary-search tree.

The header records page size, the final database page count (`Commit`), a TXID range, timestamp, pre-apply checksum, WAL provenance, flags, and node identity. `Commit` is not the number of pages present in an incremental file. A snapshot has `MinTXID == 1` and includes the database state through `MaxTXID`, excluding the reserved lock page. Incremental files require a compatible preceding state; they are not independently restorable snapshots.

The trailer contains `PostApplyChecksum` and `FileChecksum`; it has no page-index offset fields. Database checksums combine page number and content using the dependency's checksum functions. The hasher is CRC64-ISO, not ECMA. `HeaderFlagNoChecksum` disables database checksum tracking, not the file checksum. Compression and structural metadata affect file validation; do not substitute a checksum over raw bytes for the decoder's validation.

## Read, write, and close contracts

Call `Decoder.DecodeHeader` before reading `Decoder.Header`. Read pages with `DecodePage` until `io.EOF` and check `Decoder.Close` to validate the trailer and file checksum. Streaming consumers that do not need random access can disable page-index retention; see the restore path and dependency compactor for current use.

Write through `Encoder.EncodeHeader`, `EncodePage`, and `Close`. Supply the post-apply checksum when required. `Close` finishes index/trailer encoding and can fail; propagate that error before publishing output. Use encoder cleanup on failure. There are no public `EncodePageIndex` or `EncodeTrailer` methods to call separately.

LTX files are immutable after publication. Create new output for compaction, keep only the latest applicable page, respect the final commit size after truncation, and always skip `ltx.LockPgno(pageSize)`. Compaction checks ordering and continuity; a map that merely overwrites duplicate page numbers is not a replacement for those checks. See `compactor.go`, `db.go`, and the dependency's `Compactor.Compact`.

## Names and backend paths

A filename is `<MinTXID>-<MaxTXID>.ltx`, with each TXID represented by 16 lowercase hexadecimal digits. Use `ltx.FormatFilename` and `ltx.ParseFilename`.

| Backend | Storage path |
|---------|--------------|
| File | `<root>/ltx/<decimal level>/<filename>.ltx` |
| S3 | `<prefix>/<four-digit hexadecimal level>/<filename>.ltx` |

For level 9, file storage uses `ltx/9/`; S3 uses `0009/`. Preserve these existing layouts for compatibility. Neither backend writes a separate replica manifest or index object; discovery lists the level and each LTX contains its own page index. To migrate from file storage to S3, restore locally and replicate the restored database rather than copying raw backend contents.

## Compaction, snapshots, and retention

`compaction_level.go` defines defaults: L0 raw changes, L1 at 30 seconds, L2 at 5 minutes, and L3 at 1 hour. `Store.SnapshotLevel` uses level 9 with a default 24-hour interval. Snapshots use TXID filenames at level 9, not timestamp names in a separate snapshots directory.

Compacted output takes the timestamp from the input with the highest TXID, matching the state it represents. Do not overwrite that timestamp with the earliest object creation time. `Compactor.Compact` reads local input when available and falls back to the replica on local not-exist errors; remote readers support resumption. It must return other local errors and close resources on all paths.

Retention can delete files. Immutability does not promise indefinite history: evaluate available snapshots, contiguous TXID coverage, and configured retention together. Restoration and follow mode must handle gaps explicitly rather than assume every level is a complete history.

## Inspection and review

Use `litestream ltx -level all /path/to/db.sqlite` to list files across levels or `litestream ltx -level 9 s3://bucket/prefix` for snapshots. Use the pinned Go API for payload inspection and checksum validation. See [SQLite internals](SQLITE_INTERNALS.md), [architecture](ARCHITECTURE.md), and the shared [review skill](../.agents/skills/litestream-review/SKILL.md).
