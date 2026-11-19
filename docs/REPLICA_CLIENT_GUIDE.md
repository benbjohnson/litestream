# ReplicaClient Implementation Guide

A replica backend implements the interface in [`replica_client.go`](../replica_client.go). Start with the [file backend](../file/replica_client.go) for local I/O and the [S3 backend](../s3/replica_client.go) for a paginated cloud backend.

## Interface Contract

| Method | Behavior |
|--------|----------|
| `Type()` | Returns the backend identifier, such as `s3`, `gs`, or `file`. |
| `Init(ctx)` | Initializes the client. Calls after a successful initialization are safe. |
| `SetLogger(logger)` | Sets the logger used by the backend. |
| `LTXFiles(ctx, level, seek, useMetadata)` | Lists files at a level in TXID order, beginning at the requested minimum TXID or the next available one. Zero starts at the beginning. |
| `OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)` | Opens an LTX byte range. A zero size reads from the offset through the end. A missing LTX file returns an error matching `os.ErrNotExist`. |
| `WriteLTXFile(ctx, level, minTXID, maxTXID, r)` | Writes an LTX stream and returns its level, TXID range, size in bytes, and original timestamp. |
| `DeleteLTXFiles(ctx, files)` | Deletes the specified files and surfaces failures, including errors for individual items in a batch response. |
| `DeleteAll(ctx)` | Deletes files for this replica's database prefix or directory. |

The CLI builds clients from structured configuration. URL-based callers such as the VFS and control socket use registered replica client factories. Follow an existing backend for both construction paths. See [`cmd/litestream/main.go`](../cmd/litestream/main.go), [`replica_url.go`](../replica_url.go), and [`server.go`](../server.go).

## Storage Layout

Keep the layout of an existing backend compatible with its prior backups. The file backend uses `LTXFilePath` to create `ltx/<level>/<minTXID>-<maxTXID>.ltx` under its configured root. The S3 backend uses four-digit hexadecimal level prefixes directly under its configured path, for example `0000/` for L0 and `0009/` for snapshots. Use `ltx.FormatFilename` and `ltx.ParseFilename` for transaction ranges. See [`litestream.go`](../litestream.go), [`file/replica_client.go`](../file/replica_client.go), and [`s3/replica_client.go`](../s3/replica_client.go).

## File Timestamps

Preserve the timestamp encoded in the LTX header. Built-in clients inspect the header with `ltx.PeekHeader`, preserve the bytes for the upload, and report its timestamp as `FileInfo.CreatedAt`. File and SFTP clients set the file modification time. S3 and other object-store clients can store it in object metadata. See [`file/replica_client.go`](../file/replica_client.go), [`sftp/replica_client.go`](../sftp/replica_client.go), and [`s3/replica_client.go`](../s3/replica_client.go).

For timestamp-based restore, `LTXFiles` receives `useMetadata=true` and must return the accurate timestamp. When `useMetadata=false`, clients may use cheaper listing metadata such as the object's last-modified time. A backend that already exposes the preserved time in its normal listing can use that for both modes. See [`replica_client.go`](../replica_client.go) and [`s3/replica_client.go`](../s3/replica_client.go).

## Transfers and Errors

Stream uploads and range reads where the backend allows. File and SFTP clients stage writes before renaming. S3 uses the AWS SDK v2 and chooses single-part or multipart upload according to object and part size. Check backend-specific listing, range, and consistency semantics before relying on them in restore or retention. See [`file/replica_client.go`](../file/replica_client.go), [`sftp/replica_client.go`](../sftp/replica_client.go), and [`s3/replica_client.go`](../s3/replica_client.go).

Pass the caller's context to storage requests and preserve underlying errors with `%w`. Return an error matching `os.ErrNotExist` for a missing file. Surface iteration errors and release iterator resources. Paginate remote listings when supported and preserve TXID order across pages. See [`replica_client.go`](../replica_client.go) and [`s3/replica_client.go`](../s3/replica_client.go).

Long restore and compaction streams can be interrupted midway. `ResumableReader` can reopen a stream at the current byte offset after a read failure or a premature EOF when the expected size is known. Correct byte-range behavior is required for the resumed stream to reconstruct the original data. See [`internal/resumable_reader.go`](../internal/resumable_reader.go), [`replica.go`](../replica.go), and [`compactor.go`](../compactor.go).

## Testing

The shared `TestReplicaClient_*` tests exercise writes, ordered listings, partial reads, missing objects, timestamp preservation, and deletion. Use a valid LTX header in storage test data, as `createLTXDataWithTimestamp` does. Add a backend to the shared test harness in [`internal/testingutil/testingutil.go`](../internal/testingutil/testingutil.go) when applicable. Use the callback-based [`mock.ReplicaClient`](../mock/replica_client.go) for tests of callers. See [`replica_client_test.go`](../replica_client_test.go) for the shared tests.

```bash
go test -v . -run '^TestReplicaClient_' -args -integration -replica-clients=file
```

Set `-replica-clients` to the backend under test and configure its test credentials or connection settings as needed.

## Implementation Checklist

- Implement every `ReplicaClient` method and register both structured-config and URL construction where supported.
- Preserve the backend's existing object layout, ordered listings, LTX header timestamps, and zero-size range-read semantics.
- Propagate cancellation, upload/close errors, iterator errors, and per-object deletion failures; test missing objects and interrupted streams.
- Add the client to the shared test harness and exercise writes, restore, listing, ranges, timestamps, and deletion against a disposable destination.

## ReplicaClientV3 Interface (v0.3.x Restore)

The optional `ReplicaClientV3` interface supports restoring from pre-LTX backups. The file and S3 backends implement it. Use the types and path helpers in [`v3.go`](../v3.go).

| Method | Behavior |
|--------|----------|
| `GenerationsV3(ctx)` | Lists generation IDs in ascending order. |
| `SnapshotsV3(ctx, generation)` | Lists snapshots in ascending index order. |
| `WALSegmentsV3(ctx, generation)` | Lists WAL segments ordered by index, then offset. |
| `OpenSnapshotV3(ctx, generation, index)` | Opens an LZ4-decompressed snapshot stream. |
| `OpenWALSegmentV3(ctx, generation, index, offset)` | Opens an LZ4-decompressed WAL segment stream. The offset identifies the segment in its filename. |

Generation IDs contain 16 lowercase hexadecimal characters. Snapshots are stored at `generations/{generation}/snapshots/{index:08x}.snapshot.lz4`. WAL segments are stored at `generations/{generation}/wal/{index:08x}_{offset:08x}.wal.lz4`. The WAL offset field expands when needed.
