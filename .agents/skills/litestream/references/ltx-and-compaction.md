# LTX and compaction

## Implementation

Read [LTX format](../../../../docs/LTX_FORMAT.md) and trace scheduling and per-database coordination in `store.go`. `DB.Compact` delegates regular compaction to `Compactor.Compact` in `compactor.go`. `DB.Snapshot` builds full database snapshots at L9.

- LTX files are immutable. The filename encodes the inclusive TXID range as two 16-digit hexadecimal numbers. Default schedules are 30 seconds for L1, 5 minutes for L2, and 1 hour for L3. The default L9 snapshot interval is 24 hours.
- Regular compaction lists sources at the previous level. When a local opener is configured, use the local file if present, fall back to a resumable remote stream when the local file is missing, and return other local open errors. The pinned LTX library streams merged pages into a new file and retains the header timestamp from the source with the highest TXID.
- Use the pinned LTX encoder and decoder for format details and validation. Compaction must preserve restore coverage and exclude the SQLite lock-byte page.

```bash
go test -race -v -run '^(TestStore_CompactDB(_RemotePartialRead)?|TestCompaction_PreservesLastTimestamp|TestCompactor_CompactResumesRemoteSourceAfterDisconnect)$' .
```

For corruption triage, see [LTX inspection](ltx-and-compaction.md#inspect-files-and-restore-coverage). For core compaction changes, use the behavioral gate in [CONTRIBUTING.md](../../../../CONTRIBUTING.md).

## Inspect files and restore coverage

Record the database path, replica, Litestream version, target TXID or timestamp, and exact error. Use listings to compare available ranges with the restore target and distinguish files missing from the replica from files missing in the local metadata directory.

```bash
litestream ltx -level all -json file:///path/to/replica
litestream ltx -level all -json -config /path/to/litestream.yml /path/to/database.db
```

Both commands list replica metadata. For format or checksum errors, decode the affected file using the pinned LTX decoder. For a restore failure, check range coverage across available compaction levels, decode checksums, verify ascending page numbers while allowing gaps in incremental files, and check that page-copy loops exclude the lock-byte page. See [LTX format](../../../../docs/LTX_FORMAT.md), `compactor.go`, and the applicable replica client.

On a disposable fixture, build both binaries, then use the harness to restore and check a replica. Choose a fresh output path because the harness removes an existing file at `-restored-db`.

```bash
go build -o bin/litestream ./cmd/litestream
go build -o bin/litestream-test ./cmd/litestream-test
PATH="$PWD/bin:$PATH" ./bin/litestream-test validate -source-db /path/to/source.db -replica-url file:///path/to/replica -restored-db /path/to/new-restored.db -check-type integrity
```

For local LTX damage, inspect the removals with `litestream reset -dry-run`. An authorized reset clears local LTX files and forces a fresh snapshot on the next sync. It does not repair missing or corrupted history in the replica. `auto-recover: true` under `dbs[].replica` enables automatic reset for eligible local LTX errors and is off by default. See `cmd/litestream/reset.go`, `replica.go`, and `LTXError.IsAutoRecoverable` in `litestream.go`.

## Test compaction

Identify the affected levels, restore target, backend, and failure mode. Use `store_test.go` for scheduling and the compaction entry point, `store_compaction_remote_test.go` for local preference, and `compactor_test.go` for interrupted remote streams and failed uploads. The compactor falls back to remote only when the local file is missing or a local opener is unavailable.

```bash
go test -race -v -run '^(TestStore_CompactDB(_RemotePartialRead)?|TestCompaction_PreservesLastTimestamp|TestCompactor_Compact(ClosesPipeOnWriteError|ResumesRemoteSourceAfterDisconnect|DoesNotReopenSourcesAfterClose)?)$' .
```

Assert that the output covers the expected TXIDs, retains the newest source header timestamp, skips the lock-byte page, and restores the expected database. For a failure regression, assert that retries and cleanup preserve the recoverable ranges. For page iteration, add the large-database cases in `tests/integration/boundary_test.go`. For core compaction changes, run the behavioral gate in [CONTRIBUTING.md](../../../../CONTRIBUTING.md).
