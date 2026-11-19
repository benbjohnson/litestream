---
description: Inspect Litestream LTX files and investigate missing ranges or corruption.
---

Record the database path, replica, Litestream version, target TXID or timestamp, and exact error. Use listings to compare available ranges with the restore target and distinguish files missing from the replica from files missing in the local metadata directory.

```bash
litestream ltx -level all -json file:///path/to/replica
litestream ltx -level all -json -config /path/to/litestream.yml /path/to/database.db
```

Both commands list replica metadata. For format or checksum errors, decode the affected file using the pinned LTX decoder. For a restore failure, check range coverage across available compaction levels, decode checksums, verify ascending page numbers while allowing gaps in incremental files, and check that page-copy loops exclude the lock-byte page. See [LTX format](../../docs/LTX_FORMAT.md), `compactor.go`, and the applicable replica client.

On a disposable fixture, build both binaries, then use the harness to restore and check a replica. Choose a fresh output path because the harness removes an existing file at `-restored-db`.

```bash
go build -o bin/litestream ./cmd/litestream
go build -o bin/litestream-test ./cmd/litestream-test
PATH="$PWD/bin:$PATH" ./bin/litestream-test validate -source-db /path/to/source.db -replica-url file:///path/to/replica -restored-db /path/to/new-restored.db -check-type integrity
```

For local LTX damage, inspect the removals with `litestream reset -dry-run`. An authorized reset clears local LTX files and forces a fresh snapshot on the next sync. It does not repair missing or corrupted history in the replica. `auto-recover: true` under `dbs[].replica` enables automatic reset for eligible local LTX errors and is off by default. See `cmd/litestream/reset.go`, `replica.go`, and `LTXError.IsAutoRecoverable` in `litestream.go`.
