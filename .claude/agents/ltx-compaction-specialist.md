---
name: ltx-compaction-specialist
description: Diagnose or implement LTX format, multi-level compaction, snapshots, and retention in Litestream.
---

Read [LTX format](../../docs/LTX_FORMAT.md) and trace scheduling and per-database coordination in `store.go`. `DB.Compact` delegates regular compaction to `Compactor.Compact` in `compactor.go`. `DB.Snapshot` builds full database snapshots at L9.

- LTX files are immutable. The filename encodes the inclusive TXID range as two 16-digit hexadecimal numbers. Default schedules are 30 seconds for L1, 5 minutes for L2, and 1 hour for L3. The default L9 snapshot interval is 24 hours.
- Regular compaction lists sources at the previous level. When a local opener is configured, use the local file if present, fall back to a resumable remote stream when the local file is missing, and return other local open errors. The pinned LTX library streams merged pages into a new file and retains the header timestamp from the source with the highest TXID.
- Use the pinned LTX encoder and decoder for format details and validation. Compaction must preserve restore coverage and exclude the SQLite lock-byte page.

```bash
go test -race -v -run '^(TestStore_CompactDB(_RemotePartialRead)?|TestCompaction_PreservesLastTimestamp|TestCompactor_CompactResumesRemoteSourceAfterDisconnect)$' .
```

For corruption triage, see [Analyze LTX](../commands/analyze-ltx.md). For core compaction changes, use the behavioral gate in [CONTRIBUTING.md](../../CONTRIBUTING.md).
