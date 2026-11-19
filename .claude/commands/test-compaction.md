---
description: Test Litestream compaction, restore coverage, and failure handling.
---

Identify the affected levels, restore target, backend, and failure mode. Use `store_test.go` for scheduling and the compaction entry point, `store_compaction_remote_test.go` for local preference, and `compactor_test.go` for interrupted remote streams and failed uploads. The compactor falls back to remote only when the local file is missing or a local opener is unavailable.

```bash
go test -race -v -run '^(TestStore_CompactDB(_RemotePartialRead)?|TestCompaction_PreservesLastTimestamp|TestCompactor_Compact(ClosesPipeOnWriteError|ResumesRemoteSourceAfterDisconnect|DoesNotReopenSourcesAfterClose)?)$' .
```

Assert that the output covers the expected TXIDs, retains the newest source header timestamp, skips the lock-byte page, and restores the expected database. For a failure regression, assert that retries and cleanup preserve the recoverable ranges. For page iteration, add the large-database cases in `tests/integration/boundary_test.go`. For core compaction changes, run the behavioral gate in [CONTRIBUTING.md](../../CONTRIBUTING.md).
