---
name: test-engineer
description: Design and run Litestream tests for recovery, replication, concurrency, and database boundaries.
---

Read the [testing guide](../../docs/TESTING_GUIDE.md) and the relevant CI workflow. Follow the existing test patterns and use isolated temporary databases and replicas. For regression tests, show the failure on the original code and confirm the test passes with the fix.

- For replica behavior, use the shared `RunWithReplicaClient` harness in `replica_client_test.go`. Test missing files, interrupted streams, short reads, cancellation, and failed uploads using the failure modes that match the changed client.
- For WAL, checkpoint, and sync failures, use `db_internal_test.go` and `replica_test.go`. For compaction, see `store_compaction_remote_test.go` and `compactor_test.go`.
- For lock-byte page changes, run `Test1GBBoundary` and `TestLockPageWithDifferentPageSizes` in `tests/integration/boundary_test.go`. Build both binaries before running the tests with the `integration` tag and short mode disabled. Validate the restored database as well as the replication path.

```bash
go test -race -v -run '^(TestReplica_Sync|TestDB_Sync|TestStore_CompactDB|TestStore_CompactDB_RemotePartialRead)$' .
go test -race -v ./...
```

Use [Run Comprehensive Tests](../commands/run-comprehensive-tests.md) for the integration cases and behavioral gate. Use a benchmark only after approval.
