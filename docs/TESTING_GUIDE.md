# Litestream Testing Guide

Use real SQLite databases and isolated replica directories. Give concurrency tests a way to observe progress, and use fixed seeds and timestamps when the assertions depend on them. Use the [Go version in `go.mod`](../go.mod). Race detection, the `litestream-test` binary, and the tagged VFS package tests require CGO and a C compiler.

## Unit and race tests

```bash
go test -v ./...
go test -race -v ./...
go test -race -count=1 -run '^(TestReplica_Sync|TestDB_Sync|TestStore_CompactDB)$' .
```

Choose the assertions for the failure under review. [The store sync test](../store_sync_test.go) coordinates concurrent captures. [Database tests](../db_internal_test.go) cover checkpoints, WAL boundaries, cancellation, and snapshot consistency. [Compactor tests](../compactor_test.go) cover retention and partial remote reads. The VFS implementation and the SQLite integration have separate test entry points.

```bash
go test -tags vfs -race -count=1 .
CGO_ENABLED=1 go test -tags vfs -race -count=1 -v ./cmd/litestream-vfs
```

The VFS race job in [the commit workflow](../.github/workflows/commit.yml) runs both packages and installs the SQLite CLI for the loadable extension tests. For local runs, see the tool requirements in the [VFS guide](VFS.md) and check the verbose output for skipped tests.

## Replica client integration

Pass `-integration` and select the backend with `-replica-clients`. The `file` backend uses a temporary directory and needs no service or credentials. [The shared client tests](../replica_client_test.go) exercise LTX writes, listing, timestamp preservation, and restore planning.

```bash
go test -v -count=1 ./replica_client_test.go -integration -replica-clients=file
```

Use [the backend setup in CI](../.github/workflows/commit.yml) and [the test client configuration](../internal/testingutil/testingutil.go) for provider-specific coverage. Remote test clients write and delete objects during their tests. Use a disposable test destination.

## CLI integration and the LTX behavioral gate

From the repository root, build the binaries expected by [`RequireBinaries`](../tests/integration/helpers.go). Tests that use this helper skip if either binary is missing.

```bash
go build -o bin/litestream ./cmd/litestream
CGO_ENABLED=1 go build -o bin/litestream-test ./cmd/litestream-test
```

Run the [PR gate](../.github/workflows/commit.yml) for changes to core replication, compaction, or WAL handling. It exercises low-volume, high-volume, and burst writes, and checks LTX sizes, snapshot frequency, compaction timing, and profile configuration isolation. The short mode configures two minutes of writes per profile. The [integration test README](../tests/integration/README.md) covers other test tags, backends, and longer-running workloads.

```bash
LOG_LEVEL=DEBUG CGO_ENABLED=1 go test -tags 'integration,soak' \
  -run '^TestLTXBehavior($|_ProfileConfigIsolation$)' -short -v -timeout 15m ./tests/integration/
```

## Lock-page boundary

SQLite reserves the page that contains byte offset `0x40000000`. The 1-based page number is `0x40000000 / pageSize + 1`. Litestream must skip it when reading, encoding, applying, or restoring database pages. See [SQLite internals](SQLITE_INTERNALS.md).

The [boundary tests](../tests/integration/boundary_test.go) use the binaries built above and a file replica. `Test1GBBoundary` populates a 1.5 GB target with 4096-byte pages, replicates and restores it, and checks restored database integrity. `TestLockPageWithDifferentPageSizes` exercises 4096-byte and 8192-byte pages with 1200 MB targets. Allow room for the source database, WAL, LTX files, and restored database. Both tests skip with `-short`.

```bash
CGO_ENABLED=1 go test -tags integration -count=1 -v -timeout 20m \
  -run '^(Test1GBBoundary|TestLockPageWithDifferentPageSizes)$' ./tests/integration/
```

For an independent fixture, [`litestream-test populate`](../cmd/litestream-test/populate.go) accepts the flags below. It replaces the database at `-db`, so put the fixture in a new directory. When validating a boundary change, check that `PRAGMA page_count` reaches the lock-page number and verify the restored data and integrity.

```bash
boundary_dir=$(mktemp -d "${TMPDIR:-/tmp}/litestream-boundary.XXXXXXXX")
./bin/litestream-test populate -db "$boundary_dir/test.db" -target-size 1.5GB -page-size 4096
```

## Benchmark and profiles

The [review skill's performance reference](../.agents/skills/litestream-review/references/performance.md) describes when to use these workloads in a review.

[`BenchmarkVFS_LargeDatabase`](../cmd/litestream-vfs/main_test.go) seeds 20,000 rows, syncs to a local file replica, and opens the database through the VFS. After setup and warm-up it measures a query that computes `COUNT(*)` and `SUM(LENGTH(value))`. The benchmark reports allocations and skips with `-short`. Use the same count, flags, and machine when comparing revisions.

```bash
CGO_ENABLED=1 go test -tags vfs -run '^$' -bench '^BenchmarkVFS_LargeDatabase$' \
  -benchmem -benchtime=1s -count=1 -timeout 3m ./cmd/litestream-vfs
```

[`TestIdleCPUProfile`](../tests/integration/profile_test.go) starts SQLite databases with file replicas, seeds and syncs each once, then leaves their monitors idle for interactive profiling. It needs no credentials or CLI binaries. Run it on loopback and stop it with Ctrl+C when the captures finish.

```bash
PROFILE_ADDR=127.0.0.1:6060 PROFILE_DB_COUNT=10 go test -tags profile \
  -run '^TestIdleCPUProfile$' -count=1 -timeout=0 -v ./tests/integration/
```

Once the test reports that the databases have started, collect profiles from another terminal.

```bash
go tool pprof -top 'http://127.0.0.1:6060/debug/pprof/profile?seconds=30'
go tool pprof -top http://127.0.0.1:6060/debug/pprof/heap
```

## Fuzzing and coverage

Restore fuzzing tests the effect of a compacted file disappearing during restore. VFS fuzzing exercises reads and queries over a file replica. The harnesses live in [the restore fuzz test](../restore_fuzz_test.go) and [the VFS fuzz test](../cmd/litestream-vfs/fuzz_test.go).

```bash
go test -run '^$' -fuzz '^FuzzRestoreWithMissingCompactedFile$' -fuzztime 30s .
CGO_ENABLED=1 go test -tags vfs -run '^$' \
  -fuzz '^FuzzVFSReplicaReadPatterns$' -fuzztime 30s ./cmd/litestream-vfs
```

Use coverage output to find branches the chosen tests missed. Include the relevant build tags when measuring code behind a tag.

```bash
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

## Test helpers and common failures

- Use [the shared test helpers](../internal/testingutil/testingutil.go) to open SQLite databases, create temporary file replicas, and close resources. Give each test its own paths with `t.TempDir()`.
- Configure WAL mode before writes and use a busy timeout for concurrent connections. [The concurrent integration tests](../tests/integration/concurrent_test.go) exercise contention, rapid checkpoints, and WAL growth.
- Use channels when you need to coordinate a specific interleaving, and poll an observable result with a deadline when testing asynchronous replication. Close iterators, readers, and database handles, and cancel and wait for background work before cleanup.
- Use [`mock.ReplicaClient`](../mock/replica_client.go) for controlled storage failures, with methods that match [`ReplicaClient`](../replica_client.go), or wrap a real file client and override the affected operation. [The remote compaction tests](../store_compaction_remote_test.go) show how to inject delayed and partial reads.
