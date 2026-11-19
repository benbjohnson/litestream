# Test design and execution

## Design regression tests

Read the [testing guide](../../../../docs/TESTING_GUIDE.md) and the relevant CI workflow. Follow the existing test patterns and use isolated temporary databases and replicas. For regression tests, show the failure on the original code and confirm the test passes with the fix.

- For replica behavior, use the shared `RunWithReplicaClient` harness in `replica_client_test.go`. Test missing files, interrupted streams, short reads, cancellation, and failed uploads using the failure modes that match the changed client.
- For WAL, checkpoint, and sync failures, use `db_internal_test.go` and `replica_test.go`. For compaction, see `store_compaction_remote_test.go` and `compactor_test.go`.
- For lock-byte page changes, run `Test1GBBoundary` and `TestLockPageWithDifferentPageSizes` in `tests/integration/boundary_test.go`. Build both binaries before running the tests with the `integration` tag and short mode disabled. Validate the restored database as well as the replication path.

```bash
go test -race -v -run '^(TestReplica_Sync|TestDB_Sync|TestStore_CompactDB|TestStore_CompactDB_RemotePartialRead)$' .
go test -race -v ./...
```

Use [Applicable test suites](testing.md#run-applicable-suites) for the integration cases and behavioral gate. Use a benchmark only after approval.

## Run applicable suites

Use the repository's [testing guide](../../../../docs/TESTING_GUIDE.md) and the CI workflows for the changed packages. Build both binaries before running process-based integration tests. Integration and soak tests that require them can skip if binaries are missing.

### Build, race tests, and static checks

```bash
go build -o bin/litestream ./cmd/litestream
go build -o bin/litestream-test ./cmd/litestream-test
go test -race -v ./...
go vet ./...
pre-commit run --all-files
```

### Backend integration

```bash
go test -v -run '^TestReplicaClient_' . -args -integration -replica-clients=file
```

For remote backends, use the registered client name and configure disposable test storage using `internal/testingutil/testingutil.go` and the CI workflows. Verify that the expected subtest ran.

### Page boundary and core replication

```bash
go test -tags integration -run '^(Test1GBBoundary|TestLockPageWithDifferentPageSizes)$' -v ./tests/integration
LOG_LEVEL=DEBUG go test -tags 'integration,soak' -run '^TestLTXBehavior($|_ProfileConfigIsolation$)' -short -v -timeout 15m ./tests/integration/
```

Run the boundary cases without short mode. Use the behavioral gate for changes to core replication, compaction, or WAL handling.

### VFS

For changes to the VFS, use a CGO-enabled toolchain. Build the extension and run the tagged tests.

```bash
make vfs
CGO_ENABLED=1 go test -tags vfs -race -count=1 . ./cmd/litestream-vfs
```

Report the commands run, any cases that skipped, and their results. Run benchmarks only with approval.
