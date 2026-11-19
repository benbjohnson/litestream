---
description: Run Litestream unit, race, and applicable integration tests.
---

Use the repository's [testing guide](../../docs/TESTING_GUIDE.md) and the CI workflows for the changed packages. Build both binaries before running process-based integration tests. Integration and soak tests that require them can skip if binaries are missing.

## Build, race tests, and static checks

```bash
go build -o bin/litestream ./cmd/litestream
go build -o bin/litestream-test ./cmd/litestream-test
go test -race -v ./...
go vet ./...
pre-commit run --all-files
```

## Backend integration

```bash
go test -v -run '^TestReplicaClient_' . -args -integration -replica-clients=file
```

For remote backends, use the registered client name and configure disposable test storage using `internal/testingutil/testingutil.go` and the CI workflows. Verify that the expected subtest ran.

## Page boundary and core replication

```bash
go test -tags integration -run '^(Test1GBBoundary|TestLockPageWithDifferentPageSizes)$' -v ./tests/integration
LOG_LEVEL=DEBUG go test -tags 'integration,soak' -run '^TestLTXBehavior($|_ProfileConfigIsolation$)' -short -v -timeout 15m ./tests/integration/
```

Run the boundary cases without short mode. Use the behavioral gate for changes to core replication, compaction, or WAL handling.

## VFS

For changes to the VFS, use a CGO-enabled toolchain. Build the extension and run the tagged tests.

```bash
make vfs
CGO_ENABLED=1 go test -tags vfs -race -count=1 . ./cmd/litestream-vfs
```

Report the commands run, any cases that skipped, and their results. Run benchmarks only with approval.
