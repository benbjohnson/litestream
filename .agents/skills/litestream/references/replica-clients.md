# Storage backends

## Implementation

Read the [replica client guide](../../../../docs/REPLICA_CLIENT_GUIDE.md) and use `replica_client.go` as the interface source. Implement idempotent `Init` and `SetLogger` along with listing, reads, writes, and deletion. Consult `file/` for a filesystem example and the client closest to the new backend for remote behavior. Other clients include S3, GS, ABS, SFTP, WebDAV, NATS, and OSS.

- Honor TXID seek in listings and offset and size in reads, including offset-only requests used by resumable reads. Provide accurate timestamps when `useMetadata` is true for point-in-time restore.
- Return errors with enough context for callers, and preserve `errors.Is(err, os.ErrNotExist)` for missing LTX files. Ensure a failed write cannot publish an incomplete LTX object.
- Implement `ReplicaClientV3` from `v3.go` when supporting restores from v0.3.x backups. See `s3/replica_client.go` for an example.
- Wire CLI configuration through `NewReplicaFromConfig` in `cmd/litestream/main.go`. Use a singular `replica` under each database in configuration examples. For URL construction outside that path, see `replica_url.go` and the existing backend registrations.

Add backend-specific tests and register the backend with the shared client tests in `internal/testingutil/testingutil.go`. Use [Replica contract checks](replica-clients.md#validate-the-contract) to exercise the contract.

## Add a backend

1. Read [CONTRIBUTING.md](../../../../CONTRIBUTING.md) and the [replica client guide](../../../../docs/REPLICA_CLIENT_GUIDE.md). Identify the backend's naming, pagination, range, authentication, and write semantics.
2. Implement the interface in `replica_client.go`, including idempotent `Init` and `SetLogger`. Use an existing backend as a reference. Honor seek and offset/size, preserve the LTX timestamp for point-in-time restore, and make successful writes visible as complete objects. Use the backend's atomic publication mechanism.
3. Wire the CLI configuration into `NewReplicaFromConfig` in `cmd/litestream/main.go`. Follow `replica_url.go` and existing backend registrations for URL-based construction. Use `dbs[].replica` in configuration examples.
4. Add backend unit tests and register the integration setup in `internal/testingutil/testingutil.go`. Use the shared `RunWithReplicaClient` tests and the checks in [Replica contract checks](replica-clients.md#validate-the-contract).
5. Document the configuration and test requirements. Run the package's race tests and the shared integration suite with the registered backend name and its test credentials.

## Validate the contract

Read the `ReplicaClient` interface in `replica_client.go` and the [replica client guide](../../../../docs/REPLICA_CLIENT_GUIDE.md). Use the shared cases in `replica_client_test.go` and backend-specific cases for the implementation.

- Verify idempotent initialization, TXID seek, stable listing order, and iterator error and close handling.
- Verify full, offset-only, and bounded reads. Close returned readers and check read errors. For a missing file, check `errors.Is(err, os.ErrNotExist)`.
- Verify a known LTX header timestamp survives upload and metadata-enabled listing. When metadata is disabled, check the backend's fast listing path.
- Exercise write failure and cancellation without leaving a partial committed object. Verify cleanup behavior, concurrency, and context cancellation on a disposable replica. Keep `DeleteAll` scoped to its configured prefix.

```bash
go test -race -v ./file
go test -v -run '^TestReplicaClient_' . -args -integration -replica-clients=file
```

For another backend, select the corresponding package and registered test client. Configure the backend's test resources using `internal/testingutil/testingutil.go` and `.github/workflows/commit.yml`.
