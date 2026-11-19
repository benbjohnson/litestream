---
description: Check a Litestream replica client against its storage contract.
---

Read the `ReplicaClient` interface in `replica_client.go` and the [replica client guide](../../docs/REPLICA_CLIENT_GUIDE.md). Use the shared cases in `replica_client_test.go` and backend-specific cases for the implementation.

- Verify idempotent initialization, TXID seek, stable listing order, and iterator error and close handling.
- Verify full, offset-only, and bounded reads. Close returned readers and check read errors. For a missing file, check `errors.Is(err, os.ErrNotExist)`.
- Verify a known LTX header timestamp survives upload and metadata-enabled listing. When metadata is disabled, check the backend's fast listing path.
- Exercise write failure and cancellation without leaving a partial committed object. Verify cleanup behavior, concurrency, and context cancellation on a disposable replica. Keep `DeleteAll` scoped to its configured prefix.

```bash
go test -race -v ./file
go test -v -run '^TestReplicaClient_' . -args -integration -replica-clients=file
```

For another backend, select the corresponding package and registered test client. Configure the backend's test resources using `internal/testingutil/testingutil.go` and `.github/workflows/commit.yml`.
