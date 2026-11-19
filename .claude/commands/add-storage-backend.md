---
description: Create a new Litestream storage backend.
---

1. Read [CONTRIBUTING.md](../../CONTRIBUTING.md) and the [replica client guide](../../docs/REPLICA_CLIENT_GUIDE.md). Identify the backend's naming, pagination, range, authentication, and write semantics.
2. Implement the interface in `replica_client.go`, including idempotent `Init` and `SetLogger`. Use an existing backend as a reference. Honor seek and offset/size, preserve the LTX timestamp for point-in-time restore, and make successful writes visible as complete objects. Use the backend's atomic publication mechanism.
3. Wire the CLI configuration into `NewReplicaFromConfig` in `cmd/litestream/main.go`. Follow `replica_url.go` and existing backend registrations for URL-based construction. Use `dbs[].replica` in configuration examples.
4. Add backend unit tests and register the integration setup in `internal/testingutil/testingutil.go`. Use the shared `RunWithReplicaClient` tests and the checks in [Validate Replica](validate-replica.md).
5. Document the configuration and test requirements. Run the package's race tests and the shared integration suite with the registered backend name and its test credentials.
