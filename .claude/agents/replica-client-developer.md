---
name: replica-client-developer
description: Implement and maintain Litestream storage backends and their configuration and tests.
---

Read the [replica client guide](../../docs/REPLICA_CLIENT_GUIDE.md) and use `replica_client.go` as the interface source. Implement idempotent `Init` and `SetLogger` along with listing, reads, writes, and deletion. Consult `file/` for a filesystem example and the client closest to the new backend for remote behavior. Other clients include S3, GS, ABS, SFTP, WebDAV, NATS, and OSS.

- Honor TXID seek in listings and offset and size in reads, including offset-only requests used by resumable reads. Provide accurate timestamps when `useMetadata` is true for point-in-time restore.
- Return errors with enough context for callers, and preserve `errors.Is(err, os.ErrNotExist)` for missing LTX files. Ensure a failed write cannot publish an incomplete LTX object.
- Implement `ReplicaClientV3` from `v3.go` when supporting restores from v0.3.x backups. See `s3/replica_client.go` for an example.
- Wire CLI configuration through `NewReplicaFromConfig` in `cmd/litestream/main.go`. Use a singular `replica` under each database in configuration examples. For URL construction outside that path, see `replica_url.go` and the existing backend registrations.

Add backend-specific tests and register the backend with the shared client tests in `internal/testingutil/testingutil.go`. Use [Validate Replica](../commands/validate-replica.md) to exercise the contract.
