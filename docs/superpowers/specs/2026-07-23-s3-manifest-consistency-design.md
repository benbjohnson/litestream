# S3 Manifest Consistency Design

## Goal

Keep manifest-backed S3 reads correct across existing replicas, process crashes, partial object mutations, manifest I/O failures, and configuration changes while preserving manifest maintenance as an optimization after the authoritative LTX mutation succeeds.

## Consistency Protocol

Before any manifest-aware S3 mutation, the writer atomically overwrites `manifest.json` with a syntactically valid manifest whose version is intentionally unsupported. Existing manifest-aware readers reject the unsupported version and fall back to authoritative LIST operations. Readers predating manifest support ignore the object because it is not an LTX filename.

The writer holds `manifestMu` from invalidation through the LTX mutation and manifest publication so concurrent writes and deletes cannot publish state out of order. If the invalidation write fails, the operation returns before changing LTX objects because a valid stale manifest may still be visible.

After invalidation, a writer without a trusted in-memory manifest rebuilds one directly from S3 LIST results for every LTX level. The rebuild uses `newFileIterator` rather than `LTXFiles` so it cannot recursively consult the manifest. This bootstraps existing replicas and recovers after restarts or partial failures.

After a successful LTX write or deletion, the writer updates the in-memory manifest and publishes a valid manifest. If publication fails, the writer clears the in-memory cache and leaves the unsupported-version sentinel in place. The LTX mutation remains successful because readers safely fall back to LIST and the failure affects only the optimization.

If an LTX upload, batch deletion, or `DeleteAll` operation fails after invalidation, the writer clears the in-memory cache and leaves the sentinel in place. A later successful mutation rebuilds from authoritative LIST state before publishing another valid manifest.

## Disabling Manifests

Clients created from configuration retain `ManifestConfigured` so they can remove a manifest when the setting is disabled or removed. Cleanup occurs before the LTX mutation. A failed cleanup returns an error without mutating LTX objects, leaves cleanup incomplete, and is retried on the next operation. Successful cleanup is remembered to avoid repeated DELETE calls.

Programmatically constructed clients with `ManifestConfigured == false` do not perform cleanup.

## Interface Compatibility

`SetManifestEnabled` is removed from the public `ReplicaClient` interface. The restore command uses a private optional capability interface containing `SetManifestEnabled(bool)` and enables the optimization only when the concrete client implements it. S3 keeps the method; unrelated backends and test clients remove their no-op implementations.

## Logging and Test API Cleanup

Manifest diagnostics use the client’s configured `c.logger` so backend grouping and injected logging configuration are preserved. Manifest tests use `package s3`, call `newManifestIterator` directly, and remove the exported production-only `NewManifestIteratorForTest` helper.

## Test Strategy

A stateful test S3 HTTP transport stores objects across client instances, counts operations, and can inject deterministic failures for LIST, GET, PUT, DELETE, and `DeleteObjects`. Tests cover:

- Bootstrapping a valid manifest from an existing populated replica and adding the current mutation.
- Recreating a client over the same object store to simulate restart recovery.
- Reader LIST fallback while the unsupported-version sentinel exists.
- Interruption after invalidation and after an LTX object upload but before valid manifest publication.
- LTX upload failure, manifest GET failure, rebuild LIST failure, valid-manifest PUT failure, and partial batch deletion failure.
- `DeleteAll` success and failure behavior.
- Stale-manifest cleanup failure blocking the mutation and a later successful retry.
- Manifest fallback diagnostics reaching an injected client logger.
- Restore behavior for clients that do and do not implement the optional manifest capability.
- Removal of unrelated backend no-op methods while all implementations continue satisfying `ReplicaClient`.

Verification runs focused S3 and restore tests, race-enabled focused tests, `go test ./...`, formatting and whitespace checks, and `pre-commit run --all-files` before the changes are pushed and CI is watched to completion.
