# S3 Manifest Consistency Design

## Goal

Keep manifest-backed S3 reads correct across existing replicas, process crashes, partial object mutations, manifest I/O failures, and configuration changes while preserving manifest maintenance as an optimization after the authoritative LTX mutation succeeds.

## Consistency Protocol

Before any manifest-aware S3 mutation, the client acquires distributed ownership through conditional writes to `PATH/.manifest/lock.json`. The ownership object stores an owner, expiration, ETag, and persistent generation. The client renews the lease during the mutation, checks ownership before valid-manifest publication, and conditionally releases it by writing an expired state. An expired lease can be taken over after a crash. Acquisition, renewal, release, and lost-ownership errors are returned.

The client holds `manifestMu` while it owns the distributed lease so its local cache cannot change concurrently. The persistent generation detects whether another client acquired ownership since this client last published. Consecutive uncontended mutations can reuse the validated cache; a generation gap, restart, failed mutation, or uncertain storage outcome clears the cache and requires a direct LIST rebuild.

After acquiring ownership, the client atomically overwrites `manifest.json` with a syntactically valid manifest whose version is intentionally unsupported. Existing manifest-aware readers reject the unsupported version and use authoritative LIST operations. Readers predating manifest support ignore the object because it is not an LTX filename. This invalidation lifecycle applies when any manifest mode is active, including `ManifestEnabled` without manifest writing.

If invalidation fails, the operation returns before changing LTX objects. A writer without trusted cache state rebuilds directly from S3 LIST results for every LTX level using `newFileIterator`, which cannot recursively consult the manifest.

After a successful LTX write or deletion, a manifest writer renews ownership, updates the cache, and publishes a valid manifest. If publication fails, the writer clears the cache and leaves LIST fallback active. If an LTX upload, deletion, `DeleteAll`, or storage response is uncertain after invalidation, the writer clears the cache. A later owner rebuilds from LIST before publishing another valid manifest.

`DeleteAll` skips the ownership object while deleting replica data, then releases ownership. This prevents the operation from removing the lock that serializes it.

## Disabling Manifests

Clients created from configuration retain `ManifestConfigured` so they can remove a manifest when the setting is disabled or removed. Cleanup occurs before the LTX mutation. A failed cleanup returns an error without mutating LTX objects, leaves cleanup incomplete, and is retried on the next operation. Successful cleanup is remembered to avoid repeated DELETE calls.

Programmatically constructed clients with `ManifestConfigured == false` do not perform cleanup.

## Interface Compatibility

`SetManifestEnabled` is removed from the public `ReplicaClient` interface. The restore command uses a private optional capability interface containing `SetManifestEnabled(bool)` and enables the optimization only when the concrete client implements it. S3 keeps the method; unrelated backends and test clients remove their no-op implementations.

## Logging and Test API Cleanup

Manifest diagnostics use the client’s configured `c.logger` so backend grouping and injected logging configuration are preserved. Manifest tests use `package s3`, call `newManifestIterator` directly, and remove the exported production-only `NewManifestIteratorForTest` helper.

## Test Strategy

A stateful test S3 HTTP transport stores objects across client instances, counts operations, and can inject deterministic failures for LIST, GET, PUT, DELETE, and `DeleteObjects`. Tests cover:

- Bootstrapping a valid manifest from an existing populated replica and reusing validated cache state for uncontended mutations.
- Deterministic two-client write/write and write/delete interleavings that require distributed serialization.
- Ownership acquisition failure, renewal failure, lost ownership, expired-lease takeover, and `DeleteAll` lock preservation.
- Reader LIST fallback while the unsupported-version sentinel exists.
- Interruption after invalidation and after an LTX object upload but before valid manifest publication.
- LTX upload failure, manifest GET failure, rebuild LIST failure, valid-manifest PUT failure, and partial batch deletion failure.
- Fail-after-apply PUT, DELETE, and `DeleteObjects` outcomes followed by reader fallback and fresh-writer recovery.
- `DeleteAll` success and failure behavior.
- Stale-manifest cleanup failure blocking the mutation and a later successful retry.
- Manifest fallback diagnostics reaching an injected client logger.
- Restore behavior for clients that do and do not implement the optional manifest capability.
- Removal of unrelated backend no-op methods while all implementations continue satisfying `ReplicaClient`.

Verification runs focused S3 and restore tests, race-enabled focused tests, `go test ./...`, formatting and whitespace checks, and `pre-commit run --all-files` before the changes are pushed and CI is watched to completion.
