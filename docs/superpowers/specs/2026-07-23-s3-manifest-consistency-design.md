# S3 Manifest Consistency Design

## Goal

Keep manifest-backed S3 reads correct across existing replicas, process crashes, partial object mutations, manifest I/O failures, and configuration changes while preserving manifest maintenance as an optimization after the authoritative LTX mutation succeeds.

## Consistency Protocol

Before any manifest-aware S3 mutation, the client acquires distributed ownership through conditional writes to `PATH/.manifest/lock.json`. The ownership object stores an owner, expiration, ETag, and persistent generation. The client renews the lease during the mutation, checks ownership before valid-manifest publication, and conditionally releases it by writing an expired state. An expired lease can be taken over after a crash. Acquisition, renewal, release, and lost-ownership errors are returned.

The client holds `manifestMu` while it owns the distributed lease so its local cache cannot change concurrently. The persistent generation detects whether another client acquired ownership since this client last published. Consecutive uncontended mutations can reuse the validated cache; a generation gap, restart, failed mutation, or uncertain storage outcome clears the cache and requires a direct LIST rebuild.

After acquiring ownership, the client atomically overwrites `manifest.json` with a syntactically valid unsupported-version sentinel containing the ownership generation and a mutation token. The client retains the sentinel ETag. Existing manifest-aware readers reject the unsupported version and use authoritative LIST operations. Readers predating manifest support ignore the object because it is not an LTX filename. This invalidation lifecycle applies when any manifest mode is active, including `ManifestEnabled` without manifest writing.

If invalidation fails, the operation returns before changing LTX objects. A writer without trusted cache state rebuilds directly from S3 LIST results for every LTX level using `newFileIterator`, which cannot recursively consult the manifest.

After a successful LTX write or deletion, a manifest writer renews ownership, updates the cache, and publishes a valid manifest with `If-Match` against the sentinel ETag. Precondition failure maps to lost ownership, clears the cache, and returns an error without replacing newer manifest state. A definite final PUT failure leaves the sentinel active. An ambiguous final PUT may have committed the fenced valid manifest, which is complete and safe for readers. Other final publication errors remain nonfatal after the LTX mutation succeeds.

If an LTX upload, deletion, `DeleteAll`, or storage response is uncertain after invalidation, the writer clears the cache. A later owner rebuilds from LIST before publishing another valid manifest.

`DeleteAll` skips the ownership object while deleting replica data, then releases ownership. This prevents the operation from removing the lock that serializes it.

## Disabling Manifests

Configuration participates in manifest behavior only when the `manifest` key is present. `manifest: true` enables maintenance. `manifest: false` sets `ManifestConfigured` for one-time cleanup before the next LTX mutation. A failed cleanup returns an error without mutating LTX objects and is retried on the next operation. Successful cleanup is remembered to avoid repeated DELETE calls.

An absent key leaves `ManifestConfigured` false, so the client does not acquire manifest ownership or perform cleanup. To disable an enabled manifest, configure explicit false, allow one mutation to complete cleanup, then remove the key. Programmatically constructed clients with `ManifestConfigured == false` follow the same no-cleanup behavior.

## Interface Compatibility

`SetManifestEnabled` is removed from the public `ReplicaClient` interface. The restore command uses a private optional capability interface containing `SetManifestEnabled(bool)` and enables the optimization only when the concrete client implements it. S3 keeps the method; unrelated backends and test clients remove their no-op implementations.

## Logging and Test API Cleanup

Manifest diagnostics use the client’s configured `c.logger` so backend grouping and injected logging configuration are preserved. Manifest tests use `package s3`, call `newManifestIterator` directly, and remove the exported production-only `NewManifestIteratorForTest` helper.

## Test Strategy

A stateful test S3 HTTP transport stores objects across client instances, counts operations, and can inject deterministic failures for LIST, GET, PUT, DELETE, and `DeleteObjects`. Tests cover:

- Bootstrapping a valid manifest from an existing populated replica and reusing validated cache state for uncontended mutations.
- Deterministic two-client write/write and write/delete interleavings that require distributed serialization.
- Ownership acquisition failure, renewal failure, lost ownership, expired-lease takeover, and `DeleteAll` lock preservation.
- Mutation-specific sentinels, conditional final publication, ownership loss during a paused final PUT, and fail-after-apply final PUT outcomes.
- Absent, explicit-false, and true manifest configuration and mutation behavior.
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
