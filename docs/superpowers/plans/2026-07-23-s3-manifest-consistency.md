# S3 Manifest Consistency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make S3 manifests safe across existing replicas, restarts, crashes, partial mutations, manifest failures, and configuration changes without breaking the public `ReplicaClient` interface.

**Architecture:** Every manifest-aware S3 mutation first publishes an unsupported-version sentinel that forces readers to use authoritative LIST operations. The writer serializes invalidation, the LTX mutation, and valid-manifest publication with `manifestMu`; it rebuilds missing in-memory state directly from LIST and leaves the sentinel active after any uncertain or failed mutation. Restore enables manifests through a private optional capability interface rather than changing every storage backend.

**Tech Stack:** Go, AWS SDK for Go v2, Smithy HTTP test transports, `log/slog`, standard `testing`, GitHub Actions.

## Global Constraints

- Always return errors rather than logging and continuing when failure can affect correctness.
- Final valid-manifest publication remains best-effort only because the unsupported-version sentinel guarantees LIST fallback.
- Preserve current `LTXFiles` seek behavior: include files whose `MinTXID >= seek`.
- Add no new runtime dependencies.
- Match existing Litestream naming, logging, test, and error-wrapping conventions.
- Do not add comments beyond exported API documentation or logic whose safety invariant is not self-evident.
- Keep the branch rebased on `origin/main`, push only to `origin`, and use `--force-with-lease` after history changes.

---

## File Structure

- Create `s3/manifest_consistency_test.go`: stateful S3 HTTP fake plus focused bootstrap, crash, failure, cleanup, logger, and `DeleteAll` tests.
- Modify `s3/replica_client.go`: sentinel invalidation, authoritative rebuild, serialized mutation lifecycle, cleanup retry, and client-scoped logging.
- Modify `s3/manifest.go`: remove the production-only iterator test constructor.
- Modify `s3/manifest_test.go`: use `package s3` and test the private iterator directly.
- Modify `replica_client.go`: remove the S3-specific method from the public interface.
- Modify `cmd/litestream/restore.go`: use a private optional manifest capability.
- Modify `cmd/litestream/restore_test.go`: verify capability and non-capability clients.
- Modify backend and test client files that currently contain no-op `SetManifestEnabled` methods: `abs/replica_client.go`, `file/replica_client.go`, `gs/replica_client.go`, `mock/replica_client.go`, `nats/replica_client.go`, `oss/replica_client.go`, `sftp/replica_client.go`, `webdav/replica_client.go`, `db_internal_test.go`, `replica_internal_test.go`, and `store_compaction_remote_test.go`.
- Modify `docs/PATTERNS.md` and `docs/REPLICA_CLIENT_GUIDE.md`: document the sentinel protocol and restore the backend-neutral interface contract.

---

### Task 1: Add a stateful S3 manifest test transport

**Files:**
- Create: `s3/manifest_consistency_test.go`
- Reference: `s3/replica_client_test.go:103-309,608-716`

**Interfaces:**
- Produces: `newManifestTestStore(t *testing.T) *manifestTestStore`
- Produces: `(*manifestTestStore).newClient(path string) *ReplicaClient`
- Produces: deterministic `failNext(operation string, err error)` and object inspection helpers used by later tasks.

- [ ] **Step 1: Add the stateful transport and client builder**

Implement a `package s3` test helper that stores objects in a mutex-protected map and dispatches AWS SDK requests by method and query string:

```go
type manifestTestStore struct {
    t          *testing.T
    mu         sync.Mutex
    objects    map[string][]byte
    failures   map[string][]error
    operations map[string]int
}

func newManifestTestStore(t *testing.T) *manifestTestStore {
    t.Helper()
    return &manifestTestStore{
        t:          t,
        objects:    make(map[string][]byte),
        failures:   make(map[string][]error),
        operations: make(map[string]int),
    }
}

func (s *manifestTestStore) failNext(operation string, err error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.failures[operation] = append(s.failures[operation], err)
}

func (s *manifestTestStore) operationCount(operation string) int {
    s.mu.Lock()
    defer s.mu.Unlock()
    return s.operations[operation]
}
```

Use a `smithyhttp.ClientDoFunc` transport, existing static AWS credentials, path-style addressing, and `MaxAttempts = 1`. Support `GetObject`, `PutObject`, `DeleteObject`, `DeleteObjects`, and paginated `ListObjectsV2`. Preserve ETags, content length, last-modified values, XML response shapes, and object bodies expected by the real SDK and uploader.

- [ ] **Step 2: Add helper methods for LTX and manifest state**

```go
func (s *manifestTestStore) putLTX(t *testing.T, path string, level int, minTXID, maxTXID ltx.TXID) {
    t.Helper()
    key := fmt.Sprintf("%s/%04x/%s", path, level, ltx.FormatFilename(minTXID, maxTXID))
    s.putObject(key, mustLTX(t, minTXID, maxTXID))
}

func (s *manifestTestStore) manifest(t *testing.T, path string) *Manifest {
    t.Helper()
    data := s.object(t, path+"/manifest.json")
    var manifest Manifest
    if err := json.Unmarshal(data, &manifest); err != nil {
        t.Fatalf("decode manifest: %v", err)
    }
    return &manifest
}
```

`newClient()` must return a fresh `ReplicaClient` over the same store so tests simulate process restart while resetting `manifest`, cleanup flags, and logger state.

- [ ] **Step 3: Run the focused helper compilation test**

Run: `go test ./s3 -run '^TestManifestConsistencyStore$' -count=1`

Expected: PASS for a smoke test that PUTs, GETs, LISTs, and DELETEs one object through the real SDK client.

- [ ] **Step 4: Commit the test infrastructure**

```bash
git add s3/manifest_consistency_test.go
git commit -m "test(s3): add stateful manifest store"
```

---

### Task 2: Implement sentinel invalidation and authoritative bootstrap

**Files:**
- Modify: `s3/replica_client.go:704-780,1210-1377`
- Test: `s3/manifest_consistency_test.go`

**Interfaces:**
- Produces: `const manifestInvalidVersion = 0`
- Produces: `prepareManifestMutation(ctx context.Context, rebuild bool) (manifestReady bool, err error)`; caller holds `manifestMu` when manifest configuration is active.
- Produces: `rebuildManifest(ctx context.Context) (*Manifest, error)` using `newFileIterator` directly.
- Produces: `writeManifestInvalidation(ctx context.Context) error`.

- [ ] **Step 1: Write failing bootstrap and sentinel tests**

Add tests with these exact behaviors:

```go
func TestReplicaClient_ManifestBootstrapExistingReplica(t *testing.T) {
    store := newManifestTestStore(t)
    store.putLTX(t, "db", 0, 1, 1)
    store.putLTX(t, "db", 1, 1, 5)

    client := store.newClient("db")
    client.ManifestWriteEnabled = true
    if _, err := client.WriteLTXFile(context.Background(), 0, 6, 6, bytes.NewReader(mustLTX(t, 6, 6))); err != nil {
        t.Fatal(err)
    }

    manifest := store.manifest(t, "db")
    assertManifestEntries(t, manifest, 0, [][2]ltx.TXID{{1, 1}, {6, 6}})
    assertManifestEntries(t, manifest, 1, [][2]ltx.TXID{{1, 5}})
}

func TestReplicaClient_ManifestInvalidationForcesListFallback(t *testing.T) {
    store := newManifestTestStore(t)
    store.putLTX(t, "db", 0, 1, 1)
    writer := store.newClient("db")
    writer.ManifestWriteEnabled = true

    writer.manifestMu.Lock()
    ready, err := writer.prepareManifestMutation(context.Background(), true)
    writer.manifestMu.Unlock()
    if err != nil || !ready {
        t.Fatalf("prepare manifest mutation: ready=%v err=%v", ready, err)
    }

    reader := store.newClient("db")
    reader.ManifestEnabled = true
    infos := mustCollectLTXFiles(t, reader, 0)
    assertFileRanges(t, infos, [][2]ltx.TXID{{1, 1}})
    if got := store.operationCount("ListObjectsV2"); got == 0 {
        t.Fatal("expected LIST fallback for invalid manifest")
    }
}
```

- [ ] **Step 2: Run the tests and verify the current implementation fails**

Run: `go test ./s3 -run 'TestReplicaClient_Manifest(BootstrapExistingReplica|InvalidationForcesListFallback)$' -count=1`

Expected: FAIL because missing manifests currently start empty and there is no invalidation sentinel.

- [ ] **Step 3: Implement invalidation and rebuild helpers**

Add the sentinel and rebuild logic:

```go
const manifestInvalidVersion = 0

func (c *ReplicaClient) writeManifestInvalidation(ctx context.Context) error {
    manifest := NewManifest()
    manifest.Version = manifestInvalidVersion
    if err := c.writeManifest(ctx, manifest); err != nil {
        return fmt.Errorf("s3: invalidate manifest: %w", err)
    }
    return nil
}

func (c *ReplicaClient) rebuildManifest(ctx context.Context) (*Manifest, error) {
    manifest := NewManifest()
    for level := 0; level <= litestream.SnapshotLevel; level++ {
        itr := newFileIterator(ctx, c, level, 0, false)
        for itr.Next() {
            manifest.AddFile(itr.Item())
        }
        if err := itr.Err(); err != nil {
            _ = itr.Close()
            return nil, fmt.Errorf("s3: rebuild manifest level %d: %w", level, err)
        }
        if err := itr.Close(); err != nil {
            return nil, fmt.Errorf("s3: close manifest rebuild iterator: %w", err)
        }
    }
    return manifest, nil
}

func (c *ReplicaClient) prepareManifestMutation(ctx context.Context, rebuild bool) (bool, error) {
    if !c.ManifestWriteEnabled {
        if !c.ManifestConfigured || c.manifestCleanupAttempted {
            return false, nil
        }
        if err := c.deleteManifest(ctx); err != nil {
            return false, err
        }
        c.manifestCleanupAttempted = true
        return false, nil
    }

    if err := c.writeManifestInvalidation(ctx); err != nil {
        return false, err
    }
    if !rebuild || c.manifest != nil {
        return c.manifest != nil, nil
    }

    manifest, err := c.rebuildManifest(ctx)
    if err != nil {
        c.logger.Debug("manifest rebuild failed; leaving LIST fallback enabled", "error", err)
        c.manifest = nil
        return false, nil
    }
    c.manifest = manifest
    return true, nil
}
```

Add `deleteManifest(ctx) error` as the shared DELETE helper. It returns wrapped non-not-found errors and treats not-found as success.

- [ ] **Step 4: Serialize `WriteLTXFile` across the mutation lifecycle**

When `ManifestWriteEnabled || ManifestConfigured`, lock `manifestMu` before preparing the mutation and hold it through upload and final publication. If preparation fails, return before uploader invocation. On upload or ETag failure, clear `c.manifest`. After success, add the file only when `manifestReady`, then call a renamed `publishManifest(ctx)` that debug-logs through `c.logger`, clears the cache on failure, and does not delete the sentinel.

- [ ] **Step 5: Run bootstrap, sentinel, existing manifest, and race tests**

Run: `go test -race ./s3 -run 'Manifest' -count=1`

Expected: PASS.

- [ ] **Step 6: Commit the sentinel protocol**

```bash
git add s3/replica_client.go s3/manifest_consistency_test.go
git commit -m "fix(s3): make manifest writes crash safe"
```

---

### Task 3: Cover delete, partial failure, restart, and disabled cleanup

**Files:**
- Modify: `s3/replica_client.go:1060-1208,1294-1377`
- Test: `s3/manifest_consistency_test.go`

**Interfaces:**
- Consumes: `prepareManifestMutation`, `deleteManifest`, `publishManifest`, and stateful fake operations from Tasks 1-2.
- Produces: safe manifest lifecycle for `DeleteLTXFiles` and `DeleteAll`.

- [ ] **Step 1: Write the mutation failure matrix tests**

Add focused subtests that assert both returned errors and persisted manifest state:

```go
func TestReplicaClient_ManifestMutationFailures(t *testing.T) {
    t.Run("UploadFailureLeavesInvalidManifest", testManifestUploadFailure)
    t.Run("ManifestPutFailureLeavesInvalidManifest", testManifestPublishFailure)
    t.Run("PartialDeleteLeavesInvalidManifest", testManifestPartialDeleteFailure)
    t.Run("RestartRebuildsAfterInvalidManifest", testManifestRestartRebuild)
    t.Run("RebuildListFailureLeavesInvalidManifest", testManifestRebuildFailure)
}
```

Each failure test must construct a fresh reader with `ManifestEnabled = true`, collect files, and prove LIST fallback returns the authoritative surviving objects. The restart test must construct a fresh writer over the same store, perform another mutation, and assert a complete valid manifest is published.

- [ ] **Step 2: Write failing cleanup retry tests**

```go
func TestReplicaClient_ManifestCleanupRetriesBeforeMutation(t *testing.T) {
    store := newManifestTestStore(t)
    store.putManifest(t, "db", manifestWithFile(0, 1, 1))
    writer := store.newClient("db")
    writer.ManifestConfigured = true
    store.failNext("DeleteObject", errors.New("cleanup unavailable"))

    _, err := writer.WriteLTXFile(context.Background(), 0, 2, 2, bytes.NewReader(mustLTX(t, 2, 2)))
    if err == nil {
        t.Fatal("expected cleanup error")
    }
    if store.hasLTX("db", 0, 2, 2) {
        t.Fatal("LTX mutation occurred before stale manifest cleanup")
    }

    if _, err := writer.WriteLTXFile(context.Background(), 0, 2, 2, bytes.NewReader(mustLTX(t, 2, 2))); err != nil {
        t.Fatal(err)
    }
    if !store.hasLTX("db", 0, 2, 2) {
        t.Fatal("expected retried mutation to succeed")
    }
    if got := store.operationCount("DeleteObject"); got != 2 {
        t.Fatalf("cleanup attempts=%d, want 2", got)
    }
}
```

- [ ] **Step 3: Verify the tests fail before delete-path changes**

Run: `go test ./s3 -run 'TestReplicaClient_Manifest(MutationFailures|CleanupRetriesBeforeMutation)$' -count=1`

Expected: FAIL because current deletes mutate objects before invalidation and cleanup suppresses retries.

- [ ] **Step 4: Apply the lifecycle to `DeleteLTXFiles`**

Lock and call `prepareManifestMutation(ctx, true)` before the first `DeleteObjects` call. On any request or per-object failure, clear `c.manifest` and return while the sentinel remains. On complete success, remove files from the in-memory manifest and publish only when `manifestReady`.

- [ ] **Step 5: Apply the lifecycle to `DeleteAll`**

Call `prepareManifestMutation(ctx, false)` before listing or deleting objects. On any failure, clear `c.manifest`; the sentinel or missing manifest keeps readers on LIST. On success, clear `c.manifest` and do not publish an empty manifest because `DeleteAll` deletes the sentinel with every other object.

- [ ] **Step 6: Remove `cleanupStaleManifest` and use strict pre-mutation cleanup**

Delete the best-effort post-mutation cleanup function. `prepareManifestMutation` must call `deleteManifest` before mutation, set `manifestCleanupAttempted` only after success/not-found, and return errors to the caller.

- [ ] **Step 7: Run failure, restart, cleanup, and full S3 race tests**

Run: `go test -race ./s3 -count=1`

Expected: PASS.

- [ ] **Step 8: Commit deletion and cleanup safety**

```bash
git add s3/replica_client.go s3/manifest_consistency_test.go
git commit -m "fix(s3): guard manifest deletion state"
```

---

### Task 4: Restore API compatibility, logger consistency, and test encapsulation

**Files:**
- Modify: `replica_client.go:18-56`
- Modify: `cmd/litestream/restore.go:300-360`
- Modify: `cmd/litestream/restore_test.go`
- Modify: `s3/replica_client.go:645-655,1294-1377`
- Modify: `s3/manifest.go:91-121`
- Modify: `s3/manifest_test.go`
- Modify: backend and test client files listed in File Structure.

**Interfaces:**
- Produces in `cmd/litestream/restore.go`: private `manifestEnabler` interface and `enableManifest(client litestream.ReplicaClient)` helper.
- Removes `SetManifestEnabled` from public `litestream.ReplicaClient`.

- [ ] **Step 1: Write capability and logger tests**

Add a private restore helper test using one fake that implements `SetManifestEnabled` and another that implements only `ReplicaClient`:

```go
type testManifestEnabler struct {
    litestream.ReplicaClient
    enabled bool
}

func (c *testManifestEnabler) SetManifestEnabled(enabled bool) { c.enabled = enabled }

func TestEnableManifest(t *testing.T) {
    capable := &testManifestEnabler{}
    enableManifest(capable)
    if !capable.enabled {
        t.Fatal("manifest capability was not enabled")
    }

    enableManifest(&nonManifestReplicaClient{})
}
```

Add an S3 logger test that injects a debug-enabled text handler, forces manifest GET failure, calls `LTXFiles`, and asserts the buffer contains both the `s3` logger group and the fallback message.

- [ ] **Step 2: Run tests and verify failures**

Run: `go test ./cmd/litestream ./s3 -run 'TestEnableManifest|TestReplicaClient_ManifestFallbackUsesClientLogger' -count=1`

Expected: FAIL because restore calls the public method directly and manifest logs use package-level `slog.Debug`.

- [ ] **Step 3: Replace the public method with a private optional capability**

```go
type manifestEnabler interface {
    SetManifestEnabled(bool)
}

func enableManifest(client litestream.ReplicaClient) {
    if client, ok := client.(manifestEnabler); ok {
        client.SetManifestEnabled(true)
    }
}
```

Call `enableManifest(r.Client)` and `enableManifest(db.Replica.Client)` from restore. Remove `SetManifestEnabled` from `ReplicaClient`, delete all unrelated backend/test no-op methods, and keep the real S3 implementation.

- [ ] **Step 4: Route every manifest diagnostic through `c.logger`**

Replace the six manifest `slog.Debug` calls with `c.logger.Debug`, preserving message text and structured error attributes. Retain package-level `slog` only where unrelated global environment parsing already requires it.

- [ ] **Step 5: Remove the production-only test API**

Change `s3/manifest_test.go` to `package s3`, remove the self-import, replace `s3.` prefixes, call `newManifestIterator` directly, and delete `NewManifestIteratorForTest` from `s3/manifest.go`.

- [ ] **Step 6: Run focused and full compilation tests**

Run: `go test -race ./cmd/litestream ./s3 -count=1`

Run: `go test ./...`

Expected: PASS, proving all built-in and test clients still implement `ReplicaClient` without the S3-specific method.

- [ ] **Step 7: Commit compatibility and style fixes**

```bash
git add replica_client.go cmd/litestream/restore.go cmd/litestream/restore_test.go s3/replica_client.go s3/manifest.go s3/manifest_test.go abs/replica_client.go file/replica_client.go gs/replica_client.go mock/replica_client.go nats/replica_client.go oss/replica_client.go sftp/replica_client.go webdav/replica_client.go db_internal_test.go replica_internal_test.go store_compaction_remote_test.go
git commit -m "refactor(s3): isolate manifest capability"
```

---

### Task 5: Update manifest documentation

**Files:**
- Modify: `docs/PATTERNS.md:359-429`
- Modify: `docs/REPLICA_CLIENT_GUIDE.md:16-47`

**Interfaces:**
- Documents the behavior implemented in Tasks 2-4; produces no code interface.

- [ ] **Step 1: Replace best-effort stale-manifest language with the sentinel invariant**

Document these exact rules:

- Writers always use LIST for authoritative state.
- Before any LTX mutation, a manifest-enabled writer publishes an unsupported-version sentinel.
- Readers reject that version and fall back to LIST.
- Failure to publish the sentinel aborts the LTX mutation.
- Failure to publish the final valid manifest leaves the sentinel and does not fail an already-successful LTX mutation.
- Missing writer cache is rebuilt directly from LIST across levels `0..SnapshotLevel`.
- Disabling manifests removes stale state before mutation and retries cleanup errors.

- [ ] **Step 2: Restore the backend-neutral interface guide**

Remove `SetManifestEnabled` from the documented `ReplicaClient` interface and explain that restore detects manifest support through an optional capability implemented by S3.

- [ ] **Step 3: Check documentation formatting and consistency**

Run: `git diff --check`

Run: `rg -n 'SetManifestEnabled|best-effort|manifest failure' docs/PATTERNS.md docs/REPLICA_CLIENT_GUIDE.md`

Expected: references describe only the optional S3 capability and safe sentinel behavior.

- [ ] **Step 4: Commit documentation updates**

```bash
git add docs/PATTERNS.md docs/REPLICA_CLIENT_GUIDE.md
git commit -m "docs(s3): document manifest invalidation"
```

---

### Task 6: Final verification, review, and push

**Files:**
- Verify all changed files against `origin/main`.
- No new implementation files unless verification exposes a defect.

**Interfaces:**
- Produces a clean, pushed PR branch with green CI.

- [ ] **Step 1: Run formatting and whitespace checks**

```bash
git diff --name-only --diff-filter=ACMR -z origin/main...HEAD -- '*.go' | xargs -0 gofmt -l
git diff --check origin/main...HEAD
```

Expected: no output.

- [ ] **Step 2: Run focused race tests**

Run: `go test -race ./s3 ./cmd/litestream -count=1`

Expected: PASS.

- [ ] **Step 3: Run the full suite and repository checks**

Run: `go test ./...`

Run: `pre-commit run --all-files`

Expected: PASS.

- [ ] **Step 4: Review the complete PR diff**

Use `gh pr diff 1173` and verify every prior finding is closed: populated-replica bootstrap, crash/partial-failure fallback, cleanup retry, interface compatibility, client logger, test-only API removal, and coverage for each failure path.

- [ ] **Step 5: Push to origin**

```bash
git push origin issue-1172-reduce-s3-list-operations-by-introducing-manifest-files-for
```

Use `--force-with-lease` only if an additional rebase or other history rewrite occurs during implementation.

- [ ] **Step 6: Watch CI through completion**

Run: `gh pr checks 1173 --watch`

If checks have not registered, retry after ten seconds up to three times. Investigate and fix failures before reporting completion.

- [ ] **Step 7: Verify local and remote state**

```bash
git status --short --branch
printf 'local  %s\nremote %s\n' "$(git rev-parse HEAD)" "$(git rev-parse origin/issue-1172-reduce-s3-list-operations-by-introducing-manifest-files-for)"
```

Expected: clean status and identical hashes.
