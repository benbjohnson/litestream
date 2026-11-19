---
name: litestream
description: >-
  Expert knowledge for contributing to Litestream, a standalone disaster recovery
  tool for SQLite. Provides architectural understanding, code patterns, critical
  rules, and debugging procedures for WAL monitoring, LTX replication format,
  storage backend implementation, multi-level compaction, and SQLite page
  management. Use when working with Litestream source code, writing storage
  backends, debugging replication issues, implementing compaction logic, or
  handling SQLite WAL operations.
license: Apache-2.0
metadata:
  author: benbjohnson
  version: "1.0"
  repository: https://github.com/benbjohnson/litestream
---

# Litestream Agent Skill

Litestream is a standalone disaster recovery tool for SQLite. It runs as a
background process, monitors the SQLite WAL (Write-Ahead Log), converts changes
to immutable LTX files, and replicates them to cloud storage. The CLI and Go
library use `modernc.org/sqlite` and can build without CGO. Race tests,
`litestream-test`, and the tagged SQLite VFS tests require CGO and a C compiler.

To review a PR, commits, or local changes, use the `litestream-review` skill in
`.agents/skills/litestream-review/` of the Litestream repository.

## Quick Start

```bash
# Build
go build -o bin/litestream ./cmd/litestream

# Test (always use race detector)
go test -race -v ./...

# Code quality
pre-commit run --all-files
```

## Critical Rules

These invariants must never be violated:

### 1. Lock Page at 1GB

SQLite reserves a page at byte offset 0x40000000 (1 GB). Always skip it during
replication and compaction. The page number varies by page size:

| Page Size | Lock Page Number |
|-----------|------------------|
| 4 KB      | 262145           |
| 8 KB      | 131073           |
| 16 KB     | 65537            |
| 32 KB     | 32769            |

```go
lockPgno := ltx.LockPgno(uint32(db.pageSize))
if pgno == lockPgno {
    continue
}
```

### 2. LTX Files Are Immutable

Once an LTX file is written, it must never be modified. New changes create new
files. This guarantees point-in-time recovery integrity.

### 3. Single Replica per Database

Each database replicates to exactly one destination. The Replica component
manages replication mechanics; database state belongs in the DB layer.

### 4. Prefer Local Inputs During Compaction

`Compactor.Compact` in `compactor.go` reads a local copy of each source file
when the DB has one. If the local file is missing, it opens the remote file
through a resumable reader. Any other local open error is returned.

### 5. Preserve Timestamps During Compaction

A compacted file keeps the header timestamp of its final input in TXID order,
the state it represents. Storage clients preserve the timestamp from the LTX
header. Do not replace it with the upload time or the earliest source timestamp.

### 6. Use Lock() Not RLock() for Writes

```go
// CORRECT
r.mu.Lock()
defer r.mu.Unlock()
r.pos = pos

// WRONG - race condition
r.mu.RLock()
defer r.mu.RUnlock()
r.pos = pos
```

### 7. Publish Complete Files

File and SFTP replicas stage writes before renaming them to the final path. Object-store backends use their upload APIs. Preserve each backend's publication contract, check write and close errors, and remove incomplete staging files on failure. See `references/REPLICA_CLIENT_GUIDE.md`.

## Architecture

### System Layers

| Layer   | File(s)                  | Responsibility                            |
|---------|--------------------------|-------------------------------------------|
| App     | `cmd/litestream/`        | CLI commands, YAML/env config             |
| Store   | `store.go`               | Multi-DB coordination, compaction         |
| DB      | `db.go`                  | Single DB management, WAL monitoring      |
| Replica | `replica.go`             | Replication to one destination            |
| Storage | `*/replica_client.go`    | Backend implementations (S3, GCS, etc.)   |

Database state logic belongs in the DB layer, not the Replica layer.

### ReplicaClient Interface

All storage backends implement this interface from `replica_client.go`:

```go
type ReplicaClient interface {
    Type() string
    Init(ctx context.Context) error
    SetLogger(logger *slog.Logger)
    LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error)
    OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
    WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error)
    DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error
    DeleteAll(ctx context.Context) error
}
```

Key contract details:
- `OpenLTXFile` must return an error matching `os.ErrNotExist` when the file is missing
- `WriteLTXFile` must report the timestamp from the LTX header as `CreatedAt`
- `LTXFiles` with `useMetadata=true` fetches accurate timestamps (for PIT restore)
- `LTXFiles` with `useMetadata=false` uses fast timestamps (normal operations)

### Core Components

**DB** (`db.go`): Manages SQLite connection, WAL monitoring, checkpointing, and
long-running read transaction for consistency. Key fields: `path`, `db`, `rtx`
(read transaction), `pageSize`, `notify` channel.

**Replica** (`replica.go`): Tracks replication position (`ltx.Pos` with TXID and
post-apply checksum). One replica per database.

**Store** (`store.go`): Coordinates multiple databases and schedules compaction
across levels.

## LTX File Format

LTX (Log Transaction) files are immutable, checksummed archives of database
changes. Structure:

```
+------------------+
|     Header       |  100 bytes (magic "LTX1", page size, TXID range, timestamp)
+------------------+
|      Pages       |  6-byte page header + encoded page data (may be compressed)
+------------------+
|   Page Index     |  Page number to offset and size, with a length footer
+------------------+
|     Trailer      |  16 bytes (post-apply checksum, file checksum)
+------------------+
```

### Naming Convention

```
Format:  MMMMMMMMMMMMMMMM-NNNNNNNNNNNNNNNN.ltx
Example: 0000000000000001-0000000000000064.ltx  (TXID 1-100)
```

### Compaction Levels

The file backend stores levels under `ltx/<decimal level>/`, for example
`ltx/0/` and `ltx/9/`. The S3 backend uses four-digit hexadecimal prefixes
directly under its path, for example `0000/` and `0009/`.

Default compaction levels: L0 (raw), L1 (30s), L2 (5min), L3 (1h), plus L9
snapshots every 24 hours. The format authority is the `github.com/superfly/ltx`
version pinned in `go.mod`. Compaction merges files by deduplicating pages (latest version wins)
and always skips the lock page.

## Code Patterns

### DO

- Return errors immediately; let callers decide handling
- Use `fmt.Errorf("context: %w", err)` for error wrapping
- Handle database state in the DB layer, not Replica
- Use `DB.Snapshot(ctx)` to write a full L9 snapshot (don't reimplement)
- Test with race detector: `go test -race`
- Use lazy iterators for `LTXFiles` (paginate, don't load all at once)

### DON'T

- Write data at the 1 GB lock page boundary
- Modify LTX files after creation
- Put database state logic in the Replica layer
- Use `RLock()` when writing shared state
- Publish incomplete local or SFTP files at their final paths
- Ignore context cancellation in long operations
- Return generic errors instead of `os.ErrNotExist` for missing files

## Specialized Knowledge Areas

Load reference files on demand based on the task:

| Task                              | Reference File                          |
|-----------------------------------|-----------------------------------------|
| Understanding system design       | `references/ARCHITECTURE.md`            |
| Writing or reviewing code         | `references/PATTERNS.md`                |
| Working with LTX files            | `references/LTX_FORMAT.md`              |
| WAL monitoring or page operations | `references/SQLITE_INTERNALS.md`        |
| Implementing storage backends     | `references/REPLICA_CLIENT_GUIDE.md`    |
| Writing or debugging tests        | `references/TESTING_GUIDE.md`           |

## Common Debugging Procedures

### Replication Not Working

1. Verify WAL mode: `PRAGMA journal_mode` must return `wal`
2. Check monitor interval and that the monitor goroutine is running
3. Confirm `db.notify` channel is being signaled on WAL changes
4. Check replica position: `replica.Pos()` should advance with writes
5. Look for `os.ErrNotExist` from `OpenLTXFile` (file not replicated yet)

### Large Database Issues (>1 GB)

1. Verify lock page is being skipped: check `ltx.LockPgno(pageSize)`
2. Test with multiple page sizes (4K, 8K, 16K, 32K)
3. Run with databases both smaller and larger than 1 GB
4. Ensure page iteration loops include the `continue` guard for lock page

### Compaction Problems

1. Confirm the source TXID range is available locally or from the replica
2. Check that the header timestamp of the final input is preserved
3. Verify compaction level intervals in `Store.levels`
4. Check interrupted reads, missing source files, and the provider's documented listing and read consistency

### Storage Backend Issues

1. Return `os.ErrNotExist` for missing files (not generic errors)
2. Support partial reads via `offset`/`size` in `OpenLTXFile`
3. Handle context cancellation in all methods
4. Test concurrent operations with `-race` flag
5. Retry only errors that the backend contract makes retryable, with bounded backoff and caller cancellation

### Corrupted or Missing LTX Files

1. Check logs for `LTXError` messages - they include context (Op, Path, Level, TXID) and recovery hints
2. Common error messages: "nonsequential page numbers", "non-contiguous transaction files", "ltx validation failed"
3. Manual fix: `litestream reset <db-path>` clears local LTX state and forces fresh snapshot on next sync (database file is not modified)
4. Automatic fix: set `auto-recover: true` on the replica config to auto-reset on LTX errors (disabled by default)
5. Reference: `cmd/litestream/reset.go`, `replica.go` (auto-recover logic), `db.go` (`ResetLocalState`)

## Contribution Guidelines

### What's Accepted

- Bug fixes and patches (welcome)
- Documentation improvements
- Small code improvements and performance optimizations
- Security vulnerability reports (report privately)

### Discuss First

- Feature requests: open an issue before implementing
- Large changes: discuss approach in an issue first

### Pre-Submit Checklist

- [ ] Read relevant docs from the reference table above
- [ ] Follow patterns in `references/PATTERNS.md`
- [ ] Run `go test -race -v ./...`
- [ ] Run `pre-commit run --all-files`
- [ ] For page iteration: test with >1 GB databases
- [ ] Show investigation evidence in PR (see CONTRIBUTING.md)

## Testing

```bash
# Full test suite with race detection
go test -race -v ./...

# Specific areas
go test -race -v -run TestReplica_Sync ./...
go test -race -v -run TestDB_Sync ./...
go test -race -v -run TestStore_CompactDB ./...

# Coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

Key testing areas:
- Lock page handling with >1 GB databases and multiple page sizes
- Race conditions in position updates, WAL monitoring, and checkpointing
- Backend-specific listing, read, and deletion semantics
- Atomic file operations and cleanup on error paths

## Environment Validation

Run `scripts/validate-setup.sh` to verify your development environment is
correctly configured for Litestream development.
