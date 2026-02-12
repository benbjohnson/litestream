# AGENTS.md - Litestream AI Agent Guide

Litestream is a disaster recovery tool for SQLite that runs as a background process, monitors the WAL, converts changes to immutable LTX files, and replicates them to cloud storage. It uses `modernc.org/sqlite` (pure Go, no CGO required).

## Before You Start

1. Read [AI_PR_GUIDE.md](AI_PR_GUIDE.md) for contribution requirements
2. Check [CONTRIBUTING.md](CONTRIBUTING.md) for what we accept (bug fixes welcome, features need discussion)
3. Review recent PRs for current patterns

## Critical Rules

- **Lock page at 1GB**: SQLite reserves page at 0x40000000. Always skip it. See [docs/SQLITE_INTERNALS.md](docs/SQLITE_INTERNALS.md)
- **LTX files are immutable**: Never modify after creation. See [docs/LTX_FORMAT.md](docs/LTX_FORMAT.md)
- **Single replica per database**: Each DB replicates to exactly one destination
- **Use `litestream ltx`**: Not `litestream wal` (deprecated)
- **Use `litestream reset`**: Clears corrupted local LTX state for a database. See `cmd/litestream/reset.go`
- **`auto-recover` config**: Replica option that automatically resets local state on LTX errors. Disabled by default. See `replica.go`
- **Retention enabled by default**: `Store.RetentionEnabled` is `true` by default. Disable only when cloud lifecycle policies handle cleanup. See `store.go`
- **IPC socket disabled by default**: Control socket is off by default. Enable with `socket.enabled: true` in config. See `server.go`
- **`$PID` config expansion**: Config files support `$PID` to expand to the current process ID, plus standard `$ENV_VAR` expansion. See `cmd/litestream/main.go`
- **`litestream ltx -level`**: Use `-level 0`–`9` or `-level all` to inspect specific compaction levels. See `cmd/litestream/ltx.go`

## Layer Boundaries

| Layer | File | Responsibility |
|-------|------|----------------|
| DB | `db.go` | Database state, restoration, WAL monitoring, library API (`SyncStatus`, `SyncAndWait`, `EnsureExists`) |
| Replica | `replica.go` | Replication mechanics only |
| Storage | `**/replica_client.go` | Backend implementations (includes `ReplicaClientV3` for v0.3.x restore) |
| IPC | `server.go` | Unix socket control API (register/unregister, /txid, pprof) |
| Leasing | `leaser.go`, `s3/leaser.go` | Distributed lease acquisition via conditional writes |

Database state logic belongs in DB layer, not Replica layer.

## Quick Reference

**Build:**

```bash
go build -o bin/litestream ./cmd/litestream
go test -race -v ./...
```

**Code quality:**

```bash
pre-commit run --all-files
```

## Documentation

| Document | When to Read |
|----------|--------------|
| [docs/PATTERNS.md](docs/PATTERNS.md) | Code patterns and anti-patterns |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Deep component details |
| [docs/SQLITE_INTERNALS.md](docs/SQLITE_INTERNALS.md) | WAL format, lock page |
| [docs/LTX_FORMAT.md](docs/LTX_FORMAT.md) | Replication format |
| [docs/TESTING_GUIDE.md](docs/TESTING_GUIDE.md) | Test strategies |
| [docs/REPLICA_CLIENT_GUIDE.md](docs/REPLICA_CLIENT_GUIDE.md) | Adding storage backends |
| [docs/PROVIDER_COMPATIBILITY.md](docs/PROVIDER_COMPATIBILITY.md) | Provider-specific S3/cloud configs |

## Checklist

Before submitting changes:

- [ ] Read relevant docs above
- [ ] Follow patterns in [docs/PATTERNS.md](docs/PATTERNS.md)
- [ ] Test with race detector (`go test -race`)
- [ ] Run `pre-commit run --all-files`
- [ ] For page iteration: test with >1GB databases
- [ ] Show investigation evidence in PR (see [AI_PR_GUIDE.md](AI_PR_GUIDE.md))
