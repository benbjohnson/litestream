# CLAUDE.md - Claude Code Configuration

Claude-specific optimizations for Litestream. See [AGENTS.md](AGENTS.md) for project documentation.

## Context Window

With Claude's large context window, load documentation as needed:

- Start with [AGENTS.md](AGENTS.md) for overview and checklist
- Load [docs/PATTERNS.md](docs/PATTERNS.md) when writing code
- Load [docs/SQLITE_INTERNALS.md](docs/SQLITE_INTERNALS.md) for WAL/page work
- Load [docs/PROVIDER_COMPATIBILITY.md](docs/PROVIDER_COMPATIBILITY.md) for storage backend configs

## Claude-Specific Resources

### Specialized Agents (.claude/agents/)

- `sqlite-expert.md` - SQLite WAL and page management
- `replica-client-developer.md` - Storage backend implementation
- `ltx-compaction-specialist.md` - LTX format and compaction
- `test-engineer.md` - Testing strategies
- `performance-optimizer.md` - Performance optimization

### Commands (.claude/commands/)

- `/analyze-ltx` - Analyze LTX file structure
- `/debug-ipc` - Debug IPC Unix socket issues
- `/debug-wal` - Debug WAL replication issues
- `/test-compaction` - Test compaction scenarios
- `/trace-replication` - Trace replication flow
- `/validate-replica` - Validate replica client
- `/add-storage-backend` - Create new storage backend
- `/fix-common-issues` - Diagnose common problems
- `/run-comprehensive-tests` - Execute full test suite

## Quick Commands

```bash
go build -o bin/litestream ./cmd/litestream
go test -race -v ./...
pre-commit run --all-files
```
