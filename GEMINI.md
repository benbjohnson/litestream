# GEMINI.md - Gemini Code Assist Configuration

Gemini-specific configuration for Litestream. See [AGENTS.md](AGENTS.md) for project documentation.

## Before Contributing

1. Read [AI_PR_GUIDE.md](AI_PR_GUIDE.md) - PR quality requirements
2. Read [AGENTS.md](AGENTS.md) - Project overview and checklist
3. Check [CONTRIBUTING.md](CONTRIBUTING.md) - What we accept

## File Exclusions

Check `.aiexclude` for patterns of files that should not be shared with Gemini.

## Gemini Strengths for This Project

- **Test generation** - Creating comprehensive test suites
- **Documentation** - Generating and updating docs
- **Code review** - Identifying issues and security concerns
- **Local codebase awareness** - Enable for full repository understanding

## Documentation

Load as needed:

- [docs/PATTERNS.md](docs/PATTERNS.md) - Code patterns when writing code
- [docs/SQLITE_INTERNALS.md](docs/SQLITE_INTERNALS.md) - For WAL/page work
- [docs/TESTING_GUIDE.md](docs/TESTING_GUIDE.md) - For test generation

## Critical Rules

- **Lock page at 1GB** - Skip page at 0x40000000
- **LTX files are immutable** - Never modify after creation
- **Layer boundaries** - DB handles state, Replica handles replication

## Quick Commands

```bash
go build -o bin/litestream ./cmd/litestream
go test -race -v ./...
pre-commit run --all-files
```
