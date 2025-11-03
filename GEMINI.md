# GEMINI.md - Gemini Code Assist Configuration for Litestream

This file provides Gemini-specific configuration and notes. For comprehensive project documentation, see AGENTS.md.

## Primary Documentation

**See AGENTS.md** for complete architectural guidance, patterns, and anti-patterns for working with Litestream.

## Gemini-Specific Configuration

### File Exclusions
Check `.aiexclude` file for patterns of files that should not be shared with Gemini (similar to `.gitignore`).

### Strengths for This Project

1. **Test Generation**: Excellent at creating comprehensive test suites
2. **Documentation**: Strong at generating and updating documentation
3. **Code Review**: Good at identifying potential issues and security concerns
4. **Local Codebase Awareness**: Enable for full repository understanding

## Key Project Concepts

### SQLite Lock Page
- Must skip page at 1GB boundary (0x40000000)
- Page number varies by page size (262145 for 4KB pages)
- See docs/SQLITE_INTERNALS.md for details

### LTX Format
- Immutable replication files
- Named by transaction ID ranges
- See docs/LTX_FORMAT.md for specification

### Architectural Boundaries
- DB layer (db.go): Database state and restoration
- Replica layer (replica.go): Replication only
- Storage layer: ReplicaClient implementations

## Testing Focus

When generating tests:
- Include >1GB database tests for lock page verification
- Add race condition tests with -race flag
- Test various page sizes (4KB, 8KB, 16KB, 32KB)
- Include eventual consistency scenarios

## Common Tasks

### Adding Storage Backend
1. Implement ReplicaClient interface
2. Follow existing patterns (s3/, gs/, abs/)
3. Handle eventual consistency
4. Generate comprehensive tests

### Refactoring
1. Respect layer boundaries (DB vs Replica)
2. Maintain current constraints (single replica authority, LTX-only restores)
3. Use atomic file operations
4. Return errors properly (don't just log)

## Build and Test Commands

```bash
# Build without CGO
go build -o bin/litestream ./cmd/litestream

# Test with race detection
go test -race -v ./...

# Test specific backend
go test -v ./replica_client_test.go -integration s3
```

## Configuration Reference

See `etc/litestream.yml` for configuration examples. Remember: each database replicates to exactly one remote destination.

## Additional Resources

- llms.txt: Quick navigation index
- docs/: Deep technical documentation
- .claude/commands/: Task-specific commands (if using with Claude Code)
