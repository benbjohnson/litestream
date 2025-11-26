# CLAUDE.md - Claude Code Optimizations for Litestream

This file is automatically loaded by Claude Code and provides Claude-specific optimizations. For comprehensive project documentation, see AGENTS.md.

## Claude-Specific Optimizations

**Primary Documentation**: See AGENTS.md for comprehensive architectural guidance, patterns, and anti-patterns.

### Context Window Advantages

With Claude's 200k token context window, you can load the entire documentation suite:
- Full AGENTS.md for patterns and anti-patterns
- All docs/ subdirectory files for deep technical understanding
- Multiple source files simultaneously for cross-referencing

### Key Focus Areas for Claude

1. **Architectural Reasoning**: Leverage deep understanding of DB vs Replica layer boundaries
2. **Complex Analysis**: Use full context for multi-file refactoring
3. **SQLite Internals**: Reference docs/SQLITE_INTERNALS.md for WAL format details
4. **LTX Format**: Reference docs/LTX_FORMAT.md for replication specifics

### Claude-Specific Resources

#### Specialized Agents (.claude/agents/)

- **sqlite-expert.md**: SQLite WAL and page management expertise
- **replica-client-developer.md**: Storage backend implementation
- **ltx-compaction-specialist.md**: LTX format and compaction
- **test-engineer.md**: Comprehensive testing strategies
- **performance-optimizer.md**: Performance and resource optimization

#### Commands (.claude/commands/)

- `/analyze-ltx`: Analyze LTX file structure and contents
- `/debug-wal`: Debug WAL replication issues
- `/test-compaction`: Test compaction scenarios
- `/trace-replication`: Trace replication flow
- `/validate-replica`: Validate replica client implementation
- `/add-storage-backend`: Create new storage backend
- `/fix-common-issues`: Diagnose and fix common problems
- `/run-comprehensive-tests`: Execute full test suite

Use these commands with: `<command> [arguments]` in Claude Code.

## Overview

Litestream is a standalone disaster recovery tool for SQLite that runs as a background process and safely replicates changes incrementally to another file or S3. It works through the SQLite API to prevent database corruption.

## Build and Development Commands

### Building

```bash
# Build the main binary
go build ./cmd/litestream

# Install the binary
go install ./cmd/litestream

# Build for specific platforms (using Makefile)
make docker               # Build Docker image
make dist-linux          # Build Linux AMD64 distribution
make dist-linux-arm      # Build Linux ARM distribution
make dist-linux-arm64    # Build Linux ARM64 distribution
make dist-macos          # Build macOS distribution (requires LITESTREAM_VERSION env var)
```

### Testing

```bash
# Run all tests
go test -v ./...

# Run tests with coverage
go test -v -cover ./...

# Test VFS functionality (requires CGO and explicit vfs build tag)
go test -tags vfs ./cmd/litestream-vfs -v

# Test builds before committing (always use -o bin/ to avoid committing binaries)
go build -o bin/litestream ./cmd/litestream           # Test main build (no CGO required)
CGO_ENABLED=1 go build -tags vfs -o bin/litestream-vfs ./cmd/litestream-vfs  # Test VFS with CGO

# Run specific integration tests (requires environment setup)
go test -v ./replica_client_test.go -integration s3
go test -v ./replica_client_test.go -integration gcs
go test -v ./replica_client_test.go -integration abs
go test -v ./replica_client_test.go -integration oss
go test -v ./replica_client_test.go -integration sftp
```

### Code Quality

```bash
# Format code
go fmt ./...
goimports -local github.com/benbjohnson/litestream -w .

# Run linters
go vet ./...
staticcheck ./...

# Run pre-commit hooks (includes trailing whitespace, goimports, go-vet, staticcheck)
pre-commit run --all-files
```

## Architecture

### Core Components

**DB (`db.go`)**: Manages a SQLite database instance with WAL monitoring, checkpoint management, and metrics. Handles replication coordination and maintains long-running read transactions for consistency.

**Replica (`replica.go`)**: Connects a database to replication destinations via ReplicaClient interface. Manages periodic synchronization and maintains replication position.

**ReplicaClient Interface** (`replica_client.go`): Abstraction for different storage backends (S3, GCS, Azure Blob Storage, OSS, SFTP, file system, NATS). Each implementation handles snapshot/WAL segment upload and restoration. The `LTXFiles` method includes a `useMetadata` parameter: when true, it fetches accurate timestamps from backend metadata (required for point-in-time restores); when false, it uses fast timestamps for normal operations. During compaction, the system preserves the earliest CreatedAt timestamp from source files to maintain temporal granularity for restoration.

**WAL Processing**: The system monitors SQLite WAL files for changes, segments them into LTX format files, and replicates these segments to configured destinations. Uses SQLite checksums for integrity verification.

### Storage Backends

- **S3** (`s3/replica_client.go`): AWS S3 and compatible storage
- **GCS** (`gs/replica_client.go`): Google Cloud Storage
- **ABS** (`abs/replica_client.go`): Azure Blob Storage
- **OSS** (`oss/replica_client.go`): Alibaba Cloud Object Storage Service
- **SFTP** (`sftp/replica_client.go`): SSH File Transfer Protocol
- **File** (`file/replica_client.go`): Local file system replication
- **NATS** (`nats/replica_client.go`): NATS JetStream object storage

### Command Structure

Main entry point (`cmd/litestream/main.go`) provides subcommands:

- `replicate`: Primary replication daemon mode
- `restore`: Restore database from replica
- `databases`: List configured databases
- `ltx`: WAL/LTX file utilities (renamed from 'wal')
- `version`: Display version information
- `mcp`: Model Context Protocol support

## Key Design Patterns

1. **Non-invasive monitoring**: Uses SQLite API exclusively, no direct file manipulation
2. **Incremental replication**: Segments WAL into small chunks for efficient transfer
3. **Single remote authority**: Each database replicates to exactly one destination
4. **Age encryption**: Optional end-to-end encryption using age identities/recipients
5. **Prometheus metrics**: Built-in observability for monitoring replication health
6. **Timestamp preservation**: Compaction preserves earliest CreatedAt timestamp from source files to maintain temporal granularity for point-in-time restoration

## Configuration

Primary configuration via YAML file (`etc/litestream.yml`) or environment variables. Supports:

- Database paths and replica destinations
- Sync intervals and checkpoint settings
- Authentication credentials for cloud storage
- Encryption keys for age encryption

## Important Notes

- External contributions accepted for bug fixes only (not features)
- Uses pre-commit hooks for code quality enforcement
- Requires Go 1.24+ for build
- Main binary does NOT require CGO
- VFS functionality requires explicit `-tags vfs` build flag AND CGO_ENABLED=1
- **ALWAYS build binaries into `bin/` directory** which is gitignored (e.g., `go build -o bin/litestream`)
- Always test builds with different configurations before committing

## Workflows and Best Practices

- Any time you create/edit markdown files, lint and fix them with markdownlint

## Testing Considerations

### SQLite Lock Page at 1GB Boundary

Litestream handles a critical SQLite edge case: the lock page at exactly 1GB
(offset 0x40000000). This page is reserved by SQLite for file locking and
cannot contain data. The code skips this page during replication (see
db.go:951-953).

**Key Implementation Details:**

- Lock page calculation: `LockPgno = (0x40000000 / pageSize) + 1`
- Located in LTX library: `ltx.LockPgno(pageSize)`
- Must be skipped when iterating through database pages
- Affects databases larger than 1GB regardless of page size

**Testing Requirements:**

1. **Create databases >1GB** to ensure lock page handling works
2. **Test with various page sizes** as lock page number changes:
   - 4KB: page 262145 (default, most common)
   - 8KB: page 131073
   - 16KB: page 65537
   - 32KB: page 32769
3. **Verify replication** correctly skips the lock page
4. **Test restoration** to ensure databases restore properly across 1GB boundary

**Quick Test Script:**

```bash
# Create a >1GB test database
sqlite3 large.db <<EOF
PRAGMA page_size=4096;
CREATE TABLE test(data BLOB);
-- Insert enough data to exceed 1GB
WITH RECURSIVE generate_series(value) AS (
  SELECT 1 UNION ALL SELECT value+1 FROM generate_series LIMIT 300000
)
INSERT INTO test SELECT randomblob(4000) FROM generate_series;
EOF

# Verify it crosses the 1GB boundary
echo "File size: $(stat -f%z large.db 2>/dev/null || stat -c%s large.db)"
echo "Page count: $(sqlite3 large.db 'PRAGMA page_count')"
echo "Lock page should be at: $((0x40000000 / 4096 + 1))"

# Test replication
./bin/litestream replicate large.db file:///tmp/replica

# Test restoration
./bin/litestream restore -o restored.db file:///tmp/replica
sqlite3 restored.db "PRAGMA integrity_check;"
```
