# Contributing to Litestream

Thank you for your interest in contributing to Litestream! We value community contributions and appreciate your help in making Litestream better.

## Types of Contributions We Accept

### ✅ We Encourage and Accept

- **Bug fixes and patches**: If you've found a bug and have a fix, we welcome your contribution
- **Security vulnerability reports**: Please report security issues responsibly (see Security section below)
- **Documentation improvements**: Help make our docs clearer and more comprehensive
- **Testing and feedback**: Report issues, test new features, and provide feedback
- **Small code improvements**: Performance optimizations, code cleanup, and minor enhancements

### ⚠️ Discuss First

- **Feature requests**: Please open an issue to discuss new features before implementing them
- **Large changes**: For significant modifications, please discuss your approach in an issue first

### ❌ Generally Not Accepted

- **Large external feature contributions**: Features carry a long-term maintenance burden. To reduce burnout and maintain code quality, we typically implement major features internally. This allows us to ensure consistency with the overall architecture and maintain the high reliability that Litestream users depend on for disaster recovery
- **Breaking changes**: Changes that break backward compatibility require extensive discussion

## How to Contribute

### Reporting Bugs

Before reporting a bug:

1. Check the [existing issues](https://github.com/benbjohnson/litestream/issues) to avoid duplicates
2. Verify you're using the latest version of Litestream
3. Gather diagnostic information (OS, version, configuration, error messages)

When reporting a bug, please use our issue template and include:

- Your operating system and version
- Litestream version (`litestream version`)
- Relevant configuration (sanitized of sensitive data)
- Steps to reproduce the issue
- Expected vs actual behavior
- Any error messages or logs

### Submitting Pull Requests

1. **Fork the repository** and create a new branch from `main`
2. **Make your changes** following our code style (see Development section)
3. **Add or update tests** as appropriate
4. **Update documentation** if you're changing behavior
5. **Run tests and linters** locally:

   ```bash
   go test -v ./...
   go vet ./...
   go fmt ./...
   goimports -local github.com/benbjohnson/litestream -w .
   pre-commit run --all-files
   ```

6. **Submit a pull request** with a clear description of your changes

### Pull Request Guidelines

Your PR should:

- Have a clear, descriptive title
- Reference any related issues (e.g., "Fixes #123")
- Include tests for bug fixes and new features
- Pass all CI checks
- Have a focused scope (one bug fix or feature per PR)

## Development Setup

### Prerequisites

- Go 1.24 or later
- CGO enabled (for SQLite integration)
- Git
- Pre-commit (optional but recommended): `pip install pre-commit`

### Building from Source

```bash
# Clone the repository
git clone https://github.com/benbjohnson/litestream.git
cd litestream

# Build the binary
go build ./cmd/litestream

# Run tests
go test -v ./...

# Install pre-commit hooks (recommended)
pre-commit install
```

### Code Style

- Follow standard Go conventions
- Use `gofmt` and `goimports` for formatting
- Run `go vet` and `staticcheck` for static analysis
- Keep functions focused and well-documented
- Add comments for exported types and functions

### Testing

- Write unit tests for new functionality
- Ensure existing tests pass before submitting PRs
- Integration tests require specific environment setup (see test files for details)

#### Testing the 1GB Lock Page Edge Case

SQLite reserves a special "lock page" at exactly 1GB (1,073,741,824 bytes)
that cannot be written to due to file locking mechanisms, particularly on
Windows. Litestream must skip this page during replication.

**Why Test Databases >1GB?**

- Ensures lock page skipping logic works correctly
- Verifies replication doesn't break when crossing the 1GB boundary
- Tests that restoration properly handles databases with lock pages
- Validates LTX format handling of non-data pages

##### Lock Page Calculation

The lock page number varies by database page size:

- 4KB pages: page 262145 (most common, ~99% of databases)
- 8KB pages: page 131073
- 16KB pages: page 65537
- 32KB pages: page 32769

Formula: `LockPgno = (0x40000000 / pageSize) + 1`

##### Testing Approach

1. Create test databases larger than 1GB:

```bash
# Create a test database with specific page size
sqlite3 test.db "PRAGMA page_size=4096; CREATE TABLE t(data BLOB);"

# Fill database to exceed 1GB (adjust iterations as needed)
for i in {1..300000}; do
  sqlite3 test.db "INSERT INTO t VALUES(randomblob(4000));"
done

# Verify size and page count
sqlite3 test.db "PRAGMA page_count; PRAGMA page_size;"
```

1. Test with different page sizes:

```bash
# Test with 8KB pages
sqlite3 test_8k.db "PRAGMA page_size=8192; CREATE TABLE t(data BLOB);"

# Test with 16KB pages
sqlite3 test_16k.db "PRAGMA page_size=16384; CREATE TABLE t(data BLOB);"
```

1. Verify lock page handling in replication:

```bash
# Configure litestream for the test database
litestream replicate test.db s3://mybucket/test.db

# Restore and verify
litestream restore -o restored.db s3://mybucket/test.db

# Check that lock page region is handled correctly
sqlite3 restored.db "PRAGMA integrity_check;"
```

##### Automated Testing Considerations

- Use sparse files where filesystem supports it to avoid disk space issues
- Consider using in-memory databases with appropriate size limits for CI
- Test both snapshot and incremental WAL replication across 1GB boundary
- Verify page maps correctly skip the lock page

## Security

If you discover a security vulnerability, please:

1. **DO NOT** open a public issue
2. Email the maintainers directly with details
3. Allow time for the issue to be addressed before public disclosure

## Code of Conduct

We expect all contributors to:

- Be respectful and inclusive
- Welcome newcomers and help them get started
- Focus on constructive criticism
- Respect differing viewpoints and experiences

## Getting Help

- **Documentation**: [litestream.io](https://litestream.io)
- **Issues**: [GitHub Issues](https://github.com/benbjohnson/litestream/issues)
- **Discussions**: [GitHub Discussions](https://github.com/benbjohnson/litestream/discussions)

## License

By contributing to Litestream, you agree that your contributions will be licensed under the Apache License 2.0, the same as the project.

## Acknowledgments

Thank you to all our contributors! Your efforts help make Litestream a reliable disaster recovery tool for the SQLite community.
