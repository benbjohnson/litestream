# Migration to modernc.org/sqlite

## Summary

Successfully migrated the main Litestream binary from `github.com/mattn/go-sqlite3` to `modernc.org/sqlite` to enable CGO-free builds and automatic cross-platform compilation. The experimental VFS feature remains CGO-dependent for compatibility.

## Changes Made

### 1. Core SQLite Driver Migration

- Replaced imports of `github.com/mattn/go-sqlite3` with `modernc.org/sqlite` in main codebase
- Updated database connection strings from `"sqlite3"` to `"sqlite"`
- Removed custom driver registration with ConnectHook (no longer needed)
- PERSIST_WAL functionality is maintained through connection management

### 2. Build Tag Support for VFS

- Added build tags to conditionally compile VFS support (requires CGO)
- Created `vfs_stub.go` for CGO-free builds
- VFS functionality (`cmd/litestream-vfs`) intentionally remains with CGO and mattn/go-sqlite3

### 3. Files Modified

- `litestream.go` - Removed custom driver registration, updated imports
- `db.go` - Updated driver name from `"litestream-sqlite3"` to `"sqlite"`
- `cmd/litestream/main.go` - Updated imports
- `litestream_test.go` - Updated imports
- `internal/testingutil/testingutil.go` - Updated driver name
- `db_test.go` - Fixed WAL file removal test to handle non-existent files
- `vfs.go` - Added build tags for CGO support
- `vfs_stub.go` - Created stub implementation for CGO-free builds

### 4. Dual SQLite Driver Support

The project now uses two SQLite drivers for different purposes:

- **Main `litestream` binary**: Uses `modernc.org/sqlite` (CGO-free)
- **VFS `litestream-vfs` binary**: Uses `github.com/mattn/go-sqlite3` (requires CGO)

This separation is intentional:

- VFS is an experimental feature primarily for Fly.io projects
- VFS may require write support in the future
- Main binary benefits from CGO-free builds for easier distribution

## Benefits

1. **CGO-Free Builds**: Main litestream binary can now be built without CGO
2. **Cross-Platform Compilation**: Easy cross-compilation for multiple platforms:
   - Linux (amd64, arm64)
   - macOS (amd64, arm64)
   - Windows (amd64)
3. **Signed macOS Releases**: Enables automatic signing of Silicon Mac releases
4. **Simplified Build Process**: No need for cross-compilation toolchains for main binary
5. **Backward Compatibility**: All existing functionality preserved

## Testing

- Core tests pass successfully
- Successfully built binaries for multiple platforms without CGO:
  - Linux AMD64: ✅
  - Linux ARM64: ✅
  - macOS ARM64: ✅
  - Windows AMD64: ✅
- VFS functionality remains available with CGO builds

## Build Commands

### Main Binary - CGO-Free (Recommended)

```bash
CGO_ENABLED=0 go build ./cmd/litestream
```

### Cross-Platform Builds

```bash
# Linux AMD64
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/litestream

# macOS ARM64 (Silicon)
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./cmd/litestream

# Windows AMD64
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/litestream
```

### VFS Binary - Requires CGO (Experimental Feature)

```bash
CGO_ENABLED=1 go build ./cmd/litestream-vfs
```

## Notes

- The VFS feature is experimental and primarily for Fly.io projects
- Both SQLite drivers coexist without conflicts
- PERSIST_WAL is handled through connection management rather than explicit file control
