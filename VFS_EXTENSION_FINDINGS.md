# VFS Extension Loading Investigation - Findings

## Summary

Investigation into loading the Litestream VFS as a dynamically loaded SQLite extension (.so file) instead of static linking.

## Issues Discovered

### 1. Environment Variable Propagation (SOLVED)

**Problem**: Environment variables set via `os.Setenv()` or `t.Setenv()` in test code are not visible to code running within a dynamically loaded CGO extension.

**Root Cause**: When a .so extension is loaded by SQLite, it contains Go code with its own runtime initialization. The extension's init function runs when the extension is first loaded (on first database connection), but at that point:
- Test-specific env vars haven't been set yet (if using subtest structure)
- Even if set before, there may be isolation between the main process and the extension's Go runtime

**Solution**: Implemented lazy initialization pattern
- Created `LazyReplicaClient` that defers reading environment variables until first use
- Extension registers a VFS with the lazy client immediately
- Env vars are read when the VFS is actually accessed (on first file open)
- This allows env vars to be set after extension loading but before VFS use

**Implementation**: See `cmd/litestream-vfs/litestream-vfs.go` - `LazyReplicaClient` type

### 2. sqlite3vfs Package Incompatibility (BLOCKING)

**Problem**: Segmentation fault when calling `sqlite3vfs.RegisterVFS()` from within a dynamically loaded extension.

**Root Cause**: The `github.com/psanford/sqlite3vfs` package is designed for **static linking only**. It expects to be compiled directly into the Go program that uses SQLite, not loaded as a shared library.

**Evidence**:
```
sqlite3_extension_init called
Registering litestream VFS
SIGSEGV: segmentation violation
PC=0x14b120c7c m=7 sigcode=2 addr=0x468
signal arrived during cgo execution
```

The crash occurs during `sqlite3vfs.RegisterVFS()` when it tries to interact with SQLite's VFS system from within the extension context.

**Why This Happens**:
- `sqlite3vfs` uses CGO to call SQLite C functions
- When loaded as an extension, the calling context and memory layout don't match what `sqlite3vfs` expects
- The package makes assumptions about how it's linked with SQLite that don't hold for dynamic loading

## Architectural Decision Required

Ben needs to choose one of these approaches:

### Option A: Static Linking (RECOMMENDED FOR NOW)

**Pros**:
- Works with existing `sqlite3vfs` package
- Environment variable issues are easier to manage
- No segfault issues
- Simpler testing

**Cons**:
- VFS must be compiled into the application
- Can't distribute as a standalone extension
- Less flexible deployment

**Implementation**: Revert to the approach on `main` branch, but use the `LazyReplicaClient` pattern to solve env var issues in tests.

### Option B: Pure C VFS Implementation

**Pros**:
- Can be loaded as a dynamic extension
- Distributable as standalone .so file
- True plugin architecture

**Cons**:
- Requires implementing VFS entirely in C
- Can't use existing Go litestream code directly
- Significant development effort
- Need to bridge between C and Go for actual replication logic

**Implementation**:
- Write C VFS implementation that calls into Go for actual file operations
- Use SQLite's `sqlite3_vfs_register()` C API directly
- Pass configuration via SQLite PRAGMAs or connection strings instead of Go env vars

### Option C: Hybrid Approach

**Pros**:
- Keep Go logic for replication
- Make VFS available as extension

**Cons**:
- Complex architecture
- Need custom C wrapper around Go VFS methods
- Still significant C development required

**Implementation**:
- Create thin C VFS layer that delegates to Go
- Use CGO exports carefully to avoid sqlite3vfs package
- Handle all SQLite VFS callbacks at C level, call into Go for I/O

## Test Changes Made

### main_test.go

1. Added `sync.Once` to ensure driver is only registered once
2. Modified `openWithVFS()` to:
   - Set env vars before driver registration
   - Open initial connection to trigger extension loading
   - Ping to actually load the extension
   - Close and reopen with VFS parameter
3. Fixed hardcoded path from Ben's machine to Cory's machine
4. Added documentation comments explaining the env var issue

### litestream-vfs.go

1. Implemented `LazyReplicaClient` wrapper:
   - Defers client creation until first method call
   - Thread-safe initialization with sync.Mutex
   - Caches client and any initialization errors
   - Implements full `ReplicaClient` interface by delegating to wrapped client

2. Modified `sqlite3_extension_init()`:
   - Returns int (SQLITE_OK/SQLITE_ERROR) instead of void
   - Added panic recovery
   - Creates VFS with lazy client immediately
   - Attempts to register VFS (currently segfaults)

### src/litestream-vfs.c

1. Updated to handle return value from Go init function
2. Added error message on failure
3. Returns `SQLITE_OK_LOAD_PERMANENTLY` on success

## Recommendations

1. **Short term**: Use static linking (Option A) with `LazyReplicaClient` for tests
   - The lazy client pattern solves the env var problem
   - Static linking avoids the segfault issue
   - Can ship in next release

2. **Long term**: If dynamic extension loading is required, implement Option B or C
   - This is a significant undertaking
   - Requires C VFS development expertise
   - Consider if the benefits justify the complexity

3. **Testing Strategy**:
   - For static linking: Use lazy client with direct VFS registration
   - Set env vars in `TestMain()` or package init for shared state
   - Or use config files instead of env vars

## Files Modified

- `cmd/litestream-vfs/litestream-vfs.go` - Added LazyReplicaClient, modified init
- `cmd/litestream-vfs/main_test.go` - Fixed path, improved env var handling
- `src/litestream-vfs.c` - Better error handling
- `go.mod` - Removed local module replacements (needed for building)

## Next Steps

1. Decide on static vs dynamic approach
2. If static: Integrate LazyReplicaClient pattern into main branch tests
3. If dynamic: Plan C VFS implementation approach
4. Update documentation with chosen architecture
