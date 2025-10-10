# Litestream v0.5.0 Changes and Migration Guide

This document summarizes the major changes in Litestream v0.5.0, based on Ben Johnson's announcement and the current implementation.

## Executive Summary

Litestream v0.5.0 is a **major rewrite** that introduces the LTX format, improves compaction, and removes CGO dependencies. It includes breaking changes that prevent restoration from v0.3.x backups.

## Breaking Changes

### 1. Single Replica Constraint
- **Before**: Multiple replica destinations per database
- **After**: Only ONE replica destination per database
- **Impact**: Simplified configuration but reduced redundancy options

### 2. Cannot Restore from v0.3.x
- **Before**: WAL segment-based backups
- **After**: LTX page-based backups
- **Impact**: Must maintain v0.3.x installation to restore old backups

### 3. Command Changes
- **Before**: `litestream wal` commands
- **After**: `litestream ltx` commands
- **Example**: `litestream ltx info file.ltx`

### 4. Generations Removed
- **Before**: Used "generations" to track database backups
- **After**: Simplified tracking with LTX files and TXID ranges
- **Impact**: Cleaner mental model, simpler implementation

## New Features

### 1. LTX Format
- **Purpose**: Efficient page-level replication format
- **Benefits**:
  - Immutable files with TXID ranges
  - Page-level deduplication during compaction
  - Indexed pages for fast random access
  - Point-in-time restoration

### 2. Multi-Level Compaction
- **Level 0**: Raw LTX files (no compaction)
- **Level 1**: 30-second windows
- **Level 2**: 5-minute windows
- **Level 3**: 1-hour windows
- **Snapshots**: Daily full database snapshots

### 3. NATS JetStream Support
- **New replica type**: `nats://`
- **Features**: Distributed messaging with persistence
- **Use case**: Event-driven architectures

### 4. Pure Go Implementation
- **Change**: Switched from CGO to `modernc.org/sqlite`
- **Benefits**:
  - Easier cross-compilation
  - No C dependencies
  - Simplified builds
  - Better portability

## Technical Improvements

### Performance
- **Compaction**: Limited only by I/O throughput
- **Page-level operations**: More efficient than WAL segments
- **Indexed access**: Fast page lookups in LTX files

### Architecture
- **Cleaner separation**: Storage backends more modular
- **Better abstractions**: LTX format decouples from SQLite WAL
- **Simplified state**: No generations to track

## Migration Path

### From v0.3.x to v0.5.0

1. **Before upgrading**:
   ```bash
   # Create final backup with v0.3.x
   litestream snapshot -replica [destination]
   ```

2. **Install v0.5.0**:
   ```bash
   # Download and install new version
   curl -L https://github.com/benbjohnson/litestream/releases/download/v0.5.0/litestream-v0.5.0-linux-amd64.tar.gz | tar xz
   ```

3. **Update configuration**:
   ```yaml
   # Old (v0.3.x) - Multiple replicas
   dbs:
     - path: /data/db.sqlite
       replicas:
         - url: s3://bucket1/db
         - url: s3://bucket2/backup

   # New (v0.5.0) - Single replica only
   dbs:
     - path: /data/db.sqlite
       replicas:
         - url: s3://bucket1/db
   ```

4. **Start fresh replication**:
   ```bash
   # Remove old WAL segments
   rm -rf /data/db.sqlite-litestream

   # Start v0.5.0
   litestream replicate
   ```

### Rollback Procedure

If you need to restore from v0.3.x backups:

1. **Keep v0.3.x binary**: Don't delete old version
2. **Use old binary for restoration**:
   ```bash
   litestream-v0.3.x restore -o restored.db s3://bucket/db
   ```
3. **Then upgrade**: Once restored, can use v0.5.0 going forward

## Future Roadmap

### Litestream VFS (In Development)
- **Purpose**: Enable read replicas without full downloads
- **How it works**:
  - Virtual File System layer
  - On-demand page fetching from S3
  - Background hydration
  - Local caching
- **Benefits**:
  - Instant database "copies"
  - Scales read operations
  - Reduces bandwidth costs

## Best Practices for v0.5.0

### 1. Compaction Configuration
```yaml
# Use default intervals for most workloads
levels:
  - level: 1
    interval: 30s
  - level: 2
    interval: 5m
  - level: 3
    interval: 1h
```

### 2. Single Replica Strategy
Since only one replica is allowed:
- Choose most reliable storage
- Consider using RAID/redundancy at storage level
- Implement external backup rotation if needed

### 3. Monitoring
- Watch compaction metrics
- Monitor LTX file counts at each level
- Track restoration time improvements

### 4. Testing
- Test restoration regularly
- Verify point-in-time recovery works
- Benchmark compaction performance

## Common Issues and Solutions

### Issue: "Cannot restore from old backup"
**Solution**: Use v0.3.x binary to restore, then replicate with v0.5.0

### Issue: "Multiple replicas not supported"
**Solution**: Use single most reliable destination, implement redundancy at storage layer

### Issue: "`wal` command not found"
**Solution**: Use `ltx` command instead

### Issue: "CGO_ENABLED required error"
**Solution**: Not needed in v0.5.0, ensure using latest binary

## Summary

Litestream v0.5.0 represents a significant evolution:
- **Simpler**: Single replica, no generations, pure Go
- **More efficient**: Page-level operations, better compaction
- **More flexible**: LTX format enables future features
- **Breaking changes**: Cannot restore old backups directly

The tradeoffs favor simplicity and efficiency over backward compatibility, positioning Litestream for future enhancements like the VFS read replica system.
