# Litestream VFS

Litestream ships a read-only SQLite VFS for querying replicas directly from object storage. When the VFS is loaded as an extension (`dist/litestream-vfs.so`), it provides PRAGMA commands that let you jump between points in time without reopening the database.

## Time travel PRAGMA commands

The `litestream_time` PRAGMA provides three operations for time-travel queries:

- `PRAGMA litestream_time = 'timestamp'`: Sets the view to a specific RFC3339/RFC3339Nano timestamp, rebuilding the page index to reflect the database state at that point in time.
- `PRAGMA litestream_time`: Returns the current target time as an RFC3339Nano timestamp, or `latest` when viewing the most recent state.
- `PRAGMA litestream_time = latest`: Resets to the latest available state, clearing any time override.

The time override is scoped to the current SQLite connection and persists across queries until changed or reset.

### Example

```sql
.load ./dist/litestream-vfs.so

-- Set view to a specific point in time
PRAGMA litestream_time = '2024-01-01T12:00:00Z';

-- Query data as it existed at that time
SELECT * FROM table_at_that_time;

-- Check current time setting
PRAGMA litestream_time;

-- Reset to latest state
PRAGMA litestream_time = latest;

-- Verify reset
PRAGMA litestream_time;  -- Returns: latest
```
