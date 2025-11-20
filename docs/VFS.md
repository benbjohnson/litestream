# Litestream VFS

Litestream ships a read-only SQLite VFS for querying replicas directly from object storage. When the VFS is loaded as an extension (`dist/litestream-vfs.so`), it registers helper SQL functions that let you jump between points in time without reopening the database.

## Time travel SQL functions

- `litestream_set_time(timestamp TEXT)`: rebuilds the page index for a specific RFC3339/RFC3339Nano timestamp.
- `litestream_current_time()`: returns the active target time or `latest` when no override is set.
- `litestream_reset_time()`: clears the target time and returns to the latest available state.

The time override is scoped to the current SQLite connection.

### Example

```sql
.load ./dist/litestream-vfs.so
SELECT litestream_set_time('2024-01-01T12:00:00Z');
SELECT * FROM table_at_that_time;
SELECT litestream_current_time();
SELECT litestream_reset_time();
```
