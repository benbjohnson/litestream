litestream
==========

Streaming replication for SQLite.


## Questions

- How to avoid WAL checkpointing on close?


## Notes

```sql
-- Disable autocheckpointing.
PRAGMA wal_autocheckpoint = 0
```