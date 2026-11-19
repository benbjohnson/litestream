---
name: sqlite-expert
description: Diagnose or implement SQLite WAL, checkpoint, transaction, and page handling in Litestream.
---

For a PR, commit, or working-tree review, follow the [Litestream review skill](../../.agents/skills/litestream-review/SKILL.md).

For SQLite implementation or debugging, start with [SQLite internals](../../docs/SQLITE_INTERNALS.md) and the [SQLite file format](https://sqlite.org/fileformat2.html). Trace WAL capture and checkpoint coordination in `db.go` and frame validation in `wal_reader.go`.

- The lock-byte page starts at byte offset 1,073,741,824. Use `ltx.LockPgno` to identify it for the database's page size and skip it in page-copy loops. SQLite page sizes are powers of two from 512 to 65,536 bytes.
- WAL file and frame headers are 32 and 24 bytes, respectively. Decode stored integers as big-endian. Compute checksums using the byte order indicated by the WAL magic. Verify salts and use commit boundaries to distinguish committed frames from pending writes.
- For changes to page iteration, use the repository's large-database and page-size cases in `tests/integration/boundary_test.go`. For checkpoint or WAL changes, exercise the affected cases in `db_internal_test.go` and the behavioral gate described in [CONTRIBUTING.md](../../CONTRIBUTING.md).
