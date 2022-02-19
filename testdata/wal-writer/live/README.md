WAL Writer Live
=================

This test is to ensure we can copy a WAL file into place with a live DB and
trigger a checkpoint into the main DB file.

To reproduce the data files:

```sh
$ sqlite3 db

sqlite> PRAGMA journal_mode = 'wal';
sqlite> CREATE TABLE t (x);
sqlite> PRAGMA wal_checkpoint(TRUNCATE);
sqlite> INSERT INTO t (x) VALUES (1);

sqlite> CTRL-\
```

