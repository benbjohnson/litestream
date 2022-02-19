WAL Writer Static
=================

This test is to ensure that WALWriter will generate the same WAL file as
the `sqlite3` command line.

To reproduce the data file:

```sh
$ sqlite3 db

sqlite> PRAGMA journal_mode = 'wal';

sqlite> CREATE TABLE t (x);

sqlite> INSERT INTO t (x) VALUES (1);

sqlite> CTRL-\
```

then remove the db & shm files:

```sh
$ rm db db-shm
```

