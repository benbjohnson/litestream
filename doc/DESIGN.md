Litestream Design
=================

Litestream provides a file system layer to intercept writes to a SQLite database
to construct a persistent write-ahead log that can be replicated.


## File Layout

### Local

```
dir/
	db                               # SQLite database
	db-wal                           # SQLite WAL
	db.litestream                    # per-db configuration
	.db-litestream/
		log                          # recent event log
		stat                         # per-db Prometheus statistics
		snapshot                     # stores snapshot number (e.g. 0000000000000001)
		wal/                         # each WAL file contains pages in flush interval
			active                   # active WAL file exists until flush; renamed
			0000000000000001.wal.gz  # flushed, compressed WAL files
			0000000000000002.wal.gz
```

### Remote (S3)

```
bkt/
	db/                                                # database path
		0000000000000001/                              # snapshot directory
			snapshot                                   # full db snapshot
			0000000000000001.wal.gz                    # compressed WAL file
			0000000000000002.wal.gz
		0000000000000002/
			snapshot
			0000000000000001-0000000000000003.tar.gz
```


## Process

### File System Startup

File system startup:

1. Load litestream.config file.
2. Load all per-db ".litestream" files.


### DB startup:

```
IF "db" NOT EXISTS {
	ensureWALRemovedIfDBNotExist()
	restore()
	setDBStatus("ok")
	return
} 

IF "-wal" EXISTS {
	syncToShadowWAL()
	IF err {
		setDBStatus("error")
	} ELSE {
		setDBStatus("ok")
	}
} ELSE {
	ensureShadowWALMatchesDB() // check last page written to DB
	IF err {
		setDBStatus("error")
	} ELSE {
		setDBStatus("ok")
	}
}
```


### DB Recovery

TODO


### WAL Write

1. Write to regular WAL
2. On fsync to regular WAL, copy WAL to shadow WAL.
2a. On copy error, mark errored & begin recovery


