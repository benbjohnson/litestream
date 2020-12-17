DESIGN
======

Litestream is a sidecar process that replicates the write ahead log (WAL) for
a SQLite database. To ensure that it can replicate every page, litestream takes
control over the checkpointing process by issuing a long running read
transaction against the database to prevent checkpointing. It then releases
this transaction once it obtains a write lock and issues the checkpoint itself.

The daemon polls the database on an interval to breifly obtain a write
transaction lock and copy over new WAL pages. Once the WAL has reached a
threshold size, litestream will issue a checkpoint and a single page write
to a table called `_litestream` to start the new WAL.


## Workflow

When litestream first loads a database, it checks if there is an existing
sidecar directory which is named `.<DB>-litestream`. If not, it initializes
the directory and starts a new generation.

A generation is a snapshot of the database followed by a continuous stream of
WAL files. A new generation is started on initialization & whenever litestream
cannot verify that it has a continuous record of WAL files. This could happen
if litestream is stopped and another process checkpoints the WAL. In this case,
a new generation ID is randomly created and a snapshot is replicated to the
appropriate destinations.

Generations also prevent two servers from replicating to the same destination
and corrupting each other's data. In this case, each server would replicate
to a different generation directory. On recovery, there will be duplicate 
databases and the end user can choose which generation to recover but each
database will be uncorrupted.


## File Layout

Litestream maintains a shadow WAL which is a historical record of all previous
WAL files. These files can be deleted after a time or size threshold but should
be replicated before being deleted.

### Local

Given a database file named `db`, SQLite will create a WAL file called `db-wal`.
Litestream will then create a hidden directory called `.db-litestream` that
contains the historical record of all WAL files for the current generation.

```
db                                      # SQLite database
db-wal                                  # SQLite WAL
.db-litestream/
	generation                          # current generation number
	generations/
		xxxxxxxx/
			wal/                        # WAL files
				000000000000001.wal     
				000000000000002.wal     
				000000000000003.wal     # active WAL
```

### Remote (S3)

```
bkt/
	db/                                       # database path
		generations/
			xxxxxxxx/
				snapshots/                    # snapshots w/ timestamp+offset
					20000101T000000Z-000000000000023.snapshot
				wal/                          # compressed WAL files
					000000000000001-0.wal.gz    
					000000000000001-<offset>.wal.gz
					000000000000002-0.wal.gz
		00000002/
			snapshot/
				000000000000000.snapshot
				scheduled/
					daily/
						20000101T000000Z-000000000000023.snapshot
						20000102T000000Z-000000000000036.snapshot
					monthly/
						20000101T000000Z-000000000000023.snapshot

			wal/
				000000000000001.wal.gz
```


