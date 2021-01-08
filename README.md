litestream
==========

Litestream is a standalone streaming replication tool for SQLite. It runs as a
background process and safely replicates changes incrementally from one or more
SQLite databases. Litestream only communicates with SQLite through the SQLite
API so it will not corrupt your database.


## Usage

### Installation & configuration

You can download the binary from the [Releases page](https://github.com/benbjohnson/litestream/releases).

Once installed locally, you'll need to create a config file. By default, the
config file lives at `/etc/litestream.yml` but you can pass in a different
path to any `litestream` command using the `-config PATH` flag.

You can copy this configuration below and update the source path (`/path/to/db`)
and the destination path (`/path/to/replica`) to where you want to replicate
from and to.

```yaml
databases:
  - path: "/path/to/db"
    replicas:
      - type: file
        path: /path/to/replica
```


### Replication

Once your configuration is saved, run the `litestream replicate` command:

```sh
# Replicate using the /etc/litestream.yml configuration.
$ litestream replicate

# Replicate using a different configuration path.
$ litestream replicate -config /path/to/litestream.yml
```

The `litestream` command will initialize and then wait indefinitely for changes.
You should see your destination replica path is now populated with a
`generations` directory. Inside there should be a 16-character hex generation
directory and inside there should be snapshots & wal files. As you make changes
to your source database, changes will be copied over to your replica incrementally.


### Restoring a backup

Litestream can restore a previous snapshot and replay all replicated WAL files.
By default, it will restore up to the latest WAL file but you can also perform
point-in-time restores.

A database can only be restored to a path that does not exist so you don't need
to worry about accidentally overwriting your current database.

```sh
# Restore database to original path.
$ litestream restore /path/to/db

# Restore database to a new location.
$ litestream restore -o /tmp/mynewdb /path/to/db

# Restore database to a specific point-in-time.
$ litestream restore -timestamp 2020-01-01T00:00:00Z /path/to/db
```

Point-in-time restores only have the resolution of the timestamp of the WAL file
itself. By default, litestream will start a new WAL file every minute so
point-in-time restores are only accurate to the minute.


### Validating a backup

Litestream can perform a consistency check of backups. It does this by computing
a checksum of the current database file and then computing a checksum of
a restored a backup. Litestream performs physical replication so backed up
databases should be the same byte-for-byte. Be aware that this can incur some
cost if you are validating from an external replica such as S3.

```sh
$ litestream validate /path/to/db
```

Note that computing the checksum of your original database does obtain a read
lock to prevent checkpointing but this will not affect your read/write access
to the database.


## How it works

SQLite provides a WAL (write-ahead log) journaling mode which writes pages to
a `-wal` file before eventually being copied over to the original database file.
This copying process is known as checkpointing. The WAL file works as a circular
buffer so when the WAL reaches a certain size then it restarts from the beginning.

Litestream works by taking over the checkpointing process and controlling when
it is restarted to ensure that it copies every new page. Checkpointing is only
allowed when there are no read transactions so Litestream maintains a
long-running read transaction against each database until it is ready to
checkpoint.

The SQLite WAL file is copied to a separate location called the shadow WAL which
ensures that it will not be overwritten by SQLite. This shadow WAL acts as a
temporary buffer so that replicas can replicate to their destination (e.g.
another file path or to S3). The shadow WAL files are removed once they have
been fully replicated.

Litestream groups a snapshot and all subsequent WAL changes into "generations".
A generation is started on initial replication of a database and a new
generation will be started if litestream detects that the WAL replication is
no longer contiguous. This can occur if the `litestream` process is stopped and
another process is allowed to checkpoint the WAL.



## Open-Source, not Open-Contribution

[Similar to SQLite](https://www.sqlite.org/copyright.html), litestream is open
source but closed to contributions. This keeps the code base free of proprietary
or licensed code but it also helps me continue to maintain and build litestream.

As the author of [BoltDB](https://github.com/boltdb/bolt), I found that
accepting and maintaining third party patches contributed to my burn out and
eventual archival of the project. Writing databases & low-level replication
tools involves nuance and simple one line changes can have profound and
unexpected changes in correctness and performance. Even small contributions
typically required hours of my time to properly test and validate them.

I am grateful for community involvement and when folks report bugs or suggest
features. I do not wish to come off as anything but welcoming, however, I've
made the decision to keep this project closed to contribution for my own
mental health and long term viability of the project.
