Litestream
![GitHub release (latest by date)](https://img.shields.io/github/v/release/benbjohnson/litestream)
![Status](https://img.shields.io/badge/status-beta-blue)
![GitHub](https://img.shields.io/github/license/benbjohnson/litestream)
![test](https://github.com/benbjohnson/litestream/workflows/test/badge.svg)
==========

Litestream is a standalone streaming replication tool for SQLite. It runs as a
background process and safely replicates changes incrementally to another file
or S3. Litestream only communicates with SQLite through the SQLite API so it
will not corrupt your database.

If you need support or have ideas for improving Litestream, please visit the
[GitHub Discussions](https://github.com/benbjohnson/litestream/discussions) to
chat. 

If you find this project interesting, please consider starring the project on
GitHub.


## Installation

### Mac OS (Homebrew)

To install from homebrew, run the following command:

```sh
$ brew install benbjohnson/litestream/litestream
```


### Linux (Debian)

You can download the `.deb` file from the [Releases page][releases] page and
then run the following:

```sh
$ sudo dpkg -i litestream-v0.3.0-linux-amd64.deb
```

Once installed, you'll need to enable & start the service:

```sh
$ sudo systemctl enable litestream
$ sudo systemctl start litestream
```


### Release binaries

You can also download the release binary for your system from the
[releases page][releases] and run it as a standalone application.


### Building from source

Download and install the [Go toolchain](https://golang.org/) and then run:

```sh
$ go install ./cmd/litestream
```

The `litestream` binary should be in your `$GOPATH/bin` folder.


## Quick Start

Litestream provides a configuration file that can be used for production
deployments but you can also specify a single database and replica on the
command line when trying it out.

First, you'll need to create an S3 bucket that we'll call `"mybkt"` in this
example. You'll also need to set your AWS credentials:

```sh
$ export AWS_ACCESS_KEY_ID=AKIAxxxxxxxxxxxxxxxx
$ export AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/xxxxxxxxx
```

Next, you can run the `litestream replicate` command with the path to the
database you want to backup and the URL of your replica destination:

```sh
$ litestream replicate /path/to/db s3://mybkt/db
```

If you make changes to your local database, those changes will be replicated
to S3 every 10 seconds. From another terminal window, you can restore your
database from your S3 replica:

```
$ litestream restore -o /path/to/restored/db s3://mybkt/db
```

Voila! 🎉

Your database should be restored to the last replicated state that
was sent to S3. You can adjust your replication frequency and other options by
using a configuration-based approach specified below.


## Configuration

A configuration-based install gives you more replication options. By default,
the config file lives at `/etc/litestream.yml` but you can pass in a different
path to any `litestream` command using the `-config PATH` flag. You can also 
set the `LITESTREAM_CONFIG` environment variable to specify a new path.

The configuration specifies one or more `dbs` and a list of one or more replica
locations for each db. Below are some common configurations:

### Replicate to S3

This will replicate the database at `/path/to/db` to the `"/db"` path inside
the S3 bucket named `"mybkt"`.

```yaml
access-key-id:     AKIAxxxxxxxxxxxxxxxx
secret-access-key: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/xxxxxxxxx

dbs:
  - path: /path/to/db
    replicas:
      - url: s3://mybkt/db
```

### Replicate to another file path

This will replicate the database at `/path/to/db` to a directory named
`/path/to/replica`.

```yaml
dbs:
  - path: /path/to/db
    replicas:
      - path: /path/to/replica
```

### Retention period

By default, replicas will retain a snapshot & subsequent WAL changes for 24
hours. When the snapshot age exceeds the retention threshold, a new snapshot
is taken and uploaded and the previous snapshot and WAL files are removed.

You can configure this setting per-replica. Times are parsed using [Go's
duration](https://golang.org/pkg/time/#ParseDuration) so time units of hours
(`h`), minutes (`m`), and seconds (`s`) are allowed but days, weeks, months, and
years are not.


```yaml
db:
  - path: /path/to/db
    replicas:
      - url: s3://mybkt/db
        retention: 1h          # 1 hour retention
```


### Monitoring replication

You can also enable a Prometheus metrics endpoint to monitor replication by
specifying a bind address with the `addr` field:

```yml
addr: ":9090"
```

This will make metrics available at: http://localhost:9090/metrics


### Other configuration options

These are some additional configuration options available on replicas:

- `type`—Specify the type of replica (`"file"` or `"s3"`). Derived from `"path"`.
- `name`—Specify an optional name for the replica if you are using multiple replicas.
- `path`—File path to the replica location.
- `url`—URL to the replica location.
- `retention-check-interval`—Time between retention enforcement checks. Defaults to `1h`.
- `validation-interval`—Interval between periodic checks to ensure restored backup matches current database. Disabled by default.

These replica options are only available for S3 replicas:

- `bucket`—S3 bucket name. Derived from `"path"`.
- `region`—S3 bucket region. Looked up on startup if unspecified.
- `sync-interval`—Replication sync frequency.



## Usage

### Replication

Once your configuration is saved, you'll need to begin replication. If you
installed the `.deb` file then run:

```sh
$ sudo systemctl restart litestream
```

To run litestream on its own, run:

```sh
# Replicate using the /etc/litestream.yml configuration.
$ litestream replicate

# Replicate using a different configuration path.
$ litestream replicate -config /path/to/litestream.yml
```

The `litestream` command will initialize and then wait indefinitely for changes.
You should see your destination replica path is now populated with a
`generations` directory. Inside there should be a 16-character hex generation
directory and inside there should be snapshots & WAL files. As you make changes
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
$ litestream restore -o /path/to/restored/db /path/to/db

# Restore from a replica URL.
$ litestream restore -o /path/to/restored/db s3://mybkt/db

# Restore database to a specific point-in-time.
$ litestream restore -timestamp 2020-01-01T00:00:00Z /path/to/db
```

Point-in-time restores only have the resolution of the timestamp of the WAL file
itself. By default, litestream will start a new WAL file every minute so
point-in-time restores are only accurate to the minute.



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
been fully replicated. You can find the shadow directory as a hidden directory
next to your database file. If you database file is named `/var/lib/my.db` then
the shadow directory will be `/var/lib/.my.db-litestream`.

Litestream groups a snapshot and all subsequent WAL changes into "generations".
A generation is started on initial replication of a database and a new
generation will be started if litestream detects that the WAL replication is
no longer contiguous. This can occur if the `litestream` process is stopped and
another process is allowed to checkpoint the WAL.



## Open-source, not open-contribution

[Similar to SQLite](https://www.sqlite.org/copyright.html), litestream is open
source but closed to contributions. This keeps the code base free of proprietary
or licensed code but it also helps me continue to maintain and build litestream.

As the author of [BoltDB](https://github.com/boltdb/bolt), I found that
accepting and maintaining third party patches contributed to my burn out and
I eventually archived the project. Writing databases & low-level replication
tools involves nuance and simple one line changes can have profound and
unexpected changes in correctness and performance. Small contributions
typically required hours of my time to properly test and validate them.

I am grateful for community involvement, bug reports, & feature requests. I do
not wish to come off as anything but welcoming, however, I've
made the decision to keep this project closed to contributions for my own
mental health and long term viability of the project.


[releases]: https://github.com/benbjohnson/litestream/releases
