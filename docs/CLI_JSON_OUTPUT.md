# CLI JSON Output

Litestream commands that accept `-json` write a JSON document to stdout. Errors
are written to stderr by the top-level command handler and return a non-zero exit
status.

The fields below are the stable output contract for CLI consumers. New fields
may be added in future releases, so consumers should ignore unknown fields.

## `litestream databases -json`

Outputs an array of databases loaded from the configuration file.

```json
[
  {
    "path": "/var/lib/app.db",
    "replica": "s3"
  }
]
```

| Field | Type | Description |
| --- | --- | --- |
| `path` | string | SQLite database path. |
| `replica` | string | Replica client type configured for the database. |

## `litestream info -json`

Outputs daemon process information from the control socket.

```json
{
  "version": "v0.5.0",
  "pid": 12345,
  "uptime_seconds": 300,
  "started_at": "2026-04-24T12:00:00Z",
  "database_count": 2
}
```

| Field | Type | Description |
| --- | --- | --- |
| `version` | string | Litestream version reported by the daemon. |
| `pid` | number | Daemon process ID. |
| `uptime_seconds` | number | Daemon uptime in seconds. |
| `started_at` | string | Daemon start time in RFC 3339 format. |
| `database_count` | number | Number of databases currently managed by the daemon. |

## `litestream list -json`

Outputs databases managed by the running daemon.

```json
{
  "databases": [
    {
      "path": "/var/lib/app.db",
      "status": "replicating",
      "last_sync_at": "2026-04-24T12:00:00Z"
    }
  ]
}
```

| Field | Type | Description |
| --- | --- | --- |
| `databases` | array | Managed database summaries. |
| `databases[].path` | string | SQLite database path. |
| `databases[].status` | string | Current daemon state for the database, such as `replicating`, `open`, or `stopped`. |
| `databases[].last_sync_at` | string | Last successful replica sync time in RFC 3339 format. Omitted if the database has not synced successfully. |

## `litestream ltx -json`

Outputs LTX file metadata for the selected database or replica URL.

```json
[
  {
    "level": 0,
    "min_txid": "0000000000000001",
    "max_txid": "0000000000000004",
    "size": 8192,
    "timestamp": "2026-04-24T12:00:00Z"
  }
]
```

| Field | Type | Description |
| --- | --- | --- |
| `level` | number | LTX compaction level. |
| `min_txid` | string | Minimum transaction ID in the LTX file. |
| `max_txid` | string | Maximum transaction ID in the LTX file. |
| `size` | number | LTX file size in bytes. |
| `timestamp` | string | LTX file creation time in RFC 3339 format. |

## `litestream restore -json`

Outputs a final summary object after a restore completes. Restore logs are
written to stderr so stdout remains parseable JSON.

```json
{
  "db_path": "/var/lib/app.db",
  "replica": "file",
  "txid": "0000000000000004",
  "duration_ms": 125,
  "integrity_check": "quick"
}
```

| Field | Type | Description |
| --- | --- | --- |
| `db_path` | string | Restored database path. |
| `replica` | string | Replica client type used for the restore. |
| `txid` | string | Restored transaction ID, when available. |
| `duration_ms` | number | Restore duration in milliseconds. |
| `integrity_check` | string | Integrity check mode used for the restore: `none`, `quick`, or `full`. |

## `litestream status -json`

Outputs replication status for configured databases.

```json
[
  {
    "database": "/var/lib/app.db",
    "status": "ok",
    "local_txid": "0000000000000004",
    "wal_size": "32 kB"
  }
]
```

| Field | Type | Description |
| --- | --- | --- |
| `database` | string | SQLite database path. |
| `status` | string | Current local replication status: `ok`, `not initialized`, `no database`, or `error`. |
| `local_txid` | string | Latest local LTX transaction ID, or `-` if unavailable. |
| `wal_size` | string | Current WAL file size as human-readable text, or `-` if unavailable. |
