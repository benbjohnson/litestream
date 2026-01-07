# databases Command

List all databases in the configuration file.

## Synopsis

```bash
litestream databases [-config PATH]
```

## Description

The `databases` command lists all databases defined in your Litestream configuration file along with their replica type.

## Flags

| Flag | Description |
|------|-------------|
| `-config PATH` | Configuration file path (default: `/etc/litestream.yml`) |
| `-no-expand-env` | Disable environment variable expansion |

## Output Columns

| Column | Description |
|--------|-------------|
| `path` | Path to the SQLite database file |
| `replica` | Storage backend type (s3, gs, abs, sftp, nats, file, etc.) |

## Examples

### Basic Usage

```bash
$ litestream databases

path                    replica
/data/app.db            s3
/data/users.db          s3
/data/logs.db           file
```

### With Custom Config

```bash
$ litestream databases -config /etc/litestream-prod.yml

path                    replica
/data/production.db     s3
```

## Use Cases

### Verify Configuration

Ensure all expected databases are configured:

```bash
$ litestream databases
# Check output includes all your databases
```

### Script Integration

Use in scripts to iterate over databases:

```bash
#!/bin/bash
# Check status of all databases
for db in $(litestream databases | tail -n +2 | cut -f1); do
    echo "Checking: $db"
    litestream status "$db"
done
```

### Debugging

Verify configuration is loading correctly:

```bash
# With environment expansion
litestream databases

# Without environment expansion
litestream databases -no-expand-env
```

## Configuration Examples

The output reflects your configuration:

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: s3://bucket/app

  - path: /data/users.db
    replica:
      type: gs
      bucket: my-bucket
      path: users

  - path: /data/logs.db
    replica:
      path: /mnt/backup/logs
```

Output:
```
path              replica
/data/app.db      s3
/data/users.db    gs
/data/logs.db     file
```

## Common Issues

### Empty output
- Check config file exists at expected location
- Verify config file syntax with `cat /etc/litestream.yml`

### "too many arguments"
- This command takes no positional arguments
- Flags must come before any arguments

### Wrong databases shown
- Verify correct config file is being loaded
- Check `-config` flag or default location

## See Also

- [Configuration Overview](../configuration/overview.md)
- [status Command](status.md)
- [replicate Command](replicate.md)
