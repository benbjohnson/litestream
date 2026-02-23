# litestream-vfs

Litestream VFS extension for SQLite — distributed as a Python wheel.

This package bundles the [Litestream](https://litestream.io) VFS shared library
so you can load it directly into a Python `sqlite3` connection.

## Installation

```bash
pip install litestream-vfs
```

## Usage

```python
import sqlite3
import litestream_vfs

conn = sqlite3.connect(":memory:")
litestream_vfs.load(conn)
```

To get the path to the shared library (for use with other SQLite bindings):

```python
path = litestream_vfs.loadable_path()
```

## Platform Support

| Platform | Architecture |
|----------|-------------|
| Linux | x86_64, aarch64 |
| macOS | x86_64, arm64 |

## License

Apache-2.0 — see [LICENSE](https://github.com/benbjohnson/litestream/blob/main/LICENSE).
