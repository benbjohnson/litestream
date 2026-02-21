# litestream-vfs

Litestream VFS extension for SQLite — distributed via npm.

This package bundles the [Litestream](https://litestream.io) VFS shared library.
The correct platform-specific binary is automatically installed via `optionalDependencies`.

## Installation

```bash
npm install litestream-vfs
```

## Usage

```javascript
const { getLoadablePath } = require("litestream-vfs");
const path = getLoadablePath();
// Use `path` with better-sqlite3 or other SQLite bindings
```

## Platform Support

| Platform | Architecture |
|----------|-------------|
| Linux | x86_64, aarch64 |
| macOS | x86_64, arm64 |

## License

Apache-2.0 — see [LICENSE](https://github.com/benbjohnson/litestream/blob/main/LICENSE).
