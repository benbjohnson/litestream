# litestream-vfs

Litestream VFS extension for SQLite — distributed as a Ruby gem.

This gem bundles the [Litestream](https://litestream.io) VFS shared library
so you can load it directly into a SQLite connection.

## Installation

```bash
gem install litestream-vfs
```

Or add to your Gemfile:

```ruby
gem "litestream-vfs"
```

## Usage

```ruby
require "litestream_vfs"

db = SQLite3::Database.new(":memory:")
LitestreamVfs.load(db)
```

To get the path to the shared library:

```ruby
path = LitestreamVfs.loadable_path
```

## Platform Support

| Platform | Architecture |
|----------|-------------|
| Linux | x86_64, aarch64 |
| macOS | x86_64, arm64 |

## License

Apache-2.0 — see [LICENSE](https://github.com/benbjohnson/litestream/blob/main/LICENSE).
