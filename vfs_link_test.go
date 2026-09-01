//go:build vfs

package litestream

// Link SQLite symbols required by sqlite3vfs in the tagged test binary.
import _ "github.com/mattn/go-sqlite3"
