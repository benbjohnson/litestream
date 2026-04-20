//go:build SQLITE3VFS_LOADABLE_EXT
// +build SQLITE3VFS_LOADABLE_EXT

package sqlite3vfs

/*
   #cgo CFLAGS: -DSQLITE3VFS_LOADABLE_EXT=1
*/
import "C"
