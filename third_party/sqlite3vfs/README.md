# sqlite3vfs: Go sqlite3 VFS API

sqlite3vfs is a Cgo API that allows you to create custom sqlite Virtual File Systems (VFS) in Go. You can use this with the `sqlite3` https://github.com/mattn/go-sqlite3 SQL driver.


## Basic usage

To use, simply implement the [VFS](https://pkg.go.dev/github.com/psanford/sqlite3vfs?utm_source=godoc#VFS) and [File](https://pkg.go.dev/github.com/psanford/sqlite3vfs?utm_source=godoc#File) interfaces. Then register your VFS with sqlite3vfs and include the vfs name when opening the database connection:

```
	// create your VFS
	vfs := newTempVFS()

	vfsName := "tmpfs"
	err := sqlite3vfs.RegisterVFS(vfsName, vfs)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("sqlite3", fmt.Sprintf("foo.db?vfs=%s", vfsName))
	if err != nil {
		panic(err)
	}

```

A full example can be found in [sqlite3vfs_test.go](sqlite3vfs_test.go).

## Loadable SQLite3 module

sqlite3vfs can also be built as a SQLite3 loadable module. This allows you to load your vfs at runtime into applications that support SQLite3 modules, including the SQLite3 cli tool.

To build as a loadable module, set the `-tags SQLITE3VFS_LOADABLE_EXT` build tag. Both [DonutDB](https://github.com/psanford/donutdb/tree/main/donutdb-loadable) and [sqlite3vfshttp](https://github.com/psanford/sqlite3vfshttp/tree/main/sqlite3http-ext) have working examples of building a SQLite3 loadable module.

## Users

- [DonutDB](https://github.com/psanford/donutdb): SQLite on top of DynamoDB (read/write)
- [sqlite3vfshttp](https://github.com/psanford/sqlite3vfshttp): Query a SQLite database over HTTP using range requests
- [sqlitezstd](https://github.com/jtarchie/sqlitezstd): Query a SQLite database that had been compressed with zstd seekable
