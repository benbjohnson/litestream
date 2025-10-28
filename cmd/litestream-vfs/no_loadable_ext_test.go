//go:build !SQLITE3VFS_LOADABLE_EXT

package main_test

const loadableExtensionBuild = false

var _ = loadableExtensionBuild
