// +build aix darwin dragonfly freebsd linux netbsd openbsd solaris

package litestream

import (
	"os"
	"syscall"
)

// fileinfo returns syscall fields from a FileInfo object.
func fileinfo(fi os.FileInfo) (uid, gid int) {
	stat := fi.Sys().(*syscall.Stat_t)
	return int(stat.Uid), int(stat.Gid)
}

func fixRootDirectory(p string) string {
	return p
}
