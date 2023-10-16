//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris
// +build aix darwin dragonfly freebsd linux netbsd openbsd solaris

package internal

import (
	"os"
	"syscall"
)

// Fileinfo returns syscall fields from a FileInfo object.
func Fileinfo(fi os.FileInfo) (uid, gid int) {
	if fi == nil {
		return -1, -1
	}
	stat := fi.Sys().(*syscall.Stat_t)
	return int(stat.Uid), int(stat.Gid)
}

func fixRootDirectory(p string) string {
	return p
}
