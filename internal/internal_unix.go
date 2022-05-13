//go:build !windows
// +build !windows

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
