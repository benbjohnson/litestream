// +build windows

package litestream

import (
	"os"
	"syscall"
)

// fileinfo returns syscall fields from a FileInfo object.
func fileinfo(fi os.FileInfo) (uid, gid int) {
	return -1, -1
}

// fixRootDirectory is copied from the standard library for use with mkdirAll()
func fixRootDirectory(p string) string {
	if len(p) == len(`\\?\c:`) {
		if IsPathSeparator(p[0]) && IsPathSeparator(p[1]) && p[2] == '?' && IsPathSeparator(p[3]) && p[5] == ':' {
			return p + `\`
		}
	}
	return p
}
