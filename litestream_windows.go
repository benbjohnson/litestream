// +build windows

package litestream

import (
	"os"
)

// fileinfo returns syscall fields from a FileInfo object.
func fileinfo(fi os.FileInfo) (uid, gid int) {
	return -1, -1
}

// fixRootDirectory is copied from the standard library for use with mkdirAll()
func fixRootDirectory(p string) string {
	if len(p) == len(`\\?\c:`) {
		if os.IsPathSeparator(p[0]) && os.IsPathSeparator(p[1]) && p[2] == '?' && os.IsPathSeparator(p[3]) && p[5] == ':' {
			return p + `\`
		}
	}
	return p
}
