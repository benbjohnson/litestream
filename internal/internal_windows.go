//go:build windows
// +build windows

package internal

import (
	"os"
)

// Fileinfo returns syscall fields from a FileInfo object.
func Fileinfo(fi os.FileInfo) (uid, gid int) {
	return -1, -1
}
