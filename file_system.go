package litestream

import (
	"bazil.org/fuse/fs"
)

var _ fs.FS = (*FileSystem)(nil)

// FileSystem represents the file system that is mounted.
// It returns a root node that represents the root directory.
type FileSystem struct {
	SourcePath string
}

// Root returns the file system root node.
func (f *FileSystem) Root() (fs.Node, error) {
	return &Node{fs: f}, nil
}
