package main

import (
	"context"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var _ fs.FS = (*FS)(nil)
var _ fs.FSDestroyer = (*FS)(nil)
var _ fs.FSStatfser = (*FS)(nil)

// var _ fs.FSInodeGenerator = (*FS)(nil)

type FS struct {
	SourcePath string
}

// Root returns the file system root.
func (f *FS) Root() (fs.Node, error) {
	return &Node{fs: f}, nil
}

// Destroy is called when the file system is shutting down.
//
// Linux only sends this request for block device backed (fuseblk)
// filesystems, to allow them to flush writes to disk before the
// unmount completes.
func (f *FS) Destroy() {
	// TODO: Flush writes?
}

// Statfs is called to obtain file system metadata.
// It should write that data to resp.
func (f *FS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	panic("TODO")
}

// GenerateInode is called to pick a dynamic inode number when it
// would otherwise be 0.
//
// Not all filesystems bother tracking inodes, but FUSE requires
// the inode to be set, and fewer duplicates in general makes UNIX
// tools work better.
//
// Operations where the nodes may return 0 inodes include Getattr,
// Setattr and ReadDir.
//
// If FS does not implement FSInodeGenerator, GenerateDynamicInode
// is used.
//
// Implementing this is useful to e.g. constrain the range of
// inode values used for dynamic inodes.
//
// Non-zero return values should be greater than 1, as that is
// always used for the root inode.
// func (f *FS) GenerateInode(parentInode uint64, name string) uint64 {}
