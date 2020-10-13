package main

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var _ fs.Node = (*Node)(nil)
var _ fs.NodeAccesser = (*Node)(nil)
var _ fs.NodeCreater = (*Node)(nil)
var _ fs.NodeForgetter = (*Node)(nil)
var _ fs.NodeFsyncer = (*Node)(nil)
var _ fs.NodeGetxattrer = (*Node)(nil)
var _ fs.NodeLinker = (*Node)(nil)
var _ fs.NodeListxattrer = (*Node)(nil)
var _ fs.NodeMkdirer = (*Node)(nil)
var _ fs.NodeMknoder = (*Node)(nil)
var _ fs.NodeOpener = (*Node)(nil)
var _ fs.NodePoller = (*Node)(nil)
var _ fs.NodeReadlinker = (*Node)(nil)
var _ fs.NodeRemover = (*Node)(nil)
var _ fs.NodeRemovexattrer = (*Node)(nil)
var _ fs.NodeRenamer = (*Node)(nil)
var _ fs.NodeSetattrer = (*Node)(nil)
var _ fs.NodeSetxattrer = (*Node)(nil)
var _ fs.NodeStringLookuper = (*Node)(nil)
var _ fs.NodeSymlinker = (*Node)(nil)

// var _ fs.NodeRequestLookuper = (*Node)(nil)

type Node struct {
	fs   *FS    // base filesystem
	path string // path within file system
}

func NewNode(fs *FS, path string) *Node {
	return &Node{fs: fs, path: path}
}

func (n *Node) srcpath() string {
	return filepath.Join(n.fs.SourcePath, n.path)
}

func (n *Node) Attr(ctx context.Context, a *fuse.Attr) error {
	fi, err := os.Stat(n.srcpath())
	if err != nil {
		return err
	}
	statt := fi.Sys().(*syscall.Stat_t)

	// TODO: Cache attr w/ a.Valid?

	if n.path == "" {
		a.Inode = 1
	} else {
		a.Inode = statt.Ino
	}
	a.Size = uint64(fi.Size())
	a.Blocks = uint64(statt.Blocks)
	a.Atime = time.Unix(statt.Atim.Sec, statt.Atim.Nsec).UTC()
	a.Mtime = time.Unix(statt.Mtim.Sec, statt.Mtim.Nsec).UTC()
	a.Ctime = time.Unix(statt.Ctim.Sec, statt.Ctim.Nsec).UTC()
	a.Mode = fi.Mode()
	a.Nlink = uint32(statt.Nlink)
	a.Uid = uint32(statt.Uid)
	a.Gid = uint32(statt.Gid)
	a.Rdev = uint32(statt.Rdev)
	a.BlockSize = uint32(statt.Blksize)

	return nil
}

// Lookup looks up a specific entry in the receiver,
// which must be a directory.  Lookup should return a Node
// corresponding to the entry.  If the name does not exist in
// the directory, Lookup should return ENOENT.
//
// Lookup need not to handle the names "." and "..".
func (n *Node) Lookup(ctx context.Context, name string) (fs.Node, error) {
	path := filepath.Join(n.path, name)
	srcpath := filepath.Join(n.fs.SourcePath, path)
	if _, err := os.Stat(srcpath); os.IsNotExist(err) {
		return nil, syscall.ENOENT
	}
	return NewNode(n.fs, path), nil
}

func (n *Node) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	fis, err := ioutil.ReadDir(n.srcpath())
	if err != nil {
		return nil, err
	}

	ents := make([]fuse.Dirent, len(fis))
	for i, fi := range fis {
		statt := fi.Sys().(*syscall.Stat_t)
		ents[i] = fuse.Dirent{Inode: statt.Ino, Name: fi.Name()}
	}
	return ents, nil
}

// Getattr obtains the standard metadata for the receiver.
// It should store that metadata in resp.
//
// If this method is not implemented, the attributes will be
// generated based on Attr(), with zero values filled in.
// func (n *Node) Getattr(ctx context.Context, req *fuse.GetattrRequest, resp *fuse.GetattrResponse) error {
// 	panic("TODO")
// }

// Setattr sets the standard metadata for the receiver.
//
// Note, this is also used to communicate changes in the size of
// the file, outside of Writes.
//
// req.Valid is a bitmask of what fields are actually being set.
// For example, the method should not change the mode of the file
// unless req.Valid.Mode() is true.
func (n *Node) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	// Obtain current file stat.
	srcpath := n.srcpath()
	fi, err := os.Stat(srcpath)
	if err != nil {
		return err
	}
	statt := fi.Sys().(*syscall.Stat_t)

	// Update access time, if flagged.
	var atime time.Time
	if req.Valid.AtimeNow() {
		atime = time.Now()
	} else if req.Valid.Atime() {
		atime = req.Atime
	}

	// Update mod time, if flagged.
	var mtime time.Time
	if req.Valid.MtimeNow() {
		mtime = time.Now()
	} else if req.Valid.Mtime() {
		mtime = req.Mtime
	}

	// Update timestamps, if specified.
	if !atime.IsZero() || !mtime.IsZero() {
		if atime.IsZero() {
			atime = time.Unix(statt.Atim.Sec, statt.Atim.Nsec).UTC()
		}
		if mtime.IsZero() {
			mtime = time.Unix(statt.Mtim.Sec, statt.Mtim.Nsec).UTC()
		}
		if err := os.Chtimes(srcpath, atime, mtime); err != nil {
			return err
		}
	}

	// Update group id.
	if req.Valid.Gid() {
		if err := syscall.Setgid(int(req.Gid)); err != nil {
			return err
		}
	}

	// Update user id.
	if req.Valid.Uid() {
		if err := syscall.Setuid(int(req.Uid)); err != nil {
			return err
		}
	}

	// Update file permissions.
	if req.Valid.Mode() {
		if err := os.Chmod(srcpath, req.Mode); err != nil {
			return err
		}
	}

	// Update file size.
	if req.Valid.Size() {
		if err := os.Truncate(srcpath, int64(req.Size)); err != nil {
			return err
		}
	}

	// TODO: Not sure what these are for.
	if req.Valid.Handle() {
		panic("TODO?")
	}
	if req.Valid.LockOwner() {
		panic("TODO?")
	}

	// Update response attributes.
	return n.Attr(ctx, &resp.Attr)
}

// Symlink creates a new symbolic link in the receiver, which must be a directory.
func (n *Node) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	if err := os.Symlink(req.Target, req.NewName); err != nil {
		return nil, err
	}
	return NewNode(n.fs, req.NewName), nil
}

// Readlink reads a symbolic link.
func (n *Node) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	panic("TODO")
}

// Link creates a new directory entry in the receiver based on an
// existing Node. Receiver must be a directory.
func (n *Node) Link(ctx context.Context, req *fuse.LinkRequest, old fs.Node) (fs.Node, error) {
	panic("TODO")
}

// Remove removes the entry with the given name from
// the receiver, which must be a directory.  The entry to be removed
// may correspond to a file (unlink) or to a directory (rmdir).
func (n *Node) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	panic("TODO")
}

// Access checks whether the calling context has permission for
// the given operations on the receiver. If so, Access should
// return nil. If not, Access should return EPERM.
//
// Note that this call affects the result of the access(2) system
// call but not the open(2) system call. If Access is not
// implemented, the Node behaves as if it always returns nil
// (permission granted), relying on checks in Open instead.
func (n *Node) Access(ctx context.Context, req *fuse.AccessRequest) error {
	panic("TODO")
}

//type NodeRequestLookuper interface {
//	// Lookup looks up a specific entry in the receiver.
//	// See NodeStringLookuper for more.
//	Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (Node, error)
//}

func (n *Node) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	panic("TODO")
}

// Open opens the receiver. After a successful open, a client
// process has a file descriptor referring to this Handle.
//
// Open can also be also called on non-files. For example,
// directories are Opened for ReadDir or fchdir(2).
//
// If this method is not implemented, the open will always
// succeed, and the Node itself will be used as the Handle.
//
// XXX note about access.  XXX OpenFlags.
func (n *Node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	panic("TODO")
}

// Create creates a new directory entry in the receiver, which
// must be a directory.
func (n *Node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	panic("TODO")
}

// Forget about this node. This node will not receive further
// method calls.
//
// Forget is not necessarily seen on unmount, as all nodes are
// implicitly forgotten as part of the unmount.
func (n *Node) Forget() { panic("TODO") }

func (n *Node) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	panic("TODO")
}

func (n *Node) Mknod(ctx context.Context, req *fuse.MknodRequest) (fs.Node, error) {
	panic("TODO")
}

func (n *Node) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	panic("TODO")
}

// Getxattr gets an extended attribute by the given name from the
// node.
//
// If there is no xattr by that name, returns fuse.ErrNoXattr.
func (n *Node) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	panic("TODO")
}

// Listxattr lists the extended attributes recorded for the node.
func (n *Node) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	panic("TODO")
}

// Setxattr sets an extended attribute with the given name and
// value for the node.
func (n *Node) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	panic("TODO")
}

// Removexattr removes an extended attribute for the name.
//
// If there is no xattr by that name, returns fuse.ErrNoXattr.
func (n *Node) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	panic("TODO")
}

// Poll checks whether the node is currently ready for I/O, and
// may request a wakeup when it is. See HandlePoller.
func (n *Node) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	panic("TODO")
}
