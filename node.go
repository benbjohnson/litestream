package main

import (
	"context"
	"io/ioutil"
	"log"
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
var _ fs.NodeFsyncer = (*Node)(nil)
var _ fs.NodeGetxattrer = (*Node)(nil)
var _ fs.NodeLinker = (*Node)(nil)
var _ fs.NodeListxattrer = (*Node)(nil)
var _ fs.NodeMkdirer = (*Node)(nil)
var _ fs.NodeMknoder = (*Node)(nil)
var _ fs.NodeOpener = (*Node)(nil)
var _ fs.NodeReadlinker = (*Node)(nil)
var _ fs.NodeRemover = (*Node)(nil)
var _ fs.NodeRemovexattrer = (*Node)(nil)
var _ fs.NodeRenamer = (*Node)(nil)
var _ fs.NodeSetattrer = (*Node)(nil)
var _ fs.NodeSetxattrer = (*Node)(nil)
var _ fs.NodeStringLookuper = (*Node)(nil)
var _ fs.NodeSymlinker = (*Node)(nil)

// var _ fs.NodeRequestLookuper = (*Node)(nil)
// var _ fs.NodeForgetter = (*Node)(nil)
// var _ fs.NodePoller = (*Node)(nil)

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

func (n *Node) Attr(ctx context.Context, a *fuse.Attr) (err error) {
	defer func() { log.Printf("[attr] node=%q -- attr=%s err=%v", n.path, a.String(), err) }()

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
func (n *Node) Lookup(ctx context.Context, name string) (_ fs.Node, err error) {
	defer func() { log.Printf("[lookup] node=%q name=%q -- err=%v", n.path, name, err) }()

	path := filepath.Join(n.path, name)
	srcpath := filepath.Join(n.fs.SourcePath, path)
	if _, err := os.Stat(srcpath); os.IsNotExist(err) {
		return nil, syscall.ENOENT
	}
	return NewNode(n.fs, path), nil
}

func (n *Node) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	defer func() { log.Printf("[readdirall] node=%q -- ents=%d err=%v", n.path, len(ents), err) }()

	fis, err := ioutil.ReadDir(n.srcpath())
	if err != nil {
		return nil, err
	}

	ents = make([]fuse.Dirent, len(fis))
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
func (n *Node) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) (err error) {
	defer func() { log.Printf("[setattr] node=%q req=%s -- err=%v", n.path, req.String(), err) }()

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
		println("TODO: setattr.handle")
	}
	if req.Valid.LockOwner() {
		println("TODO: setattr.lockowner")
	}

	// Update response attributes.
	return n.Attr(ctx, &resp.Attr)
}

// Symlink creates a new symbolic link in the receiver, which must be a directory.
func (n *Node) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (_ fs.Node, err error) {
	defer func() {
		log.Printf("[symlink] node=%q newname=%q target=%q -- err=%v", n.path, req.NewName, req.Target, err)
	}()
	if err := os.Symlink(req.Target, req.NewName); err != nil {
		return nil, err
	}
	return NewNode(n.fs, req.NewName), nil
}

// Readlink reads a symbolic link.
func (n *Node) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (_ string, err error) {
	defer func() { log.Printf("[readlink] node=%q -- err=%v", n.path, err) }()
	return os.Readlink(n.srcpath())
}

// Link creates a new directory entry in the receiver based on an
// existing Node. Receiver must be a directory.
func (n *Node) Link(ctx context.Context, req *fuse.LinkRequest, _old fs.Node) (_ fs.Node, err error) {
	old := _old.(*Node)

	defer func() {
		log.Printf("[link] node=%q oldnode=%q name=%q -- err=%v", n.path, old.srcpath(), req.NewName, err)
	}()
	// assert(n.IsDir())

	if err := os.Link(old.srcpath(), req.NewName); err != nil {
		return nil, err
	}
	return NewNode(n.fs, req.NewName), nil
}

// Remove removes the entry with the given name from
// the receiver, which must be a directory.  The entry to be removed
// may correspond to a file (unlink) or to a directory (rmdir).
func (n *Node) Remove(ctx context.Context, req *fuse.RemoveRequest) (err error) {
	defer func() { log.Printf("[remove] node=%q name=%q dir=%v -- err=%v", n.path, req.Name, req.Dir, err) }()

	if req.Dir {
		return syscall.Rmdir(filepath.Join(n.srcpath(), req.Name))
	}
	return syscall.Unlink(filepath.Join(n.srcpath(), req.Name))
}

// Access checks whether the calling context has permission for
// the given operations on the receiver. If so, Access should
// return nil. If not, Access should return EPERM.
//
// Note that this call affects the result of the access(2) system
// call but not the open(2) system call. If Access is not
// implemented, the Node behaves as if it always returns nil
// (permission granted), relying on checks in Open instead.
func (n *Node) Access(ctx context.Context, req *fuse.AccessRequest) (err error) {
	defer func() { log.Printf("[access] node=%q mask=%#o -- err=%v", n.path, req.Mask, err) }()

	return syscall.Access(n.srcpath(), req.Mask)
}

//type NodeRequestLookuper interface {
//	// Lookup looks up a specific entry in the receiver.
//	// See NodeStringLookuper for more.
//	Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (Node, error)
//}

func (n *Node) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (_ fs.Node, err error) {
	defer func() { log.Printf("[mkdir] node=%q mode=%#o umask=%#o -- err=%v", n.path, req.Mode, req.Umask, err) }()
	if err := syscall.Mkdir(filepath.Join(n.srcpath(), req.Name), uint32(req.Mode^req.Umask)); err != nil {
		return nil, err
	}
	return NewNode(n.fs, filepath.Join(n.path, req.Name)), nil
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
func (n *Node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (_ fs.Handle, err error) {
	defer func() { log.Printf("[open] node=%q dir=%v flags=%#o -- err=%v", n.path, req.Dir, req.Flags, err) }()

	// TODO(bbj): Where does mode come from?
	f, err := os.OpenFile(n.srcpath(), int(req.Flags), 0777)
	if err != nil {
		return nil, err
	}
	return &Handle{f: f}, nil
}

// Create creates a new directory entry in the receiver, which must be a directory.
func (n *Node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (_ fs.Node, _ fs.Handle, err error) {
	defer func() {
		log.Printf("[create] node=%q name=%q flags=%#o mode=%#o umask=%#o -- err=%v", n.path, req.Name, req.Flags, req.Mode, req.Umask, err)
	}()

	f, err := os.OpenFile(filepath.Join(n.srcpath(), req.Name), int(req.Flags), req.Mode^req.Umask)
	if err != nil {
		return nil, nil, err
	}
	return NewNode(n.fs, filepath.Join(n.path, req.Name)), Handle{f: f}, nil
}

// Forget about this node. This node will not receive further
// method calls.
//
// Forget is not necessarily seen on unmount, as all nodes are
// implicitly forgotten as part of the unmount.
// func (n *Node) Forget() { panic("TODO") }

func (n *Node) Rename(ctx context.Context, req *fuse.RenameRequest, _newDir fs.Node) (err error) {
	newDir := _newDir.(*Node)
	func() {
		log.Printf("[rename] node=%q oldname=%q newnode=%q newname=%q -- err=%v", n.path, req.NewName, newDir.path, req.OldName, err)
	}()
	return os.Rename(filepath.Join(n.srcpath(), req.OldName), filepath.Join(newDir.srcpath(), req.NewName))
}

func (n *Node) Mknod(ctx context.Context, req *fuse.MknodRequest) (_ fs.Node, err error) {
	defer func() {
		log.Printf("[mknod] node=%q name=%q mode=%#o rdev=%q newname=%q -- err=%v", n.path, req.Name, req.Mode, req.Rdev, req.Umask, err)
	}()
	if err := syscall.Mknod(filepath.Join(n.srcpath(), req.Name), uint32(req.Mode^req.Umask), int(req.Rdev)); err != nil {
		return nil, err
	}
	return NewNode(n.fs, filepath.Join(n.path, req.Name)), nil
}

func (n *Node) Fsync(ctx context.Context, req *fuse.FsyncRequest) (err error) {
	defer func() { log.Printf("[fsync] node=%q flags=%#o dir=%v -- err=%v", n.path, req.Flags, req.Dir, err) }()

	f, err := os.Open(n.srcpath())
	if err != nil {
		return err
	}
	defer f.Close()

	// TODO(bbj): Handle fdatasync()
	return f.Sync()
}

// Getxattr gets an extended attribute by the given name from the
// node.
//
// If there is no xattr by that name, returns fuse.ErrNoXattr.
func (n *Node) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) (err error) {
	defer func() { log.Printf("[getxattr] node=%q name=%q -- err=%T", n.path, req.Name, err) }()

	// TODO(bbj): Handle req.Size & returned syscall.Getxattr() size.
	if _, err = syscall.Getxattr(n.srcpath(), req.Name, resp.Xattr); err == syscall.ENODATA {
		return fuse.ErrNoXattr
	}
	return err
}

// Listxattr lists the extended attributes recorded for the node.
func (n *Node) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) (err error) {
	defer func() { log.Printf("[listxattr] node=%q -- err=%v", n.path, err) }()

	// TODO(bbj): Handle req.Size & returned syscall.Getxattr() size.
	_, err = syscall.Listxattr(n.srcpath(), resp.Xattr)
	return err
}

// Setxattr sets an extended attribute with the given name and
// value for the node.
func (n *Node) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) (err error) {
	defer func() {
		log.Printf("[setxattr] node=%q name=%q data=%q flags=%#o", n.path, req.Name, string(req.Xattr), req.Flags, err)
	}()
	return syscall.Setxattr(n.srcpath(), req.Name, req.Xattr, int(req.Flags))
}

// Removexattr removes an extended attribute for the name.
//
// If there is no xattr by that name, returns fuse.ErrNoXattr.
func (n *Node) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) (err error) {
	defer func() { log.Printf("[removexattr] node=%q name=%q -- err=%v", n.path, req.Name, err) }()
	return syscall.Removexattr(n.srcpath(), req.Name)
}

// Poll checks whether the node is currently ready for I/O, and
// may request a wakeup when it is. See HandlePoller.
// func (n *Node) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
// 	panic("TODO")
// }
