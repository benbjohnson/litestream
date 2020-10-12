package main

import (
	"context"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var _ fs.HandleFlockLocker = (*Handle)(nil)
var _ fs.HandleFlusher = (*Handle)(nil)
var _ fs.HandleLocker = (*Handle)(nil)
var _ fs.HandlePOSIXLocker = (*Handle)(nil)
var _ fs.HandlePoller = (*Handle)(nil)
var _ fs.HandleReadAller = (*Handle)(nil)
var _ fs.HandleReadDirAller = (*Handle)(nil)
var _ fs.HandleReader = (*Handle)(nil)
var _ fs.HandleReleaser = (*Handle)(nil)
var _ fs.HandleWriter = (*Handle)(nil)

type Handle struct{}

// Flush is called each time the file or directory is closed.
// Because there can be multiple file descriptors referring to a
// single opened file, Flush can be called multiple times.
func (h *Handle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	panic("TODO")
}

func (h *Handle) ReadAll(ctx context.Context) ([]byte, error) {
	panic("TODO")
}

func (h *Handle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	panic("TODO")
}

// Read requests to read data from the handle.
//
// There is a page cache in the kernel that normally submits only
// page-aligned reads spanning one or more pages. However, you
// should not rely on this. To see individual requests as
// submitted by the file system clients, set OpenDirectIO.
//
// Note that reads beyond the size of the file as reported by Attr
// are not even attempted (except in OpenDirectIO mode).
func (h *Handle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	panic("TODO")
}

// Write requests to write data into the handle at the given offset.
// Store the amount of data written in resp.Size.
//
// There is a writeback page cache in the kernel that normally submits
// only page-aligned writes spanning one or more pages. However,
// you should not rely on this. To see individual requests as
// submitted by the file system clients, set OpenDirectIO.
//
// Writes that grow the file are expected to update the file size
// (as seen through Attr). Note that file size changes are
// communicated also through Setattr.
func (h *Handle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	panic("TODO")
}

func (h *Handle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	panic("TODO")
}

// Poll checks whether the handle is currently ready for I/O, and
// may request a wakeup when it is.
//
// Poll should always return quickly. Clients waiting for
// readiness can be woken up by passing the return value of
// PollRequest.Wakeup to fs.Server.NotifyPollWakeup or
// fuse.Conn.NotifyPollWakeup.
//
// To allow supporting poll for only some of your Nodes/Handles,
// the default behavior is to report immediate readiness. If your
// FS does not support polling and you want to minimize needless
// requests and log noise, implement NodePoller and return
// syscall.ENOSYS.
//
// The Go runtime uses epoll-based I/O whenever possible, even for
// regular files.
func (h *Handle) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	panic("TODO")
}

// Lock tries to acquire a lock on a byte range of the node. If a
// conflicting lock is already held, returns syscall.EAGAIN.
//
// LockRequest.LockOwner is a file-unique identifier for this
// lock, and will be seen in calls releasing this lock
// (UnlockRequest, ReleaseRequest, FlushRequest) and also
// in e.g. ReadRequest, WriteRequest.
func (h *Handle) Lock(ctx context.Context, req *fuse.LockRequest) error {
	panic("TODO")
}

// LockWait acquires a lock on a byte range of the node, waiting
// until the lock can be obtained (or context is canceled).
func (h *Handle) LockWait(ctx context.Context, req *fuse.LockWaitRequest) error {
	panic("TODO")
}

// Unlock releases the lock on a byte range of the node. Locks can
// be released also implicitly, see HandleFlockLocker and
// HandlePOSIXLocker.
func (h *Handle) Unlock(ctx context.Context, req *fuse.UnlockRequest) error {
	panic("TODO")
}

// QueryLock returns the current state of locks held for the byte
// range of the node.
//
// See QueryLockRequest for details on how to respond.
//
// To simplify implementing this method, resp.Lock is prefilled to
// have Lock.Type F_UNLCK, and the whole struct should be
// overwritten for in case of conflicting locks.
func (h *Handle) QueryLock(ctx context.Context, req *fuse.QueryLockRequest, resp *fuse.QueryLockResponse) error {
	panic("TODO")
}
