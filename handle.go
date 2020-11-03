package litestream

import (
	"context"
	"io"
	"log"
	"os"
	"sort"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

const (
	F_OFD_GETLK  = 0x24
	F_OFD_SETLK  = 0x25
	F_OFD_SETLKW = 0x26
)

var _ fs.HandleFlockLocker = (*Handle)(nil)
var _ fs.HandleFlusher = (*Handle)(nil)
var _ fs.HandleLocker = (*Handle)(nil)
var _ fs.HandlePOSIXLocker = (*Handle)(nil)
var _ fs.HandleReadDirAller = (*Handle)(nil)
var _ fs.HandleReader = (*Handle)(nil)
var _ fs.HandleReleaser = (*Handle)(nil)
var _ fs.HandleWriter = (*Handle)(nil)

// var _ fs.HandleReadAller = (*Handle)(nil)

// Handle represents a FUSE file handle.
type Handle struct {
	f *os.File
}

// Release closes the underlying file descriptor.
func (h *Handle) Release(ctx context.Context, req *fuse.ReleaseRequest) (err error) {
	return h.f.Close()
}

// Read reads data from a given offset in the underlying file.
func (h *Handle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) (err error) {
	buf := make([]byte, req.Size)
	n, err := h.f.ReadAt(buf, req.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	resp.Data = buf[:n]
	return nil
}

// Write writes data at a given offset to the underlying file.
func (h *Handle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	log.Printf("write: name=%s offset=%d n=%d", h.f.Name(), req.Offset, len(req.Data))
	println(HexDump(req.Data))

	resp.Size, err = h.f.WriteAt(req.Data, req.Offset)
	return err
}

// Flush is called when a file handle is synced to disk. Implements fs.HandleFlusher.
func (h *Handle) Flush(ctx context.Context, req *fuse.FlushRequest) (err error) {
	return h.f.Sync()
}

// ReadDirAll returns a list of all entries in a directory. Implements fs.HandleReadDirAller.
func (h *Handle) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	fis, err := h.f.Readdir(-1)
	if err != nil {
		return nil, err
	}

	// Convert FileInfo objects to FUSE directory entries.
	ents = make([]fuse.Dirent, len(fis))
	for i, fi := range fis {
		statt := fi.Sys().(*syscall.Stat_t)
		ents[i] = fuse.Dirent{Inode: statt.Ino, Name: fi.Name()}
	}

	sort.Slice(ents, func(i, j int) bool { return ents[i].Name < ents[j].Name })
	return ents, nil
}

// Lock tries to acquire a lock on a byte range of the node. If a
// conflicting lock is already held, returns syscall.EAGAIN.
func (h *Handle) Lock(ctx context.Context, req *fuse.LockRequest) error {
	log.Printf("dbg/lock %s -- %#v", h.f.Name(), req.Lock)
	return syscall.FcntlFlock(h.f.Fd(), F_OFD_SETLK, &syscall.Flock_t{
		Type:   int16(req.Lock.Type),
		Whence: io.SeekStart,
		Start:  int64(req.Lock.Start),
		Len:    int64(req.Lock.End) - int64(req.Lock.Start),
	})
}

// LockWait acquires a lock on a byte range of the node, waiting
// until the lock can be obtained (or context is canceled).
func (h *Handle) LockWait(ctx context.Context, req *fuse.LockWaitRequest) error {
	log.Printf("dbg/lockwait %s -- %#v", h.f.Name(), req.Lock)
	return syscall.FcntlFlock(h.f.Fd(), F_OFD_SETLKW, &syscall.Flock_t{
		Type:   int16(req.Lock.Type),
		Whence: io.SeekStart,
		Start:  int64(req.Lock.Start),
		Len:    int64(req.Lock.End) - int64(req.Lock.Start),
	})
}

// Unlock releases the lock on a byte range of the node. Locks can
// be released also implicitly, see HandleFlockLocker and
// HandlePOSIXLocker.
func (h *Handle) Unlock(ctx context.Context, req *fuse.UnlockRequest) error {
	log.Printf("dbg/unlock %s -- %#v", h.f.Name(), req.Lock)
	return syscall.FcntlFlock(h.f.Fd(), F_OFD_SETLK, &syscall.Flock_t{
		Type:   int16(req.Lock.Type),
		Whence: io.SeekStart,
		Start:  int64(req.Lock.Start),
		Len:    int64(req.Lock.End) - int64(req.Lock.Start),
	})

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
	flock_t := syscall.Flock_t{
		Type:   int16(req.Lock.Type),
		Whence: io.SeekStart,
		Start:  int64(req.Lock.Start),
		Len:    int64(req.Lock.End) - int64(req.Lock.Start),
	}
	if err := syscall.FcntlFlock(h.f.Fd(), F_OFD_GETLK, &flock_t); err != nil {
		return err
	}

	resp.Lock = fuse.FileLock{
		Type:  fuse.LockType(flock_t.Type),
		Start: uint64(flock_t.Start),
		End:   uint64(flock_t.Start + flock_t.Len),
		PID:   flock_t.Pid,
	}
	return nil
}
