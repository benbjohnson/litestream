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
	"github.com/benbjohnson/litestream/sqlite"
)

var _ fs.HandleFlusher = (*Handle)(nil)
var _ fs.HandleReadDirAller = (*Handle)(nil)
var _ fs.HandleReader = (*Handle)(nil)
var _ fs.HandleReleaser = (*Handle)(nil)
var _ fs.HandleWriter = (*Handle)(nil)

// var _ fs.HandleReadAller = (*Handle)(nil)
// var _ fs.HandleFlockLocker = (*Handle)(nil)
//var _ fs.HandleLocker = (*Handle)(nil)
//var _ fs.HandlePOSIXLocker = (*Handle)(nil)

// Handle represents a FUSE file handle.
type Handle struct {
	node *Node
	f    *os.File
}

// NewHandle returns a new instance of Handle.
func NewHandle(n *Node, f *os.File) *Handle {
	return &Handle{node: n, f: f}
}

// Release closes the underlying file descriptor.
func (h *Handle) Release(ctx context.Context, req *fuse.ReleaseRequest) (err error) {
	if err := h.node.Sync(); err != nil {
		return err
	}
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

	if resp.Size, err = h.f.WriteAt(req.Data, req.Offset); err != nil {
		// TODO: Invalidate node DB state.
		return err
	}

	// Check if handle reference a managed database.
	db := h.node.DB()
	if db == nil {
		return nil
	}

	// If this is the DB file, update the DB state based on the header.
	if !sqlite.IsWALPath(h.node.Path()) {
		// TODO: Header write could theoretically occur anywhere in first 100 bytes.
		// If updating the header page, first validate it.
		if req.Offset == 0 {
			db.SetHeader(req.Data)
		}
		return nil
	}

	// Ignore if the DB is not in a valid state (header + wal enabled).
	if !db.Valid() {
		return nil
	}

	// Otherwise this is the WAL file so we should append the WAL data.
	db.AddPendingWALByteN(int64(len(req.Data)))

	return nil
}

// Flush is called when a file handle is synced to disk. Implements fs.HandleFlusher.
func (h *Handle) Flush(ctx context.Context, req *fuse.FlushRequest) (err error) {
	if err := h.node.Sync(); err != nil {
		return err
	}
	return h.f.Sync()
}

// ReadDirAll returns a list of all entries in a directory. Implements fs.HandleReadDirAller.
func (h *Handle) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	fis, err := h.f.Readdir(-1)
	if err != nil {
		return nil, err
	}

	// Convert FileInfo objects to FUSE directory entries.
	ents = make([]fuse.Dirent, 0, len(fis))
	for _, fi := range fis {
		// Skip any meta directories.
		if IsMetaDir(fi.Name()) {
			continue
		}

		statt := fi.Sys().(*syscall.Stat_t)
		ents = append(ents, fuse.Dirent{Inode: statt.Ino, Name: fi.Name()})
	}

	sort.Slice(ents, func(i, j int) bool { return ents[i].Name < ents[j].Name })
	return ents, nil
}
