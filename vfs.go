package litestream

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/psanford/sqlite3vfs"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/internal"
)

// VFS implements the SQLite VFS interface for Litestream.
// It is intended to be used for read replicas that read directly from S3.
type VFS struct {
	client ReplicaClient
	logger *slog.Logger
}

func NewVFS(client ReplicaClient, logger *slog.Logger) *VFS {
	return &VFS{
		client: client,
		logger: logger,
	}
}

func (vfs *VFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	slog.Info("opening file", "name", name, "flags", flags)

	// TODO: Clone client w/ new path based on name.

	f := NewVFSFile(vfs.client, name, vfs.logger.With("name", name))
	if err := f.Open(); err != nil {
		return nil, 0, err
	}
	return f, flags, nil
}

func (vfs *VFS) Delete(name string, dirSync bool) error {
	slog.Info("deleting file", "name", name, "dirSync", dirSync)
	return fmt.Errorf("cannot delete vfs file")
}

func (vfs *VFS) Access(name string, flag sqlite3vfs.AccessFlag) (bool, error) {
	slog.Info("accessing file", "name", name, "flag", flag)

	if strings.HasSuffix(name, "-wal") {
		return vfs.accessWAL(name, flag)
	}
	return false, nil
}

func (vfs *VFS) accessWAL(name string, flag sqlite3vfs.AccessFlag) (bool, error) {
	return false, nil
}

func (vfs *VFS) FullPathname(name string) string {
	slog.Info("full pathname", "name", name)
	return name
}

// VFSFile implements the SQLite VFS file interface.
type VFSFile struct {
	client ReplicaClient
	name   string

	index map[uint32]ltx.PageIndexElem

	logger *slog.Logger
}

func NewVFSFile(client ReplicaClient, name string, logger *slog.Logger) *VFSFile {
	return &VFSFile{
		client: client,
		name:   name,
		index:  make(map[uint32]ltx.PageIndexElem),
		logger: logger,
	}
}

func (f *VFSFile) Open() error {
	f.logger.Info("opening file")

	infos, err := CalcRestorePlan(context.Background(), f.client, 0, time.Time{}, f.logger)
	if err != nil {
		f.logger.Error("cannot calc restore plan", "error", err)
		return fmt.Errorf("cannot calc restore plan: %w", err)
	} else if len(infos) == 0 {
		f.logger.Error("no backup files available")
		return fmt.Errorf("no backup files available") // TODO: Open even when no files available.
	}

	index := make(map[uint32]ltx.PageIndexElem)
	for _, info := range infos {
		f.logger.Info("opening page index", "level", info.Level, "min", info.MinTXID, "max", info.MaxTXID)

		// Read page index.
		idx, err := FetchPageIndex(context.Background(), f.client, info)
		if err != nil {
			f.logger.Error("cannot fetch page index", "error", err)
			return fmt.Errorf("fetch page index: %w", err)
		}

		// Replace pages in overall index with new pages.
		for k, v := range idx {
			f.logger.Debug("adding page index", "page", k, "elem", v)
			index[k] = v
		}
	}
	f.index = index

	return nil
}

func (f *VFSFile) Close() error {
	f.logger.Info("closing file")
	return nil
}

func (f *VFSFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.logger.Info("reading at", "off", off, "len", len(p))
	pgno := uint32(off/4096) + 1
	elem, ok := f.index[pgno]
	if !ok {
		f.logger.Error("page not found", "page", pgno)
		return 0, fmt.Errorf("page not found: %d", pgno)
	}

	_, data, err := FetchPage(context.Background(), f.client, elem.Level, elem.MinTXID, elem.MaxTXID, elem.Offset, elem.Size)
	if err != nil {
		f.logger.Error("cannot fetch page", "error", err)
		return 0, fmt.Errorf("fetch page: %w", err)
	}

	// TODO: Implement a size check, if possible. Not all reads are full pages.
	//if len(data) != len(p) {
	//	f.logger.Error("page data length mismatch", "expected", len(p), "actual", len(data))
	//	return 0, fmt.Errorf("page data length mismatch: %d != %d", len(p), len(data))
	//}

	n = copy(p, data)
	f.logger.Info("data read", "n", n, "data", len(data))

	// Update the first page to pretend like we are in journal mode.
	if off == 0 {
		p[18], p[19] = 0x01, 0x01
	}

	println(internal.Hexdump(p))

	return n, nil
}

func (f *VFSFile) WriteAt(b []byte, off int64) (n int, err error) {
	f.logger.Info("write at", "off", off, "len", len(b))
	return 0, fmt.Errorf("litestream is a read only vfs")
}

func (f *VFSFile) Truncate(size int64) error {
	f.logger.Info("truncating file", "size", size)
	return fmt.Errorf("litestream is a read only vfs")
}

func (f *VFSFile) Sync(flag sqlite3vfs.SyncType) error {
	f.logger.Info("syncing file", "flag", flag)
	return nil
}

func (f *VFSFile) FileSize() (size int64, err error) {
	const pageSize = 4096
	for pgno := range f.index {
		if int64(pgno)*pageSize > int64(size) {
			size = int64(pgno * pageSize)
		}
	}
	f.logger.Info("file size", "size", size)
	return size, nil
}

func (f *VFSFile) Lock(elock sqlite3vfs.LockType) error {
	f.logger.Info("locking file", "lock", elock)
	return nil // TODO: Implement locking for internal state only
}

func (f *VFSFile) Unlock(elock sqlite3vfs.LockType) error {
	f.logger.Info("unlocking file", "lock", elock)
	return nil // TODO: Implement unlocking for internal state only
}

func (f *VFSFile) CheckReservedLock() (bool, error) {
	f.logger.Info("checking reserved lock")
	return false, nil // TODO: Implement reserved lock checking
}

func (f *VFSFile) SectorSize() int64 {
	f.logger.Info("sector size")
	return 0
}

func (f *VFSFile) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	f.logger.Info("device characteristics")
	return 0
}
