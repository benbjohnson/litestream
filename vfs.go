//go:build vfs
// +build vfs

package litestream

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/psanford/sqlite3vfs"
	"github.com/superfly/ltx"
)

const (
	DefaultPollInterval = 1 * time.Second
)

// VFS implements the SQLite VFS interface for Litestream.
// It is intended to be used for read replicas that read directly from S3.
type VFS struct {
	client ReplicaClient
	logger *slog.Logger

	// PollInterval is the interval at which to poll the replica client for new
	// LTX files. The index will be fetched for the new files automatically.
	PollInterval time.Duration
}

func NewVFS(client ReplicaClient, logger *slog.Logger) *VFS {
	return &VFS{
		client:       client,
		logger:       logger.With("vfs", "true"),
		PollInterval: DefaultPollInterval,
	}
}

func (vfs *VFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	slog.Info("opening file", "name", name, "flags", flags)

	// TODO: Clone client w/ new path based on name.

	f := NewVFSFile(vfs.client, name, vfs.logger.With("name", name))
	f.PollInterval = vfs.PollInterval
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
	mu     sync.Mutex
	client ReplicaClient
	name   string

	pos   ltx.Pos
	index map[uint32]ltx.PageIndexElem

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	logger *slog.Logger

	PollInterval time.Duration
}

func NewVFSFile(client ReplicaClient, name string, logger *slog.Logger) *VFSFile {
	f := &VFSFile{
		client:       client,
		name:         name,
		index:        make(map[uint32]ltx.PageIndexElem),
		logger:       logger,
		PollInterval: DefaultPollInterval,
	}
	f.ctx, f.cancel = context.WithCancel(context.Background())
	return f
}

// Pos returns the current position of the file.
func (f *VFSFile) Pos() ltx.Pos {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pos
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

	// Determine the current position based off the latest LTX file.
	var pos ltx.Pos
	if len(infos) > 0 {
		pos = ltx.Pos{TXID: infos[len(infos)-1].MaxTXID}
	}
	f.pos = pos

	// Build the page index so we can lookup individual pages.
	if err := f.buildIndex(f.ctx, infos); err != nil {
		f.logger.Error("cannot build index", "error", err)
		return fmt.Errorf("cannot build index: %w", err)
	}

	// Continuously monitor the replica client for new LTX files.
	f.wg.Add(1)
	go func() { defer f.wg.Done(); f.monitorReplicaClient(f.ctx) }()

	return nil
}

// buildIndex constructs a lookup of pgno to LTX file offsets.
func (f *VFSFile) buildIndex(ctx context.Context, infos []*ltx.FileInfo) error {
	index := make(map[uint32]ltx.PageIndexElem)
	for _, info := range infos {
		f.logger.Debug("opening page index", "level", info.Level, "min", info.MinTXID, "max", info.MaxTXID)

		// Read page index.
		idx, err := FetchPageIndex(context.Background(), f.client, info)
		if err != nil {
			return fmt.Errorf("fetch page index: %w", err)
		}

		// Replace pages in overall index with new pages.
		for k, v := range idx {
			f.logger.Debug("adding page index", "page", k, "elem", v)
			index[k] = v
		}
	}

	f.mu.Lock()
	f.index = index
	f.mu.Unlock()

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

	n = copy(p, data)
	f.logger.Info("data read", "n", n, "data", len(data))

	// Update the first page to pretend like we are in journal mode.
	if off == 0 {
		p[18], p[19] = 0x01, 0x01
	}

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

func (f *VFSFile) monitorReplicaClient(ctx context.Context) {
	ticker := time.NewTicker(f.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := f.pollReplicaClient(ctx); err != nil {
				f.logger.Error("cannot fetch new ltx files", "error", err)
			}
		}
	}
}

// pollReplicaClient fetches new LTX files from the replica client and updates
// the page index & the current position.
func (f *VFSFile) pollReplicaClient(ctx context.Context) error {
	pos := f.Pos()
	f.logger.Debug("polling replica client", "txid", pos.TXID.String())

	// Start reading from the next LTX file after the current position.
	itr, err := f.client.LTXFiles(ctx, 0, f.pos.TXID+1)
	if err != nil {
		return fmt.Errorf("ltx files: %w", err)
	}

	// Build an update across all new LTX files.
	for itr.Next() {
		info := itr.Item()

		// Ensure we are fetching the next transaction from our current position.
		f.mu.Lock()
		isNextTXID := info.MinTXID == f.pos.TXID+1
		f.mu.Unlock()
		if !isNextTXID {
			return fmt.Errorf("non-contiguous ltx file: current=%s, next=%s-%s", f.pos.TXID, info.MinTXID, info.MaxTXID)
		}

		f.logger.Debug("new ltx file", "level", info.Level, "min", info.MinTXID, "max", info.MaxTXID)

		// Read page index.
		idx, err := FetchPageIndex(context.Background(), f.client, info)
		if err != nil {
			return fmt.Errorf("fetch page index: %w", err)
		}

		// Update the page index & current position.
		f.mu.Lock()
		for k, v := range idx {
			f.logger.Debug("adding new page index", "page", k, "elem", v)
			f.index[k] = v
		}
		f.pos.TXID = info.MaxTXID
		f.mu.Unlock()
	}

	return nil
}
