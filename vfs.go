//go:build vfs
// +build vfs

package litestream

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	_ "unsafe"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/markusmobius/go-dateparser"
	"github.com/psanford/sqlite3vfs"
	"github.com/superfly/ltx"
)

const (
	DefaultPollInterval = 1 * time.Second
	DefaultCacheSize    = 10 * 1024 * 1024 // 10MB

	pageFetchRetryAttempts = 6
	pageFetchRetryDelay    = 15 * time.Millisecond
)

var (
	//go:linkname sqlite3vfsFileMap github.com/psanford/sqlite3vfs.fileMap
	sqlite3vfsFileMap map[uint64]sqlite3vfs.File

	//go:linkname sqlite3vfsFileMux github.com/psanford/sqlite3vfs.fileMux
	sqlite3vfsFileMux sync.Mutex

	vfsConnectionMap sync.Map // map[uintptr]uint64
)

// VFS implements the SQLite VFS interface for Litestream.
// It is intended to be used for read replicas that read directly from S3.
type VFS struct {
	client ReplicaClient
	logger *slog.Logger

	// PollInterval is the interval at which to poll the replica client for new
	// LTX files. The index will be fetched for the new files automatically.
	PollInterval time.Duration

	// CacheSize is the maximum size of the page cache in bytes.
	CacheSize int

	tempDirOnce sync.Once
	tempDir     string
	tempDirErr  error
	tempFiles   sync.Map // canonical name -> absolute path
	tempNames   sync.Map // canonical name -> struct{}{}
}

func NewVFS(client ReplicaClient, logger *slog.Logger) *VFS {
	return &VFS{
		client:       client,
		logger:       logger.With("vfs", "true"),
		PollInterval: DefaultPollInterval,
		CacheSize:    DefaultCacheSize,
	}
}

func (vfs *VFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	slog.Debug("opening file", "name", name, "flags", flags)

	switch {
	case flags&sqlite3vfs.OpenMainDB != 0:
		return vfs.openMainDB(name, flags)
	case vfs.requiresTempFile(flags):
		return vfs.openTempFile(name, flags)
	default:
		return nil, flags, sqlite3vfs.CantOpenError
	}
}

func (vfs *VFS) openMainDB(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	f := NewVFSFile(vfs.client, name, vfs.logger.With("name", name))
	f.PollInterval = vfs.PollInterval
	f.CacheSize = vfs.CacheSize
	if err := f.Open(); err != nil {
		return nil, 0, err
	}

	flags |= sqlite3vfs.OpenReadOnly
	return f, flags, nil
}

func (vfs *VFS) Delete(name string, dirSync bool) error {
	slog.Debug("deleting file", "name", name, "dirSync", dirSync)
	err := vfs.deleteTempFile(name)
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if errors.Is(err, errTempFileNotFound) {
		return fmt.Errorf("cannot delete vfs file")
	}
	return err
}

func (vfs *VFS) Access(name string, flag sqlite3vfs.AccessFlag) (bool, error) {
	slog.Debug("accessing file", "name", name, "flag", flag)

	if strings.HasSuffix(name, "-wal") {
		return vfs.accessWAL(name, flag)
	}
	if vfs.isTempFileName(name) {
		return vfs.accessTempFile(name, flag)
	}
	return false, nil
}

func (vfs *VFS) accessWAL(name string, flag sqlite3vfs.AccessFlag) (bool, error) {
	return false, nil
}

func (vfs *VFS) FullPathname(name string) string {
	slog.Debug("full pathname", "name", name)
	return name
}

func (vfs *VFS) requiresTempFile(flags sqlite3vfs.OpenFlag) bool {
	const tempMask = sqlite3vfs.OpenTempDB |
		sqlite3vfs.OpenTempJournal |
		sqlite3vfs.OpenSubJournal |
		sqlite3vfs.OpenSuperJournal |
		sqlite3vfs.OpenTransientDB
	if flags&tempMask != 0 {
		return true
	}
	return flags&sqlite3vfs.OpenDeleteOnClose != 0
}

func (vfs *VFS) ensureTempDir() (string, error) {
	vfs.tempDirOnce.Do(func() {
		dir, err := os.MkdirTemp("", "litestream-vfs-*")
		if err != nil {
			vfs.tempDirErr = fmt.Errorf("create temp dir: %w", err)
			return
		}
		vfs.tempDir = dir
	})
	return vfs.tempDir, vfs.tempDirErr
}

func (vfs *VFS) canonicalTempName(name string) string {
	if name == "" {
		return ""
	}
	name = filepath.Clean(name)
	if name == "." || name == string(filepath.Separator) {
		return ""
	}
	return name
}

func tempFilenameFromCanonical(canonical string) (string, error) {
	base := filepath.Base(canonical)
	if base == "." || base == string(filepath.Separator) {
		return "", fmt.Errorf("invalid temp file name: %q", canonical)
	}

	h := fnv.New64a()
	if _, err := h.Write([]byte(canonical)); err != nil {
		return "", fmt.Errorf("hash temp name: %w", err)
	}
	return fmt.Sprintf("%s-%016x", base, h.Sum64()), nil
}

func (vfs *VFS) openTempFile(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	dir, err := vfs.ensureTempDir()
	if err != nil {
		return nil, flags, err
	}
	deleteOnClose := flags&sqlite3vfs.OpenDeleteOnClose != 0 || name == ""
	var f *os.File
	var onClose func()
	if name == "" {
		f, err = os.CreateTemp(dir, "temp-*")
		if err != nil {
			return nil, flags, sqlite3vfs.CantOpenError
		}
	} else {
		canonical := vfs.canonicalTempName(name)
		if canonical == "" {
			return nil, flags, sqlite3vfs.CantOpenError
		}
		fname, err := tempFilenameFromCanonical(canonical)
		if err != nil {
			return nil, flags, sqlite3vfs.CantOpenError
		}
		path := filepath.Join(dir, fname)
		flag := openFlagToOSFlag(flags)
		if flag == 0 {
			flag = os.O_RDWR
		}
		f, err = os.OpenFile(path, flag|os.O_CREATE, 0o600)
		if err != nil {
			return nil, flags, sqlite3vfs.CantOpenError
		}
		onClose = vfs.trackTempFile(canonical, path)
	}

	return newLocalTempFile(f, deleteOnClose, onClose), flags, nil
}

func (vfs *VFS) deleteTempFile(name string) error {
	path, ok := vfs.loadTempFilePath(name)
	if !ok {
		if vfs.wasTempFileName(name) {
			vfs.unregisterTempFile(name)
			return os.ErrNotExist
		}
		return errTempFileNotFound
	}
	if err := os.Remove(path); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	vfs.unregisterTempFile(name)
	return nil
}

func (vfs *VFS) isTempFileName(name string) bool {
	_, ok := vfs.loadTempFilePath(name)
	return ok
}

func (vfs *VFS) wasTempFileName(name string) bool {
	canonical := vfs.canonicalTempName(name)
	if canonical == "" {
		return false
	}
	_, ok := vfs.tempNames.Load(canonical)
	return ok
}

func (vfs *VFS) unregisterTempFile(name string) {
	canonical := vfs.canonicalTempName(name)
	if canonical == "" {
		return
	}
	vfs.tempFiles.Delete(canonical)
}

func (vfs *VFS) accessTempFile(name string, flag sqlite3vfs.AccessFlag) (bool, error) {
	path, ok := vfs.loadTempFilePath(name)
	if !ok {
		return false, nil
	}
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (vfs *VFS) trackTempFile(canonical, path string) func() {
	if canonical == "" {
		return func() {}
	}
	vfs.tempFiles.Store(canonical, path)
	vfs.tempNames.Store(canonical, struct{}{})
	return func() { vfs.tempFiles.Delete(canonical) }
}

func (vfs *VFS) loadTempFilePath(name string) (string, bool) {
	canonical := vfs.canonicalTempName(name)
	if canonical == "" {
		return "", false
	}
	if path, ok := vfs.tempFiles.Load(canonical); ok {
		return path.(string), true
	}
	return "", false
}

func openFlagToOSFlag(flag sqlite3vfs.OpenFlag) int {
	var v int
	if flag&sqlite3vfs.OpenReadWrite != 0 {
		v |= os.O_RDWR
	} else if flag&sqlite3vfs.OpenReadOnly != 0 {
		v |= os.O_RDONLY
	}
	if flag&sqlite3vfs.OpenCreate != 0 {
		v |= os.O_CREATE
	}
	if flag&sqlite3vfs.OpenExclusive != 0 {
		v |= os.O_EXCL
	}
	return v
}

var errTempFileNotFound = fmt.Errorf("temp file not tracked")

// localTempFile fulfills sqlite3vfs.File solely for SQLite temp & transient files.
// These files stay on the local filesystem and optionally delete themselves
// when SQLite closes them (DeleteOnClose flag).
type localTempFile struct {
	f             *os.File
	deleteOnClose bool
	lockType      atomic.Int32
	onClose       func()
}

func newLocalTempFile(f *os.File, deleteOnClose bool, onClose func()) *localTempFile {
	return &localTempFile{f: f, deleteOnClose: deleteOnClose, onClose: onClose}
}

func (tf *localTempFile) Close() error {
	err := tf.f.Close()
	if tf.deleteOnClose {
		if removeErr := os.Remove(tf.f.Name()); removeErr != nil && !os.IsNotExist(removeErr) && err == nil {
			err = removeErr
		}
	}
	if tf.onClose != nil {
		tf.onClose()
	}
	return err
}

func (tf *localTempFile) ReadAt(p []byte, off int64) (n int, err error) {
	return tf.f.ReadAt(p, off)
}

func (tf *localTempFile) WriteAt(b []byte, off int64) (n int, err error) {
	return tf.f.WriteAt(b, off)
}

func (tf *localTempFile) Truncate(size int64) error {
	return tf.f.Truncate(size)
}

func (tf *localTempFile) Sync(flag sqlite3vfs.SyncType) error {
	return tf.f.Sync()
}

func (tf *localTempFile) FileSize() (int64, error) {
	info, err := tf.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (tf *localTempFile) Lock(elock sqlite3vfs.LockType) error {
	if elock == sqlite3vfs.LockNone {
		return nil
	}
	tf.lockType.Store(int32(elock))
	return nil
}

func (tf *localTempFile) Unlock(elock sqlite3vfs.LockType) error {
	tf.lockType.Store(int32(elock))
	return nil
}

func (tf *localTempFile) CheckReservedLock() (bool, error) {
	return sqlite3vfs.LockType(tf.lockType.Load()) >= sqlite3vfs.LockReserved, nil
}

func (tf *localTempFile) SectorSize() int64 {
	return 0
}

func (tf *localTempFile) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return 0
}

// VFSFile implements the SQLite VFS file interface.
type VFSFile struct {
	mu     sync.Mutex
	client ReplicaClient
	name   string

	pos             ltx.Pos  // Last TXID read from level 0 or 1
	maxTXID1        ltx.TXID // Last TXID read from level 1
	index           map[uint32]ltx.PageIndexElem
	pending         map[uint32]ltx.PageIndexElem
	pendingReplace  bool
	cache           *lru.Cache[uint32, []byte] // LRU cache for page data
	targetTime      *time.Time                 // Target view time; nil means latest
	latestLTXTime   time.Time                  // Timestamp of most recent LTX file
	lastPollSuccess time.Time                  // Time of last successful poll
	lockType        sqlite3vfs.LockType        // Current lock state
	pageSize        uint32
	commit          uint32

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	logger *slog.Logger

	PollInterval time.Duration
	CacheSize    int
}

func NewVFSFile(client ReplicaClient, name string, logger *slog.Logger) *VFSFile {
	f := &VFSFile{
		client:       client,
		name:         name,
		index:        make(map[uint32]ltx.PageIndexElem),
		pending:      make(map[uint32]ltx.PageIndexElem),
		logger:       logger,
		PollInterval: DefaultPollInterval,
		CacheSize:    DefaultCacheSize,
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

// MaxTXID1 returns the last TXID read from level 1.
func (f *VFSFile) MaxTXID1() ltx.TXID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxTXID1
}

// LockType returns the current lock type of the file.
func (f *VFSFile) LockType() sqlite3vfs.LockType {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lockType
}

// TargetTime returns the current target time for the VFS file (nil for latest).
func (f *VFSFile) TargetTime() *time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.targetTime == nil {
		return nil
	}
	t := *f.targetTime
	return &t
}

// LatestLTXTime returns the timestamp of the most recent LTX file.
func (f *VFSFile) LatestLTXTime() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.latestLTXTime
}

// LastPollSuccess returns the time of the last successful poll.
func (f *VFSFile) LastPollSuccess() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastPollSuccess
}

func (f *VFSFile) hasTargetTime() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.targetTime != nil
}

func (f *VFSFile) Open() error {
	f.logger.Debug("opening file")

	infos, err := f.waitForRestorePlan()
	if err != nil {
		return err
	}

	pageSize, err := detectPageSizeFromInfos(f.ctx, f.client, infos)
	if err != nil {
		f.logger.Error("cannot detect page size", "error", err)
		return fmt.Errorf("detect page size: %w", err)
	}
	f.pageSize = pageSize

	// Initialize page cache. Convert byte size to number of pages.
	cacheEntries := f.CacheSize / int(pageSize)
	if cacheEntries < 1 {
		cacheEntries = 1
	}
	cache, err := lru.New[uint32, []byte](cacheEntries)
	if err != nil {
		return fmt.Errorf("create page cache: %w", err)
	}
	f.cache = cache

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

// SetTargetTime rebuilds the page index to view the database at a specific time.
func (f *VFSFile) SetTargetTime(ctx context.Context, timestamp time.Time) error {
	if timestamp.IsZero() {
		return fmt.Errorf("target time required")
	}

	infos, err := CalcRestorePlan(ctx, f.client, 0, timestamp, f.logger)
	if err != nil {
		return fmt.Errorf("cannot calc restore plan: %w", err)
	} else if len(infos) == 0 {
		return fmt.Errorf("no backup files available")
	}

	return f.rebuildIndex(ctx, infos, &timestamp)
}

// ResetTime rebuilds the page index to the latest available state.
func (f *VFSFile) ResetTime(ctx context.Context) error {
	infos, err := CalcRestorePlan(ctx, f.client, 0, time.Time{}, f.logger)
	if err != nil {
		return fmt.Errorf("cannot calc restore plan: %w", err)
	} else if len(infos) == 0 {
		return fmt.Errorf("no backup files available")
	}

	return f.rebuildIndex(ctx, infos, nil)
}

// rebuildIndex constructs a fresh page index and swaps it into the VFSFile.
func (f *VFSFile) rebuildIndex(ctx context.Context, infos []*ltx.FileInfo, target *time.Time) error {
	index, err := f.buildIndexMap(ctx, infos)
	if err != nil {
		return err
	}

	var pos ltx.Pos
	if len(infos) > 0 {
		pos = ltx.Pos{TXID: infos[len(infos)-1].MaxTXID}
	}

	maxTXID1 := maxLevelTXID(infos, 1)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.index = index
	f.pending = make(map[uint32]ltx.PageIndexElem)
	f.pendingReplace = false
	f.pos = pos
	f.maxTXID1 = maxTXID1
	if len(infos) > 0 {
		f.latestLTXTime = infos[len(infos)-1].CreatedAt
	}
	if f.cache != nil {
		f.cache.Purge()
	}
	if target == nil {
		f.targetTime = nil
	} else {
		t := *target
		f.targetTime = &t
	}

	return nil
}

func maxLevelTXID(infos []*ltx.FileInfo, level int) ltx.TXID {
	var maxTXID ltx.TXID
	for _, info := range infos {
		if info.Level == level && info.MaxTXID > maxTXID {
			maxTXID = info.MaxTXID
		}
	}
	return maxTXID
}

// buildIndexMap constructs a lookup of pgno to LTX file offsets.
func (f *VFSFile) buildIndexMap(ctx context.Context, infos []*ltx.FileInfo) (map[uint32]ltx.PageIndexElem, error) {
	index := make(map[uint32]ltx.PageIndexElem)
	var commit uint32
	for _, info := range infos {
		f.logger.Debug("opening page index", "level", info.Level, "min", info.MinTXID, "max", info.MaxTXID)

		// Read page index.
		idx, err := FetchPageIndex(ctx, f.client, info)
		if err != nil {
			return nil, fmt.Errorf("fetch page index: %w", err)
		}

		// Replace pages in overall index with new pages.
		for k, v := range idx {
			f.logger.Debug("adding page index", "page", k, "elem", v)
			index[k] = v
		}
		hdr, err := FetchLTXHeader(ctx, f.client, info)
		if err != nil {
			return nil, fmt.Errorf("fetch header: %w", err)
		}
		commit = hdr.Commit
	}

	f.mu.Lock()
	f.commit = commit
	f.mu.Unlock()

	return index, nil
}

// buildIndex constructs a lookup of pgno to LTX file offsets (legacy wrapper).
func (f *VFSFile) buildIndex(ctx context.Context, infos []*ltx.FileInfo) error {
	return f.rebuildIndex(ctx, infos, nil)
}

func (f *VFSFile) Close() error {
	f.logger.Debug("closing file")
	f.cancel()
	f.wg.Wait()
	return nil
}

func (f *VFSFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.logger.Debug("reading at", "off", off, "len", len(p))
	pageSize, err := f.pageSizeBytes()
	if err != nil {
		return 0, err
	}

	pgno := uint32(off/int64(pageSize)) + 1

	// Check cache first (cache is thread-safe)
	if data, ok := f.cache.Get(pgno); ok {
		pageOffset := int(off % int64(pageSize))
		n = copy(p, data[pageOffset:])
		f.logger.Debug("cache hit", "page", pgno, "n", n)

		// Update the first page to pretend like we are in journal mode.
		if off == 0 {
			p[18], p[19] = 0x01, 0x01
			_, _ = rand.Read(p[24:28])
		}

		return n, nil
	}

	// Get page index element
	f.mu.Lock()
	elem, ok := f.index[pgno]
	f.mu.Unlock()

	if !ok {
		f.logger.Error("page not found", "page", pgno)
		return 0, fmt.Errorf("page not found: %d", pgno)
	}

	var data []byte
	var lastErr error
	ctx := f.ctx
	for attempt := 0; attempt < pageFetchRetryAttempts; attempt++ {
		_, data, lastErr = FetchPage(ctx, f.client, elem.Level, elem.MinTXID, elem.MaxTXID, elem.Offset, elem.Size)
		if lastErr == nil {
			break
		}
		if !isRetryablePageError(lastErr) {
			f.logger.Error("cannot fetch page", "page", pgno, "attempt", attempt+1, "error", lastErr)
			return 0, fmt.Errorf("fetch page: %w", lastErr)
		}

		if attempt == pageFetchRetryAttempts-1 {
			f.logger.Error("cannot fetch page after retries", "page", pgno, "attempts", pageFetchRetryAttempts, "error", lastErr)
			return 0, sqlite3vfs.BusyError
		}

		delay := pageFetchRetryDelay * time.Duration(attempt+1)
		f.logger.Warn("transient page fetch error, retrying", "page", pgno, "attempt", attempt+1, "delay", delay, "error", lastErr)

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-f.ctx.Done():
			timer.Stop()
			return 0, fmt.Errorf("fetch page: %w", lastErr)
		}
		timer.Stop()
	}

	// Add to cache (cache is thread-safe)
	f.cache.Add(pgno, data)

	pageOffset := int(off % int64(pageSize))
	n = copy(p, data[pageOffset:])
	f.logger.Debug("data read from storage", "page", pgno, "n", n, "data", len(data))

	// Update the first page to pretend like we are in journal mode.
	if off == 0 {
		p[18], p[19] = 0x01, 0x01
		_, _ = rand.Read(p[24:28])
	}

	return n, nil
}

func (f *VFSFile) WriteAt(b []byte, off int64) (n int, err error) {
	f.logger.Debug("write at", "off", off, "len", len(b))
	return 0, fmt.Errorf("litestream is a read only vfs")
}

func (f *VFSFile) Truncate(size int64) error {
	f.logger.Debug("truncating file", "size", size)
	return fmt.Errorf("litestream is a read only vfs")
}

func (f *VFSFile) Sync(flag sqlite3vfs.SyncType) error {
	f.logger.Debug("syncing file", "flag", flag)
	return nil
}

func (f *VFSFile) FileSize() (size int64, err error) {
	pageSize, err := f.pageSizeBytes()
	if err != nil {
		return 0, err
	}

	f.mu.Lock()
	for pgno := range f.index {
		if v := int64(pgno) * int64(pageSize); v > size {
			size = v
		}
	}
	for pgno := range f.pending {
		if v := int64(pgno) * int64(pageSize); v > size {
			size = v
		}
	}
	f.mu.Unlock()

	f.logger.Debug("file size", "size", size)
	return size, nil
}

func (f *VFSFile) Lock(elock sqlite3vfs.LockType) error {
	f.logger.Debug("locking file", "lock", elock)

	f.mu.Lock()
	defer f.mu.Unlock()

	if elock < f.lockType {
		return fmt.Errorf("invalid lock downgrade: current=%s target=%s", f.lockType, elock)
	}
	f.lockType = elock
	return nil
}

func (f *VFSFile) Unlock(elock sqlite3vfs.LockType) error {
	f.logger.Debug("unlocking file", "lock", elock)

	f.mu.Lock()
	defer f.mu.Unlock()

	if elock != sqlite3vfs.LockShared && elock != sqlite3vfs.LockNone {
		return fmt.Errorf("invalid unlock target: %s", elock)
	}

	f.lockType = elock

	// Copy pending index to main index and invalidate affected pages in cache.
	if f.pendingReplace {
		// Replace entire index
		count := len(f.index)
		f.index = f.pending
		f.logger.Debug("cache invalidated all pages", "count", count)
		// Invalidate entire cache since we replaced the index
		f.cache.Purge()
	} else if len(f.pending) > 0 {
		// Merge pending into index
		count := len(f.pending)
		for k, v := range f.pending {
			f.index[k] = v
			f.cache.Remove(k)
		}
		f.logger.Debug("cache invalidated pages", "count", count)
	}
	f.pending = make(map[uint32]ltx.PageIndexElem)
	f.pendingReplace = false

	return nil
}

func (f *VFSFile) CheckReservedLock() (bool, error) {
	f.logger.Debug("checking reserved lock")
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lockType >= sqlite3vfs.LockReserved, nil
}

func (f *VFSFile) SectorSize() int64 {
	f.logger.Debug("sector size")
	return 0
}

func (f *VFSFile) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	f.logger.Debug("device characteristics")
	return 0
}

// parseTimeValue parses a timestamp string, trying RFC3339 first, then relative expressions.
func parseTimeValue(value string) (time.Time, error) {
	// Try RFC3339Nano first (existing behavior)
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t, nil
	}

	// Try RFC3339 (without nanoseconds)
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, nil
	}

	// Fall back to dateparser for relative expressions
	cfg := &dateparser.Configuration{
		CurrentTime: time.Now().UTC(),
	}
	result, err := dateparser.Parse(cfg, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timestamp (expected RFC3339 or relative time like '5 minutes ago'): %s", value)
	}
	if result.Time.IsZero() {
		return time.Time{}, fmt.Errorf("could not parse time: %s", value)
	}
	return result.Time.UTC(), nil
}

// FileControl handles file control operations, specifically PRAGMA commands for time travel.
func (f *VFSFile) FileControl(op int, pragmaName string, pragmaValue *string) (*string, error) {
	const SQLITE_FCNTL_PRAGMA = 14

	if op != SQLITE_FCNTL_PRAGMA {
		return nil, fmt.Errorf("unsupported file control op: %d", op)
	}

	name := strings.ToLower(pragmaName)

	f.logger.Debug("file control", "pragma", name, "value", pragmaValue)

	switch name {
	case "litestream_txid":
		if pragmaValue != nil {
			return nil, fmt.Errorf("litestream_txid is read-only")
		}
		txid := f.Pos().TXID
		result := txid.String()
		return &result, nil

	case "litestream_lag":
		if pragmaValue != nil {
			return nil, fmt.Errorf("litestream_lag is read-only")
		}
		lastPoll := f.LastPollSuccess()
		if lastPoll.IsZero() {
			result := "-1" // Never polled successfully
			return &result, nil
		}
		lag := int64(time.Since(lastPoll).Seconds())
		result := strconv.FormatInt(lag, 10)
		return &result, nil

	case "litestream_time":
		if pragmaValue == nil {
			result := f.currentTimeString()
			return &result, nil
		}

		if strings.EqualFold(*pragmaValue, "latest") {
			if err := f.ResetTime(context.Background()); err != nil {
				return nil, err
			}
			return nil, nil
		}

		t, err := parseTimeValue(*pragmaValue)
		if err != nil {
			return nil, err
		}
		if err := f.SetTargetTime(context.Background(), t); err != nil {
			return nil, err
		}
		return nil, nil

	default:
		return nil, sqlite3vfs.NotFoundError
	}
}

// currentTimeString returns the current target time as a string.
func (f *VFSFile) currentTimeString() string {
	if t := f.TargetTime(); t != nil {
		return t.Format(time.RFC3339Nano)
	}
	if t := f.LatestLTXTime(); !t.IsZero() {
		return t.Format(time.RFC3339Nano)
	}
	return "latest" // Fallback if no LTX files loaded
}

func isRetryablePageError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	// Some remote clients wrap EOF in custom errors so we fall back to string matching.
	if strings.Contains(err.Error(), "unexpected EOF") {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	return false
}

func (f *VFSFile) monitorReplicaClient(ctx context.Context) {
	ticker := time.NewTicker(f.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if f.hasTargetTime() {
				continue
			}
			if err := f.pollReplicaClient(ctx); err != nil {
				// Don't log context cancellation errors during shutdown
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					f.logger.Error("cannot fetch new ltx files", "error", err)
				}
			} else {
				// Track successful poll time
				f.mu.Lock()
				f.lastPollSuccess = time.Now()
				f.mu.Unlock()
			}
		}
	}
}

// pollReplicaClient fetches new LTX files from the replica client and updates
// the page index & the current position.
func (f *VFSFile) pollReplicaClient(ctx context.Context) error {
	pos := f.Pos()
	f.logger.Debug("polling replica client", "txid", pos.TXID.String())

	combined := make(map[uint32]ltx.PageIndexElem)
	baseCommit := f.commit
	newCommit := baseCommit
	replaceIndex := false

	maxTXID0, idx0, commit0, replace0, err := f.pollLevel(ctx, 0, pos.TXID, baseCommit)
	if err != nil {
		return fmt.Errorf("poll L0: %w", err)
	}
	if replace0 {
		replaceIndex = true
		baseCommit = commit0
		newCommit = commit0
		combined = idx0
	} else {
		if len(idx0) > 0 {
			baseCommit = commit0
		}
		for k, v := range idx0 {
			combined[k] = v
		}
		if commit0 > newCommit {
			newCommit = commit0
		}
	}

	maxTXID1, idx1, commit1, replace1, err := f.pollLevel(ctx, 1, f.maxTXID1, baseCommit)
	if err != nil {
		return fmt.Errorf("poll L1: %w", err)
	}
	if replace1 {
		replaceIndex = true
		baseCommit = commit1
		newCommit = commit1
		combined = idx1
	} else {
		for k, v := range idx1 {
			combined[k] = v
		}
		if commit1 > newCommit {
			newCommit = commit1
		}
	}

	// Send updates to a pending list if there are active readers.
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.targetTime != nil {
		// Skip applying updates while time travel is active to avoid
		// overwriting the historical snapshot state.
		return nil
	}

	// Apply updates and invalidate cache entries for updated pages
	invalidateN := 0
	target := f.index
	targetIsMain := true
	if f.lockType >= sqlite3vfs.LockShared {
		target = f.pending
		targetIsMain = false
	} else {
		f.pendingReplace = false
	}
	if replaceIndex {
		if f.lockType < sqlite3vfs.LockShared {
			f.index = make(map[uint32]ltx.PageIndexElem)
			target = f.index
			targetIsMain = true
			f.pendingReplace = false
		} else {
			f.pending = make(map[uint32]ltx.PageIndexElem)
			target = f.pending
			targetIsMain = false
			f.pendingReplace = true
		}
	}
	for k, v := range combined {
		target[k] = v
		// Invalidate cache if we're updating the main index
		if targetIsMain {
			f.cache.Remove(k)
			invalidateN++
		}
	}

	if invalidateN > 0 {
		f.logger.Debug("cache invalidated pages due to new ltx files", "count", invalidateN)
	}

	if replaceIndex {
		f.commit = newCommit
	} else if len(combined) > 0 && newCommit > f.commit {
		f.commit = newCommit
	}

	if maxTXID0 > maxTXID1 {
		f.pos.TXID = maxTXID0
	} else {
		f.pos.TXID = maxTXID1
	}

	f.maxTXID1 = maxTXID1
	f.logger.Debug("txid updated", "txid", f.pos.TXID.String(), "maxTXID1", f.maxTXID1.String())

	return nil
}

// pollLevel fetches LTX files for a specific level and returns the highest TXID seen,
// any index updates, the latest commit value, and if the index should be replaced.
func (f *VFSFile) pollLevel(ctx context.Context, level int, prevMaxTXID ltx.TXID, baseCommit uint32) (ltx.TXID, map[uint32]ltx.PageIndexElem, uint32, bool, error) {
	itr, err := f.client.LTXFiles(ctx, level, prevMaxTXID+1, false)
	if err != nil {
		return prevMaxTXID, nil, baseCommit, false, fmt.Errorf("ltx files: %w", err)
	}
	defer func() { _ = itr.Close() }()

	index := make(map[uint32]ltx.PageIndexElem)
	maxTXID := prevMaxTXID
	lastCommit := baseCommit
	newCommit := baseCommit
	replaceIndex := false

	for itr.Next() {
		info := itr.Item()

		f.mu.Lock()
		isNextTXID := info.MinTXID == maxTXID+1
		f.mu.Unlock()
		if !isNextTXID {
			if level == 0 && info.MinTXID > maxTXID+1 {
				f.logger.Warn("ltx gap detected at L0, deferring to higher levels", "expected", maxTXID+1, "next", info.MinTXID)
				break
			}
			return maxTXID, nil, newCommit, replaceIndex, fmt.Errorf("non-contiguous ltx file: level=%d, current=%s, next=%s-%s", level, maxTXID, info.MinTXID, info.MaxTXID)
		}

		f.logger.Debug("new ltx file", "level", info.Level, "min", info.MinTXID, "max", info.MaxTXID)

		idx, err := FetchPageIndex(ctx, f.client, info)
		if err != nil {
			return maxTXID, nil, newCommit, replaceIndex, fmt.Errorf("fetch page index: %w", err)
		}
		hdr, err := FetchLTXHeader(ctx, f.client, info)
		if err != nil {
			return maxTXID, nil, newCommit, replaceIndex, fmt.Errorf("fetch header: %w", err)
		}

		if hdr.Commit < lastCommit {
			replaceIndex = true
			index = make(map[uint32]ltx.PageIndexElem)
		}
		lastCommit = hdr.Commit
		newCommit = hdr.Commit

		for k, v := range idx {
			f.logger.Debug("adding new page index", "page", k, "elem", v)
			index[k] = v
		}
		maxTXID = info.MaxTXID
	}

	return maxTXID, index, newCommit, replaceIndex, nil
}

func (f *VFSFile) pageSizeBytes() (uint32, error) {
	f.mu.Lock()
	pageSize := f.pageSize
	f.mu.Unlock()
	if pageSize == 0 {
		return 0, fmt.Errorf("page size not initialized")
	}
	return pageSize, nil
}

func detectPageSizeFromInfos(ctx context.Context, client ReplicaClient, infos []*ltx.FileInfo) (uint32, error) {
	var lastErr error
	for i := len(infos) - 1; i >= 0; i-- {
		pageSize, err := readPageSizeFromInfo(ctx, client, infos[i])
		if err != nil {
			lastErr = err
			continue
		}
		if !isSupportedPageSize(pageSize) {
			return 0, fmt.Errorf("unsupported page size: %d", pageSize)
		}
		return pageSize, nil
	}
	if lastErr != nil {
		return 0, fmt.Errorf("read ltx header: %w", lastErr)
	}
	return 0, fmt.Errorf("no ltx file available to determine page size")
}

func readPageSizeFromInfo(ctx context.Context, client ReplicaClient, info *ltx.FileInfo) (uint32, error) {
	rc, err := client.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, 0, ltx.HeaderSize)
	if err != nil {
		return 0, fmt.Errorf("open ltx file: %w", err)
	}
	defer rc.Close()
	dec := ltx.NewDecoder(rc)
	if err := dec.DecodeHeader(); err != nil {
		return 0, fmt.Errorf("decode ltx header: %w", err)
	}
	return dec.Header().PageSize, nil
}

func isSupportedPageSize(pageSize uint32) bool {
	switch pageSize {
	case 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536:
		return true
	default:
		return false
	}
}

func (f *VFSFile) waitForRestorePlan() ([]*ltx.FileInfo, error) {
	for {
		infos, err := CalcRestorePlan(f.ctx, f.client, 0, time.Time{}, f.logger)
		if err == nil {
			return infos, nil
		}
		if !errors.Is(err, ErrTxNotAvailable) {
			return nil, fmt.Errorf("cannot calc restore plan: %w", err)
		}

		f.logger.Debug("no backup files available yet, waiting", "interval", f.PollInterval)
		select {
		case <-time.After(f.PollInterval):
		case <-f.ctx.Done():
			return nil, fmt.Errorf("no backup files available: %w", f.ctx.Err())
		}
	}
}

// RegisterVFSConnection maps a SQLite connection handle to its VFS file ID.
func RegisterVFSConnection(dbPtr uintptr, fileID uint64) error {
	if _, ok := lookupVFSFile(fileID); !ok {
		return fmt.Errorf("vfs file not found: id=%d", fileID)
	}
	vfsConnectionMap.Store(dbPtr, fileID)
	return nil
}

// UnregisterVFSConnection removes a connection mapping.
func UnregisterVFSConnection(dbPtr uintptr) {
	vfsConnectionMap.Delete(dbPtr)
}

// SetVFSConnectionTime rebuilds the VFS index for a connection at a timestamp.
func SetVFSConnectionTime(dbPtr uintptr, timestamp string) error {
	file, err := vfsFileForConnection(dbPtr)
	if err != nil {
		return err
	}

	t, err := parseTimeValue(timestamp)
	if err != nil {
		return err
	}
	return file.SetTargetTime(context.Background(), t)
}

// ResetVFSConnectionTime rebuilds the VFS index to the latest state.
func ResetVFSConnectionTime(dbPtr uintptr) error {
	file, err := vfsFileForConnection(dbPtr)
	if err != nil {
		return err
	}
	return file.ResetTime(context.Background())
}

// GetVFSConnectionTime returns the current time for a connection.
func GetVFSConnectionTime(dbPtr uintptr) (string, error) {
	file, err := vfsFileForConnection(dbPtr)
	if err != nil {
		return "", err
	}
	return file.currentTimeString(), nil
}

// GetVFSConnectionTXID returns the current transaction ID for a connection as a hex string.
func GetVFSConnectionTXID(dbPtr uintptr) (string, error) {
	file, err := vfsFileForConnection(dbPtr)
	if err != nil {
		return "", err
	}
	return file.Pos().TXID.String(), nil
}

// GetVFSConnectionLag returns seconds since last successful poll for a connection.
func GetVFSConnectionLag(dbPtr uintptr) (int64, error) {
	file, err := vfsFileForConnection(dbPtr)
	if err != nil {
		return 0, err
	}
	lastPoll := file.LastPollSuccess()
	if lastPoll.IsZero() {
		return -1, nil
	}
	return int64(time.Since(lastPoll).Seconds()), nil
}

func vfsFileForConnection(dbPtr uintptr) (*VFSFile, error) {
	v, ok := vfsConnectionMap.Load(dbPtr)
	if !ok {
		return nil, fmt.Errorf("connection not registered")
	}
	fileID, ok := v.(uint64)
	if !ok {
		return nil, fmt.Errorf("invalid connection mapping")
	}
	file, ok := lookupVFSFile(fileID)
	if !ok {
		return nil, fmt.Errorf("vfs file not found: id=%d", fileID)
	}
	return file, nil
}

func lookupVFSFile(fileID uint64) (*VFSFile, bool) {
	sqlite3vfsFileMux.Lock()
	defer sqlite3vfsFileMux.Unlock()

	file, ok := sqlite3vfsFileMap[fileID]
	if !ok {
		return nil, false
	}

	vfsFile, ok := file.(*VFSFile)
	return vfsFile, ok
}
