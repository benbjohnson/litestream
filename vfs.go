//go:build vfs
// +build vfs

package litestream

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
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
	slog.Info("opening file", "name", name, "flags", flags)

	switch {
	case flags&sqlite3vfs.OpenMainDB != 0:
		return vfs.openMainDB(name, flags)
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

	pos             ltx.Pos  // Last TXID read from level 0 or 1
	maxTXID1        ltx.TXID // Last TXID read from level 1
	index           map[uint32]ltx.PageIndexElem
	pending         map[uint32]ltx.PageIndexElem
	cache           *lru.Cache[uint32, []byte] // LRU cache for page data
	targetTime      *time.Time                 // Target view time; nil means latest
	latestLTXTime   time.Time                  // Timestamp of most recent LTX file
	lastPollSuccess time.Time                  // Time of last successful poll
	lockType        sqlite3vfs.LockType        // Current lock state

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

func (f *VFSFile) Open() error {
	f.logger.Info("opening file")

	// Initialize page cache. Convert byte size to number of 4KB pages.
	const pageSize = 4096
	cacheEntries := f.CacheSize / pageSize
	if cacheEntries < 1 {
		cacheEntries = 1
	}
	cache, err := lru.New[uint32, []byte](cacheEntries)
	if err != nil {
		return fmt.Errorf("create page cache: %w", err)
	}
	f.cache = cache

	infos, err := CalcRestorePlan(context.Background(), f.client, 0, time.Time{}, f.logger)
	if err != nil {
		f.logger.Error("cannot calc restore plan", "error", err)
		return fmt.Errorf("cannot calc restore plan: %w", err)
	} else if len(infos) == 0 {
		f.logger.Error("no backup files available")
		return fmt.Errorf("no backup files available") // TODO: Open even when no files available.
	}

	// Build the page index so we can lookup individual pages.
	if err := f.rebuildIndex(f.ctx, infos, nil); err != nil {
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

// buildIndexMap constructs a lookup of pgno to LTX file offsets.
func (f *VFSFile) buildIndexMap(ctx context.Context, infos []*ltx.FileInfo) (map[uint32]ltx.PageIndexElem, error) {
	index := make(map[uint32]ltx.PageIndexElem)
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
	}
	return index, nil
}

// buildIndex constructs a lookup of pgno to LTX file offsets.
func (f *VFSFile) buildIndex(ctx context.Context, infos []*ltx.FileInfo) error {
	return f.rebuildIndex(ctx, infos, nil)
}

func (f *VFSFile) Close() error {
	f.logger.Info("closing file")
	f.cancel()
	f.wg.Wait()
	return nil
}

func (f *VFSFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.logger.Info("reading at", "off", off, "len", len(p))
	pgno := uint32(off/4096) + 1

	// Check cache first (cache is thread-safe)
	if data, ok := f.cache.Get(pgno); ok {
		n = copy(p, data[off%4096:])
		f.logger.Info("cache hit", "page", pgno, "n", n)

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

	// Fetch from storage (cache miss)
	_, data, err := FetchPage(context.Background(), f.client, elem.Level, elem.MinTXID, elem.MaxTXID, elem.Offset, elem.Size)
	if err != nil {
		f.logger.Error("cannot fetch page", "error", err)
		return 0, fmt.Errorf("fetch page: %w", err)
	}

	// Add to cache (cache is thread-safe)
	f.cache.Add(pgno, data)

	n = copy(p, data[off%4096:])
	f.logger.Info("data read from storage", "page", pgno, "n", n, "data", len(data))

	// Update the first page to pretend like we are in journal mode.
	if off == 0 {
		p[18], p[19] = 0x01, 0x01
		_, _ = rand.Read(p[24:28])
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

	f.mu.Lock()
	for pgno := range f.index {
		if int64(pgno)*pageSize > int64(size) {
			size = int64(pgno * pageSize)
		}
	}
	f.mu.Unlock()

	f.logger.Info("file size", "size", size)
	return size, nil
}

func (f *VFSFile) Lock(elock sqlite3vfs.LockType) error {
	f.logger.Info("locking file", "lock", elock)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.lockType = elock
	return nil
}

func (f *VFSFile) Unlock(elock sqlite3vfs.LockType) error {
	f.logger.Info("unlocking file", "lock", elock)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.lockType = elock

	// Copy pending index to main index and invalidate affected pages in cache.
	if len(f.pending) > 0 {
		count := len(f.pending)
		for k, v := range f.pending {
			f.index[k] = v
			f.cache.Remove(k)
		}
		f.pending = make(map[uint32]ltx.PageIndexElem)
		f.logger.Debug("cache invalidated pages", "count", count)
	}

	return nil
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

	f.logger.Info("file control", "pragma", name, "value", pragmaValue)

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
	index := make(map[uint32]ltx.PageIndexElem)
	f.logger.Debug("polling replica client", "txid", pos.TXID.String())

	result0, err := f.pollLevel(ctx, 0, pos.TXID, index)
	if err != nil {
		return fmt.Errorf("poll L0: %w", err)
	}

	result1, err := f.pollLevel(ctx, 1, f.maxTXID1, index)
	if err != nil {
		return fmt.Errorf("poll L1: %w", err)
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
	for k, v := range index {
		// If we are holding a shared lock, add to pending index instead of main index.
		// We will copy these over once the shared lock is released.
		if f.lockType >= sqlite3vfs.LockShared {
			f.pending[k] = v
			continue
		}

		// Otherwise update main index and invalidate cache entry.
		f.index[k] = v
		f.cache.Remove(k)
		invalidateN++
	}

	if invalidateN > 0 {
		f.logger.Debug("cache invalidated pages due to new ltx files", "count", invalidateN)
	}

	// Update to max TXID
	f.pos.TXID = max(result0.maxTXID, result1.maxTXID)
	f.maxTXID1 = result1.maxTXID
	f.logger.Debug("txid updated", "txid", f.pos.TXID.String(), "maxTXID1", f.maxTXID1.String())

	// Update latestLTXTime to the most recent timestamp from either level
	if !result0.createdAt.IsZero() && result0.createdAt.After(f.latestLTXTime) {
		f.latestLTXTime = result0.createdAt
	}
	if !result1.createdAt.IsZero() && result1.createdAt.After(f.latestLTXTime) {
		f.latestLTXTime = result1.createdAt
	}

	return nil
}

func (f *VFSFile) hasTargetTime() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.targetTime != nil
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

// pollLevelResult contains the results of polling a single level.
type pollLevelResult struct {
	maxTXID   ltx.TXID
	createdAt time.Time // CreatedAt of the most recent LTX file processed
}

func (f *VFSFile) pollLevel(ctx context.Context, level int, prevMaxTXID ltx.TXID, index map[uint32]ltx.PageIndexElem) (pollLevelResult, error) {
	// Start reading from the next LTX file after the current position.
	itr, err := f.client.LTXFiles(ctx, level, prevMaxTXID+1, false)
	if err != nil {
		return pollLevelResult{}, fmt.Errorf("ltx files: %w", err)
	}

	// Build an update across all new LTX files.
	result := pollLevelResult{maxTXID: prevMaxTXID}
	for itr.Next() {
		info := itr.Item()

		// Ensure we are fetching the next transaction from our current position.
		f.mu.Lock()
		isNextTXID := info.MinTXID == result.maxTXID+1
		f.mu.Unlock()
		if !isNextTXID {
			return result, fmt.Errorf("non-contiguous ltx file: level=%d, current=%s, next=%s-%s", level, prevMaxTXID, info.MinTXID, info.MaxTXID)
		}

		f.logger.Debug("new ltx file", "level", info.Level, "min", info.MinTXID, "max", info.MaxTXID)

		// Read page index.
		idx, err := FetchPageIndex(context.Background(), f.client, info)
		if err != nil {
			return result, fmt.Errorf("fetch page index: %w", err)
		}

		// Update the page index & current position.
		for k, v := range idx {
			f.logger.Debug("adding new page index", "page", k, "elem", v)
			index[k] = v
		}
		result.maxTXID = info.MaxTXID
		result.createdAt = info.CreatedAt
	}

	return result, nil
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
