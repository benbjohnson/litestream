package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/benbjohnson/litestream"
)

const debounceInterval = 250 * time.Millisecond

// DirectoryMonitor watches a directory tree for SQLite databases and dynamically
// manages database instances within the store as files are created or removed.
type DirectoryMonitor struct {
	store     *litestream.Store
	config    *DBConfig
	dirPath   string
	pattern   string
	recursive bool

	watcher *fsnotify.Watcher
	ctx     context.Context
	cancel  context.CancelFunc

	logger *slog.Logger

	mu          sync.Mutex
	dbs         map[string]*litestream.DB
	watchedDirs map[string]struct{}

	// Only accessed from the run() goroutine, so no mutex is needed.
	pendingEvents  map[string]fsnotify.Op
	debounceActive bool

	wg sync.WaitGroup
}

// NewDirectoryMonitor returns a new monitor for directory-based replication.
func NewDirectoryMonitor(ctx context.Context, store *litestream.Store, dbc *DBConfig, existing []*litestream.DB) (*DirectoryMonitor, error) {
	if dbc == nil {
		return nil, errors.New("database config required")
	}
	if store == nil {
		return nil, errors.New("store required")
	}

	dirPath, err := expand(dbc.Dir)
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(dirPath); err != nil {
		return nil, err
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	monitorCtx, cancel := context.WithCancel(ctx)
	dm := &DirectoryMonitor{
		store:         store,
		config:        dbc,
		dirPath:       dirPath,
		pattern:       dbc.Pattern,
		recursive:     dbc.Recursive,
		watcher:       watcher,
		ctx:           monitorCtx,
		cancel:        cancel,
		logger:        slog.With("dir", dirPath),
		dbs:           make(map[string]*litestream.DB),
		watchedDirs:   make(map[string]struct{}),
		pendingEvents: make(map[string]fsnotify.Op),
	}

	for _, db := range existing {
		dm.dbs[db.Path()] = db
	}

	if err := dm.addInitialWatches(); err != nil {
		watcher.Close()
		cancel()
		return nil, err
	}

	dm.scanDirectory(dm.dirPath)

	dm.wg.Add(1)
	go dm.run()

	return dm, nil
}

// Close stops the directory monitor and releases resources.
func (dm *DirectoryMonitor) Close() {
	dm.cancel()
	_ = dm.watcher.Close()
	dm.wg.Wait()
}

func (dm *DirectoryMonitor) run() {
	defer dm.wg.Done()

	// Debounce timer lives in this goroutine so shutdown via dm.wg.Wait() is clean.
	debounceTimer := time.NewTimer(debounceInterval)
	debounceTimer.Stop()
	defer debounceTimer.Stop()

	for {
		select {
		case <-dm.ctx.Done():
			return
		case event, ok := <-dm.watcher.Events:
			if !ok {
				return
			}
			if dm.handleEvent(event) && !dm.debounceActive {
				debounceTimer.Reset(debounceInterval)
				dm.debounceActive = true
			}
		case <-debounceTimer.C:
			dm.flushPendingEvents()
		case err, ok := <-dm.watcher.Errors:
			if !ok {
				return
			}
			dm.logger.Error("directory watcher error", "error", err)
		}
	}
}

func (dm *DirectoryMonitor) addInitialWatches() error {
	if dm.recursive {
		return filepath.WalkDir(dm.dirPath, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				return nil
			}
			return dm.addDirectoryWatch(path)
		})
	}

	return dm.addDirectoryWatch(dm.dirPath)
}

func (dm *DirectoryMonitor) addDirectoryWatch(path string) error {
	abspath := filepath.Clean(path)

	dm.mu.Lock()
	defer dm.mu.Unlock()

	if _, ok := dm.watchedDirs[abspath]; ok {
		return nil
	}
	dm.watchedDirs[abspath] = struct{}{}

	if err := dm.watcher.Add(abspath); err != nil {
		delete(dm.watchedDirs, abspath)
		return err
	}

	dm.logger.Debug("watching directory", "path", abspath)
	return nil
}

func (dm *DirectoryMonitor) removeDirectoryWatch(path string) {
	abspath := filepath.Clean(path)

	dm.mu.Lock()
	defer dm.mu.Unlock()

	if _, ok := dm.watchedDirs[abspath]; !ok {
		return
	}
	delete(dm.watchedDirs, abspath)

	if err := dm.watcher.Remove(abspath); err != nil {
		dm.logger.Debug("remove directory watch", "path", abspath, "error", err)
	}
}

// handleEvent processes a single fsnotify event. Returns true if pending events
// were queued and the debounce timer should be reset.
func (dm *DirectoryMonitor) handleEvent(event fsnotify.Event) bool {
	path := filepath.Clean(event.Name)
	if path == "" {
		return false
	}

	if dm.shouldSkipPath(path) {
		return false
	}

	info, statErr := os.Stat(path)
	isDir := statErr == nil && info.IsDir()

	// Early pattern check for create/write only. Rename must pass through
	// to removal handling below, since the old path may be a tracked DB.
	if !isDir && event.Op&(fsnotify.Create|fsnotify.Write) != 0 {
		if !dm.matchesPattern(path) {
			return false
		}
	}

	dm.mu.Lock()
	_, wasWatchedDir := dm.watchedDirs[path]
	dm.mu.Unlock()

	if isDir && event.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
		if dm.recursive {
			if err := dm.addDirectoryWatch(path); err != nil {
				dm.logger.Error("add directory watch", "path", path, "error", err)
			}
			dm.scanDirectory(path)
		}
	}

	if (isDir || wasWatchedDir) && event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		dm.removeDirectoryWatch(path)
		dm.removeDatabasesUnder(path)
		return false
	}

	if isDir {
		return false
	}

	if statErr != nil && !os.IsNotExist(statErr) {
		dm.logger.Debug("stat event path", "path", path, "error", statErr)
		return false
	}

	if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		dm.removeDatabase(path)
		return false
	}

	if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) != 0 {
		dm.pendingEvents[path] |= event.Op
		return true
	}

	return false
}

// flushPendingEvents processes all queued potential database events.
func (dm *DirectoryMonitor) flushPendingEvents() {
	for path := range dm.pendingEvents {
		select {
		case <-dm.ctx.Done():
			return
		default:
		}
		dm.handlePotentialDatabase(path)
	}
	dm.pendingEvents = make(map[string]fsnotify.Op)
	dm.debounceActive = false
}

func (dm *DirectoryMonitor) handlePotentialDatabase(path string) {
	if !dm.matchesPattern(path) {
		return
	}

	dm.mu.Lock()
	if _, exists := dm.dbs[path]; exists {
		dm.mu.Unlock()
		return
	}
	dm.dbs[path] = nil
	dm.mu.Unlock()

	var db *litestream.DB
	success := false
	defer func() {
		if !success {
			if db != nil {
				_ = dm.store.UnregisterDB(dm.ctx, db.Path())
			}
			dm.mu.Lock()
			delete(dm.dbs, path)
			dm.mu.Unlock()
		}
	}()

	if !IsSQLiteDatabase(path) {
		return
	}

	var err error
	db, err = newDBFromDirectoryEntry(dm.config, dm.dirPath, path)
	if err != nil {
		dm.logger.Error("configure database", "path", path, "error", err)
		return
	}

	if err := dm.store.RegisterDB(db); err != nil {
		dm.logger.Error("register database with store", "path", path, "error", err)
		return
	}

	dm.mu.Lock()
	dm.dbs[path] = db
	dm.mu.Unlock()

	success = true
	dm.logger.Info("added database to replication", "path", path)
}

func (dm *DirectoryMonitor) removeDatabase(path string) {
	dm.mu.Lock()
	db := dm.dbs[path]
	dm.mu.Unlock()

	if db == nil {
		return
	}

	if err := dm.store.UnregisterDB(dm.ctx, db.Path()); err != nil {
		dm.logger.Error("unregister database from store", "path", path, "error", err)
		return
	}

	dm.mu.Lock()
	delete(dm.dbs, path)
	dm.mu.Unlock()

	dm.logger.Info("removed database from replication", "path", path)
}

func (dm *DirectoryMonitor) removeDatabasesUnder(dir string) {
	prefix := dir + string(os.PathSeparator)

	dm.mu.Lock()
	var toClose []*litestream.DB
	var toClosePaths []string
	for path, db := range dm.dbs {
		if path == dir || strings.HasPrefix(path, prefix) {
			toClose = append(toClose, db)
			toClosePaths = append(toClosePaths, path)
		}
	}
	dm.mu.Unlock()

	for i, db := range toClose {
		if db == nil {
			dm.mu.Lock()
			delete(dm.dbs, toClosePaths[i])
			dm.mu.Unlock()
			continue
		}
		if err := dm.store.UnregisterDB(dm.ctx, db.Path()); err != nil {
			dm.logger.Error("unregister database from store", "path", db.Path(), "error", err)
			continue
		}

		dm.mu.Lock()
		delete(dm.dbs, toClosePaths[i])
		dm.mu.Unlock()
	}
}

func (dm *DirectoryMonitor) matchesPattern(path string) bool {
	matched, err := filepath.Match(dm.pattern, filepath.Base(path))
	if err != nil {
		dm.logger.Error("pattern match failed", "pattern", dm.pattern, "path", path, "error", err)
		return false
	}
	return matched
}

// shouldSkipPath returns true for SQLite auxiliary files (WAL, SHM, journal)
// that generate many events but are never databases themselves.
func (dm *DirectoryMonitor) shouldSkipPath(path string) bool {
	base := filepath.Base(path)
	return strings.HasSuffix(base, "-wal") ||
		strings.HasSuffix(base, "-shm") ||
		strings.HasSuffix(base, "-journal")
}

// scanDirectory discovers pre-existing databases and is also called when new
// directories appear to close the race window between watch registration and file creation.
func (dm *DirectoryMonitor) scanDirectory(dir string) {
	if !dm.recursive {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				dm.logger.Debug("read directory", "path", dir, "error", err)
			}
			return
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			dm.handlePotentialDatabase(path)
		}
		return
	}

	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			dm.logger.Debug("scan directory entry", "path", path, "error", err)
			return nil
		}

		if d.IsDir() {
			if path != dir {
				if err := dm.addDirectoryWatch(path); err != nil {
					dm.logger.Error("add directory watch", "path", path, "error", err)
				}
			}
			return nil
		}

		dm.handlePotentialDatabase(path)
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		dm.logger.Debug("scan directory", "path", dir, "error", err)
	}
}
