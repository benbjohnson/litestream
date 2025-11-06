package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"

	"github.com/benbjohnson/litestream"
)

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

	wg sync.WaitGroup
}

// NewDirectoryMonitor returns a new monitor for directory-based replication.
func NewDirectoryMonitor(ctx context.Context, store *litestream.Store, dbc *DBConfig, existing []*litestream.DB) (*DirectoryMonitor, error) {
	if dbc == nil {
		return nil, errors.New("directory config required")
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
		store:       store,
		config:      dbc,
		dirPath:     dirPath,
		pattern:     dbc.Pattern,
		recursive:   dbc.Recursive,
		watcher:     watcher,
		ctx:         monitorCtx,
		cancel:      cancel,
		logger:      slog.With("dir", dirPath),
		dbs:         make(map[string]*litestream.DB),
		watchedDirs: make(map[string]struct{}),
	}

	for _, db := range existing {
		dm.dbs[db.Path()] = db
	}

	if err := dm.addInitialWatches(); err != nil {
		watcher.Close()
		cancel()
		return nil, err
	}

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

	for {
		select {
		case <-dm.ctx.Done():
			return
		case event, ok := <-dm.watcher.Events:
			if !ok {
				return
			}
			dm.handleEvent(event)
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
	if _, ok := dm.watchedDirs[abspath]; ok {
		dm.mu.Unlock()
		return nil
	}
	dm.watchedDirs[abspath] = struct{}{}
	dm.mu.Unlock()

	if err := dm.watcher.Add(abspath); err != nil {
		dm.mu.Lock()
		delete(dm.watchedDirs, abspath)
		dm.mu.Unlock()
		return err
	}

	dm.logger.Debug("watching directory", "path", abspath)
	return nil
}

func (dm *DirectoryMonitor) removeDirectoryWatch(path string) {
	abspath := filepath.Clean(path)

	dm.mu.Lock()
	if _, ok := dm.watchedDirs[abspath]; !ok {
		dm.mu.Unlock()
		return
	}
	delete(dm.watchedDirs, abspath)
	dm.mu.Unlock()

	if err := dm.watcher.Remove(abspath); err != nil {
		dm.logger.Debug("remove directory watch", "path", abspath, "error", err)
	}
}

func (dm *DirectoryMonitor) handleEvent(event fsnotify.Event) {
	path := filepath.Clean(event.Name)
	if path == "" {
		return
	}

	info, statErr := os.Stat(path)
	isDir := statErr == nil && info.IsDir()

	if dm.recursive && isDir && event.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
		if err := dm.addDirectoryWatch(path); err != nil {
			dm.logger.Error("add directory watch", "path", path, "error", err)
		}
	}

	if isDir {
		if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
			dm.removeDirectoryWatch(path)
			dm.removeDatabasesUnder(path)
		}
		return
	}

	if statErr != nil && !os.IsNotExist(statErr) {
		dm.logger.Debug("stat event path", "path", path, "error", statErr)
		return
	}

	if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		dm.removeDatabase(path)
		return
	}

	if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) != 0 {
		dm.handlePotentialDatabase(path)
	}
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

	if !IsSQLiteDatabase(path) {
		dm.mu.Lock()
		delete(dm.dbs, path)
		dm.mu.Unlock()
		return
	}

	db, err := newDBFromDirectoryEntry(dm.config, dm.dirPath, path)
	if err != nil {
		dm.mu.Lock()
		delete(dm.dbs, path)
		dm.mu.Unlock()
		dm.logger.Error("configure database", "path", path, "error", err)
		return
	}

	if err := dm.store.AddDB(db); err != nil {
		dm.mu.Lock()
		delete(dm.dbs, path)
		dm.mu.Unlock()
		dm.logger.Error("add database to store", "path", path, "error", err)
		return
	}

	dm.mu.Lock()
	dm.dbs[path] = db
	dm.mu.Unlock()

	dm.logger.Info("added database to replication", "path", path)
}

func (dm *DirectoryMonitor) removeDatabase(path string) {
	dm.mu.Lock()
	db := dm.dbs[path]
	delete(dm.dbs, path)
	dm.mu.Unlock()

	if db == nil {
		return
	}

	if err := dm.store.RemoveDB(context.Background(), db.Path()); err != nil {
		dm.logger.Error("remove database from store", "path", path, "error", err)
		return
	}

	dm.logger.Info("removed database from replication", "path", path)
}

func (dm *DirectoryMonitor) removeDatabasesUnder(dir string) {
	prefix := dir + string(os.PathSeparator)

	dm.mu.Lock()
	var toClose []*litestream.DB
	for path, db := range dm.dbs {
		if path == dir || strings.HasPrefix(path, prefix) {
			toClose = append(toClose, db)
			delete(dm.dbs, path)
		}
	}
	dm.mu.Unlock()

	for _, db := range toClose {
		if db == nil {
			continue
		}
		if err := dm.store.RemoveDB(context.Background(), db.Path()); err != nil {
			dm.logger.Error("remove database from store", "path", db.Path(), "error", err)
		}
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
