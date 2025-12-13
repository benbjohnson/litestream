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

	// Scan for existing databases after watches are set up
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

func (dm *DirectoryMonitor) handleEvent(event fsnotify.Event) {
	path := filepath.Clean(event.Name)
	if path == "" {
		return
	}

	info, statErr := os.Stat(path)
	isDir := statErr == nil && info.IsDir()

	// Check if this path was previously a watched directory (needed for removal detection)
	dm.mu.Lock()
	_, wasWatchedDir := dm.watchedDirs[path]
	dm.mu.Unlock()

	// Handle directory creation/rename
	// Note: In non-recursive mode, only the root directory is watched.
	// Subdirectories are completely ignored, and their databases are not replicated.
	// In recursive mode, all subdirectories are watched and scanned.
	if isDir && event.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
		// Only add watches for: (1) root directory, or (2) subdirectories when recursive=true
		if dm.recursive {
			if err := dm.addDirectoryWatch(path); err != nil {
				dm.logger.Error("add directory watch", "path", path, "error", err)
			}
			// Scan to catch files created during watch registration race window
			dm.scanDirectory(path)
		}
	}

	// Handle directory removal/rename
	// Check both current state (isDir) AND previous state (wasWatchedDir)
	// because os.Stat fails for deleted directories, leaving watches orphaned
	if (isDir || wasWatchedDir) && event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
		dm.removeDirectoryWatch(path)
		dm.removeDatabasesUnder(path)
		return
	}

	// If it's still a directory, don't process as a file
	if isDir {
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

	var db *litestream.DB
	success := false
	defer func() {
		if !success {
			if db != nil {
				_ = dm.store.RemoveDB(dm.ctx, db.Path())
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

	if err := dm.store.AddDB(db); err != nil {
		dm.logger.Error("add database to store", "path", path, "error", err)
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

	if err := dm.store.RemoveDB(dm.ctx, db.Path()); err != nil {
		dm.logger.Error("remove database from store", "path", path, "error", err)
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

	// Remove from store first, only delete from local map on success
	for i, db := range toClose {
		if db == nil {
			// Clean up nil entries (in-progress databases) from local map
			dm.mu.Lock()
			delete(dm.dbs, toClosePaths[i])
			dm.mu.Unlock()
			continue
		}
		if err := dm.store.RemoveDB(dm.ctx, db.Path()); err != nil {
			dm.logger.Error("remove database from store", "path", db.Path(), "error", err)
			continue
		}

		// Only remove from local map after successful store removal
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

// scanDirectory walks a directory to discover pre-existing databases.
// In non-recursive mode, only scans files directly in the directory (subdirectories are ignored).
// In recursive mode, walks the entire tree and adds watches for all subdirectories.
// This is called on startup and when new directories are created to catch files created
// during the fsnotify watch registration race window.
func (dm *DirectoryMonitor) scanDirectory(dir string) {
	if !dm.recursive {
		// Non-recursive mode: Only scan files in the immediate directory.
		// Subdirectories and their contents are completely ignored.
		entries, err := os.ReadDir(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				dm.logger.Debug("read directory", "path", dir, "error", err)
			}
			return
		}

		for _, entry := range entries {
			// Skip subdirectories - they're not watched in non-recursive mode
			if entry.IsDir() {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			dm.handlePotentialDatabase(path)
		}
		return
	}

	// Recursive mode: scan full tree
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
