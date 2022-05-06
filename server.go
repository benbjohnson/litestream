package litestream

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"golang.org/x/sync/errgroup"
)

// Server represents the top-level container.
// It manage databases and routes global file system events.
type Server struct {
	mu      sync.Mutex
	dbs     map[string]*DB // databases by path
	watcher *fsnotify.Watcher

	ctx      context.Context
	cancel   func()
	errgroup errgroup.Group
}

// NewServer returns a new instance of Server.
func NewServer() *Server {
	return &Server{
		dbs: make(map[string]*DB),
	}
}

// Open initializes the server and begins watching for file system events.
func (s *Server) Open() error {
	var err error
	s.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.errgroup.Go(func() error {
		if err := s.monitor(s.ctx); err != nil && err != context.Canceled {
			return fmt.Errorf("server monitor error: %w", err)
		}
		return nil
	})
	return nil
}

// Close shuts down the server and all databases it manages.
func (s *Server) Close() (err error) {
	// Cancel context and wait for goroutines to finish.
	s.cancel()
	if e := s.errgroup.Wait(); e != nil && err == nil {
		err = e
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.watcher != nil {
		if e := s.watcher.Close(); e != nil && err == nil {
			err = fmt.Errorf("close watcher: %w", e)
		}
	}

	for _, db := range s.dbs {
		if e := db.Close(); e != nil && err == nil {
			err = fmt.Errorf("close db: path=%s err=%w", db.Path(), e)
		}
	}
	s.dbs = make(map[string]*DB)

	return err
}

// DB returns the database with the given path, if it's managed by the server.
func (s *Server) DB(path string) *DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dbs[path]
}

// DBs returns a slice of all databases managed by the server.
func (s *Server) DBs() []*DB {
	s.mu.Lock()
	defer s.mu.Unlock()

	a := make([]*DB, 0, len(s.dbs))
	for _, db := range s.dbs {
		a = append(a, db)
	}
	return a
}

// Watch adds a database path to be managed by the server.
func (s *Server) Watch(path string, fn func(path string) (*DB, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Instantiate DB from factory function.
	db, err := fn(path)
	if err != nil {
		return fmt.Errorf("new database: %w", err)
	}

	// Start watching the database for changes.
	if err := db.Open(); err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	s.dbs[path] = db

	// Watch for changes on the database file & WAL.
	if err := s.watcher.Add(filepath.Dir(path)); err != nil {
		return fmt.Errorf("watch db file: %w", err)
	}

	// Kick off an initial sync.
	select {
	case db.NotifyCh() <- struct{}{}:
	default:
	}

	return nil
}

// Unwatch removes a database path from being managed by the server.
func (s *Server) Unwatch(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	db := s.dbs[path]
	if db == nil {
		return nil
	}
	delete(s.dbs, path)

	// Stop watching for changes on the database WAL.
	if err := s.watcher.Remove(path + "-wal"); err != nil {
		return fmt.Errorf("unwatch file: %w", err)
	}

	// Shut down database.
	if err := db.Close(); err != nil {
		return fmt.Errorf("close db: %w", err)
	}

	return nil
}

func (s *Server) isWatched(event fsnotify.Event) bool {
	path := event.Name
	path = strings.TrimSuffix(path, "-wal")

	if _, ok := s.dbs[path]; ok {
		return true
	}
	return false
}

// monitor runs in a separate goroutine and dispatches notifications to managed DBs.
func (s *Server) monitor(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-s.watcher.Events:
			if !s.isWatched(event) {
				continue
			}
			if err := s.dispatchFileEvent(ctx, event); err != nil {
				return err
			}
		}
	}
}

// dispatchFileEvent dispatches a notification to the database which owns the file.
func (s *Server) dispatchFileEvent(ctx context.Context, event fsnotify.Event) error {
	path := event.Name
	path = strings.TrimSuffix(path, "-wal")

	db := s.DB(path)
	if db == nil {
		return nil
	}

	// TODO: If deleted, remove from server and close DB.

	select {
	case <-ctx.Done():
		return ctx.Err()
	case db.NotifyCh() <- struct{}{}:
		return nil // notify db
	default:
		return nil // already pending notification, skip
	}
}
