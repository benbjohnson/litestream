//go:build freebsd || openbsd || netbsd || dragonfly || darwin

package internal

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

var _ FileWatcher = (*KqueueFileWatcher)(nil)

// KqueueFileWatcher watches files and is notified of events on them.
//
// Watcher code based on https://github.com/fsnotify/fsnotify
type KqueueFileWatcher struct {
	fd     int
	events chan FileEvent

	mu        sync.Mutex
	watches   map[string]int
	paths     map[int]string
	notExists map[string]struct{}

	g      errgroup.Group
	ctx    context.Context
	cancel func()
}

// NewKqueueFileWatcher returns a new instance of KqueueFileWatcher.
func NewKqueueFileWatcher() *KqueueFileWatcher {
	return &KqueueFileWatcher{
		events: make(chan FileEvent),

		watches:   make(map[string]int),
		paths:     make(map[int]string),
		notExists: make(map[string]struct{}),
	}
}

// NewFileWatcher returns an instance of KqueueFileWatcher on BSD systems.
func NewFileWatcher() FileWatcher {
	return NewKqueueFileWatcher()
}

// Events returns a read-only channel of file events.
func (w *KqueueFileWatcher) Events() <-chan FileEvent {
	return w.events
}

// Open initializes the watcher and begins listening for file events.
func (w *KqueueFileWatcher) Open() (err error) {
	if w.fd, err = unix.Kqueue(); err != nil {
		return err
	}

	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.g.Go(func() error {
		if err := w.monitor(w.ctx); err != nil && w.ctx.Err() == nil {
			return err
		}
		return nil
	})
	w.g.Go(func() error {
		if err := w.monitorNotExists(w.ctx); err != nil && w.ctx.Err() == nil {
			return err
		}
		return nil
	})

	return nil
}

// Close stops watching for file events and cleans up resources.
func (w *KqueueFileWatcher) Close() (err error) {
	w.cancel()

	if w.fd != 0 {
		if e := unix.Close(w.fd); e != nil && err == nil {
			err = e
		}
	}

	if e := w.g.Wait(); e != nil && err == nil {
		err = e
	}
	return err
}

// Watch begins watching the given file or directory.
func (w *KqueueFileWatcher) Watch(filename string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	filename = filepath.Clean(filename)

	// If file doesn't exist, monitor separately until it does exist as we
	// can't watch non-existent files with kqueue.
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		w.notExists[filename] = struct{}{}
		return nil
	}

	return w.addWatch(filename)
}

func (w *KqueueFileWatcher) addWatch(filename string) error {
	wd, err := unix.Open(filename, unix.O_NONBLOCK|unix.O_RDONLY|unix.O_CLOEXEC, 0700)
	if err != nil {
		return err
	}

	// TODO: Handle return count different than 1.
	kevent := unix.Kevent_t{Fflags: unix.NOTE_DELETE | unix.NOTE_WRITE}
	unix.SetKevent(&kevent, wd, unix.EVFILT_VNODE, unix.EV_ADD|unix.EV_CLEAR|unix.EV_ENABLE)
	if _, err := unix.Kevent(w.fd, []unix.Kevent_t{kevent}, nil, nil); err != nil {
		return err
	}

	w.watches[filename] = wd
	w.paths[wd] = filename

	delete(w.notExists, filename)

	return err
}

// Unwatch stops watching the given file or directory.
func (w *KqueueFileWatcher) Unwatch(filename string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	filename = filepath.Clean(filename)

	// Look up watch ID by filename.
	wd, ok := w.watches[filename]
	if !ok {
		return nil
	}

	// TODO: Handle return count different than 1.
	var kevent unix.Kevent_t
	unix.SetKevent(&kevent, wd, unix.EVFILT_VNODE, unix.EV_DELETE)
	if _, err := unix.Kevent(w.fd, []unix.Kevent_t{kevent}, nil, nil); err != nil {
		return err
	}
	unix.Close(wd)

	delete(w.paths, wd)
	delete(w.watches, filename)
	delete(w.notExists, filename)

	return nil
}

// monitorNotExist runs in a separate goroutine and monitors for the creation of
// watched files that do not yet exist.
func (w *KqueueFileWatcher) monitorNotExists(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.checkNotExists(ctx)
		}
	}
}

func (w *KqueueFileWatcher) checkNotExists(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for filename := range w.notExists {
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			continue
		}

		if err := w.addWatch(filename); err != nil {
			log.Printf("non-existent file monitor: cannot add watch: %s", err)
			continue
		}

		// Send event to channel.
		select {
		case w.events <- FileEvent{
			Name: filename,
			Mask: FileEventCreated,
		}:
		default:
		}
	}
}

// monitor runs in a separate goroutine and monitors the inotify event queue.
func (w *KqueueFileWatcher) monitor(ctx context.Context) error {
	kevents := make([]unix.Kevent_t, 10)
	timeout := unix.NsecToTimespec(int64(100 * time.Millisecond))

	for {
		n, err := unix.Kevent(w.fd, nil, kevents, &timeout)
		if err != nil && err != unix.EINTR {
			return err
		} else if n < 0 {
			continue
		}

		for _, kevent := range kevents[:n] {
			if err := w.recv(ctx, &kevent); err != nil {
				return err
			}
		}
	}
}

// recv processes a single event from kqeueue.
func (w *KqueueFileWatcher) recv(ctx context.Context, kevent *unix.Kevent_t) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Look up filename & remove from watcher if this is a delete.
	w.mu.Lock()
	filename, ok := w.paths[int(kevent.Ident)]
	if ok && kevent.Fflags&unix.NOTE_DELETE != 0 {
		delete(w.paths, int(kevent.Ident))
		delete(w.watches, filename)
		unix.Close(int(kevent.Ident))
	}
	w.mu.Unlock()

	// Convert to generic file event mask.
	var mask int
	if kevent.Fflags&unix.NOTE_WRITE != 0 {
		mask |= FileEventModified
	}
	if kevent.Fflags&unix.NOTE_DELETE != 0 {
		mask |= FileEventDeleted
	}

	// Send event to channel or wait for close.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.events <- FileEvent{
		Name: filename,
		Mask: mask,
	}:
		return nil
	}
}
