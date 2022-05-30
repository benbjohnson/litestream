//go:build linux

package internal

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

var _ FileWatcher = (*InotifyFileWatcher)(nil)

// InotifyFileWatcher watches files and is notified of events on them.
//
// Watcher code based on https://github.com/fsnotify/fsnotify
type InotifyFileWatcher struct {
	inotify struct {
		fd  int
		buf []byte
	}
	epoll struct {
		fd     int // epoll_create1() file descriptor
		events []unix.EpollEvent
	}
	pipe struct {
		r int // read pipe file descriptor
		w int // write pipe file descriptor
	}

	events chan FileEvent

	mu        sync.Mutex
	watches   map[string]int
	paths     map[int]string
	notExists map[string]struct{}

	g      errgroup.Group
	ctx    context.Context
	cancel func()
}

// NewInotifyFileWatcher returns a new instance of InotifyFileWatcher.
func NewInotifyFileWatcher() *InotifyFileWatcher {
	w := &InotifyFileWatcher{
		events: make(chan FileEvent),

		watches:   make(map[string]int),
		paths:     make(map[int]string),
		notExists: make(map[string]struct{}),
	}

	w.inotify.buf = make([]byte, 4096*unix.SizeofInotifyEvent)
	w.epoll.events = make([]unix.EpollEvent, 64)

	return w
}

// NewFileWatcher returns an instance of InotifyFileWatcher on Linux systems.
func NewFileWatcher() FileWatcher {
	return NewInotifyFileWatcher()
}

// Events returns a read-only channel of file events.
func (w *InotifyFileWatcher) Events() <-chan FileEvent {
	return w.events
}

// Open initializes the watcher and begins listening for file events.
func (w *InotifyFileWatcher) Open() (err error) {
	w.inotify.fd, err = unix.InotifyInit1(unix.IN_CLOEXEC)
	if err != nil {
		return fmt.Errorf("cannot init inotify: %w", err)
	}

	// Initialize epoll and create a non-blocking pipe.
	if w.epoll.fd, err = unix.EpollCreate1(unix.EPOLL_CLOEXEC); err != nil {
		return fmt.Errorf("cannot create epoll: %w", err)
	}

	pipe := []int{-1, -1}
	if err := unix.Pipe2(pipe[:], unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
		return fmt.Errorf("cannot create epoll pipe: %w", err)
	}
	w.pipe.r, w.pipe.w = pipe[0], pipe[1]

	// Register inotify fd with epoll
	if err := unix.EpollCtl(w.epoll.fd, unix.EPOLL_CTL_ADD, w.inotify.fd, &unix.EpollEvent{
		Fd:     int32(w.inotify.fd),
		Events: unix.EPOLLIN,
	}); err != nil {
		return fmt.Errorf("cannot add inotify to epoll: %w", err)
	}

	// Register pipe fd with epoll
	if err := unix.EpollCtl(w.epoll.fd, unix.EPOLL_CTL_ADD, w.pipe.r, &unix.EpollEvent{
		Fd:     int32(w.pipe.r),
		Events: unix.EPOLLIN,
	}); err != nil {
		return fmt.Errorf("cannot add pipe to epoll: %w", err)
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
func (w *InotifyFileWatcher) Close() (err error) {
	w.cancel()

	if e := w.wake(); e != nil && err == nil {
		err = e
	}
	if e := w.g.Wait(); e != nil && err == nil {
		err = e
	}
	return err
}

// Watch begins watching the given file or directory.
func (w *InotifyFileWatcher) Watch(filename string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	filename = filepath.Clean(filename)

	// If file doesn't exist, monitor separately until it does exist as we
	// can't watch non-existent files with inotify.
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		w.notExists[filename] = struct{}{}
		return nil
	}

	return w.addWatch(filename)
}

func (w *InotifyFileWatcher) addWatch(filename string) error {
	wd, err := unix.InotifyAddWatch(w.inotify.fd, filename, unix.IN_MODIFY|unix.IN_DELETE_SELF)
	if err != nil {
		return err
	}

	w.watches[filename] = wd
	w.paths[wd] = filename

	delete(w.notExists, filename)

	return err
}

// Unwatch stops watching the given file or directory.
func (w *InotifyFileWatcher) Unwatch(filename string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	filename = filepath.Clean(filename)

	// Look up watch ID by filename.
	wd, ok := w.watches[filename]
	if !ok {
		return nil
	}

	if _, err := unix.InotifyRmWatch(w.inotify.fd, uint32(wd)); err != nil {
		return err
	}

	delete(w.paths, wd)
	delete(w.watches, filename)
	delete(w.notExists, filename)

	return nil
}

// monitorNotExist runs in a separate goroutine and monitors for the creation of
// watched files that do not yet exist.
func (w *InotifyFileWatcher) monitorNotExists(ctx context.Context) error {
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

func (w *InotifyFileWatcher) checkNotExists(ctx context.Context) {
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
func (w *InotifyFileWatcher) monitor(ctx context.Context) error {
	// Close all file descriptors once monitor exits.
	defer func() {
		unix.Close(w.inotify.fd)
		unix.Close(w.epoll.fd)
		unix.Close(w.pipe.w)
		unix.Close(w.pipe.r)
	}()

	for {
		if err := w.wait(ctx); err != nil {
			return err
		} else if err := w.read(ctx); err != nil {
			return err
		}
	}
}

// read reads from the inotify file descriptor. Automatically retry on EINTR.
func (w *InotifyFileWatcher) read(ctx context.Context) error {
	for {
		n, err := unix.Read(w.inotify.fd, w.inotify.buf)
		if err != nil && err != unix.EINTR {
			return err
		} else if n < 0 {
			continue
		}

		return w.recv(ctx, w.inotify.buf[:n])
	}
}

func (w *InotifyFileWatcher) recv(ctx context.Context, b []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	for {
		if len(b) == 0 {
			return nil
		} else if len(b) < unix.SizeofInotifyEvent {
			return fmt.Errorf("InotifyFileWatcher.recv(): inotify short record: n=%d", len(b))
		}

		event := (*unix.InotifyEvent)(unsafe.Pointer(&b[0]))
		if event.Mask&unix.IN_Q_OVERFLOW != 0 {
			// TODO: Change to notify all watches.
			return ErrFileEventQueueOverflow
		}

		// Remove deleted files from the lookups.
		w.mu.Lock()
		name, ok := w.paths[int(event.Wd)]
		if ok && event.Mask&unix.IN_DELETE_SELF != 0 {
			delete(w.paths, int(event.Wd))
			delete(w.watches, name)
		}
		w.mu.Unlock()

		//if nameLen > 0 {
		//	// Point "bytes" at the first byte of the filename
		//	bytes := (*[unix.PathMax]byte)(unsafe.Pointer(&buf[offset+unix.SizeofInotifyEvent]))[:nameLen:nameLen]
		//	// The filename is padded with NULL bytes. TrimRight() gets rid of those.
		//	name += "/" + strings.TrimRight(string(bytes[0:nameLen]), "\000")
		//}

		// Move to next event.
		b = b[unix.SizeofInotifyEvent+event.Len:]

		// Skip event if ignored.
		if event.Mask&unix.IN_IGNORED != 0 {
			continue
		}

		// Convert to generic file event mask.
		var mask int
		if event.Mask&unix.IN_MODIFY != 0 {
			mask |= FileEventModified
		}
		if event.Mask&unix.IN_DELETE_SELF != 0 {
			mask |= FileEventDeleted
		}

		// Send event to channel or wait for close.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case w.events <- FileEvent{
			Name: name,
			Mask: mask,
		}:
		}
	}
}

func (w *InotifyFileWatcher) wait(ctx context.Context) error {
	for {
		n, err := unix.EpollWait(w.epoll.fd, w.epoll.events, -1)
		if n == 0 || err == unix.EINTR {
			continue
		} else if err != nil {
			return err
		}

		// Read events to see if we have data available on inotify or if we are awaken.
		var hasData bool
		for _, event := range w.epoll.events[:n] {
			switch event.Fd {
			case int32(w.inotify.fd): // inotify file descriptor
				hasData = hasData || event.Events&(unix.EPOLLHUP|unix.EPOLLERR|unix.EPOLLIN) != 0

			case int32(w.pipe.r): // epoll file descriptor
				if _, err := unix.Read(w.pipe.r, make([]byte, 1024)); err != nil && err != unix.EAGAIN {
					return fmt.Errorf("epoll pipe error: %w", err)
				}
			}
		}

		// Check if context is closed and then exit if data is available.
		if err := ctx.Err(); err != nil {
			return err
		} else if hasData {
			return nil
		}
	}
}

func (w *InotifyFileWatcher) wake() error {
	if _, err := unix.Write(w.pipe.w, []byte{0}); err != nil && err != unix.EAGAIN {
		return err
	}
	return nil
}
