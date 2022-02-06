package internal

import (
	"errors"
)

// File event mask constants.
const (
	FileEventCreated = 1 << iota
	FileEventModified
	FileEventDeleted
)

// FileEvent represents an event on a watched file.
type FileEvent struct {
	Name string
	Mask int
}

// ErrFileEventQueueOverflow is returned when the file event queue has overflowed.
var ErrFileEventQueueOverflow = errors.New("file event queue overflow")

// FileWatcher represents a watcher of file events.
type FileWatcher interface {
	Open() error
	Close() error

	// Returns a channel of events for watched files.
	Events() <-chan FileEvent

	// Adds a specific file to be watched.
	Watch(filename string) error

	// Removes a specific file from being watched.
	Unwatch(filename string) error
}
