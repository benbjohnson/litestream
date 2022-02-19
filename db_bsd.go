//go:build !linux

package litestream

import (
	"io"
	"os"
)

// WithFile executes fn with a file handle for the main database file.
// On Linux, this is a unique file handle for each call. On non-Linux
// systems, the file handle is shared because of lock semantics.
func (db *DB) WithFile(fn func(f *os.File) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, err := db.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return fn(db.f)
}
