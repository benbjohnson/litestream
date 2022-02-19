//go:build linux

package litestream

import "os"

// WithFile executes fn with a file handle for the main database file.
// On Linux, this is a unique file handle for each call. On non-Linux
// systems, the file handle is shared because of lock semantics.
func (db *DB) WithFile(fn func(f *os.File) error) error {
	f, err := os.Open(db.path)
	if err != nil {
		return err
	}
	defer f.Close()

	return fn(f)
}
