package internal

import (
	"io"
	"log/slog"
	"os"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const LevelTrace = slog.LevelDebug - 4

// ReadCloser wraps a reader to also attach a separate closer.
type ReadCloser struct {
	r io.Reader
	c io.Closer
}

// NewReadCloser returns a new instance of ReadCloser.
func NewReadCloser(r io.Reader, c io.Closer) *ReadCloser {
	return &ReadCloser{r, c}
}

// Read reads bytes into the underlying reader.
func (r *ReadCloser) Read(p []byte) (n int, err error) {
	return r.r.Read(p)
}

// Close closes the reader (if implementing io.ReadCloser) and the Closer.
func (r *ReadCloser) Close() error {
	if rc, ok := r.r.(io.Closer); ok {
		if err := rc.Close(); err != nil {
			r.c.Close()
			return err
		}
	}
	return r.c.Close()
}

// ReadCounter wraps an io.Reader and counts the total number of bytes read.
type ReadCounter struct {
	r io.Reader
	n int64
}

// NewReadCounter returns a new instance of ReadCounter that wraps r.
func NewReadCounter(r io.Reader) *ReadCounter {
	return &ReadCounter{r: r}
}

// Read reads from the underlying reader into p and adds the bytes read to the counter.
func (r *ReadCounter) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

// N returns the total number of bytes read.
func (r *ReadCounter) N() int64 { return r.n }

// CreateFile creates the file and matches the mode & uid/gid of fi.
func CreateFile(filename string, fi os.FileInfo) (*os.File, error) {
	mode := os.FileMode(0600)
	if fi != nil {
		mode = fi.Mode()
	}

	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return nil, err
	}

	uid, gid := Fileinfo(fi)
	_ = f.Chown(uid, gid)
	return f, nil
}

// MkdirAll is a copy of os.MkdirAll() except that it attempts to set the
// mode/uid/gid to match fi for each created directory.
func MkdirAll(path string, fi os.FileInfo) error {
	uid, gid := Fileinfo(fi)

	// Fast path: if we can tell whether path is a directory or file, stop with success or error.
	dir, err := os.Stat(path)
	if err == nil {
		if dir.IsDir() {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(path)
	for i > 0 && os.IsPathSeparator(path[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(path[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent.
		err = MkdirAll(fixRootDirectory(path[:j-1]), fi)
		if err != nil {
			return err
		}
	}

	// Parent now exists; invoke Mkdir and use its result.
	mode := os.FileMode(0700)
	if fi != nil {
		mode = fi.Mode()
	}
	err = os.Mkdir(path, mode)
	if err != nil {
		// Handle arguments like "foo/." by
		// double-checking that directory doesn't exist.
		dir, err1 := os.Lstat(path)
		if err1 == nil && dir.IsDir() {
			_ = os.Chown(path, uid, gid)
			return nil
		}
		return err
	}
	_ = os.Chown(path, uid, gid)
	return nil
}

func ReplaceAttr(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.LevelKey && a.Value.Any() == LevelTrace {
		a.Value = slog.StringValue("TRACE")
	}
	return a
}

// Shared replica metrics.
var (
	OperationTotalCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litestream_replica_operation_total",
		Help: "The number of replica operations performed",
	}, []string{"replica_type", "operation"})

	OperationBytesCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litestream_replica_operation_bytes",
		Help: "The number of bytes used by replica operations",
	}, []string{"replica_type", "operation"})
)
