package internal

import (
	"crypto/md5"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Platform-independent maximum integer sizes.
const (
	MaxUint = ^uint(0)
	MaxInt  = int(MaxUint >> 1)
)

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

// MultiReadCloser is a logical concatenation of io.ReadCloser.
// It works like io.MultiReader except all objects are closed when Close() is called.
type MultiReadCloser struct {
	mr      io.Reader
	closers []io.Closer
}

// NewMultiReadCloser returns a new instance of MultiReadCloser.
func NewMultiReadCloser(a []io.ReadCloser) *MultiReadCloser {
	readers := make([]io.Reader, len(a))
	closers := make([]io.Closer, len(a))
	for i, rc := range a {
		readers[i] = rc
		closers[i] = rc
	}
	return &MultiReadCloser{mr: io.MultiReader(readers...), closers: closers}
}

// Read reads from the next available reader.
func (mrc *MultiReadCloser) Read(p []byte) (n int, err error) {
	return mrc.mr.Read(p)
}

// Close closes all underlying ReadClosers and returns first error encountered.
func (mrc *MultiReadCloser) Close() (err error) {
	for _, c := range mrc.closers {
		if e := c.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
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
func CreateFile(filename string, mode os.FileMode, uid, gid int) (*os.File, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return nil, err
	}

	_ = f.Chown(uid, gid)
	return f, nil
}

// WriteFile writes data to a named file and sets the mode & uid/gid.
func WriteFile(name string, data []byte, perm os.FileMode, uid, gid int) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_ = f.Chown(uid, gid)

	_, err = f.Write(data)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}

// MkdirAll is a copy of os.MkdirAll() except that it attempts to set the
// mode/uid/gid to match fi for each created directory.
func MkdirAll(path string, mode os.FileMode, uid, gid int) error {
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
		err = MkdirAll(path[:j-1], mode, uid, gid)
		if err != nil {
			return err
		}
	}

	// Parent now exists; invoke Mkdir and use its result.
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

// Fileinfo returns syscall fields from a FileInfo object.
func Fileinfo(fi os.FileInfo) (uid, gid int) {
	if fi == nil {
		return -1, -1
	}
	stat := fi.Sys().(*syscall.Stat_t)
	return int(stat.Uid), int(stat.Gid)
}

// ParseSnapshotPath parses the index from a snapshot filename. Used by path-based replicas.
func ParseSnapshotPath(s string) (index int, err error) {
	a := snapshotPathRegex.FindStringSubmatch(s)
	if a == nil {
		return 0, fmt.Errorf("invalid snapshot path")
	}

	i64, _ := strconv.ParseUint(a[1], 16, 64)
	if i64 > uint64(MaxInt) {
		return 0, fmt.Errorf("index too large in snapshot path %q", s)
	}
	return int(i64), nil
}

var snapshotPathRegex = regexp.MustCompile(`^([0-9a-f]{16})\.snapshot\.lz4$`)

// ParseWALSegmentPath parses the index/offset from a segment filename. Used by path-based replicas.
func ParseWALSegmentPath(s string) (index int, offset int64, err error) {
	a := walSegmentPathRegex.FindStringSubmatch(s)
	if a == nil {
		return 0, 0, fmt.Errorf("invalid wal segment path")
	}

	i64, _ := strconv.ParseUint(a[1], 16, 64)
	if i64 > uint64(MaxInt) {
		return 0, 0, fmt.Errorf("index too large in wal segment path %q", s)
	}
	off64, _ := strconv.ParseUint(a[2], 16, 64)
	if off64 > math.MaxInt64 {
		return 0, 0, fmt.Errorf("offset too large in wal segment path %q", s)
	}
	return int(i64), int64(off64), nil
}

var walSegmentPathRegex = regexp.MustCompile(`^([0-9a-f]{16})\/([0-9a-f]{16})\.wal\.lz4$`)

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

// TruncateDuration truncates d to the nearest major unit (s, ms, µs, ns).
func TruncateDuration(d time.Duration) time.Duration {
	if d < 0 {
		if d < -10*time.Second {
			return d.Truncate(time.Second)
		} else if d < -time.Second {
			return d.Truncate(time.Second / 10)
		} else if d < -time.Millisecond {
			return d.Truncate(time.Millisecond)
		} else if d < -time.Microsecond {
			return d.Truncate(time.Microsecond)
		}
		return d
	}

	if d > 10*time.Second {
		return d.Truncate(time.Second)
	} else if d > time.Second {
		return d.Truncate(time.Second / 10)
	} else if d > time.Millisecond {
		return d.Truncate(time.Millisecond)
	} else if d > time.Microsecond {
		return d.Truncate(time.Microsecond)
	}
	return d
}

// MD5Hash returns a hex-encoded MD5 hash of b.
func MD5Hash(b []byte) string {
	return fmt.Sprintf("%x", md5.Sum(b))
}

// OnceCloser returns a closer that will only ignore duplicate closes.
func OnceCloser(c io.Closer) io.Closer {
	return &onceCloser{Closer: c}
}

type onceCloser struct {
	sync.Once
	io.Closer
}

func (c *onceCloser) Close() (err error) {
	c.Once.Do(func() { err = c.Closer.Close() })
	return err
}
