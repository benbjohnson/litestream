package internal

import (
	"io"
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
