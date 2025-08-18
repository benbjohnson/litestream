package internal

import "io"

// Copied from the io package to implement io.Closer.
func LimitReadCloser(r io.ReadCloser, n int64) io.ReadCloser {
	return &LimitedReadCloser{r, n}
}

type LimitedReadCloser struct {
	R io.ReadCloser // underlying reader
	N int64         // max bytes remaining
}

func (l *LimitedReadCloser) Close() error {
	return l.R.Close()
}

func (l *LimitedReadCloser) Read(p []byte) (n int, err error) {
	if l.N <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > l.N {
		p = p[0:l.N]
	}
	n, err = l.R.Read(p)
	l.N -= int64(n)
	return
}
