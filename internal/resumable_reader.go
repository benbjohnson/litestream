package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/superfly/ltx"
)

type LTXFileOpener interface {
	OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
}

// resumableReader wraps an io.ReadCloser from a remote storage backend with
// automatic reconnection on read errors.
//
// During restore, the LTX compactor opens all LTX file streams upfront, then
// processes pages in page-number order. Incremental LTX files that only contain
// high-numbered pages may have their S3/storage streams sit idle for minutes
// while the compactor works through lower-numbered pages from the snapshot.
// Storage providers (S3, Tigris, etc.) may close these idle connections,
// causing "unexpected EOF" errors.
//
// This reader detects two failure modes:
//  1. Non-EOF errors (connection reset, timeout) - the stream broke mid-transfer.
//  2. Premature EOF - the server closed the connection cleanly, but we haven't
//     read all bytes yet (detected by comparing offset against known file size).
//
// On failure, it closes the dead stream and reopens from the current byte
// offset using the storage backend's range request support (the offset parameter
// of OpenLTXFile). Callers like io.ReadFull see a seamless byte stream because
// partial reads are returned without error, prompting the caller to request
// remaining bytes on the next Read call.
type ResumableReader struct {
	ctx     context.Context
	client  LTXFileOpener
	level   int
	minTXID ltx.TXID
	maxTXID ltx.TXID
	size    int64 // expected total file size from FileInfo; 0 means unknown
	offset  int64
	retryN  int
	err     error
	logger  *slog.Logger

	// mu guards rc and closed. Close may be called from a different goroutine
	// than Read: Compactor.Compact closes its source readers when it returns
	// while its compaction goroutine may still be reading them. Once closed is
	// set, Read must not reopen the stream or it would leak the new connection.
	mu     sync.Mutex
	rc     io.ReadCloser
	cancel context.CancelFunc
	closed bool
}

// NewResumableReader creates a ResumableReader. Primarily exposed for testing.
func NewResumableReader(ctx context.Context, client LTXFileOpener, level int, minTXID, maxTXID ltx.TXID, size int64, rc io.ReadCloser, logger *slog.Logger) *ResumableReader {
	return &ResumableReader{
		ctx:     ctx,
		client:  client,
		level:   level,
		minTXID: minTXID,
		maxTXID: maxTXID,
		size:    size,
		rc:      rc,
		logger:  logger,
	}
}

const resumableReaderMaxRetries = 3

// resumableReaderBackoff is the base delay between retry attempts, doubling
// per attempt. Zero-delay retries land every attempt inside the same provider
// throttle window (e.g. Tigris 408 load shedding), guaranteeing exhaustion.
const resumableReaderBackoff = 250 * time.Millisecond

// resumableReaderStallTimeout bounds a single Read of an open stream. Nothing
// else does: the S3 client sets no ResponseHeaderTimeout and no read deadline on
// the response body, and TCP keepalive does not fire against a peer that is
// alive but simply sending nothing. It is a var rather than a const only so
// tests can shorten it.
var resumableReaderStallTimeout = 30 * time.Second

// ErrReadStalled reports a read that produced nothing for
// resumableReaderStallTimeout and had its stream cancelled to force a reconnect.
var ErrReadStalled = errors.New("ltx stream read stalled")

func (r *ResumableReader) Read(p []byte) (int, error) {
	for {
		rc, cancel, err := r.stream()
		if err != nil {
			return 0, err
		}
		if r.err != nil {
			return 0, r.err
		}

		// Reopen the stream from the current offset if the previous
		// connection was closed (the stream is nil after a retry).
		if rc == nil {
			streamCtx, streamCancel := context.WithCancel(r.ctx)
			newRC, err := r.client.OpenLTXFile(streamCtx, r.level, r.minTXID, r.maxTXID, r.offset, 0)
			if err != nil {
				streamCancel()
				if errors.Is(err, os.ErrNotExist) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return 0, fmt.Errorf("reopen ltx file at offset %d: %w", r.offset, err)
				}
				if ctxErr := r.ctx.Err(); ctxErr != nil {
					r.err = fmt.Errorf("reopen ltx file at offset %d: %v: %w", r.offset, err, ctxErr)
					return 0, r.err
				}
				if retryErr := r.retry(fmt.Errorf("reopen ltx file at offset %d: %w", r.offset, err)); retryErr != nil {
					return 0, retryErr
				}
				r.logger.Debug("reopen ltx file failed, retrying",
					"level", r.level, "min", r.minTXID, "max", r.maxTXID,
					"offset", r.offset, "error", err, "attempt", r.retryN)
				continue
			}
			if !r.setStream(newRC, streamCancel) {
				// Closed while the stream was being reopened.
				streamCancel()
				_ = newRC.Close()
				return 0, os.ErrClosed
			}
			rc, cancel = newRC, streamCancel
		}

		n, err := r.readStream(rc, cancel, p)
		r.offset += int64(n)

		if err == nil {
			if n > 0 && n == len(p) {
				r.retryN = 0
			}
			return n, nil
		}

		if err == io.EOF {
			// Distinguish legitimate EOF (fully read) from premature EOF
			// (server closed idle connection). When the file size is known
			// and we haven't read it all, treat as a connection drop.
			if r.size > 0 && r.offset < r.size {
				r.logger.Debug("premature EOF on ltx file, reconnecting",
					"level", r.level, "min", r.minTXID, "max", r.maxTXID,
					"offset", r.offset, "size", r.size, "attempt", r.retryN+1)
				r.dropStream()
				if retryErr := r.retry(io.ErrUnexpectedEOF); retryErr != nil {
					return n, retryErr
				}
				if n > 0 {
					// Return the bytes we did get. The caller (e.g. io.ReadFull)
					// will call Read again, which will trigger the reopen above.
					return n, nil
				}
				continue
			}
			return n, io.EOF
		}

		// Non-EOF error (connection reset, timeout, etc.). Close the dead
		// stream so the next iteration reopens from the current offset.
		r.logger.Debug("read error on ltx file, reconnecting",
			"level", r.level, "min", r.minTXID, "max", r.maxTXID,
			"error", err, "offset", r.offset, "attempt", r.retryN+1)
		r.dropStream()
		if retryErr := r.retry(err); retryErr != nil {
			return n, retryErr
		}
		if n > 0 {
			return n, nil
		}
	}
}

// readStream reads from the current stream under a stall timeout. A read that
// produces nothing before the timeout expires has its stream's request context
// cancelled, which unblocks the read with an error and sends the caller down the
// reconnect path that already handles a dropped connection.
//
// Cancellation rather than Close: net/http's (*body).Read holds the body mutex
// for the duration of the network read and (*body).Close wants that same mutex,
// so a watchdog calling Close would queue behind the very read it is trying to
// interrupt. Only the request context reaches a parked read.
func (r *ResumableReader) readStream(rc io.ReadCloser, cancel context.CancelFunc, p []byte) (int, error) {
	// Captured per stream: by the time the timer fires, r.cancel may already
	// belong to a healthy replacement, and cancelling that one would be a
	// self-inflicted drop.
	timeout, offset := resumableReaderStallTimeout, r.offset
	if cancel == nil || timeout <= 0 {
		return rc.Read(p)
	}

	var stalled atomic.Bool
	timer := time.AfterFunc(timeout, func() {
		stalled.Store(true)
		r.logger.Debug("ltx file read stalled, cancelling stream",
			"level", r.level, "min", r.minTXID, "max", r.maxTXID,
			"offset", offset, "timeout", timeout)
		cancel()
	})
	defer timer.Stop()

	n, err := rc.Read(p)
	if err != nil && stalled.Load() {
		// %v, not %w: the underlying error is context.Canceled, and a stall is
		// retryable where a cancelled parent context is terminal. Letting
		// context.Canceled into the chain invites an errors.Is check above to
		// abandon a restore that only needed to reconnect.
		err = fmt.Errorf("%w after %s: %v", ErrReadStalled, timeout, err)
	}
	return n, err
}

// Close closes the current underlying stream, if any, and marks the reader as
// closed. Closing is permanent: subsequent reads fail with os.ErrClosed rather
// than reopening the remote stream, which would leak the new connection.
func (r *ResumableReader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	rc, cancel := r.rc, r.cancel
	r.rc, r.cancel = nil, nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rc != nil {
		return rc.Close()
	}
	return nil
}

// stream returns the current underlying stream, or nil if the next read should
// reopen it. Returns os.ErrClosed if the reader has been closed.
func (r *ResumableReader) stream() (io.ReadCloser, context.CancelFunc, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, nil, os.ErrClosed
	}
	return r.rc, r.cancel, nil
}

// setStream installs a newly reopened stream. It reports false if the reader
// was closed while the stream was being opened, in which case the caller must
// close the new stream itself.
func (r *ResumableReader) setStream(rc io.ReadCloser, cancel context.CancelFunc) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.rc, r.cancel = rc, cancel
	return true
}

// dropStream closes and clears the current stream after a read failure so the
// next read reopens from the current offset. The stream is already being
// discarded, so a close error should not stop recovery. Log it only to aid
// debugging.
func (r *ResumableReader) dropStream() {
	r.mu.Lock()
	rc, cancel := r.rc, r.cancel
	r.rc, r.cancel = nil, nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if rc == nil {
		return
	}
	if err := rc.Close(); err != nil {
		r.logger.Debug("close ltx file",
			"level", r.level, "min", r.minTXID, "max", r.maxTXID,
			"offset", r.offset, "error", err)
	}
}

func (r *ResumableReader) retry(err error) error {
	r.retryN++
	if r.retryN > resumableReaderMaxRetries {
		r.err = fmt.Errorf("max retries exceeded reading ltx file (level=%d, min=%s, max=%s, offset=%d): %w",
			r.level, r.minTXID, r.maxTXID, r.offset, err)
		return r.err
	}

	// Wait before the caller reopens. Retrying with no delay lands every
	// attempt inside the same provider throttle window, so the attempts are
	// spent without the provider ever getting a chance to recover.
	select {
	case <-r.ctx.Done():
		r.err = r.ctx.Err()
		return r.err
	case <-time.After(resumableReaderBackoff << (r.retryN - 1)):
	}
	return nil
}
