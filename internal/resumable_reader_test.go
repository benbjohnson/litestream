package internal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

func TestResumableReader(t *testing.T) {
	// The resumableReader wraps a storage stream to handle connection drops
	// during long restore operations. These tests simulate the failure modes
	// that occur when S3/Tigris closes idle connections.

	t.Run("NormalRead", func(t *testing.T) {
		// Verify that a healthy stream passes through unchanged.
		data := []byte("hello world")
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(data[offset:])), nil
			},
		}

		r := newTestResumableReader(client, int64(len(data)), data)
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("got %q, want %q", got, data)
		}
	})

	t.Run("ReconnectOnError", func(t *testing.T) {
		// Simulate a connection reset after reading 5 bytes of a 11-byte file.
		// The reader should transparently reconnect from offset 5 and deliver
		// the remaining bytes.
		data := []byte("hello world")
		callCount := 0
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
				callCount++
				if callCount == 1 {
					// First open: return a reader that errors after 5 bytes.
					return io.NopCloser(&errorAfterN{data: data, n: 5, err: fmt.Errorf("connection reset")}), nil
				}
				// Reconnect: serve from the requested offset.
				return io.NopCloser(bytes.NewReader(data[offset:])), nil
			},
		}

		r := newTestResumableReader(client, int64(len(data)), data)
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("got %q, want %q", got, data)
		}
		if callCount != 2 {
			t.Fatalf("expected 2 OpenLTXFile calls (original + reconnect), got %d", callCount)
		}
	})

	t.Run("RetryInitialOpenError", func(t *testing.T) {
		data := []byte("hello world")
		callCount := 0
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
				callCount++
				if callCount <= 2 {
					return nil, fmt.Errorf("net/http: TLS handshake timeout")
				}
				return io.NopCloser(bytes.NewReader(data[offset:])), nil
			},
		}

		r := NewResumableReader(context.Background(), client, 0, 1, 1, int64(len(data)), nil, slog.Default())
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("got %q, want %q", got, data)
		}
		if got, want := callCount, 3; got != want {
			t.Fatalf("OpenLTXFile() count=%d, want %d", got, want)
		}
	})

	t.Run("ReconnectOnPrematureEOF", func(t *testing.T) {
		// Simulate a server that closes the connection cleanly (returns io.EOF)
		// before all bytes are transferred. The reader detects this by comparing
		// bytes read against the known file size.
		data := []byte("hello world")
		callCount := 0
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
				callCount++
				if callCount == 1 {
					// First open: return only the first 5 bytes, then EOF.
					return io.NopCloser(bytes.NewReader(data[:5])), nil
				}
				// Reconnect: serve from the requested offset.
				return io.NopCloser(bytes.NewReader(data[offset:])), nil
			},
		}

		r := newTestResumableReader(client, int64(len(data)), data)
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("got %q, want %q", got, data)
		}
		if callCount != 2 {
			t.Fatalf("expected 2 OpenLTXFile calls, got %d", callCount)
		}
	})

	t.Run("ReadFullAcrossReconnect", func(t *testing.T) {
		// Simulate io.ReadFull reading a 6-byte page header where the
		// connection drops after 3 bytes. This is the exact scenario from
		// the original bug: the LTX compactor calls io.ReadFull for a
		// 6-byte PageHeader, but the stream is dead.
		data := []byte("ABCDEF remainder of file")
		callCount := 0
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
				callCount++
				if callCount == 1 {
					return io.NopCloser(&errorAfterN{data: data, n: 3, err: fmt.Errorf("connection reset")}), nil
				}
				return io.NopCloser(bytes.NewReader(data[offset:])), nil
			},
		}

		r := newTestResumableReader(client, int64(len(data)), data)

		// Read exactly 6 bytes, like the LTX decoder does for page headers.
		buf := make([]byte, 6)
		_, err := io.ReadFull(r, buf)
		if err != nil {
			t.Fatalf("io.ReadFull failed: %v", err)
		}
		if !bytes.Equal(buf, []byte("ABCDEF")) {
			t.Fatalf("got %q, want %q", buf, "ABCDEF")
		}
	})

	t.Run("MaxRetriesExceeded", func(t *testing.T) {
		// If the connection keeps failing, the reader should give up after
		// the maximum retry count rather than looping forever.
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
				return io.NopCloser(&errorAfterN{data: nil, n: 0, err: fmt.Errorf("persistent failure")}), nil
			},
		}

		r := newTestResumableReader(client, 100, nil)
		buf := make([]byte, 10)
		_, err := r.Read(buf)
		if err == nil {
			t.Fatal("expected error after max retries, got nil")
		}
		if !strings.Contains(err.Error(), "max retries exceeded") {
			t.Fatalf("expected 'max retries exceeded' error, got: %v", err)
		}
	})

	t.Run("ReopenFailure", func(t *testing.T) {
		// If the initial stream dies and the reopen also fails (e.g., 404),
		// the error should propagate.
		data := []byte("hello world")
		callCount := 0
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
				callCount++
				if callCount == 1 {
					return io.NopCloser(&errorAfterN{data: data, n: 3, err: fmt.Errorf("connection reset")}), nil
				}
				return nil, fmt.Errorf("file not found")
			},
		}

		r := newTestResumableReader(client, int64(len(data)), data)

		// First read gets 3 bytes, then error triggers reconnect attempt.
		buf := make([]byte, 10)
		n, err := r.Read(buf)
		if n != 3 {
			t.Fatalf("expected 3 bytes on first read, got %d", n)
		}
		// The error is suppressed on partial reads; next call hits reopen failure.
		if err != nil {
			t.Fatalf("expected nil error on partial read, got: %v", err)
		}

		// Second read should fail with reopen error.
		_, err = r.Read(buf)
		if err == nil {
			t.Fatal("expected error on reopen failure, got nil")
		}
		if !strings.Contains(err.Error(), "reopen ltx file") {
			t.Fatalf("expected 'reopen ltx file' error, got: %v", err)
		}
	})

	t.Run("UnknownSize", func(t *testing.T) {
		// When file size is unknown (size=0), premature EOF cannot be detected,
		// so a clean EOF from a truncated stream is treated as legitimate.
		data := []byte("hello world")
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
				// Always return only first 5 bytes.
				return io.NopCloser(bytes.NewReader(data[:5])), nil
			},
		}

		r := newTestResumableReader(client, 0 /* unknown size */, data)
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Without size info, we can't detect the truncation.
		if !bytes.Equal(got, data[:5]) {
			t.Fatalf("got %q, want %q", got, data[:5])
		}
	})

	t.Run("CorrectOffsetOnReopen", func(t *testing.T) {
		// Verify the reader passes the correct byte offset when reopening.
		data := []byte("0123456789abcdef")
		var reopenOffset int64
		callCount := 0
		client := &testLTXFileOpener{
			OpenLTXFileFunc: func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
				callCount++
				if callCount == 1 {
					return io.NopCloser(&errorAfterN{data: data, n: 7, err: fmt.Errorf("timeout")}), nil
				}
				reopenOffset = offset
				return io.NopCloser(bytes.NewReader(data[offset:])), nil
			},
		}

		r := newTestResumableReader(client, int64(len(data)), data)
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("got %q, want %q", got, data)
		}
		if reopenOffset != 7 {
			t.Fatalf("reopen offset = %d, want 7", reopenOffset)
		}
	})
}

func TestResumableReader_BoundsConnectionsAcrossPartialReads(t *testing.T) {
	data := []byte("0123456789abcdef")
	var connectionN atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(data)-offset))
		_, _ = w.Write(data[offset : offset+1])
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connectionN.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)

	client := server.Client()
	opener := &testLTXFileOpener{
		OpenLTXFileFunc: func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s?offset=%d", server.URL, offset), nil)
			if err != nil {
				return nil, err
			}
			resp, err := client.Do(req)
			if err != nil {
				return nil, err
			}
			return resp.Body, nil
		},
	}

	r := newTestResumableReader(opener, int64(len(data)), data)
	_, err := io.ReadAll(r)
	if err == nil {
		t.Error("ReadAll() error=nil, want retry limit error")
	}
	if got, max := connectionN.Load(), int64(resumableReaderMaxRetries+1); got > max {
		t.Errorf("connections=%d, want at most %d", got, max)
	}
}

// newTestResumableReader creates a resumableReader for testing. The initial
// stream is opened from the client; data is only used for reference.
func newTestResumableReader(client *testLTXFileOpener, size int64, data []byte) *ResumableReader {
	rc, _ := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
	return NewResumableReader(
		context.Background(),
		client,
		0,    // level
		1,    // minTXID
		1,    // maxTXID
		size, // expected file size
		rc,
		slog.Default(),
	)
}

type testLTXFileOpener struct {
	OpenLTXFileFunc func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
}

func (t *testLTXFileOpener) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	return t.OpenLTXFileFunc(ctx, level, minTXID, maxTXID, offset, size)
}

// errorAfterN is a reader that returns data normally for the first n bytes,
// then returns the specified error. This simulates a connection that drops
// mid-transfer.
type errorAfterN struct {
	data []byte
	n    int // bytes to return before erroring
	pos  int
	err  error
}

func (r *errorAfterN) Read(p []byte) (int, error) {
	if r.pos >= r.n {
		return 0, r.err
	}
	remaining := r.n - r.pos
	if len(p) > remaining {
		p = p[:remaining]
	}
	n := copy(p, r.data[r.pos:r.pos+len(p)])
	r.pos += n
	return n, nil
}

func TestResumableReader_BackoffBetweenReopens(t *testing.T) {
	// Burst-retrying a throttling provider (e.g. Tigris load-shedding with
	// 408 RequestCanceled) lands every reopen attempt inside the same
	// throttle window. Reopens must back off between attempts.
	data := []byte("hello world")
	var callTimes []time.Time
	client := &testLTXFileOpener{
		OpenLTXFileFunc: func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			callTimes = append(callTimes, time.Now())
			if len(callTimes) <= 2 {
				return nil, fmt.Errorf("operation error S3: GetObject, api error RequestCanceled: Request is canceled.")
			}
			return io.NopCloser(bytes.NewReader(data[offset:])), nil
		},
	}

	r := NewResumableReader(context.Background(), client, 2, 1, 2, int64(len(data)), nil, slog.Default())
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}
	if len(callTimes) != 3 {
		t.Fatalf("expected 3 OpenLTXFile calls, got %d", len(callTimes))
	}
	for i := 1; i < len(callTimes); i++ {
		if gap := callTimes[i].Sub(callTimes[i-1]); gap < 100*time.Millisecond {
			t.Fatalf("reopen attempt %d fired %v after attempt %d; want >= 100ms backoff", i+1, gap, i)
		}
	}
}

func TestResumableReader_ContextCancelAbortsBackoff(t *testing.T) {
	// Cancellation has to interrupt the backoff itself, not just be noticed
	// before the next attempt. Read() already returns early when the context
	// is done at reopen time, so the wait is cancelled from another goroutine
	// while it is in progress.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opened := make(chan struct{}, 1)
	client := &testLTXFileOpener{
		OpenLTXFileFunc: func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			select {
			case opened <- struct{}{}:
			default:
			}
			return nil, fmt.Errorf("connection reset")
		},
	}

	go func() {
		<-opened
		cancel()
	}()

	r := NewResumableReader(ctx, client, 2, 1, 2, 11, nil, slog.Default())
	start := time.Now()
	_, err := io.ReadAll(r)
	if err == nil {
		t.Fatal("expected error after context cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want an error wrapping context.Canceled", err)
	}
	// Full backoff without cancellation would be 250ms+500ms+1s.
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("read blocked %v after cancellation; backoff must abort on ctx.Done", elapsed)
	}

	// The cancellation is terminal, matching how the reader treats its other
	// terminal errors: a later Read must not reopen the file.
	before := len(opened)
	if _, err := r.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("read after cancellation returned %v, want context.Canceled", err)
	}
	if len(opened) != before {
		t.Fatal("read after cancellation reopened the file; cancellation must be sticky")
	}
}

func TestResumableReader_ContextCancelDuringReopen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opened := make(chan struct{})
	openN := 0
	client := &testLTXFileOpener{
		OpenLTXFileFunc: func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			openN++
			if openN == 1 {
				close(opened)
			}
			<-ctx.Done()
			return nil, fmt.Errorf("connection reset")
		},
	}

	go func() {
		<-opened
		cancel()
	}()

	r := NewResumableReader(ctx, client, 2, 1, 2, 11, nil, slog.Default())
	_, err := r.Read(make([]byte, 1))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Read() error=%v, want an error wrapping context.Canceled", err)
	}
	if err == nil || !strings.Contains(err.Error(), "connection reset") {
		t.Errorf("Read() error=%v, want connection reset context", err)
	}
	if _, err := r.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Errorf("second Read() error=%v, want an error wrapping context.Canceled", err)
	}
	if got, want := openN, 1; got != want {
		t.Errorf("OpenLTXFile() count=%d, want %d", got, want)
	}
}

// A stream that goes silent is not a stream that breaks. Before the stall
// timeout existed, both of these tests hung forever instead of failing.
//
// A third case belongs here and is deliberately left out for now:
// Compactor.Compact closes its source readers when it returns, while the
// compaction goroutine may still be parked in a read, and cancelStream is what
// makes that Close land rather than queue behind the read on net/http's body
// mutex. Asserting it needs a ResumableReader whose Close is sticky, so that a
// cancelled read ends the reader instead of sending the retry loop off to open a
// replacement stream. #1493 and #1500 add exactly that flag; once either is
// merged, this file should grow a TestResumableReader_CloseUnblocksStalledRead
// that parks a reader inside a silent read, calls Close, and requires it to
// return.

func TestResumableReader_ReconnectsOnStalledStream(t *testing.T) {
	defer setStallTimeout(50 * time.Millisecond)()

	data := []byte("hello world")
	var opens atomic.Int32
	client := &testLTXFileOpener{
		OpenLTXFileFunc: func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			if opens.Add(1) == 1 {
				// The first stream hands over 5 bytes and then goes quiet.
				// Only cancelling its request context can free the reader.
				return io.NopCloser(newSilentAfterN(ctx, data, 5)), nil
			}
			return io.NopCloser(bytes.NewReader(data[offset:])), nil
		},
	}

	// rc is nil so the reader opens every stream itself, as restore does.
	r := NewResumableReader(context.Background(), client, 0, 1, 1, int64(len(data)), nil, slog.Default())
	got, err := readAllWithin(t, r, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}
	if n := opens.Load(); n != 2 {
		t.Fatalf("OpenLTXFile calls = %d, want 2 (original + reconnect past the stall)", n)
	}
}

func TestResumableReader_StalledReadIsNotAUserCancel(t *testing.T) {
	// A stall is retryable; a cancelled parent context is terminal. The stall
	// path cancels a child context, so context.Canceled must not survive into
	// the returned error where an errors.Is check would confuse the two.
	defer setStallTimeout(20 * time.Millisecond)()

	data := []byte("hello world")
	client := &testLTXFileOpener{
		OpenLTXFileFunc: func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return io.NopCloser(newSilentAfterN(ctx, data, 5)), nil
		},
	}

	r := NewResumableReader(context.Background(), client, 0, 1, 1, int64(len(data)), nil, slog.Default())
	_, err := readAllWithin(t, r, 10*time.Second)
	if err == nil {
		t.Fatal("expected the retry budget to be exhausted by repeated stalls")
	}
	if !errors.Is(err, ErrReadStalled) {
		t.Fatalf("error %v does not wrap ErrReadStalled", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("stall error leaks context.Canceled, which callers treat as terminal: %v", err)
	}
}

// setStallTimeout shortens the stall bound and returns a func restoring it.
func setStallTimeout(d time.Duration) func() {
	prev := resumableReaderStallTimeout
	resumableReaderStallTimeout = d
	return func() { resumableReaderStallTimeout = prev }
}

func readAllWithin(t *testing.T, r io.Reader, d time.Duration) ([]byte, error) {
	t.Helper()
	type result struct {
		b   []byte
		err error
	}
	ch := make(chan result, 1)
	go func() {
		b, err := io.ReadAll(r)
		ch <- result{b, err}
	}()
	select {
	case res := <-ch:
		return res.b, res.err
	case <-time.After(d):
		t.Fatalf("read hung for %s: the stalled stream was never abandoned", d)
		return nil, nil
	}
}

// silentAfterN serves n bytes and then blocks until its request context is
// cancelled, imitating a provider connection that stops delivering bytes
// without ever closing.
type silentAfterN struct {
	ctx  context.Context
	data []byte
	n    int
	off  int
}

func newSilentAfterN(ctx context.Context, data []byte, n int) *silentAfterN {
	return &silentAfterN{ctx: ctx, data: data, n: n}
}

func (s *silentAfterN) Read(p []byte) (int, error) {
	if s.off < s.n {
		n := copy(p, s.data[s.off:s.n])
		s.off += n
		return n, nil
	}
	<-s.ctx.Done()
	return 0, s.ctx.Err()
}
