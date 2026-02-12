package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

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
