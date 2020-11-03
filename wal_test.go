package litestream_test

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/benbjohnson/litestream"
)

func TestWALHeader_MarshalTo(t *testing.T) {
	// Ensure the WAL header can be marshaled and unmarshaled correctly.
	t.Run("OK", func(t *testing.T) {
		hdr := litestream.WALHeader{
			Magic:             1000,
			FileFormatVersion: 1001,
			PageSize:          1002,
			CheckpointSeqNo:   1003,
			Salt:              1004,
			Checksum:          1005,
		}
		b := make([]byte, litestream.WALHeaderSize)
		if err := hdr.MarshalTo(b); err != nil {
			t.Fatal(err)
		}

		var other litestream.WALHeader
		if err := other.Unmarshal(b); err != nil {
			t.Fatal(err)
		} else if got, want := hdr, other; got != want {
			t.Fatalf("mismatch: got %#v, want %#v", got, want)
		}
	})

	// Ensure that marshaling to a small byte slice returns an error.
	t.Run("ErrShortWrite", func(t *testing.T) {
		var hdr litestream.WALHeader
		if err := hdr.MarshalTo(make([]byte, litestream.WALHeaderSize-1)); err != io.ErrShortWrite {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

func TestWALHeader_Unmarshal(t *testing.T) {
	// Ensure that unmarshaling from a small byte slice returns an error.
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		var hdr litestream.WALHeader
		if err := hdr.Unmarshal(make([]byte, litestream.WALHeaderSize-1)); err != io.ErrUnexpectedEOF {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

func TestWALFrameHeader_MarshalTo(t *testing.T) {
	// Ensure the WAL header can be marshaled and unmarshaled correctly.
	t.Run("OK", func(t *testing.T) {
		hdr := litestream.WALFrameHeader{
			Pgno:     1000,
			PageN:    1001,
			Salt:     1002,
			Checksum: 1003,
		}
		b := make([]byte, litestream.WALFrameHeaderSize)
		if err := hdr.MarshalTo(b); err != nil {
			t.Fatal(err)
		}

		var other litestream.WALFrameHeader
		if err := other.Unmarshal(b); err != nil {
			t.Fatal(err)
		} else if got, want := hdr, other; got != want {
			t.Fatalf("mismatch: got %#v, want %#v", got, want)
		}
	})

	// Ensure that marshaling to a small byte slice returns an error.
	t.Run("ErrShortWrite", func(t *testing.T) {
		var hdr litestream.WALFrameHeader
		if err := hdr.MarshalTo(make([]byte, litestream.WALFrameHeaderSize-1)); err != io.ErrShortWrite {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

func TestWALFrameHeader_Unmarshal(t *testing.T) {
	// Ensure that unmarshaling from a small byte slice returns an error.
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		var hdr litestream.WALFrameHeader
		if err := hdr.Unmarshal(make([]byte, litestream.WALFrameHeaderSize-1)); err != io.ErrUnexpectedEOF {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

// MustOpenWALFile returns a new, open instance of WALFile written to a temp dir.
func MustOpenWALFile(tb testing.TB, name string) *litestream.WALFile {
	tb.Helper()

	f := litestream.NewWALFile(filepath.Join(tb.TempDir(), name))
	if err := f.Open(); err != nil {
		tb.Fatal(err)
	}
	return f
}

// MustCloseWALFile closes an instance of WALFile.
func MustCloseWALFile(tb testing.TB, f *litestream.WALFile) {
	tb.Helper()
	if err := f.Close(); err != nil {
		tb.Fatal(err)
	}
}
