package litestream

import (
	"encoding/binary"
	"fmt"
	"os"
)

// WALWriter represents a writer to a SQLite WAL file.
type WALWriter struct {
	path     string
	mode     os.FileMode
	pageSize int

	f   *os.File // WAL file handle
	buf []byte   // frame buffer

	chksum0, chksum1 uint32 // ongoing checksum

	Salt0, Salt1 uint32
}

// NewWALWriter returns a new instance of WALWriter.
func NewWALWriter(path string, mode os.FileMode, pageSize int) *WALWriter {
	return &WALWriter{
		path:     path,
		mode:     mode,
		pageSize: pageSize,

		buf: make([]byte, WALFrameHeaderSize+pageSize),
	}
}

// Open opens the file handle to the WAL file.
func (w *WALWriter) Open() (err error) {
	w.f, err = os.OpenFile(w.path, os.O_WRONLY|os.O_TRUNC, w.mode)
	return err
}

// Close closes the file handle to the WAL file.
func (w *WALWriter) Close() error {
	if w.f == nil {
		return nil
	}
	return w.f.Close()
}

// WriteHeader writes the WAL header to the beginning of the file.
func (w *WALWriter) WriteHeader() error {
	// Build WAL header byte slice. Page size and checksum set afterward.
	hdr := []byte{
		0x37, 0x7f, 0x06, 0x82, // magic (little-endian)
		0x00, 0x2d, 0xe2, 0x18, // file format version (3007000)
		0x00, 0x00, 0x00, 0x00, // page size
		0x00, 0x00, 0x00, 0x00, // checkpoint sequence number
		0x00, 0x00, 0x00, 0x00, // salt
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, // checksum
		0x00, 0x00, 0x00, 0x00,
	}

	// Set page size on header
	binary.BigEndian.PutUint32(hdr[8:], uint32(w.pageSize))

	// Set salt
	binary.BigEndian.PutUint32(hdr[16:], w.Salt0)
	binary.BigEndian.PutUint32(hdr[20:], w.Salt1)

	// Compute header checksum.
	w.chksum0, w.chksum1 = Checksum(binary.LittleEndian, w.chksum0, w.chksum1, hdr[:24])
	binary.BigEndian.PutUint32(hdr[24:], w.chksum0)
	binary.BigEndian.PutUint32(hdr[28:], w.chksum1)

	// Write header to WAL.
	_, err := w.f.Write(hdr)
	return err
}

func (w *WALWriter) WriteFrame(pgno, commit uint32, data []byte) error {
	// Ensure data matches page size.
	if len(data) != w.pageSize {
		return fmt.Errorf("data size %d must match page size %d", len(data), w.pageSize)
	}

	// Write frame header.
	binary.BigEndian.PutUint32(w.buf[0:], pgno)    // page number
	binary.BigEndian.PutUint32(w.buf[4:], commit)  // commit record (page count)
	binary.BigEndian.PutUint32(w.buf[8:], w.Salt0) // salt
	binary.BigEndian.PutUint32(w.buf[12:], w.Salt1)

	// Copy data to frame.
	copy(w.buf[WALFrameHeaderSize:], data)

	// Compute checksum for frame.
	w.chksum0, w.chksum1 = Checksum(binary.LittleEndian, w.chksum0, w.chksum1, w.buf[:8])
	w.chksum0, w.chksum1 = Checksum(binary.LittleEndian, w.chksum0, w.chksum1, w.buf[24:])
	binary.BigEndian.PutUint32(w.buf[16:], w.chksum0)
	binary.BigEndian.PutUint32(w.buf[20:], w.chksum1)

	// Write to local WAL
	_, err := w.f.Write(w.buf)
	return err
}
