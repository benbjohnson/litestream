package litestream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// TODO: Pages can be written multiple times before 3.11.0 (https://sqlite.org/releaselog/3_11_0.html)

var (
	// ErrWALHeaderEmpty is returned when writing an empty header.
	ErrWALHeaderEmpty = errors.New("wal header empty")

	// ErrWALFileInitialized is returned when writing a header to a
	// WAL file that has already has its header written.
	ErrWALFileInitialized = errors.New("wal file already initialized")

	// ErrChecksumMisaligned is returned when input byte length is not divisible by 8.
	ErrChecksumMisaligned = errors.New("checksum input misaligned")
)

// WALFile represents a write-ahead log file.
type WALFile struct {
	path string
	hdr  WALHeader

	f *os.File
}

// NewWALFile returns a new instance of WALFile.
func NewWALFile(path string) *WALFile {
	return &WALFile{path: path}
}

// WALHeader returns the WAL header. The return is the zero value if unset.
func (s *WALFile) Header() WALHeader {
	return s.hdr
}

// Open initializes the WAL file descriptor. Creates the file if it doesn't exist.
func (s *WALFile) Open() (err error) {
	// TODO: Validate file contents if non-zero. Return ErrWALFileInvalidHeader if header invalid.
	// TODO: Truncate transaction if commit record is invalid.

	if s.f, err = os.OpenFile(s.path, os.O_RDWR|os.O_CREATE, 0666); err != nil {
		return err
	}
	return nil
}

// Close syncs the WAL file and closes the file descriptor.
func (s *WALFile) Close() error {
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}
	return s.f.Close()
}

// Sync calls Sync() on the underlying file descriptor.
func (s *WALFile) Sync() error {
	return s.f.Sync()
}

// WriteHeader writes hdr to the WAL file.
// Returns an error if hdr is empty or if the file already has a header.
func (s *WALFile) WriteHeader(hdr WALHeader) error {
	if hdr.IsZero() {
		return ErrWALHeaderEmpty
	} else if !s.hdr.IsZero() {
		return ErrWALFileInitialized
	}
	s.hdr = hdr

	// Marshal header & write to file.
	b := make([]byte, WALHeaderSize)
	if err := s.hdr.MarshalTo(b); err != nil {
		return fmt.Errorf("marshal wal header: %w", err)
	} else if _, err := s.f.Write(b); err != nil {
		return err
	}

	return nil
}

func (s *WALFile) WriteFrame(hdr WALFrameHeader, buf []byte) error {
	panic("TODO")
}

// WALHeaderSize is the size of the WAL header, in bytes.
const WALHeaderSize = 32

type WALHeader struct {
	Magic             uint32
	FileFormatVersion uint32
	PageSize          uint32
	CheckpointSeqNo   uint32
	Salt              uint64
	Checksum          uint64
}

// IsZero returns true if hdr is the zero value.
func (hdr WALHeader) IsZero() bool {
	return hdr == (WALHeader{})
}

// ByteOrder returns the byte order based on the hdr magic.
func (hdr WALHeader) ByteOrder() binary.ByteOrder {
	switch hdr.Magic {
	case MagicLittleEndian:
		return binary.LittleEndian
	case MagicBigEndian:
		return binary.BigEndian
	default:
		return nil
	}
}

// MarshalTo encodes the header to b.
// Returns io.ErrShortWrite if len(b) is less than WALHeaderSize.
func (hdr *WALHeader) MarshalTo(b []byte) error {
	if len(b) < WALHeaderSize {
		return io.ErrShortWrite
	}
	binary.BigEndian.PutUint32(b[0:], hdr.Magic)
	binary.BigEndian.PutUint32(b[4:], hdr.FileFormatVersion)
	binary.BigEndian.PutUint32(b[8:], hdr.PageSize)
	binary.BigEndian.PutUint32(b[12:], hdr.CheckpointSeqNo)
	binary.BigEndian.PutUint64(b[16:], hdr.Salt)
	binary.BigEndian.PutUint64(b[24:], hdr.Checksum)
	return nil
}

// Unmarshal decodes the header from b.
// Returns io.ErrUnexpectedEOF if len(b) is less than WALHeaderSize.
func (hdr *WALHeader) Unmarshal(b []byte) error {
	if len(b) < WALHeaderSize {
		return io.ErrUnexpectedEOF
	}
	hdr.Magic = binary.BigEndian.Uint32(b[0:])
	hdr.FileFormatVersion = binary.BigEndian.Uint32(b[4:])
	hdr.PageSize = binary.BigEndian.Uint32(b[8:])
	hdr.CheckpointSeqNo = binary.BigEndian.Uint32(b[12:])
	hdr.Salt = binary.BigEndian.Uint64(b[16:])
	hdr.Checksum = binary.BigEndian.Uint64(b[24:])
	return nil
}

// WALFrameHeaderSize is the size of the WAL frame header, in bytes.
const WALFrameHeaderSize = 24

// WALFrameHeader represents a SQLite WAL frame header.
type WALFrameHeader struct {
	Pgno     uint32
	PageN    uint32 // only set for commit records
	Salt     uint64
	Checksum uint64
}

// IsZero returns true if hdr is the zero value.
func (hdr WALFrameHeader) IsZero() bool {
	return hdr == (WALFrameHeader{})
}

// IsCommit returns true if the frame represents a commit header.
func (hdr *WALFrameHeader) IsCommit() bool {
	return hdr.PageN != 0
}

// MarshalTo encodes the frame header to b.
// Returns io.ErrShortWrite if len(b) is less than WALHeaderSize.
func (hdr *WALFrameHeader) MarshalTo(b []byte) error {
	if len(b) < WALFrameHeaderSize {
		return io.ErrShortWrite
	}
	binary.BigEndian.PutUint32(b[0:], hdr.Pgno)
	binary.BigEndian.PutUint32(b[4:], hdr.PageN)
	binary.BigEndian.PutUint64(b[8:], hdr.Salt)
	binary.BigEndian.PutUint64(b[16:], hdr.Checksum)
	return nil
}

// Unmarshal decodes the frame header from b.
// Returns io.ErrUnexpectedEOF if len(b) is less than WALHeaderSize.
func (hdr *WALFrameHeader) Unmarshal(b []byte) error {
	if len(b) < WALFrameHeaderSize {
		return io.ErrUnexpectedEOF
	}
	hdr.Pgno = binary.BigEndian.Uint32(b[0:])
	hdr.PageN = binary.BigEndian.Uint32(b[4:])
	hdr.Salt = binary.BigEndian.Uint64(b[8:])
	hdr.Checksum = binary.BigEndian.Uint64(b[16:])
	return nil
}
