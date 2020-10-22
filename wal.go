package litestream

import (
	"encoding/binary"
	"io"
	"os"
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

func (s *WALFile) Open() error {
	panic("TODO")
}

func (s *WALFile) Close() error {
	panic("TODO")
}

func (s *WALFile) WriteHeader(hdr WALHeader) error {
	s.hdr = hdr
	panic("TODO")
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
