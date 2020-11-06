package sqlite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
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

// HeaderSize is the size of a SQLite 3 database header, in bytes.
const HeaderSize = 100

// WALSuffix is the suffix appended to the end of SQLite WAL path names.
const WALSuffix = "-wal"

// Magic number specified at the beginning of WAL files.
const (
	MagicLittleEndian = 0x377f0682
	MagicBigEndian    = 0x377f0683
)

// IsWALPath returns true if path ends with WALSuffix.
func IsWALPath(path string) bool {
	return strings.HasSuffix(path, WALSuffix)
}

// IsValidHeader returns true if page contains the standard SQLITE3 header.
func IsValidHeader(page []byte) bool {
	return bytes.HasPrefix(page, []byte("SQLite format 3\x00"))
}

// IsWALEnabled returns true if header page has the file format read & write
// version set to 2 (which indicates WAL).
func IsWALEnabled(page []byte) bool {
	return len(page) >= 19 && page[18] == 2 && page[19] == 2
}

// Checksum computes a running checksum over a byte slice.
func Checksum(bo binary.ByteOrder, s uint64, b []byte) (_ uint64, err error) {
	// Ensure byte slice length is divisible by 8.
	if len(b)%8 != 0 {
		return 0, ErrChecksumMisaligned
	}

	// Iterate over 8-byte units and compute checksum.
	s0, s1 := uint32(s>>32), uint32(s&0xFFFFFFFF)
	for i := 0; i < len(b); i += 8 {
		s0 += bo.Uint32(b[i:]) + s1
		s1 += bo.Uint32(b[i+4:]) + s0
	}
	return uint64(s0)<<32 | uint64(s1), nil
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

// ReadFrom reads hdr from r.
func (hdr *WALHeader) ReadFrom(r io.Reader) (n int64, err error) {
	b := make([]byte, WALHeaderSize)
	nn, err := io.ReadFull(r, b)
	if n = int64(nn); err != nil {
		return n, err
	}
	return n, hdr.Unmarshal(b)
}

// WriteTo writes hdr to r.
func (hdr *WALHeader) WriteTo(w io.Writer) (n int64, err error) {
	b := make([]byte, WALHeaderSize)
	if err := hdr.MarshalTo(b); err != nil {
		return 0, err
	}
	nn, err := w.Write(b)
	return int64(nn), err
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

// ReadFrom reads hdr from r.
func (hdr *WALFrameHeader) ReadFrom(r io.Reader) (n int64, err error) {
	b := make([]byte, WALFrameHeaderSize)
	nn, err := io.ReadFull(r, b)
	if n = int64(nn); err != nil {
		return n, err
	}
	return n, hdr.Unmarshal(b)
}

// WriteTo writes hdr to r.
func (hdr *WALFrameHeader) WriteTo(w io.Writer) (n int64, err error) {
	b := make([]byte, WALFrameHeaderSize)
	if err := hdr.MarshalTo(b); err != nil {
		return 0, err
	}
	nn, err := w.Write(b)
	return int64(nn), err
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
