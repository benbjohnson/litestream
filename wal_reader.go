package litestream

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/benbjohnson/litestream/internal"
)

// WALReader wraps an io.Reader and parses SQLite WAL frames.
//
// This reader verifies the salt & checksum integrity while it reads. It does
// not enforce transaction boundaries (i.e. it may return uncommitted frames).
// It is the responsibility of the caller to handle this.
type WALReader struct {
	r      io.ReaderAt
	frameN int

	bo       binary.ByteOrder
	pageSize uint32
	seq      uint32

	salt1, salt2     uint32
	chksum1, chksum2 uint32

	logger *slog.Logger
}

// NewWALReader returns a new instance of WALReader.
func NewWALReader(rd io.ReaderAt, logger *slog.Logger) (*WALReader, error) {
	r := &WALReader{r: rd, logger: logger}
	if err := r.readHeader(); err != nil {
		return nil, err
	}
	return r, nil
}

// NewWALReaderWithOffset returns a new instance of WALReader at a given offset.
// Salt must match or else no frames will be returned. Checksum calculated from
// from previous page.
func NewWALReaderWithOffset(ctx context.Context, rd io.ReaderAt, offset int64, salt1, salt2 uint32, logger *slog.Logger) (*WALReader, error) {
	// Ensure we are not starting on the first page since we need to read the previous.
	if offset <= WALHeaderSize {
		return nil, fmt.Errorf("offset (%d) must be greater than the wal header size (%d)", offset, WALHeaderSize)
	}

	r := &WALReader{r: rd, logger: logger}

	// Read header to determine page size & byte order.
	if err := r.readHeader(); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	// Load in salt in case the beginning of the file has been overwritten.
	r.salt1, r.salt2 = salt1, salt2

	// Ensure offset is positioned on a frame start.
	frameSize := int64(r.pageSize + WALFrameHeaderSize)
	if (offset-WALHeaderSize)%frameSize != 0 {
		return nil, fmt.Errorf("unaligned wal offset %d for page size %d", offset, r.pageSize)
	}
	r.frameN = int((offset - WALHeaderSize) / frameSize)

	// Read previous page to load checksum.
	r.frameN--
	if _, _, err := r.readFrame(ctx, make([]byte, r.pageSize), false); err != nil {
		return nil, &PrevFrameMismatchError{Err: err}
	}

	return r, nil
}

// PageSize returns the page size from the header. Must call ReadHeader() first.
func (r *WALReader) PageSize() uint32 { return r.pageSize }

// Offset returns the file offset of the last read frame.
// Returns zero if no frames have been read.
func (r *WALReader) Offset() int64 {
	if r.frameN == 0 {
		return 0
	}
	return WALHeaderSize + ((int64(r.frameN) - 1) * (WALFrameHeaderSize + int64(r.pageSize)))
}

// readHeader reads the WAL header into the reader. Returns io.EOF if WAL is invalid.
func (r *WALReader) readHeader() error {
	// If we have a partial WAL, then mark WAL as done.
	hdr := make([]byte, WALHeaderSize)
	if n, err := r.r.ReadAt(hdr, 0); n < len(hdr) {
		return io.EOF
	} else if err != nil {
		return err
	}

	// Determine byte order of checksums.
	switch magic := binary.BigEndian.Uint32(hdr[0:]); magic {
	case 0x377f0682:
		r.bo = binary.LittleEndian
	case 0x377f0683:
		r.bo = binary.BigEndian
	default:
		return fmt.Errorf("invalid wal header magic: %x", magic)
	}

	// If the header checksum doesn't match then we may have failed with
	// a partial WAL header write during checkpointing.
	chksum1 := binary.BigEndian.Uint32(hdr[24:])
	chksum2 := binary.BigEndian.Uint32(hdr[28:])
	if v0, v1 := WALChecksum(r.bo, 0, 0, hdr[:24]); v0 != chksum1 || v1 != chksum2 {
		return io.EOF
	}

	// Verify version is correct.
	if version := binary.BigEndian.Uint32(hdr[4:]); version != 3007000 {
		return fmt.Errorf("unsupported wal version: %d", version)
	}

	r.pageSize = binary.BigEndian.Uint32(hdr[8:])
	r.seq = binary.BigEndian.Uint32(hdr[12:])
	r.salt1 = binary.BigEndian.Uint32(hdr[16:])
	r.salt2 = binary.BigEndian.Uint32(hdr[20:])
	r.chksum1, r.chksum2 = chksum1, chksum2

	return nil
}

// ReadFrame reads the next frame from the WAL and returns the page number.
// Returns io.EOF at the end of the valid WAL.
func (r *WALReader) ReadFrame(ctx context.Context, data []byte) (pgno, commit uint32, err error) {
	return r.readFrame(ctx, data, true)
}

func (r *WALReader) readFrame(_ context.Context, data []byte, verifyChecksum bool) (pgno, commit uint32, err error) {
	if len(data) != int(r.pageSize) {
		return 0, 0, fmt.Errorf("WALReader.ReadFrame(): buffer size (%d) must match page size (%d)", len(data), r.pageSize)
	}

	frameSize := r.pageSize + WALFrameHeaderSize
	offset := WALHeaderSize + (int64(r.frameN) * int64(frameSize))

	// Read WAL frame header.
	hdr := make([]byte, WALFrameHeaderSize)
	if n, err := r.r.ReadAt(hdr, offset); n != len(hdr) {
		return 0, 0, io.EOF
	} else if err != nil {
		return 0, 0, err
	}

	// Read WAL page data.
	if n, err := r.r.ReadAt(data, offset+WALFrameHeaderSize); n != len(data) {
		return 0, 0, io.EOF
	} else if err != nil {
		return 0, 0, err
	}

	// Verify salt matches the salt in the header.
	salt1 := binary.BigEndian.Uint32(hdr[8:])
	salt2 := binary.BigEndian.Uint32(hdr[12:])
	if r.salt1 != salt1 || r.salt2 != salt2 {
		return 0, 0, io.EOF
	}

	// Verify the checksum is valid. If checksum verification is disabled, it
	// is because we are jumping to an offset and not checksumming from the beginning.
	chksum1 := binary.BigEndian.Uint32(hdr[16:])
	chksum2 := binary.BigEndian.Uint32(hdr[20:])
	if verifyChecksum {
		r.chksum1, r.chksum2 = WALChecksum(r.bo, r.chksum1, r.chksum2, hdr[:8]) // frame header
		r.chksum1, r.chksum2 = WALChecksum(r.bo, r.chksum1, r.chksum2, data)    // frame data
		if r.chksum1 != chksum1 || r.chksum2 != chksum2 {
			return 0, 0, io.EOF
		}
	} else {
		r.chksum1, r.chksum2 = chksum1, chksum2
	}

	pgno = binary.BigEndian.Uint32(hdr[0:])
	commit = binary.BigEndian.Uint32(hdr[4:])

	r.frameN++

	return pgno, commit, nil
}

// PageMap reads all committed frames until the end of the file and returns a
// map of pgno to offset of the latest version of each page. Also returns the
// max offset of the wal segment read, and the final database size, in pages.
func (r *WALReader) PageMap(ctx context.Context) (m map[uint32]int64, maxOffset int64, commit uint32, err error) {
	m = make(map[uint32]int64)
	txMap := make(map[uint32]int64)
	data := make([]byte, r.pageSize)
	for i := 0; ; i++ {
		pgno, fcommit, err := r.ReadFrame(ctx, data)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, 0, 0, err
		}

		// Update latest offset for the page for this transaction.
		// Pages should not be saved to full map until we know txn is committed.
		offset := r.Offset()
		txMap[pgno] = offset

		// For commit records, transfer offsets to full map and update db size.
		if fcommit != 0 {
			for pgno, offset := range txMap {
				m[pgno] = offset
			}
			commit = fcommit
		}
	}

	// Remove pages that exceed the final commit size. This can occur when the
	// database shrinks (e.g., via VACUUM) between transactions in the WAL.
	for pgno := range m {
		if pgno > commit {
			delete(m, pgno)
		}
	}

	// If full transactions available, return the original offset.
	if len(m) == 0 {
		return m, 0, 0, nil
	}

	// Compute the highest page offsets.
	var end int64
	for _, offset := range m {
		if end == 0 || offset > end {
			end = offset
		}
	}

	// Extend to the end of the last frame read.
	end += WALFrameHeaderSize + int64(r.pageSize)

	r.logger.Log(ctx, internal.LevelTrace, "page map complete", "n", len(m), "end", end, "commit", commit)
	return m, end, commit, nil
}

// FrameSaltsUntil returns a set of all unique frame salts in the WAL file.
func (r *WALReader) FrameSaltsUntil(ctx context.Context, until [2]uint32) (map[[2]uint32]struct{}, error) {
	m := make(map[[2]uint32]struct{})
	for offset := int64(WALHeaderSize); ; offset += int64(WALFrameHeaderSize + r.pageSize) {
		hdr := make([]byte, WALFrameHeaderSize)
		if n, err := r.r.ReadAt(hdr, offset); n != len(hdr) {
			break
		} else if err != nil {
			return nil, err
		}

		salt1 := binary.BigEndian.Uint32(hdr[8:])
		salt2 := binary.BigEndian.Uint32(hdr[12:])

		// Track unique salts.
		m[[2]uint32{salt1, salt2}] = struct{}{}

		// Only read salts until the last one we expect.
		if salt1 == until[0] && salt2 == until[1] {
			break
		}
	}

	return m, nil
}

// WALChecksum computes a running SQLite WAL checksum over a byte slice.
func WALChecksum(bo binary.ByteOrder, s0, s1 uint32, b []byte) (uint32, uint32) {
	assert(len(b)%8 == 0, "misaligned checksum byte slice")

	// Iterate over 8-byte units and compute checksum.
	for i := 0; i < len(b); i += 8 {
		s0 += bo.Uint32(b[i:]) + s1
		s1 += bo.Uint32(b[i+4:]) + s0
	}
	return s0, s1
}

type PrevFrameMismatchError struct {
	Err error
}

func (e *PrevFrameMismatchError) Error() string {
	return fmt.Sprintf("prev frame mismatch: %s", e.Err)
}

func (e *PrevFrameMismatchError) Unwrap() error {
	return e.Err
}
