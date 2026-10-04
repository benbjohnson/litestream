package litestream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/benbjohnson/litestream/internal"
)

const walContinuityChecksumVersion = 1

type walContinuityChecksum struct {
	Version   int    `json:"version"`
	TXID      uint64 `json:"txid"`
	Offset    int64  `json:"offset"`
	Salt1     uint32 `json:"salt1"`
	Salt2     uint32 `json:"salt2"`
	Checksum1 uint32 `json:"checksum1"`
	Checksum2 uint32 `json:"checksum2"`
}

func (db *DB) walContinuityChecksumPath() string {
	return filepath.Join(db.LTXDir(), "wal-checksum.json")
}

func (db *DB) readWALContinuityChecksum() (*walContinuityChecksum, error) {
	b, err := os.ReadFile(db.walContinuityChecksumPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("read wal continuity checksum: %w", err)
	}

	var checksum walContinuityChecksum
	if err := json.Unmarshal(b, &checksum); err != nil {
		return nil, nil
	}
	if checksum.Version != walContinuityChecksumVersion {
		return nil, nil
	}
	return &checksum, nil
}

// writeWALContinuityChecksum atomically persists the checksum metadata before
// the corresponding LTX file is published. A crash between these writes can
// cause a safe resnapshot on the next open, but cannot leave an unverified LTX
// position appearing verified.
func (db *DB) writeWALContinuityChecksum(checksum walContinuityChecksum) error {
	path := db.walContinuityChecksumPath()
	dir := filepath.Dir(path)
	if err := internal.MkdirAll(dir, db.dirInfo); err != nil {
		return fmt.Errorf("create wal continuity checksum directory: %w", err)
	}

	f, err := os.CreateTemp(dir, "wal-checksum-*.tmp")
	if err != nil {
		return fmt.Errorf("create wal continuity checksum temp file: %w", err)
	}
	tmpPath := f.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	b, err := json.Marshal(checksum)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("encode wal continuity checksum: %w", err)
	}
	if n, err := f.Write(b); err != nil {
		_ = f.Close()
		return fmt.Errorf("write wal continuity checksum: %w", err)
	} else if n != len(b) {
		_ = f.Close()
		return io.ErrShortWrite
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync wal continuity checksum: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close wal continuity checksum: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename wal continuity checksum: %w", err)
	}
	if err := internal.FsyncDir(dir); err != nil {
		return fmt.Errorf("sync wal continuity checksum directory: %w", err)
	}
	return nil
}

func (db *DB) verifyWALContinuity(ctx context.Context, checksum *walContinuityChecksum, txid uint64, offset int64, salt1, salt2 uint32) (bool, error) {
	if checksum == nil || checksum.TXID != txid || checksum.Offset != offset || checksum.Salt1 != salt1 || checksum.Salt2 != salt2 {
		return false, nil
	}
	f, err := os.Open(db.WALPath())
	if err != nil {
		return false, fmt.Errorf("open wal for continuity verification: %w", err)
	}
	defer func() { _ = f.Close() }()

	rd, err := NewWALReader(f, db.Logger.With(LogKeySubsystem, LogSubsystemWALReader))
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, fmt.Errorf("create wal reader for continuity verification: %w", err)
	}
	if offset == WALHeaderSize {
		checksum1, checksum2 := rd.Checksum()
		return checksum1 == checksum.Checksum1 && checksum2 == checksum.Checksum2, nil
	}
	frameSize := int64(rd.PageSize() + WALFrameHeaderSize)
	if offset < WALHeaderSize || (offset-WALHeaderSize)%frameSize != 0 {
		return false, nil
	}
	page := make([]byte, rd.PageSize())
	for walOffset := int64(WALHeaderSize); walOffset < offset; walOffset += frameSize {
		if _, _, err := rd.ReadFrame(ctx, page); err != nil {
			if errors.Is(err, io.EOF) {
				return false, nil
			}
			return false, fmt.Errorf("read wal frame during continuity verification: %w", err)
		}
	}
	checksum1, checksum2 := rd.Checksum()
	return checksum1 == checksum.Checksum1 && checksum2 == checksum.Checksum2, nil
}
