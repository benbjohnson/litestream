package internal

import (
	"context"
	"fmt"
	"io"
	"log/slog"

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
	rc      io.ReadCloser
	logger  *slog.Logger
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

func (r *ResumableReader) Read(p []byte) (int, error) {
	for attempt := 0; attempt <= resumableReaderMaxRetries; attempt++ {
		// Reopen the stream from the current offset if the previous
		// connection was closed (rc is nil after a retry).
		if r.rc == nil {
			rc, err := r.client.OpenLTXFile(r.ctx, r.level, r.minTXID, r.maxTXID, r.offset, 0)
			if err != nil {
				return 0, fmt.Errorf("reopen ltx file at offset %d: %w", r.offset, err)
			}
			r.rc = rc
		}

		n, err := r.rc.Read(p)
		r.offset += int64(n)

		if err == nil {
			return n, nil
		}

		if err == io.EOF {
			// Distinguish legitimate EOF (fully read) from premature EOF
			// (server closed idle connection). When the file size is known
			// and we haven't read it all, treat as a connection drop.
			if r.size > 0 && r.offset < r.size {
				r.logger.Debug("premature EOF on ltx file, reconnecting",
					"level", r.level, "min", r.minTXID, "max", r.maxTXID,
					"offset", r.offset, "size", r.size, "attempt", attempt+1)
				r.rc.Close()
				r.rc = nil
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
			"error", err, "offset", r.offset, "attempt", attempt+1)
		r.rc.Close()
		r.rc = nil
		if n > 0 {
			return n, nil
		}
		continue
	}

	return 0, fmt.Errorf("max retries exceeded reading ltx file (level=%d, min=%s, max=%s, offset=%d)",
		r.level, r.minTXID, r.maxTXID, r.offset)
}

func (r *ResumableReader) Close() error {
	if r.rc != nil {
		return r.rc.Close()
	}
	return nil
}
