package litestream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/superfly/ltx"
)

var ErrStopIter = errors.New("stop iterator")

// ReplicaClient represents client to connect to a Replica.
type ReplicaClient interface {
	// Type returns the type of client.
	Type() string

	// LTXFiles returns an iterator of all LTX files on the replica for a given level.
	// If seek is specified, the iterator start from the given TXID or the next available if not found.
	LTXFiles(ctx context.Context, level int, seek ltx.TXID) (ltx.FileIterator, error)

	// OpenLTXFile returns a reader that contains an LTX file at a given TXID.
	// If seek is specified, the reader will start at the given offset.
	// Returns an os.ErrNotFound error if the LTX file does not exist.
	OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)

	// WriteLTXFile writes an LTX file to the replica.
	// Returns metadata for the written file.
	WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error)

	// DeleteLTXFiles deletes one or more LTX files.
	DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error

	// DeleteAll deletes all files.
	DeleteAll(ctx context.Context) error
}

// FindLTXFiles returns a list of files that match filter.
func FindLTXFiles(ctx context.Context, client ReplicaClient, level int, filter func(*ltx.FileInfo) (bool, error)) ([]*ltx.FileInfo, error) {
	itr, err := client.LTXFiles(ctx, level, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = itr.Close() }()

	var a []*ltx.FileInfo
	for itr.Next() {
		item := itr.Item()

		match, err := filter(item)
		if match {
			a = append(a, item)
		}

		if errors.Is(err, ErrStopIter) {
			break
		} else if err != nil {
			return a, err
		}
	}

	if err := itr.Close(); err != nil {
		return nil, err
	}
	return a, nil
}

// DefaultEstimatedPageIndexSize is size that is first fetched when fetching the page index.
// If the fetch was smaller than the actual page index, another call is made to fetch the rest.
const DefaultEstimatedPageIndexSize = 32 * 1024 // 32KB

func FetchPageIndex(ctx context.Context, client ReplicaClient, info *ltx.FileInfo) (map[uint32]ltx.PageIndexElem, error) {
	rc, err := fetchPageIndexData(ctx, client, info)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return ltx.DecodePageIndex(bufio.NewReader(rc), info.Level, info.MinTXID, info.MaxTXID)
}

// fetchPageIndexData fetches a chunk of the end of the file to get the page index.
// If the fetch was smaller than the actual page index, another call is made to fetch the rest.
func fetchPageIndexData(ctx context.Context, client ReplicaClient, info *ltx.FileInfo) (io.ReadCloser, error) {
	// Fetch the end of the file to get the page index.
	offset := info.Size - DefaultEstimatedPageIndexSize
	if offset < 0 {
		offset = 0
	}

	f, err := client.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, offset, 0)
	if err != nil {
		return nil, fmt.Errorf("open ltx file: %w", err)
	}
	defer f.Close()

	// If we have read the full size of the page index, return the page index block as a reader.
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read ltx page index: %w", err)
	}
	size := binary.BigEndian.Uint64(b[len(b)-ltx.TrailerSize-8:])
	if off := len(b) - int(size) - ltx.TrailerSize - 8; off > 0 {
		return io.NopCloser(bytes.NewReader(b[off:])), nil
	}

	// Otherwise read the file from the start of the page index.
	f, err = client.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, info.Size-ltx.TrailerSize-8-int64(size), 0)
	if err != nil {
		return nil, fmt.Errorf("open ltx file: %w", err)
	}
	return f, nil
}

// FetchPage fetches and decodes a single page frame from an LTX file.
func FetchPage(ctx context.Context, client ReplicaClient, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (ltx.PageHeader, []byte, error) {
	f, err := client.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
	if err != nil {
		return ltx.PageHeader{}, nil, fmt.Errorf("open ltx file: %w", err)
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		return ltx.PageHeader{}, nil, fmt.Errorf("read ltx page frame: %w", err)
	}
	return ltx.DecodePageData(b)
}
