package litestream

import (
	"context"
	"errors"
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
	// Returns an os.ErrNotFound error if the LTX file does not exist.
	OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (io.ReadCloser, error)

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

		if err == ErrStopIter {
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
