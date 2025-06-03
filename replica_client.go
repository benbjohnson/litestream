package litestream

import (
	"context"
	"io"

	"github.com/superfly/ltx"
)

// ReplicaClient represents client to connect to a Replica.
type ReplicaClient interface {
	// Type returns the type of client.
	Type() string

	// LTXFiles returns an iterator of all LTX files on the replica for a given level.
	LTXFiles(ctx context.Context, level int) (ltx.FileIterator, error)

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
