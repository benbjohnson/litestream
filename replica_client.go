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

	// DeleteAll deletes all snapshots & WAL segments.
	DeleteAll(ctx context.Context) error

	// LTXFiles returns an iterator of all LTX files on the replica for a given index. Order is undefined.
	LTXFiles(ctx context.Context, level int) (LTXFileIterator, error)

	// WriteLTXFile writes an LTX file to the replica.
	// Returns metadata for the written segment.
	WriteLTXFile(ctx context.Context, minTXID, maxTXID ltx.TXID, r io.Reader) (*LTXFileInfo, error)

	// ReadLTXFile returns a reader that contains an LZ4 compressed WAL segment at a given
	// index/offset. Returns an os.ErrNotFound error if the WAL segment does not exist.
	ReadLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (io.ReadCloser, error)

	// DeleteLTXFiles deletes one or more LTX files.
	DeleteLTXFiles(ctx context.Context, level int, a [][2]ltx.TXID) error
}
