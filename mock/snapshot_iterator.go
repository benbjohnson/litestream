package mock

import (
	"github.com/benbjohnson/litestream"
)

type SnapshotIterator struct {
	CloseFunc    func() error
	NextFunc     func() bool
	ErrFunc      func() error
	SnapshotFunc func() litestream.SnapshotInfo
}

func (itr *SnapshotIterator) Close() error {
	return itr.CloseFunc()
}

func (itr *SnapshotIterator) Next() bool {
	return itr.NextFunc()
}

func (itr *SnapshotIterator) Err() error {
	return itr.ErrFunc()
}

func (itr *SnapshotIterator) Snapshot() litestream.SnapshotInfo {
	return itr.SnapshotFunc()
}
