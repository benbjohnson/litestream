package mock

import (
	"github.com/benbjohnson/litestream"
)

type WALSegmentIterator struct {
	CloseFunc      func() error
	NextFunc       func() bool
	ErrFunc        func() error
	WALSegmentFunc func() litestream.WALSegmentInfo
}

func (itr *WALSegmentIterator) Close() error {
	return itr.CloseFunc()
}

func (itr *WALSegmentIterator) Next() bool {
	return itr.NextFunc()
}

func (itr *WALSegmentIterator) Err() error {
	return itr.ErrFunc()
}

func (itr *WALSegmentIterator) WALSegment() litestream.WALSegmentInfo {
	return itr.WALSegmentFunc()
}
