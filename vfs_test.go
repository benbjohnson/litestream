//go:build vfs
// +build vfs

package litestream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/superfly/ltx"
)

func TestVFSFile_PollReplicaClientIgnoresWhileTimeTravel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	info1, data1 := mustTestLTXFile(t, 0, 1, 1, 1, 0x11)
	info2, data2 := mustTestLTXFile(t, 0, 2, 2, 1, 0x22)

	client := newBlockingReplicaClient()
	client.addFile(info1, data1, false)
	client.addFile(info2, data2, true) // block while polling the newer LTX

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	f := NewVFSFile(client, "db", logger)
	cache, err := lru.New[uint32, []byte](4)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	f.cache = cache

	if err := f.rebuildIndex(ctx, []*ltx.FileInfo{info1}, nil); err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}

	done := make(chan struct{})
	var pollErr error
	go func() {
		defer close(done)
		pollErr = f.pollReplicaClient(ctx)
	}()

	client.waitForBlock(t)

	targetTime := time.Now().Add(-time.Hour)
	if err := f.rebuildIndex(ctx, []*ltx.FileInfo{info1}, &targetTime); err != nil {
		t.Fatalf("time travel rebuild: %v", err)
	}

	client.unblock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pollReplicaClient did not complete")
	}

	if pollErr != nil {
		t.Fatalf("pollReplicaClient error: %v", pollErr)
	}

	f.mu.Lock()
	elem, ok := f.index[1]
	pos := f.pos
	timed := f.targetTime
	f.mu.Unlock()
	if !ok {
		t.Fatalf("page 1 missing from index")
	}
	if got, want := elem.MaxTXID, info1.MaxTXID; got != want {
		t.Fatalf("page index overwritten: max=%s, want %s", got, want)
	}
	if got, want := pos.TXID, info1.MaxTXID; got != want {
		t.Fatalf("pos advanced: %s, want %s", got, want)
	}
	if timed == nil {
		t.Fatalf("target time cleared unexpectedly")
	}
}

func mustTestLTXFile(t *testing.T, level int, minTXID, maxTXID ltx.TXID, pgno uint32, fill byte) (*ltx.FileInfo, []byte) {
	t.Helper()

	buf := new(bytes.Buffer)
	enc, err := ltx.NewEncoder(buf)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}

	hdr := ltx.Header{
		Version:   ltx.Version,
		PageSize:  4096,
		Commit:    pgno,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: time.Now().UnixMilli(),
	}
	if !hdr.IsSnapshot() {
		hdr.PreApplyChecksum = ltx.ChecksumFlag | 1
	}

	if err := enc.EncodeHeader(hdr); err != nil {
		t.Fatalf("EncodeHeader: %v", err)
	}

	pageData := bytes.Repeat([]byte{fill}, int(hdr.PageSize))
	if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, pageData); err != nil {
		t.Fatalf("EncodePage: %v", err)
	}

	enc.SetPostApplyChecksum(ltx.ChecksumFlag | 2)
	if err := enc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info := &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      int64(buf.Len()),
		CreatedAt: time.Now(),
	}
	return info, buf.Bytes()
}

type blockingReplicaClient struct {
	mu         sync.Mutex
	files      map[string]*blockingReplicaFile
	blockCh    chan struct{}
	continueCh chan struct{}
}

type blockingReplicaFile struct {
	info  *ltx.FileInfo
	data  []byte
	block bool
}

func newBlockingReplicaClient() *blockingReplicaClient {
	return &blockingReplicaClient{
		files:      make(map[string]*blockingReplicaFile),
		blockCh:    make(chan struct{}, 1),
		continueCh: make(chan struct{}),
	}
}

func (c *blockingReplicaClient) addFile(info *ltx.FileInfo, data []byte, block bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	infoCopy := *info
	c.files[c.key(info.Level, info.MinTXID, info.MaxTXID)] = &blockingReplicaFile{
		info:  &infoCopy,
		data:  append([]byte(nil), data...),
		block: block,
	}
}

func (c *blockingReplicaClient) key(level int, minTXID, maxTXID ltx.TXID) string {
	return fmt.Sprintf("%d:%s-%s", level, minTXID, maxTXID)
}

func (c *blockingReplicaClient) waitForBlock(t *testing.T) {
	t.Helper()
	select {
	case <-c.blockCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for blocking read")
	}
}

func (c *blockingReplicaClient) unblock() {
	select {
	case <-c.continueCh:
	default:
		close(c.continueCh)
	}
}

func (c *blockingReplicaClient) Type() string { return "blocking" }

func (c *blockingReplicaClient) LTXFiles(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var infos []*ltx.FileInfo
	for _, file := range c.files {
		if file.info.Level != level || file.info.MaxTXID < seek {
			continue
		}
		infoCopy := *file.info
		infos = append(infos, &infoCopy)
	}
	slices.SortFunc(infos, func(a, b *ltx.FileInfo) int {
		switch {
		case a.MinTXID < b.MinTXID:
			return -1
		case a.MinTXID > b.MinTXID:
			return 1
		default:
			return 0
		}
	})
	return ltx.NewFileInfoSliceIterator(infos), nil
}

func (c *blockingReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.mu.Lock()
	file, ok := c.files[c.key(level, minTXID, maxTXID)]
	c.mu.Unlock()
	if !ok {
		return nil, os.ErrNotExist
	}

	if file.block {
		select {
		case c.blockCh <- struct{}{}:
		default:
		}

		select {
		case <-c.continueCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	data := file.data
	if offset > int64(len(data)) {
		data = nil
	} else {
		data = data[offset:]
	}
	if size > 0 && int64(len(data)) > size {
		data = data[:size]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (c *blockingReplicaClient) WriteLTXFile(context.Context, int, ltx.TXID, ltx.TXID, io.Reader) (*ltx.FileInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *blockingReplicaClient) DeleteLTXFiles(context.Context, []*ltx.FileInfo) error { return nil }
func (c *blockingReplicaClient) DeleteAll(context.Context) error                       { return nil }
