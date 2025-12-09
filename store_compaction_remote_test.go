package litestream_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// TestStore_CompactDB_RemotePartialRead ensures that compactions do not rely on
// immediately consistent remote reads. Some object stores (or custom replica
// clients) can expose a newly written object before all bytes are available.
// Without additional safeguards, compaction can read the partial object and
// generate a corrupted snapshot which then fails during restore.
func TestStore_CompactDB_RemotePartialRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	client := newDelayedReplicaClient(200 * time.Millisecond)

	dbPath := filepath.Join(t.TempDir(), "db")
	db := litestream.NewDB(dbPath)
	db.MonitorInterval = 0
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.MonitorEnabled = false

	levels := litestream.CompactionLevels{
		{Level: 0},
		{Level: 1, Interval: time.Second},
	}
	store := litestream.NewStore([]*litestream.DB{db}, levels)
	store.CompactionMonitorEnabled = false

	if err := store.Open(ctx); err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() {
		if err := store.Close(ctx); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()

	sqldb := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb)

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	insert := func(start, end int) {
		for i := start; i < end; i++ {
			if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (val) VALUES (?)`, fmt.Sprintf("value-%d", i)); err != nil {
				t.Fatalf("insert %d: %v", i, err)
			}
		}
	}

	// Generate two consecutive L0 files.
	insert(0, 256)
	if err := db.Sync(ctx); err != nil {
		t.Fatalf("sync #1: %v", err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatalf("replica sync #1: %v", err)
	}

	insert(256, 512)
	if err := db.Sync(ctx); err != nil {
		t.Fatalf("sync #2: %v", err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatalf("replica sync #2: %v", err)
	}

	// Compact level 0 into level 1. The delayed replica returns a partial view
	// for newly written files which previously resulted in corrupted snapshots.
	if _, err := store.CompactDB(ctx, db, levels[1]); err != nil {
		t.Fatalf("compact: %v", err)
	}

	client.waitForAvailability()

	restorePath := filepath.Join(t.TempDir(), "restore.db")
	if err := db.Replica.Restore(ctx, litestream.RestoreOptions{OutputPath: restorePath}); err != nil {
		t.Fatalf("restore: %v", err)
	}
}

// delayedReplicaClient simulates an eventually-consistent object store where a
// newly written object can be observed before all of its content is available.
// Prior to availability, OpenLTXFile returns a valid but truncated LTX file.
type delayedReplicaClient struct {
	mu    sync.Mutex
	files map[string]*delayedFile
	delay time.Duration
}

type delayedFile struct {
	level       int
	min         ltx.TXID
	max         ltx.TXID
	data        []byte
	partial     []byte
	createdAt   time.Time
	availableAt time.Time
}

func newDelayedReplicaClient(delay time.Duration) *delayedReplicaClient {
	return &delayedReplicaClient{
		files: make(map[string]*delayedFile),
		delay: delay,
	}
}

func (c *delayedReplicaClient) Type() string { return "delayed" }

func (c *delayedReplicaClient) Init(context.Context) error { return nil }

func (c *delayedReplicaClient) key(level int, min, max ltx.TXID) string {
	return fmt.Sprintf("%d:%s:%s", level, min.String(), max.String())
}

func (c *delayedReplicaClient) LTXFiles(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	infos := make([]*ltx.FileInfo, 0, len(c.files))
	for _, file := range c.files {
		if file.level != level {
			continue
		}
		if file.max < seek {
			continue
		}
		infos = append(infos, &ltx.FileInfo{
			Level:     file.level,
			MinTXID:   file.min,
			MaxTXID:   file.max,
			Size:      int64(len(file.data)),
			CreatedAt: file.createdAt,
		})
	}

	return ltx.NewFileInfoSliceIterator(infos), nil
}

func (c *delayedReplicaClient) OpenLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.mu.Lock()
	file, ok := c.files[c.key(level, minTXID, maxTXID)]
	c.mu.Unlock()
	if !ok {
		return nil, os.ErrNotExist
	}

	data := file.data
	if time.Now().Before(file.availableAt) && len(file.partial) > 0 {
		data = file.partial
	}

	if offset > int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	data = data[offset:]
	if size > 0 && size < int64(len(data)) {
		data = data[:size]
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

func (c *delayedReplicaClient) WriteLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	partial, err := buildPartialSnapshot(data)
	if err != nil {
		return nil, err
	}

	info := &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      int64(len(data)),
		CreatedAt: time.Now().UTC(),
	}

	c.mu.Lock()
	c.files[c.key(level, minTXID, maxTXID)] = &delayedFile{
		level:       level,
		min:         minTXID,
		max:         maxTXID,
		data:        data,
		partial:     partial,
		createdAt:   info.CreatedAt,
		availableAt: time.Now().Add(c.delay),
	}
	c.mu.Unlock()

	return info, nil
}

func (c *delayedReplicaClient) DeleteLTXFiles(_ context.Context, a []*ltx.FileInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, info := range a {
		delete(c.files, c.key(info.Level, info.MinTXID, info.MaxTXID))
	}
	return nil
}

func (c *delayedReplicaClient) DeleteAll(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.files = make(map[string]*delayedFile)
	return nil
}

func (c *delayedReplicaClient) waitForAvailability() {
	time.Sleep(c.delay)
}

// buildPartialSnapshot returns a valid LTX snapshot that only includes the
// first portion of pages from data.
func buildPartialSnapshot(data []byte) ([]byte, error) {
	dec := ltx.NewDecoder(bytes.NewReader(data))
	if err := dec.DecodeHeader(); err != nil {
		return nil, err
	}
	hdr := dec.Header()

	buf := new(bytes.Buffer)
	enc, err := ltx.NewEncoder(buf)
	if err != nil {
		return nil, err
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		return nil, err
	}

	// Copy only a subset of pages so the resulting snapshot is incomplete.
	maxPages := int(hdr.Commit / 4)
	if maxPages < 1 {
		maxPages = 1
	}
	var page ltx.PageHeader
	pageBuf := make([]byte, hdr.PageSize)
	for i := 0; i < maxPages; i++ {
		if err := dec.DecodePage(&page, pageBuf); err != nil {
			return nil, err
		}
		if err := enc.EncodePage(page, pageBuf); err != nil {
			return nil, err
		}
	}

	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
