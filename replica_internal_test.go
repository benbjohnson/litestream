package litestream

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

func TestReplica_ApplyNewLTXFiles_FillGapWithOverlappingCompactedFile(t *testing.T) {
	const pageSize = 4096

	compactedInfo := &ltx.FileInfo{Level: 1, MinTXID: 100, MaxTXID: 200}
	l0Info := &ltx.FileInfo{Level: 0, MinTXID: 201, MaxTXID: 201}

	fixtures := map[string][]byte{
		ltxFixtureKey(compactedInfo.Level, compactedInfo.MinTXID, compactedInfo.MaxTXID): mustBuildIncrementalLTX(t, compactedInfo.MinTXID, compactedInfo.MaxTXID, pageSize, 1, 0xA1),
		ltxFixtureKey(l0Info.Level, l0Info.MinTXID, l0Info.MaxTXID):                      mustBuildIncrementalLTX(t, l0Info.MinTXID, l0Info.MaxTXID, pageSize, 1, 0xB2),
	}

	client := &followTestReplicaClient{}
	client.LTXFilesFunc = func(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
		var all []*ltx.FileInfo
		switch level {
		case 0:
			all = []*ltx.FileInfo{l0Info}
		case 1:
			all = []*ltx.FileInfo{compactedInfo}
		default:
			all = nil
		}

		infos := make([]*ltx.FileInfo, 0, len(all))
		for _, info := range all {
			if info.MinTXID >= seek {
				infos = append(infos, info)
			}
		}
		return ltx.NewFileInfoSliceIterator(infos), nil
	}
	client.OpenLTXFileFunc = func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, _, _ int64) (io.ReadCloser, error) {
		key := ltxFixtureKey(level, minTXID, maxTXID)
		data, ok := fixtures[key]
		if !ok {
			return nil, os.ErrNotExist
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}

	r := NewReplicaWithClient(nil, client)
	f := mustCreateWritableDBFile(t)
	defer func() { _ = f.Close() }()

	got, err := r.applyNewLTXFiles(context.Background(), f, 150, pageSize)
	if err != nil {
		t.Fatalf("apply new ltx files: %v", err)
	}
	if got != 201 {
		t.Fatalf("txid=%s, want %s", got, ltx.TXID(201))
	}
}

func TestReplica_ApplyNewLTXFiles_LevelZeroEmptyFallsBackToCompaction(t *testing.T) {
	const pageSize = 4096

	compactedInfo := &ltx.FileInfo{Level: 1, MinTXID: 11, MaxTXID: 12}
	fixtures := map[string][]byte{
		ltxFixtureKey(compactedInfo.Level, compactedInfo.MinTXID, compactedInfo.MaxTXID): mustBuildIncrementalLTX(t, compactedInfo.MinTXID, compactedInfo.MaxTXID, pageSize, 1, 0xC3),
	}

	client := &followTestReplicaClient{}
	client.LTXFilesFunc = func(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
		switch level {
		case 0:
			return ltx.NewFileInfoSliceIterator(nil), nil
		case 1:
			if compactedInfo.MinTXID < seek {
				return ltx.NewFileInfoSliceIterator(nil), nil
			}
			return ltx.NewFileInfoSliceIterator([]*ltx.FileInfo{compactedInfo}), nil
		default:
			return ltx.NewFileInfoSliceIterator(nil), nil
		}
	}
	client.OpenLTXFileFunc = func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, _, _ int64) (io.ReadCloser, error) {
		key := ltxFixtureKey(level, minTXID, maxTXID)
		data, ok := fixtures[key]
		if !ok {
			return nil, os.ErrNotExist
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}

	r := NewReplicaWithClient(nil, client)
	f := mustCreateWritableDBFile(t)
	defer func() { _ = f.Close() }()

	got, err := r.applyNewLTXFiles(context.Background(), f, 10, pageSize)
	if err != nil {
		t.Fatalf("apply new ltx files: %v", err)
	}
	if got != 12 {
		t.Fatalf("txid=%s, want %s", got, ltx.TXID(12))
	}
}

func TestReplica_ApplyNewLTXFiles_IteratorCloseError(t *testing.T) {
	client := &followTestReplicaClient{}
	client.LTXFilesFunc = func(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
		if level == 0 {
			return &errorFileIterator{closeErr: fmt.Errorf("level 0 listing failed")}, nil
		}
		return ltx.NewFileInfoSliceIterator(nil), nil
	}
	client.OpenLTXFileFunc = func(_ context.Context, _ int, _, _ ltx.TXID, _, _ int64) (io.ReadCloser, error) {
		return nil, fmt.Errorf("unexpected open")
	}

	r := NewReplicaWithClient(nil, client)
	f := mustCreateWritableDBFile(t)
	defer func() { _ = f.Close() }()

	_, err := r.applyNewLTXFiles(context.Background(), f, 10, 4096)
	if err == nil {
		t.Fatal("expected error")
	}
	if got, want := err.Error(), "level 0 listing failed"; !bytes.Contains([]byte(got), []byte(want)) {
		t.Fatalf("error=%q, want substring %q", got, want)
	}
}

func TestReplica_ApplyLTXFile_VerifiesChecksumOnClose(t *testing.T) {
	const pageSize = 4096

	info := &ltx.FileInfo{Level: 0, MinTXID: 20, MaxTXID: 20}
	data := mustBuildIncrementalLTX(t, info.MinTXID, info.MaxTXID, pageSize, 1, 0xD4)
	data[len(data)-1] ^= 0xFF

	client := &followTestReplicaClient{}
	client.OpenLTXFileFunc = func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, _, _ int64) (io.ReadCloser, error) {
		if level != info.Level || minTXID != info.MinTXID || maxTXID != info.MaxTXID {
			return nil, os.ErrNotExist
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}

	r := NewReplicaWithClient(nil, client)
	f := mustCreateWritableDBFile(t)
	defer func() { _ = f.Close() }()

	err := r.applyLTXFile(context.Background(), f, info, pageSize)
	if err == nil {
		t.Fatal("expected checksum validation error")
	}
}

type errorFileIterator struct {
	closeErr error
}

type followTestReplicaClient struct {
	LTXFilesFunc       func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error)
	OpenLTXFileFunc    func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error)
	WriteLTXFileFunc   func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error)
	DeleteLTXFilesFunc func(ctx context.Context, a []*ltx.FileInfo) error
	DeleteAllFunc      func(ctx context.Context) error
}

func (*followTestReplicaClient) Type() string { return "test" }

func (*followTestReplicaClient) Init(context.Context) error { return nil }

func (c *followTestReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	if c.LTXFilesFunc != nil {
		return c.LTXFilesFunc(ctx, level, seek, useMetadata)
	}
	return ltx.NewFileInfoSliceIterator(nil), nil
}

func (c *followTestReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if c.OpenLTXFileFunc != nil {
		return c.OpenLTXFileFunc(ctx, level, minTXID, maxTXID, offset, size)
	}
	return nil, os.ErrNotExist
}

func (c *followTestReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if c.WriteLTXFileFunc != nil {
		return c.WriteLTXFileFunc(ctx, level, minTXID, maxTXID, r)
	}
	return nil, fmt.Errorf("not implemented")
}

func (c *followTestReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	if c.DeleteLTXFilesFunc != nil {
		return c.DeleteLTXFilesFunc(ctx, a)
	}
	return nil
}

func (c *followTestReplicaClient) DeleteAll(ctx context.Context) error {
	if c.DeleteAllFunc != nil {
		return c.DeleteAllFunc(ctx)
	}
	return nil
}

func (itr *errorFileIterator) Close() error {
	return itr.closeErr
}

func (itr *errorFileIterator) Next() bool {
	return false
}

func (itr *errorFileIterator) Err() error {
	return itr.closeErr
}

func (itr *errorFileIterator) Item() *ltx.FileInfo {
	return nil
}

func mustBuildIncrementalLTX(tb testing.TB, minTXID, maxTXID ltx.TXID, pageSize, pgno uint32, fill byte) []byte {
	tb.Helper()

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		tb.Fatal(err)
	}

	hdr := ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    pgno,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: time.Now().UnixMilli(),
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		tb.Fatal(err)
	}
	page := bytes.Repeat([]byte{fill}, int(pageSize))
	if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, page); err != nil {
		tb.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		tb.Fatal(err)
	}

	return buf.Bytes()
}

func mustCreateWritableDBFile(tb testing.TB) *os.File {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), "follower.db")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		tb.Fatal(err)
	}
	if err := f.Truncate(128 * 1024); err != nil {
		_ = f.Close()
		tb.Fatal(err)
	}
	return f
}

func ltxFixtureKey(level int, minTXID, maxTXID ltx.TXID) string {
	return fmt.Sprintf("%d:%s:%s", level, minTXID, maxTXID)
}

// TestApplyLTXFile_TruncatesAfterWrite verifies that applyLTXFile correctly
// truncates the database file to match the commit size in the LTX header.
// This tests the fix from commit 9b7db29 which adds f.Sync() before truncate.
func TestApplyLTXFile_TruncatesAfterWrite(t *testing.T) {
	const pageSize = 4096

	info := &ltx.FileInfo{Level: 0, MinTXID: 10, MaxTXID: 10}

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	hdr := ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    2, // 2 pages = 8192 bytes
		MinTXID:   info.MinTXID,
		MaxTXID:   info.MaxTXID,
		Timestamp: time.Now().UnixMilli(),
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		t.Fatal(err)
	}
	// Write page 1 with SQLite-like header bytes.
	page1 := make([]byte, pageSize)
	copy(page1, []byte("SQLite format 3\000"))
	page1[18], page1[19] = 0x01, 0x01
	binary.BigEndian.PutUint16(page1[16:18], pageSize)
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, page1); err != nil {
		t.Fatal(err)
	}
	// Write page 2.
	page2 := bytes.Repeat([]byte{0xAB}, pageSize)
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 2}, page2); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	ltxData := buf.Bytes()
	client := &followTestReplicaClient{}
	client.OpenLTXFileFunc = func(_ context.Context, level int, minTXID, maxTXID ltx.TXID, _, _ int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(ltxData)), nil
	}

	r := NewReplicaWithClient(nil, client)

	// Create a file that's larger than the commit size (simulating a DB
	// that will be truncated down).
	f := mustCreateWritableDBFile(t) // 128KB
	defer func() { _ = f.Close() }()

	if err := r.applyLTXFile(context.Background(), f, info, pageSize); err != nil {
		t.Fatalf("applyLTXFile: %v", err)
	}

	// Verify the file was truncated to Commit * pageSize = 8192 bytes.
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	expectedSize := int64(2) * int64(pageSize)
	if fi.Size() != expectedSize {
		t.Fatalf("file size=%d, want %d (truncate failed)", fi.Size(), expectedSize)
	}

	// Verify page 2 data was written correctly.
	readBuf := make([]byte, pageSize)
	if _, err := f.ReadAt(readBuf, int64(pageSize)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readBuf, page2) {
		t.Fatal("page 2 data mismatch after applyLTXFile")
	}
}

// TestApplyLTXFile_MultiplePages verifies that applying an LTX file with
// multiple pages produces a valid, correctly sized database.
func TestApplyLTXFile_MultiplePages(t *testing.T) {
	const pageSize = 4096
	const numPages = 5

	info := &ltx.FileInfo{Level: 0, MinTXID: 1, MaxTXID: 1}

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	hdr := ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    numPages,
		MinTXID:   info.MinTXID,
		MaxTXID:   info.MaxTXID,
		Timestamp: time.Now().UnixMilli(),
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		t.Fatal(err)
	}

	// Write pages with distinct fill patterns.
	pages := make([][]byte, numPages)
	for i := uint32(0); i < numPages; i++ {
		pages[i] = bytes.Repeat([]byte{byte(0x10 + i)}, pageSize)
		if i == 0 {
			copy(pages[i], []byte("SQLite format 3\000"))
			pages[i][18], pages[i][19] = 0x01, 0x01
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: i + 1}, pages[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	ltxData := buf.Bytes()
	client := &followTestReplicaClient{}
	client.OpenLTXFileFunc = func(_ context.Context, _ int, _, _ ltx.TXID, _, _ int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(ltxData)), nil
	}

	r := NewReplicaWithClient(nil, client)
	f := mustCreateWritableDBFile(t)
	defer func() { _ = f.Close() }()

	if err := r.applyLTXFile(context.Background(), f, info, pageSize); err != nil {
		t.Fatalf("applyLTXFile: %v", err)
	}

	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	expectedSize := int64(numPages) * int64(pageSize)
	if fi.Size() != expectedSize {
		t.Fatalf("file size=%d, want %d", fi.Size(), expectedSize)
	}

	// Verify each page's content.
	readBuf := make([]byte, pageSize)
	for i := uint32(0); i < numPages; i++ {
		if _, err := f.ReadAt(readBuf, int64(i)*int64(pageSize)); err != nil {
			t.Fatalf("read page %d: %v", i+1, err)
		}
		// Page 1 has modified header bytes (journal mode + schema counter),
		// so only check non-header pages exactly.
		if i > 0 && !bytes.Equal(readBuf, pages[i]) {
			t.Fatalf("page %d data mismatch", i+1)
		}
	}
}
