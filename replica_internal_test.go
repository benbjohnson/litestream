package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/superfly/ltx"

	_ "modernc.org/sqlite"
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

func mustCreateValidSQLiteDB(tb testing.TB) string {
	tb.Helper()
	dbPath := filepath.Join(tb.TempDir(), "test.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		tb.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t (name) VALUES ('a'), ('b'), ('c')"); err != nil {
		tb.Fatal(err)
	}
	if _, err := db.Exec("CREATE INDEX idx_t_name ON t(name)"); err != nil {
		tb.Fatal(err)
	}
	return dbPath
}

func TestCheckIntegrity_Quick_ValidDB(t *testing.T) {
	dbPath := mustCreateValidSQLiteDB(t)
	if err := checkIntegrity(dbPath, IntegrityCheckQuick); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestCheckIntegrity_Full_ValidDB(t *testing.T) {
	dbPath := mustCreateValidSQLiteDB(t)
	if err := checkIntegrity(dbPath, IntegrityCheckFull); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestCheckIntegrity_None_Skips(t *testing.T) {
	if err := checkIntegrity("/nonexistent/path.db", IntegrityCheckNone); err != nil {
		t.Fatalf("expected nil for IntegrityCheckNone, got: %v", err)
	}
}

func TestCheckIntegrity_CorruptDB(t *testing.T) {
	dbPath := mustCreateValidSQLiteDB(t)

	// Remove any WAL/SHM files so we have a clean single-file database.
	_ = os.Remove(dbPath + "-wal")
	_ = os.Remove(dbPath + "-shm")

	// Read the page size from the database header (bytes 16-17, big-endian).
	f, err := os.OpenFile(dbPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt page 2 onwards. Page 1 is the header/schema page. Corrupting
	// pages that contain table/index data triggers integrity check failures.
	// We overwrite from byte offset 4096 (start of page 2 for 4096-byte pages,
	// which is the default) with garbage data.
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}

	// Overwrite everything after the first page with garbage to ensure corruption.
	pageSize := int64(4096)
	if info.Size() > pageSize {
		garbage := bytes.Repeat([]byte{0xDE}, int(info.Size()-pageSize))
		if _, err := f.WriteAt(garbage, pageSize); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
	}
	_ = f.Close()

	err = checkIntegrity(dbPath, IntegrityCheckFull)
	if err == nil {
		t.Fatal("expected integrity check to fail on corrupt database")
	}
}
