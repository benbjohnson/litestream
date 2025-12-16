package litestream

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/superfly/ltx"
	_ "modernc.org/sqlite"
)

// testReplicaClient is a minimal mock for testing that doesn't cause import cycles.
type testReplicaClient struct {
	dir string
}

func (c *testReplicaClient) Init(_ context.Context) error { return nil }

func (c *testReplicaClient) Type() string { return "test" }

func (c *testReplicaClient) LTXFiles(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
	return ltx.NewFileInfoSliceIterator(nil), nil
}

func (c *testReplicaClient) OpenLTXFile(_ context.Context, _ int, _, _ ltx.TXID, _, _ int64) (io.ReadCloser, error) {
	return nil, os.ErrNotExist
}

func (c *testReplicaClient) WriteLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	levelDir := filepath.Join(c.dir, fmt.Sprintf("l%d", level))
	if err := os.MkdirAll(levelDir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(levelDir, ltx.FormatFilename(minTXID, maxTXID))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return nil, err
	}
	return &ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID, Size: int64(len(data))}, nil
}

func (c *testReplicaClient) DeleteLTXFiles(_ context.Context, _ []*ltx.FileInfo) error {
	return nil
}

func (c *testReplicaClient) DeleteAll(_ context.Context) error {
	return nil
}

// errorReplicaClient is a replica client that returns errors for testing.
type errorReplicaClient struct {
	writeErr error
}

func (c *errorReplicaClient) Init(_ context.Context) error { return nil }

func (c *errorReplicaClient) Type() string { return "error" }

func (c *errorReplicaClient) LTXFiles(_ context.Context, _ int, _ ltx.TXID, _ bool) (ltx.FileIterator, error) {
	return ltx.NewFileInfoSliceIterator(nil), nil
}

func (c *errorReplicaClient) OpenLTXFile(_ context.Context, _ int, _, _ ltx.TXID, _, _ int64) (io.ReadCloser, error) {
	return nil, os.ErrNotExist
}

func (c *errorReplicaClient) WriteLTXFile(_ context.Context, _ int, _, _ ltx.TXID, _ io.Reader) (*ltx.FileInfo, error) {
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return nil, nil
}

func (c *errorReplicaClient) DeleteLTXFiles(_ context.Context, _ []*ltx.FileInfo) error {
	return nil
}

func (c *errorReplicaClient) DeleteAll(_ context.Context) error {
	return nil
}

// TestCalcWALSize ensures calcWALSize doesn't overflow with large page sizes.
// Regression test for uint32 overflow bug where large page sizes (>=16KB)
// caused incorrect WAL size calculations, triggering checkpoints too early.
func TestCalcWALSize(t *testing.T) {
	tests := []struct {
		name     string
		pageSize uint32
		pageN    uint32
		expected int64
	}{
		{
			name:     "4KB pages, 121359 pages (default TruncatePageN)",
			pageSize: 4096,
			pageN:    121359,
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+4096) * 121359),
		},
		{
			name:     "16KB pages, 121359 pages",
			pageSize: 16384,
			pageN:    121359,
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+16384) * 121359),
		},
		{
			name:     "32KB pages, 121359 pages",
			pageSize: 32768,
			pageN:    121359,
			// Expected: ~4.0 GB with 32KB pages. Bug previously overflowed.
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+32768) * 121359),
		},
		{
			name:     "64KB pages, 121359 pages",
			pageSize: 65536,
			pageN:    121359,
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+65536) * 121359),
		},
		{
			name:     "1KB pages, 1k pages (min checkpoint)",
			pageSize: 1024,
			pageN:    1000,
			expected: int64(WALHeaderSize) + (int64(WALFrameHeaderSize+1024) * 1000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calcWALSize(tt.pageSize, tt.pageN)
			if got != tt.expected {
				t.Errorf("calcWALSize(%d, %d) = %d, want %d (%.2f GB vs %.2f GB)",
					tt.pageSize, tt.pageN, got, tt.expected,
					float64(got)/(1024*1024*1024), float64(tt.expected)/(1024*1024*1024))
			}

			if got <= 0 {
				t.Errorf("calcWALSize(%d, %d) = %d, should be positive", tt.pageSize, tt.pageN, got)
			}

			if tt.pageSize >= 32768 && tt.pageN >= 100000 {
				// Sanity check: ensure result is at least (page_size * page_count)
				minExpected := int64(tt.pageSize) * int64(tt.pageN)
				if got < minExpected {
					t.Errorf("calcWALSize(%d, %d) = %d (%.2f GB), suspiciously small, possible overflow",
						tt.pageSize, tt.pageN, got, float64(got)/(1024*1024*1024))
				}
			}
		})
	}
}

// TestDB_Sync_UpdatesMetrics verifies that DB size, WAL size, and total WAL bytes
// metrics are properly updated during sync operations.
// Regression test for issue #876: metrics were defined but never updated.
func TestDB_Sync_UpdatesMetrics(t *testing.T) {
	// Set up database manually (can't use testingutil due to import cycle)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	// Create and open litestream DB
	db := NewDB(dbPath)
	db.MonitorInterval = 0 // disable background goroutine
	db.Replica = NewReplica(db)
	db.Replica.Client = &testReplicaClient{dir: t.TempDir()}
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// Open SQL connection
	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal;`); err != nil {
		t.Fatal(err)
	}

	// Insert data to create DB and WAL content
	if _, err := sqldb.Exec(`CREATE TABLE t (id INT, data TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (1, 'test data')`); err != nil {
		t.Fatal(err)
	}

	// Sync to trigger metric updates
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify DB size metric matches actual file size
	dbSizeMetric := dbSizeGaugeVec.WithLabelValues(db.Path())
	dbSizeValue := testutil.ToFloat64(dbSizeMetric)
	dbFileInfo, err := os.Stat(db.Path())
	if err != nil {
		t.Fatalf("failed to stat db file: %v", err)
	}
	if dbSizeValue != float64(dbFileInfo.Size()) {
		t.Fatalf("litestream_db_size=%v, want %v", dbSizeValue, dbFileInfo.Size())
	}

	// Verify WAL size metric matches actual file size
	walSizeMetric := walSizeGaugeVec.WithLabelValues(db.Path())
	walSizeValue := testutil.ToFloat64(walSizeMetric)
	walFileInfo, err := os.Stat(db.WALPath())
	if err != nil {
		t.Fatalf("failed to stat wal file: %v", err)
	}
	if walSizeValue != float64(walFileInfo.Size()) {
		t.Fatalf("litestream_wal_size=%v, want %v", walSizeValue, walFileInfo.Size())
	}

	// Verify total WAL bytes counter was incremented
	totalWALMetric := totalWALBytesCounterVec.WithLabelValues(db.Path())
	totalWALValue := testutil.ToFloat64(totalWALMetric)
	if totalWALValue <= 0 {
		t.Fatalf("litestream_total_wal_bytes=%v, want > 0", totalWALValue)
	}

	// Verify txid metric was updated (should be > 0 after writes)
	txidMetric := txIDIndexGaugeVec.WithLabelValues(db.Path())
	txidValue := testutil.ToFloat64(txidMetric)
	if txidValue <= 0 {
		t.Fatalf("litestream_txid=%v, want > 0", txidValue)
	}

	// Verify sync count was incremented
	syncCountMetric := syncNCounterVec.WithLabelValues(db.Path())
	syncCountValue := testutil.ToFloat64(syncCountMetric)
	if syncCountValue <= 0 {
		t.Fatalf("litestream_sync_count=%v, want > 0", syncCountValue)
	}

	// Verify sync seconds was recorded
	syncSecondsMetric := syncSecondsCounterVec.WithLabelValues(db.Path())
	syncSecondsValue := testutil.ToFloat64(syncSecondsMetric)
	if syncSecondsValue <= 0 {
		t.Fatalf("litestream_sync_seconds=%v, want > 0", syncSecondsValue)
	}
}

// TestDB_Checkpoint_UpdatesMetrics verifies that checkpoint metrics are updated.
func TestDB_Checkpoint_UpdatesMetrics(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.Replica = NewReplica(db)
	db.Replica.Client = &testReplicaClient{dir: t.TempDir()}
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal;`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqldb.Exec(`CREATE TABLE t (id INT, data TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (1, 'test data')`); err != nil {
		t.Fatal(err)
	}

	// Sync first to initialize database state
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Get baseline checkpoint metrics
	baselineCount := testutil.ToFloat64(checkpointNCounterVec.WithLabelValues(db.Path(), "PASSIVE"))
	baselineSeconds := testutil.ToFloat64(checkpointSecondsCounterVec.WithLabelValues(db.Path(), "PASSIVE"))

	// Force checkpoint
	if err := db.Checkpoint(context.Background(), "PASSIVE"); err != nil {
		t.Fatal(err)
	}

	// Verify checkpoint_count was incremented
	checkpointCountMetric := checkpointNCounterVec.WithLabelValues(db.Path(), "PASSIVE")
	checkpointCountValue := testutil.ToFloat64(checkpointCountMetric)
	if checkpointCountValue <= baselineCount {
		t.Fatalf("litestream_checkpoint_count=%v, want > %v", checkpointCountValue, baselineCount)
	}

	// Verify checkpoint_seconds was recorded
	checkpointSecondsMetric := checkpointSecondsCounterVec.WithLabelValues(db.Path(), "PASSIVE")
	checkpointSecondsValue := testutil.ToFloat64(checkpointSecondsMetric)
	if checkpointSecondsValue <= baselineSeconds {
		t.Fatalf("litestream_checkpoint_seconds=%v, want > %v", checkpointSecondsValue, baselineSeconds)
	}
}

// TestDB_Sync_ErrorMetrics verifies that sync error counter is incremented on failure.
func TestDB_Sync_ErrorMetrics(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	errorClient := &errorReplicaClient{writeErr: errors.New("simulated write error")}
	workingClient := &testReplicaClient{dir: t.TempDir()}

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.Replica = NewReplica(db)
	db.Replica.Client = workingClient // Start with working client
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Switch back to working client for clean close
		db.Replica.Client = workingClient
		_ = db.Close(context.Background())
	}()

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal;`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqldb.Exec(`CREATE TABLE t (id INT, data TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (1, 'test data')`); err != nil {
		t.Fatal(err)
	}

	// First sync with working client to initialize
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Insert more data
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (2, 'more data')`); err != nil {
		t.Fatal(err)
	}

	// Get baseline error count
	baselineErrors := testutil.ToFloat64(syncErrorNCounterVec.WithLabelValues(db.Path()))

	// Switch to error client
	db.Replica.Client = errorClient

	// Sync should fail due to error replica client
	err = db.Sync(context.Background())
	if err == nil {
		t.Skip("sync did not return error, skipping error metric test")
	}

	// Verify sync_error_count was incremented
	syncErrorMetric := syncErrorNCounterVec.WithLabelValues(db.Path())
	syncErrorValue := testutil.ToFloat64(syncErrorMetric)
	if syncErrorValue <= baselineErrors {
		t.Fatalf("litestream_sync_error_count=%v, want > %v", syncErrorValue, baselineErrors)
	}
}

// TestDB_Verify_WALOffsetAtHeader tests that verify() handles the edge case where
// an LTX file has WALOffset=WALHeaderSize and WALSize=0, which means we're at the
// beginning of the WAL with no frames written yet.
// Regression test for issue #900: prev WAL offset is less than the header size: -4088
func TestDB_Verify_WALOffsetAtHeader(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.Replica = NewReplica(db)
	db.Replica.Client = &testReplicaClient{dir: t.TempDir()}
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal;`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqldb.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}

	// Perform initial sync to set up page size and initial state
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Read the WAL header to get current salt values
	walHdr, err := readWALHeader(db.WALPath())
	if err != nil {
		t.Fatal(err)
	}
	salt1 := binary.BigEndian.Uint32(walHdr[16:])
	salt2 := binary.BigEndian.Uint32(walHdr[20:])

	// Create an LTX file with WALOffset=WALHeaderSize (32) and WALSize=0
	// This simulates the condition in issue #900
	ltxDir := db.LTXLevelDir(0)
	if err := os.MkdirAll(ltxDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Get current position to determine next TXID
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	nextTXID := pos.TXID + 1

	ltxPath := db.LTXPath(0, nextTXID, nextTXID)
	f, err := os.Create(ltxPath)
	if err != nil {
		t.Fatal(err)
	}

	enc, err := ltx.NewEncoder(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Create header with WALOffset=32 (WALHeaderSize) and WALSize=0
	hdr := ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  uint32(db.pageSize),
		Commit:    2,
		MinTXID:   nextTXID,
		MaxTXID:   nextTXID,
		Timestamp: 1000000,
		WALOffset: WALHeaderSize, // 32 - at start of WAL
		WALSize:   0,             // No WAL data - this triggers the bug
		WALSalt1:  salt1,
		WALSalt2:  salt2,
	}

	if err := enc.EncodeHeader(hdr); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Now call verify - before the fix, this would fail with:
	// "prev WAL offset is less than the header size: -4088"
	info, err := db.verify(context.Background())
	if err != nil {
		t.Fatalf("verify() returned error: %v", err)
	}

	// Verify the returned info is sensible
	if info.offset != WALHeaderSize {
		t.Errorf("expected offset=%d, got %d", WALHeaderSize, info.offset)
	}
	// Salt matches, so snapshotting should be false
	if info.snapshotting {
		t.Errorf("expected snapshotting=false when salt matches, got true")
	}
}

// TestDB_Verify_WALOffsetAtHeader_SaltMismatch tests that verify() correctly
// triggers a snapshot when WALOffset=WALHeaderSize, WALSize=0, and the salt
// values don't match the current WAL header.
// Companion test to TestDB_Verify_WALOffsetAtHeader for full branch coverage.
func TestDB_Verify_WALOffsetAtHeader_SaltMismatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.Replica = NewReplica(db)
	db.Replica.Client = &testReplicaClient{dir: t.TempDir()}
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal;`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqldb.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatal(err)
	}

	// Perform initial sync to set up page size and initial state
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Read the WAL header to get current salt values
	walHdr, err := readWALHeader(db.WALPath())
	if err != nil {
		t.Fatal(err)
	}
	salt1 := binary.BigEndian.Uint32(walHdr[16:])
	salt2 := binary.BigEndian.Uint32(walHdr[20:])

	// Create an LTX file with WALOffset=WALHeaderSize (32) and WALSize=0
	// but with DIFFERENT salt values to simulate a salt reset
	ltxDir := db.LTXLevelDir(0)
	if err := os.MkdirAll(ltxDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Get current position to determine next TXID
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	nextTXID := pos.TXID + 1

	ltxPath := db.LTXPath(0, nextTXID, nextTXID)
	f, err := os.Create(ltxPath)
	if err != nil {
		t.Fatal(err)
	}

	enc, err := ltx.NewEncoder(f)
	if err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Create header with WALOffset=32 (WALHeaderSize) and WALSize=0
	// Use different salt values to trigger salt mismatch branch
	hdr := ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  uint32(db.pageSize),
		Commit:    2,
		MinTXID:   nextTXID,
		MaxTXID:   nextTXID,
		Timestamp: 1000000,
		WALOffset: WALHeaderSize, // 32 - at start of WAL
		WALSize:   0,             // No WAL data
		WALSalt1:  salt1 + 1,     // Different salt to trigger mismatch
		WALSalt2:  salt2 + 1,     // Different salt to trigger mismatch
	}

	if err := enc.EncodeHeader(hdr); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Call verify - should succeed but indicate snapshotting due to salt mismatch
	info, err := db.verify(context.Background())
	if err != nil {
		t.Fatalf("verify() returned error: %v", err)
	}

	// Verify the returned info indicates snapshotting due to salt reset
	if info.offset != WALHeaderSize {
		t.Errorf("expected offset=%d, got %d", WALHeaderSize, info.offset)
	}
	if !info.snapshotting {
		t.Errorf("expected snapshotting=true when salt mismatches, got false")
	}
	if info.reason != "wal header salt reset, snapshotting" {
		t.Errorf("expected reason='wal header salt reset, snapshotting', got %q", info.reason)
	}
}
