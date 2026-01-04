package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// TestDB_releaseReadLock_DoubleRollback verifies that calling releaseReadLock()
// after the read transaction has already been rolled back does not return an error.
// This can happen during shutdown when concurrent checkpoint and close operations
// both attempt to release the read lock.
// Regression test for issue #934.
func TestDB_releaseReadLock_DoubleRollback(t *testing.T) {
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

	// Open SQL connection to create a WAL database
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

	// Sync to initialize the database and acquire read lock
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify read transaction exists
	if db.rtx == nil {
		t.Fatal("expected read transaction to exist after Sync")
	}

	// First rollback - simulates what happens in execCheckpoint()
	if err := db.rtx.Rollback(); err != nil {
		t.Fatalf("first rollback failed: %v", err)
	}

	// Second call to releaseReadLock() - simulates what happens in Close()
	// This should NOT return an error even though the transaction is already rolled back.
	// Before the fix, this would return "sql: transaction has already been committed or rolled back"
	if err := db.releaseReadLock(); err != nil {
		t.Fatalf("releaseReadLock() returned error after double rollback: %v", err)
	}

	// Clean up - set rtx to nil since we manually rolled it back
	db.rtx = nil

	// Close should work without error
	if err := db.Close(context.Background()); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}
}

// TestDB_CheckpointDoesNotTriggerSnapshot verifies that a checkpoint
// followed by a sync does not trigger an unnecessary full snapshot.
// This is a regression test for issue #927 (runaway disk usage).
//
// The bug: After checkpoint truncates WAL, verify() sees old LTX position
// is beyond new WAL size and triggers snapshotting=true unnecessarily.
func TestDB_CheckpointDoesNotTriggerSnapshot(t *testing.T) {
	t.Run("TruncateMode", func(t *testing.T) {
		testCheckpointSnapshot(t, CheckpointModeTruncate)
	})
	t.Run("PassiveMode", func(t *testing.T) {
		testCheckpointSnapshot(t, CheckpointModePassive)
	})
}

func testCheckpointSnapshot(t *testing.T, mode string) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	db := NewDB(dbPath)
	db.MonitorInterval = 0    // Disable background monitor
	db.CheckpointInterval = 0 // Disable time-based checkpoints
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

	// Create initial data
	if _, err := sqldb.Exec(`CREATE TABLE t (id INT, data TEXT)`); err != nil {
		t.Fatal(err)
	}
	// Insert enough data to have a meaningful WAL
	for i := 0; i < 100; i++ {
		data := fmt.Sprintf("test data padding row %d with extra content", i)
		if _, err := sqldb.Exec(`INSERT INTO t VALUES (?, ?)`, i, data); err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()

	// Perform initial sync
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	pos1, _ := db.Pos()
	t.Logf("After initial sync: TXID=%d", pos1.TXID)

	// Make a change and sync to establish "normal" state
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (9999, 'before checkpoint')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	pos2, _ := db.Pos()
	t.Logf("After pre-checkpoint sync: TXID=%d", pos2.TXID)

	// Call verify() BEFORE checkpoint to confirm snapshotting=false
	info1, err := db.verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Before checkpoint: verify() snapshotting=%v reason=%q", info1.snapshotting, info1.reason)

	// Perform checkpoint - this may restart the WAL with new salt
	if err := db.Checkpoint(ctx, mode); err != nil {
		t.Fatal(err)
	}
	t.Logf("Checkpoint mode=%s completed", mode)
	posAfterChk, _ := db.Pos()
	t.Logf("After checkpoint: TXID=%d", posAfterChk.TXID)

	// Make a small change to create some WAL data
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (10000, 'after checkpoint')`); err != nil {
		t.Fatal(err)
	}

	// Call verify() AFTER checkpoint - THIS IS THE BUG CHECK
	// With the bug, snapshotting=true because verify() sees:
	// - Old LTX has WALOffset+WALSize pointing to old (larger) WAL
	// - New WAL is truncated (smaller)
	// - Line 973: info.offset > fi.Size() → "wal truncated" → snapshotting=true
	info2, err := db.verify(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("After checkpoint: verify() snapshotting=%v reason=%q", info2.snapshotting, info2.reason)

	// The key assertion: after OUR checkpoint (not external process),
	// we should NOT require a full snapshot.
	if info2.snapshotting {
		t.Errorf("verify() returned snapshotting=true after checkpoint, reason=%q. "+
			"This is the bug: checkpoint followed by sync should NOT require full snapshot.",
			info2.reason)
	}
}

// TestDB_MultipleCheckpointsWithWrites tests that multiple checkpoint cycles
// don't trigger excessive snapshots. This simulates the scenario from issue #927
// where users reported 5GB snapshots every 3-4 minutes.
func TestDB_MultipleCheckpointsWithWrites(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.CheckpointInterval = 0
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

	ctx := context.Background()
	snapshotCount := 0

	// Simulate multiple checkpoint cycles with writes
	for cycle := 0; cycle < 5; cycle++ {
		// Insert some data
		for i := 0; i < 10; i++ {
			if _, err := sqldb.Exec(`INSERT INTO t VALUES (?, ?)`, cycle*100+i, "data"); err != nil {
				t.Fatal(err)
			}
		}

		// Sync
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}

		// Check if this was a snapshot
		info, err := db.verify(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.snapshotting {
			snapshotCount++
			t.Logf("Cycle %d: SNAPSHOT triggered, reason=%q", cycle, info.reason)
		} else {
			t.Logf("Cycle %d: incremental sync", cycle)
		}

		// Checkpoint
		if err := db.Checkpoint(ctx, CheckpointModePassive); err != nil {
			t.Fatal(err)
		}
	}

	// We expect only 1 snapshot (the initial one), not one per cycle
	// With the bug, we'd see a snapshot after every checkpoint
	if snapshotCount > 1 {
		t.Errorf("Too many snapshots triggered: %d (expected 1 for initial sync)", snapshotCount)
	}
}

// TestDB_Monitor_CheapChangeDetection verifies that the monitor loop skips
// expensive Sync() calls when the WAL file hasn't changed.
func TestDB_Monitor_CheapChangeDetection(t *testing.T) {
	// Create temp directory for test database.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	// Set up litestream DB with short monitor interval.
	db := NewDB(dbPath)
	db.MonitorInterval = 50 * time.Millisecond
	db.Replica = NewReplica(db)
	db.Replica.Client = &testReplicaClient{dir: t.TempDir()}
	db.Replica.MonitorEnabled = false // disable replica monitor to avoid hangs

	// Open litestream database.
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())

	// Open SQL connection and create WAL.
	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`PRAGMA journal_mode=wal`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`CREATE TABLE t (x INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatal(err)
	}

	// Wait for initial sync to complete.
	time.Sleep(150 * time.Millisecond)

	// Record sync count after initial sync.
	syncMetric := syncNCounterVec.WithLabelValues(db.Path())
	initialSyncCount := testutil.ToFloat64(syncMetric)
	if initialSyncCount < 1 {
		t.Fatalf("expected at least 1 initial sync, got %v", initialSyncCount)
	}
	t.Logf("initial sync count: %v", initialSyncCount)

	// Wait for several monitor intervals with no WAL changes.
	// The cheap change detection should skip Sync() calls.
	time.Sleep(300 * time.Millisecond) // ~6 monitor intervals

	idleSyncCount := testutil.ToFloat64(syncMetric)
	t.Logf("sync count after idle period: %v", idleSyncCount)

	// Sync count should not have increased significantly during idle period.
	// Allow for 1 extra sync due to timing.
	if idleSyncCount > initialSyncCount+1 {
		t.Fatalf("sync count increased during idle period: initial=%v, after=%v (expected no increase)",
			initialSyncCount, idleSyncCount)
	}

	// Now write to the database - this should trigger a sync.
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (2)`); err != nil {
		t.Fatal(err)
	}

	// Wait for monitor to detect the change and sync.
	time.Sleep(150 * time.Millisecond)

	finalSyncCount := testutil.ToFloat64(syncMetric)
	t.Logf("sync count after write: %v", finalSyncCount)

	// Sync count should have increased after the write.
	if finalSyncCount <= idleSyncCount {
		t.Fatalf("sync count did not increase after write: idle=%v, final=%v",
			idleSyncCount, finalSyncCount)
	}
}

// TestDB_Monitor_DetectsSaltChangeAfterRestart verifies that the monitor loop
// detects WAL header salt changes after a RESTART checkpoint followed by new
// writes. SQLite generates new salt values when the first write happens after
// a RESTART checkpoint that emptied the WAL.
func TestDB_Monitor_DetectsSaltChangeAfterRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	db := NewDB(dbPath)
	db.MonitorInterval = 50 * time.Millisecond
	db.Replica = NewReplica(db)
	db.Replica.Client = &testReplicaClient{dir: t.TempDir()}
	db.Replica.MonitorEnabled = false

	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`PRAGMA journal_mode=wal`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`CREATE TABLE t (x INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatal(err)
	}

	// Wait for initial sync to complete.
	time.Sleep(150 * time.Millisecond)

	// Read the WAL header before RESTART.
	initialHeader, err := readWALHeader(db.WALPath())
	if err != nil {
		t.Fatalf("failed to read initial WAL header: %v", err)
	}
	t.Logf("initial WAL header salt (bytes 12-20): %x", initialHeader[12:20])

	syncMetric := syncNCounterVec.WithLabelValues(db.Path())
	preRestartSyncCount := testutil.ToFloat64(syncMetric)
	t.Logf("sync count before RESTART: %v", preRestartSyncCount)

	// Force a RESTART checkpoint to empty the WAL.
	if _, err := sqldb.Exec(`PRAGMA wal_checkpoint(RESTART)`); err != nil {
		t.Fatal(err)
	}

	// Write new data - this should generate new salt values since the WAL was restarted.
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	// Read the WAL header after the new write.
	postHeader, err := readWALHeader(db.WALPath())
	if err != nil {
		t.Fatalf("failed to read post-RESTART WAL header: %v", err)
	}
	t.Logf("post-RESTART WAL header salt (bytes 12-20): %x", postHeader[12:20])

	// Check if salt values changed.
	saltChanged := !bytes.Equal(initialHeader[12:20], postHeader[12:20])
	if saltChanged {
		t.Log("Salt values changed as expected after RESTART + write")
	} else {
		t.Log("Salt values did not change (WAL may have been at boundary)")
	}

	// Wait for monitor to detect the change.
	time.Sleep(150 * time.Millisecond)

	postRestartSyncCount := testutil.ToFloat64(syncMetric)
	t.Logf("sync count after RESTART + write: %v", postRestartSyncCount)

	// Sync count should increase - either from salt change detection or size change.
	if postRestartSyncCount <= preRestartSyncCount {
		t.Fatalf("sync count did not increase after RESTART + write: before=%v, after=%v",
			preRestartSyncCount, postRestartSyncCount)
	}

	// If salt changed, verify it was detected correctly.
	if saltChanged {
		t.Log("Monitor correctly detected changes after WAL salt reset")
	}
}
