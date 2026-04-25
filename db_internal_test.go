package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/superfly/ltx"
	_ "modernc.org/sqlite"

	"github.com/benbjohnson/litestream/internal"
)

// testReplicaClient is a minimal mock for testing that doesn't cause import cycles.
type testReplicaClient struct {
	dir string
}

func (c *testReplicaClient) Init(_ context.Context) error { return nil }

func (c *testReplicaClient) SetLogger(_ *slog.Logger) {}

func (c *testReplicaClient) Type() string { return "test" }

func (c *testReplicaClient) LTXFiles(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
	internal.OperationTotalCounterVec.WithLabelValues(c.Type(), "LIST").Inc()

	levelDir := filepath.Join(c.dir, fmt.Sprintf("l%d", level))
	entries, err := os.ReadDir(levelDir)
	if os.IsNotExist(err) {
		return ltx.NewFileInfoSliceIterator(nil), nil
	} else if err != nil {
		return nil, err
	}

	var infos []*ltx.FileInfo
	for _, entry := range entries {
		minTXID, maxTXID, err := ltx.ParseFilename(entry.Name())
		if err != nil {
			continue
		}
		if minTXID < seek {
			continue
		}
		fi, _ := entry.Info()
		var size int64
		if fi != nil {
			size = fi.Size()
		}
		infos = append(infos, &ltx.FileInfo{
			Level:   level,
			MinTXID: minTXID,
			MaxTXID: maxTXID,
			Size:    size,
		})
	}
	return ltx.NewFileInfoSliceIterator(infos), nil
}

func (c *testReplicaClient) OpenLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, _, _ int64) (io.ReadCloser, error) {
	internal.OperationTotalCounterVec.WithLabelValues(c.Type(), "GET").Inc()

	path := filepath.Join(c.dir, fmt.Sprintf("l%d", level), ltx.FormatFilename(minTXID, maxTXID))
	return os.Open(path)
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

	internal.OperationTotalCounterVec.WithLabelValues(c.Type(), "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(c.Type(), "PUT").Add(float64(len(data)))

	return &ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID, Size: int64(len(data))}, nil
}

func (c *testReplicaClient) DeleteLTXFiles(_ context.Context, infos []*ltx.FileInfo) error {
	internal.OperationTotalCounterVec.WithLabelValues(c.Type(), "DELETE").Add(float64(len(infos)))
	return nil
}

func (c *testReplicaClient) DeleteAll(_ context.Context) error {
	return nil
}

func TestDB_SyncHonorsContextWaitingForExecLock(t *testing.T) {
	db := NewDB(filepath.Join(t.TempDir(), "db"))
	db.execMu.Lock()
	defer db.execMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := db.Sync(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want context deadline exceeded", err)
	}
}

func TestDB_WriteLTXFromWALHonorsCanceledContext(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	walPath := filepath.Join(dir, "db-wal")

	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	db := NewDB(dbPath)
	db.pageSize = 1024
	db.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:  ltx.Version,
		Flags:    ltx.HeaderFlagNoChecksum,
		PageSize: 1024,
		Commit:   1,
		MinTXID:  1,
		MaxTXID:  1,
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = db.writeLTXFromWAL(ctx, enc, walFile, 0, 1, map[uint32]int64{1: 0})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context canceled", err)
	}
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

// TestDB_ReplicaSync_OperationMetrics verifies that replica operation metrics
// (PUT total and bytes) are incremented when Replica.Sync() uploads LTX files.
func TestDB_ReplicaSync_OperationMetrics(t *testing.T) {
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

	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`CREATE TABLE t (id INT, data TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (1, 'test data')`); err != nil {
		t.Fatal(err)
	}

	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	baselinePutTotal := testutil.ToFloat64(
		internal.OperationTotalCounterVec.WithLabelValues("test", "PUT"))
	baselinePutBytes := testutil.ToFloat64(
		internal.OperationBytesCounterVec.WithLabelValues("test", "PUT"))

	if err := db.Replica.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	putTotal := testutil.ToFloat64(
		internal.OperationTotalCounterVec.WithLabelValues("test", "PUT"))
	putBytes := testutil.ToFloat64(
		internal.OperationBytesCounterVec.WithLabelValues("test", "PUT"))

	if putTotal <= baselinePutTotal {
		t.Fatalf("litestream_replica_operation_total[test,PUT]=%v, want > %v", putTotal, baselinePutTotal)
	}
	if putBytes <= baselinePutBytes {
		t.Fatalf("litestream_replica_operation_bytes[test,PUT]=%v, want > %v", putBytes, baselinePutBytes)
	}
}

// TestDB_Sync_ErrorMetrics verifies that sync error counter is incremented on failure.
func TestDB_Sync_ErrorMetrics(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	workingClient := &testReplicaClient{dir: t.TempDir()}

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.Replica = NewReplica(db)
	db.Replica.Client = workingClient
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close(context.Background()) }()

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`CREATE TABLE t (id INT, data TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (1, 'test data')`); err != nil {
		t.Fatal(err)
	}

	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := sqldb.Exec(`INSERT INTO t VALUES (2, 'more data')`); err != nil {
		t.Fatal(err)
	}

	baselineErrors := testutil.ToFloat64(syncErrorNCounterVec.WithLabelValues(db.Path()))

	if err := os.Remove(db.WALPath()); err != nil {
		t.Fatal(err)
	}

	if err := db.Sync(context.Background()); err == nil {
		t.Fatal("expected error from sync with missing WAL")
	}

	syncErrorValue := testutil.ToFloat64(syncErrorNCounterVec.WithLabelValues(db.Path()))
	if syncErrorValue <= baselineErrors {
		t.Fatalf("litestream_sync_error_count=%v, want > %v", syncErrorValue, baselineErrors)
	}
}

// TestDB_Checkpoint_ErrorMetrics verifies that checkpoint error counter is incremented on failure.
func TestDB_Checkpoint_ErrorMetrics(t *testing.T) {
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
	defer func() { _ = db.Close(context.Background()) }()

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`CREATE TABLE t (id INT, data TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (1, 'test data')`); err != nil {
		t.Fatal(err)
	}

	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	baselineErrors := testutil.ToFloat64(checkpointErrorNCounterVec.WithLabelValues(db.Path(), "PASSIVE"))

	db.db.Close()

	if _, err := db.execCheckpoint(context.Background(), "PASSIVE"); err == nil {
		t.Fatal("expected error from checkpoint with closed db")
	}

	checkpointErrorValue := testutil.ToFloat64(checkpointErrorNCounterVec.WithLabelValues(db.Path(), "PASSIVE"))
	if checkpointErrorValue <= baselineErrors {
		t.Fatalf("litestream_checkpoint_error_count=%v, want > %v", checkpointErrorValue, baselineErrors)
	}
}

// TestDB_L0RetentionMetrics verifies that L0 retention gauges are set during enforcement.
func TestDB_L0RetentionMetrics(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	client := &testReplicaClient{dir: t.TempDir()}

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.L0Retention = 1 * time.Nanosecond
	db.Replica = NewReplica(db)
	db.Replica.Client = client
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close(context.Background()) }()

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`CREATE TABLE t (id INT, data TEXT)`); err != nil {
		t.Fatal(err)
	}

	for i := range 3 {
		if _, err := sqldb.Exec(`INSERT INTO t VALUES (?, 'data')`, i); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := db.Replica.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	compactor := NewCompactor(client, slog.Default())
	if _, err := compactor.Compact(context.Background(), 1); err != nil {
		t.Fatal(err)
	}

	dbName := filepath.Base(db.Path())
	if err := db.EnforceL0RetentionByTime(context.Background()); err != nil {
		t.Fatal(err)
	}

	eligible := testutil.ToFloat64(internal.L0RetentionGaugeVec.WithLabelValues(dbName, "eligible"))
	notCompacted := testutil.ToFloat64(internal.L0RetentionGaugeVec.WithLabelValues(dbName, "not_compacted"))
	tooRecent := testutil.ToFloat64(internal.L0RetentionGaugeVec.WithLabelValues(dbName, "too_recent"))

	if eligible+notCompacted+tooRecent == 0 {
		t.Fatalf("expected at least one L0 retention gauge > 0, got eligible=%v not_compacted=%v too_recent=%v",
			eligible, notCompacted, tooRecent)
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

	// Invalidate cached position since we wrote an L0 file directly.
	db.invalidatePosCache()

	// Now call verify - before the fix, this would fail with:
	// "prev WAL offset is less than the header size: -4088"
	info, err := db.verify(context.Background(), &db.syncState)
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

	// Invalidate cached position since we wrote an L0 file directly.
	db.invalidatePosCache()

	// Call verify - should succeed but indicate snapshotting due to salt mismatch
	info, err := db.verify(context.Background(), &db.syncState)
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
	info1, err := db.verify(ctx, &db.syncState)
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
	info2, err := db.verify(ctx, &db.syncState)
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
		info, err := db.verify(ctx, &db.syncState)
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

// TestIsDiskFullError tests the disk full error detection helper.
func TestIsDiskFullError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "no space left on device",
			err:      errors.New("write /tmp/file: no space left on device"),
			expected: true,
		},
		{
			name:     "No Space Left On Device (uppercase)",
			err:      errors.New("No Space Left On Device"),
			expected: true,
		},
		{
			name:     "disk quota exceeded",
			err:      errors.New("write: disk quota exceeded"),
			expected: true,
		},
		{
			name:     "ENOSPC",
			err:      errors.New("ENOSPC: cannot write file"),
			expected: true,
		},
		{
			name:     "EDQUOT",
			err:      errors.New("error EDQUOT while writing"),
			expected: true,
		},
		{
			name:     "regular error",
			err:      errors.New("connection refused"),
			expected: false,
		},
		{
			name:     "permission denied",
			err:      errors.New("permission denied"),
			expected: false,
		},
		{
			name:     "wrapped disk full error",
			err:      fmt.Errorf("sync failed: %w", errors.New("no space left on device")),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDiskFullError(tt.err)
			if result != tt.expected {
				t.Errorf("isDiskFullError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

// TestIsSQLiteBusyError tests the SQLite busy error detection helper.
func TestIsSQLiteBusyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "database is locked",
			err:      errors.New("database is locked"),
			expected: true,
		},
		{
			name:     "SQLITE_BUSY",
			err:      errors.New("SQLITE_BUSY: cannot commit"),
			expected: true,
		},
		{
			name:     "regular error",
			err:      errors.New("connection refused"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSQLiteBusyError(tt.err)
			if result != tt.expected {
				t.Errorf("isSQLiteBusyError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

// TestDB_IdleCheckpointSnapshotLoop tests for the feedback loop described in issue #997.
// After bulk inserts trigger a checkpoint, litestream should NOT enter a self-perpetuating
// loop where checkpoint triggers cause repeated LTX file creation on an idle database.
//
// The bug occurred because:
// 1. PASSIVE checkpoint completes but doesn't truncate WAL file
// 2. WAL salt changes, new _litestream_seq write goes to offset 32 with new salt
// 3. Old WAL frames (with old salt) make file size exceed checkpoint threshold
// 4. checkpointIfNeeded() uses file size, triggering another checkpoint
// 5. Loop repeats, creating LTX files every sync cycle
//
// The fix uses logical WAL offset (from LTX) instead of file size for checkpoint decisions.
func TestDB_IdleCheckpointSnapshotLoop(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.CheckpointInterval = 0
	db.MinCheckpointPageN = 10 // Low threshold to trigger checkpoint easily
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
	if _, err := sqldb.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)`); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Initial sync
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Bulk inserts WITHOUT transaction (as in the bug report)
	// This creates many WAL frames that will trigger a checkpoint
	for i := 0; i < 100; i++ {
		if _, err := sqldb.Exec(`INSERT INTO test VALUES (?, ?)`, i, "test data padding"); err != nil {
			t.Fatal(err)
		}
	}

	// Sync and trigger checkpoint via size threshold
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Force a checkpoint that will reset WAL salt
	if err := db.Checkpoint(ctx, CheckpointModePassive); err != nil {
		t.Fatal(err)
	}
	afterCheckpointPos, _ := db.Pos()

	// Now the database is IDLE - no more application writes
	// Simulate multiple sync cycles (as would happen with MonitorInterval)
	for cycle := 0; cycle < 5; cycle++ {
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}

	finalPos, _ := db.Pos()

	// The key assertion: TXID should not be incrementing every cycle.
	// With the bug, TXID would increment 5 times (one per cycle).
	// The fix ensures checkpoint decisions use logical WAL size,
	// preventing spurious checkpoints when WAL file contains stale frames.
	txidGrowth := int(finalPos.TXID - afterCheckpointPos.TXID)
	if txidGrowth > 1 {
		t.Errorf("TXID grew by %d during idle cycles (expected <= 1). "+
			"This is issue #997: checkpoint triggers infinite LTX creation loop.", txidGrowth)
	}
}

// TestDB_Issue994_RunawayDiskUsage reproduces the scenario from issue #994 where
// the local -litestream directory grows unboundedly. The reporter saw ~10MB/s growth
// in LTX files. This test verifies that after bulk writes and idle sync cycles,
// local LTX file count and total size stabilize rather than growing linearly.
func TestDB_Issue994_RunawayDiskUsage(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.CheckpointInterval = 0
	db.MinCheckpointPageN = 10
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

	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)`); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Bulk inserts without a wrapping transaction (matches the #994 scenario).
	// This builds up WAL frames and will trigger checkpoint thresholds.
	for i := 0; i < 200; i++ {
		if _, err := sqldb.Exec(`INSERT INTO test VALUES (?, ?)`, i, "padding data for disk usage test"); err != nil {
			t.Fatal(err)
		}
	}

	// Sync to create LTX files from the WAL data.
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Force a checkpoint (mirrors what happens in production after bulk writes).
	if err := db.Checkpoint(ctx, CheckpointModePassive); err != nil {
		t.Fatal(err)
	}

	// Measure the baseline LTX directory size after initial sync + checkpoint.
	baselineSize := dirSize(t, db.LTXDir())
	baselineFiles := dirFileCount(t, db.LTXDir())
	t.Logf("baseline: %d bytes, %d files", baselineSize, baselineFiles)

	// Run 20 idle sync cycles (no application writes).
	// With the #994 bug, each cycle would create a new LTX snapshot file,
	// causing linear disk growth.
	for cycle := 0; cycle < 20; cycle++ {
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}

	finalSize := dirSize(t, db.LTXDir())
	finalFiles := dirFileCount(t, db.LTXDir())
	t.Logf("after 20 idle cycles: %d bytes, %d files", finalSize, finalFiles)

	// Allow for at most 1 additional LTX file (the _litestream_seq bookkeeping write).
	// With the bug, we'd see 20+ new files.
	newFiles := finalFiles - baselineFiles
	if newFiles > 2 {
		t.Errorf("LTX file count grew by %d during 20 idle sync cycles (expected <= 2). "+
			"This indicates issue #994: runaway LTX file creation.", newFiles)
	}

	// Size should not grow significantly. Allow 2x as generous margin.
	if baselineSize > 0 && finalSize > baselineSize*2 {
		t.Errorf("LTX directory grew from %d to %d bytes during idle cycles (>2x growth). "+
			"This indicates issue #994: runaway disk usage.", baselineSize, finalSize)
	}
}

func dirSize(t *testing.T, path string) int64 {
	t.Helper()
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return size
}

func dirFileCount(t *testing.T, path string) int {
	t.Helper()
	var count int
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}

// TestDB_WALPageCoverage_AllNewPagesPresent verifies that when SQLite grows a
// database (increases page count), ALL new pages appear as WAL frames. This
// test exercises SQLite's allocateBtreePage code path which calls
// sqlite3PagerWrite on every newly allocated page.
//
// If this test passes, it confirms that SQLite does not skip WAL writes when
// growing the database — Ben Bjohnson's skepticism about the zero-fill fix
// (PR #1087 comment) is well-founded at the SQLite level.
func TestDB_WALPageCoverage_AllNewPagesPresent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqldb.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)`); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		blob := make([]byte, 3000)
		if _, err := sqldb.Exec(`INSERT INTO t VALUES (?, ?)`, i, blob); err != nil {
			t.Fatal(err)
		}
	}

	walFile, err := os.Open(dbPath + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	rd, err := NewWALReader(walFile, slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	pageMap, _, commit, err := rd.PageMap(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if commit == 0 {
		t.Fatal("expected non-zero commit from WAL")
	}

	lockPgno := ltx.LockPgno(4096)
	var missing []uint32
	for pgno := uint32(1); pgno <= commit; pgno++ {
		if pgno == lockPgno {
			continue
		}
		if _, ok := pageMap[pgno]; !ok {
			missing = append(missing, pgno)
		}
	}

	t.Logf("commit=%d, pages_in_wal=%d, missing=%d", commit, len(pageMap), len(missing))
	if len(missing) > 0 {
		first := missing[0]
		last := missing[len(missing)-1]
		t.Errorf("pages missing from WAL: %d total (first=%d, last=%d, commit=%d)",
			len(missing), first, last, commit)
	}
}

// TestDB_WriteLTXFromWAL_PageGrowthCoverage verifies that an incremental LTX
// file produced by writeLTXFromWAL contains all new pages when the database
// grows between syncs. This tests the full Litestream sync path.
func TestDB_WriteLTXFromWAL_PageGrowthCoverage(t *testing.T) {
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
	defer db.Close(context.Background())

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqldb.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := sqldb.Exec(`INSERT INTO t VALUES (?, ?)`, i, make([]byte, 100)); err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()

	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	pos1, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}

	f1, err := os.Open(db.LTXPath(0, pos1.TXID, pos1.TXID))
	if err != nil {
		t.Fatal(err)
	}
	dec1 := ltx.NewDecoder(f1)
	if err := dec1.DecodeHeader(); err != nil {
		f1.Close()
		t.Fatal(err)
	}
	prevCommit := dec1.Header().Commit
	f1.Close()
	t.Logf("after sync 1: txid=%d, commit=%d", pos1.TXID, prevCommit)

	for i := 5; i < 150; i++ {
		blob := make([]byte, 3000)
		if _, err := sqldb.Exec(`INSERT INTO t VALUES (?, ?)`, i, blob); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	pos2, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}

	f2, err := os.Open(db.LTXPath(0, pos2.TXID, pos2.TXID))
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	dec2 := ltx.NewDecoder(f2)
	if err := dec2.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	newCommit := dec2.Header().Commit

	ltx2Pages := make(map[uint32]bool)
	pageBuf := make([]byte, dec2.Header().PageSize)
	for {
		var phdr ltx.PageHeader
		if err := dec2.DecodePage(&phdr, pageBuf); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		ltx2Pages[phdr.Pgno] = true
	}

	lockPgno := ltx.LockPgno(dec2.Header().PageSize)
	var missing []uint32
	for pgno := prevCommit + 1; pgno <= newCommit; pgno++ {
		if pgno == lockPgno {
			continue
		}
		if !ltx2Pages[pgno] {
			missing = append(missing, pgno)
		}
	}

	t.Logf("after sync 2: txid=%d, prevCommit=%d, newCommit=%d, pages_in_ltx=%d, missing=%d",
		pos2.TXID, prevCommit, newCommit, len(ltx2Pages), len(missing))
	if len(missing) > 0 {
		first := missing[0]
		last := missing[len(missing)-1]
		t.Errorf("pages missing from incremental LTX: %d total (first=%d, last=%d, prevCommit=%d, newCommit=%d)",
			len(missing), first, last, prevCommit, newCommit)
	}
}

func TestDB_WriteLTXFromWAL_FillsMissingGrowthPagesFromDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	walPath := filepath.Join(dir, "db-wal")

	const pageSize = 1024

	dbFile, err := os.OpenFile(dbPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer dbFile.Close()

	for pgno := 1; pgno <= 5; pgno++ {
		page := bytes.Repeat([]byte{byte(pgno)}, pageSize)
		if _, err := dbFile.WriteAt(page, int64(pgno-1)*pageSize); err != nil {
			t.Fatal(err)
		}
	}

	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer walFile.Close()

	frameSize := int64(WALFrameHeaderSize + pageSize)
	pageMap := map[uint32]int64{
		3: 0,
		5: frameSize,
	}
	for _, pgno := range []uint32{3, 5} {
		offset := pageMap[pgno] + WALFrameHeaderSize
		page := bytes.Repeat([]byte{byte(pgno + 10)}, pageSize)
		if _, err := walFile.WriteAt(page, offset); err != nil {
			t.Fatal(err)
		}
	}

	db := NewDB(dbPath)
	db.pageSize = pageSize
	db.f = dbFile
	db.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    5,
		MinTXID:   2,
		MaxTXID:   2,
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.writeLTXFromWAL(context.Background(), enc, walFile, 2, 5, pageMap); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	dec := ltx.NewDecoder(bytes.NewReader(buf.Bytes()))
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}

	var pgnos []uint32
	got := make(map[uint32][]byte)
	pageBuf := make([]byte, pageSize)
	for {
		var phdr ltx.PageHeader
		if err := dec.DecodePage(&phdr, pageBuf); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		pgnos = append(pgnos, phdr.Pgno)
		got[phdr.Pgno] = append([]byte(nil), pageBuf...)
	}

	if !reflect.DeepEqual([]uint32{3, 4, 5}, pgnos) {
		t.Fatalf("page numbers mismatch: got=%v", pgnos)
	}
	if !bytes.Equal(got[3], bytes.Repeat([]byte{13}, pageSize)) {
		t.Fatal("expected page 3 to come from WAL")
	}
	if !bytes.Equal(got[4], bytes.Repeat([]byte{4}, pageSize)) {
		t.Fatal("expected page 4 to come from database")
	}
	if !bytes.Equal(got[5], bytes.Repeat([]byte{15}, pageSize)) {
		t.Fatal("expected page 5 to come from WAL")
	}

	snapshot := new(bytes.Buffer)
	snapshotEnc, err := ltx.NewEncoder(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshotEnc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    2,
		MinTXID:   1,
		MaxTXID:   1,
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	for _, pgno := range []uint32{1, 2} {
		page := bytes.Repeat([]byte{byte(pgno)}, pageSize)
		if err := snapshotEnc.EncodePage(ltx.PageHeader{Pgno: uint32(pgno)}, page); err != nil {
			t.Fatal(err)
		}
	}
	if err := snapshotEnc.Close(); err != nil {
		t.Fatal(err)
	}

	compacted := new(bytes.Buffer)
	c, err := ltx.NewCompactor(compacted, []io.Reader{
		bytes.NewReader(snapshot.Bytes()),
		bytes.NewReader(buf.Bytes()),
	})
	if err != nil {
		t.Fatal(err)
	}
	c.HeaderFlags = ltx.HeaderFlagNoChecksum
	if err := c.Compact(context.Background()); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}
}

// TestDB_Sync_CompactionValidAfterGrowthAndCheckpoint verifies that compaction
// produces valid snapshots after a cycle of: grow DB, sync, checkpoint, grow
// more, sync. If the zero-fill bug existed, compaction would fail with
// "nonsequential page numbers in snapshot transaction".
func TestDB_Sync_CompactionValidAfterGrowthAndCheckpoint(t *testing.T) {
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
	defer db.Close(context.Background())

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()

	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal`); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	if _, err := sqldb.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		blob := make([]byte, 3000)
		if _, err := sqldb.Exec(`INSERT INTO t VALUES (?, ?)`, i, blob); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	if err := db.Checkpoint(ctx, CheckpointModeTruncate); err != nil {
		t.Fatal(err)
	}

	for i := 50; i < 100; i++ {
		blob := make([]byte, 3000)
		if _, err := sqldb.Exec(`INSERT INTO t VALUES (?, ?)`, i, blob); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("final txid=%d", pos.TXID)

	var readers []io.ReadCloser
	for txid := ltx.TXID(1); txid <= pos.TXID; txid++ {
		path := db.LTXPath(0, txid, txid)
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatal(err)
		}
		readers = append(readers, f)
	}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	if len(readers) < 2 {
		t.Fatalf("expected at least 2 LTX files, got %d", len(readers))
	}

	ioReaders := make([]io.Reader, len(readers))
	for i, r := range readers {
		ioReaders[i] = r
	}

	var buf bytes.Buffer
	c, err := ltx.NewCompactor(&buf, ioReaders)
	if err != nil {
		t.Fatalf("new compactor: %v", err)
	}
	c.HeaderFlags = ltx.HeaderFlagNoChecksum
	if err := c.Compact(ctx); err != nil {
		t.Fatalf("compaction failed (this would indicate the zero-fill bug): %v", err)
	}

	t.Logf("compaction succeeded: %d bytes, %d input files", buf.Len(), len(readers))
}

// TestDB_CheckpointCreatesSnapshotL0 verifies that TRUNCATE checkpoints
// create a full snapshot L0 to guarantee complete page coverage.
//
// After a TRUNCATE checkpoint restarts the WAL, there's a TOCTOU gap where
// application commits can arrive between releasing the write lock and the
// checkpoint executing. Those frames get checkpointed from WAL to DB but
// are never captured in an L0 file. The post-checkpoint snapshot ensures
// all pages are captured. See issues #927, #1198.
func TestDB_CheckpointCreatesSnapshotL0(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.CheckpointInterval = 0
	db.MinCheckpointPageN = 1000000
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
	if _, err := sqldb.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)`); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Initial sync
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Write data to grow WAL
	for i := range 50 {
		blob := make([]byte, 2000)
		if _, err := sqldb.Exec(`INSERT INTO t VALUES (?, ?)`, i, blob); err != nil {
			t.Fatal(err)
		}
	}

	// Sync all frames
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Record L0 file count before checkpoint
	l0Dir := db.LTXLevelDir(0)
	l0BeforeEntries, _ := os.ReadDir(l0Dir)
	l0BeforeNames := make(map[string]bool)
	for _, e := range l0BeforeEntries {
		l0BeforeNames[e.Name()] = true
	}

	// TRUNCATE checkpoint should create a full snapshot L0 to ensure
	// complete page coverage across the checkpoint boundary.
	if err := db.checkpoint(ctx, CheckpointModeTruncate, &db.syncState); err != nil {
		t.Fatal(err)
	}

	// Verify a snapshot L0 was created during checkpoint.
	l0AfterEntries, _ := os.ReadDir(l0Dir)
	newL0Count := 0
	for _, entry := range l0AfterEntries {
		if !l0BeforeNames[entry.Name()] {
			newL0Count++
		}
	}
	if newL0Count == 0 {
		t.Fatal("expected checkpoint to create at least one new L0 file")
	}
}

// TestDB_CheckpointPageGapWithConcurrentWrites verifies that pages written
// concurrently with checkpoint execution are not lost.
//
// Root cause: checkpoint() does a pre-checkpoint sync to capture WAL state,
// then executes PRAGMA wal_checkpoint(TRUNCATE). Under concurrent writes,
// new commits can arrive between the pre-sync and the checkpoint. These commits
// are checkpointed (moved from WAL to DB file) and then the WAL is truncated.
// The post-checkpoint sync reads only the NEW WAL — the missed pages are in
// the DB file but not in any L0 file. When compaction merges L0 files into
// a snapshot (MinTXID=1), the missing pages cause "nonsequential page numbers".
//
// This test exercises the race by:
// 1. Doing an initial sync (snapshot L0)
// 2. Writing data to grow the database
// 3. Syncing to capture the growth
// 4. Running checkpoint CONCURRENTLY with more writes
// 5. Verifying all pages are covered across L0 files
func TestDB_CheckpointPageGapWithConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.CheckpointInterval = 0
	db.MinCheckpointPageN = 1000000
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

	if _, err := sqldb.Exec(`PRAGMA journal_mode = wal`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)`); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Step 1: Initial sync — creates snapshot L0 with all current pages
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Step 2: Write data to grow the database significantly
	for i := 0; i < 100; i++ {
		blob := make([]byte, 4000)
		if _, err := sqldb.Exec(`INSERT INTO t VALUES (?, ?)`, i, blob); err != nil {
			t.Fatal(err)
		}
	}

	// Step 3: Sync to capture the growth — creates incremental L0
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	pos1, _ := db.Pos()
	t.Logf("after growth sync: txid=%d", pos1.TXID)

	// Step 4: Run checkpoint CONCURRENTLY with more writes.
	// The writer goroutine continuously inserts rows while checkpoint runs.
	// This exercises the TOCTOU window where frames arrive after the
	// pre-checkpoint sync but before WAL truncation.
	writerCtx, cancelWriter := context.WithCancel(ctx)
	writerDone := make(chan error, 1)
	writerStarted := make(chan struct{})
	var writtenRows int64

	go func() {
		var i int64 = 100
		started := false
		for {
			select {
			case <-writerCtx.Done():
				writerDone <- nil
				return
			default:
				blob := make([]byte, 4000)
				_, err := sqldb.Exec(`INSERT INTO t VALUES (?, ?)`, i, blob)
				if err != nil {
					// SQLITE_BUSY is expected during concurrent checkpoint - retry
					if strings.Contains(err.Error(), "database is locked") ||
						strings.Contains(err.Error(), "SQLITE_BUSY") {
						time.Sleep(time.Millisecond)
						continue
					}
					writerDone <- err
					return
				}
				atomic.AddInt64(&writtenRows, 1)
				if !started {
					close(writerStarted)
					started = true
				}
				i++
			}
		}
	}()

	// Wait for the writer to confirm at least one successful write before
	// starting the checkpoint. This avoids a timing-dependent 10ms sleep.
	select {
	case <-writerStarted:
	case err := <-writerDone:
		t.Fatalf("writer exited before starting: %v", err)
	}
	checkpointDeadline := time.Now().Add(5 * time.Second)
	for {
		err := db.Checkpoint(ctx, CheckpointModeTruncate)
		if err == nil {
			break
		}
		if !isSQLiteBusyError(err) || time.Now().After(checkpointDeadline) {
			cancelWriter()
			<-writerDone
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	cancelWriter()
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}

	rows := atomic.LoadInt64(&writtenRows)
	t.Logf("concurrent writer inserted %d rows during/around checkpoint", rows)
	if rows == 0 {
		t.Fatal("concurrent writer inserted 0 rows — race was not exercised")
	}

	// Log diagnostic info about what we expect
	pos2, _ := db.Pos()
	t.Logf("after checkpoint: txid=%d", pos2.TXID)

	// Check the DB file size — the checkpoint should have extended it
	dbInfo, _ := os.Stat(dbPath)
	dbPages := dbInfo.Size() / 4096
	t.Logf("DB file: %d bytes (%d pages)", dbInfo.Size(), dbPages)

	// Log each L0 file's header and page count
	for txid := ltx.TXID(1); txid <= pos2.TXID; txid++ {
		path := db.LTXPath(0, txid, txid)
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		dec := ltx.NewDecoder(f)
		if err := dec.DecodeHeader(); err != nil {
			f.Close()
			continue
		}
		hdrInfo := dec.Header()
		// Count pages in this L0 file
		var pageCount int
		var firstPgno, lastPgno uint32
		data := make([]byte, hdrInfo.PageSize)
		for {
			var phdr ltx.PageHeader
			if err := dec.DecodePage(&phdr, data); err == io.EOF {
				break
			} else if err != nil {
				t.Logf("  decode error: %v", err)
				break
			}
			pageCount++
			if firstPgno == 0 {
				firstPgno = phdr.Pgno
			}
			lastPgno = phdr.Pgno
		}
		fi, _ := os.Stat(path)
		t.Logf("L0 %s: commit=%d, pages=%d [%d..%d], size=%d, isSnapshot=%v",
			filepath.Base(path), hdrInfo.Commit, pageCount, firstPgno, lastPgno,
			fi.Size(), hdrInfo.IsSnapshot())
		f.Close()
	}

	// Step 6: Verify all pages are covered across L0 files.
	// The last L0's commit tells us the database has N pages. ALL pages
	// 1..N (except the lock page) must exist in at least one L0 file.
	// If the checkpoint race caused page loss, pages between the pre-checkpoint
	// sync's coverage and the final commit will be missing.
	allPages := make(map[uint32]bool)
	var maxCommit uint32
	for txid := ltx.TXID(1); txid <= pos2.TXID; txid++ {
		path := db.LTXPath(0, txid, txid)
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		dec := ltx.NewDecoder(f)
		if err := dec.DecodeHeader(); err != nil {
			f.Close()
			continue
		}
		hdr := dec.Header()
		if hdr.Commit > maxCommit {
			maxCommit = hdr.Commit
		}
		data := make([]byte, hdr.PageSize)
		for {
			var phdr ltx.PageHeader
			if err := dec.DecodePage(&phdr, data); err == io.EOF {
				break
			} else if err != nil {
				break
			}
			allPages[phdr.Pgno] = true
		}
		f.Close()
	}

	lockPgno := ltx.LockPgno(4096)
	var missing []uint32
	for pgno := uint32(1); pgno <= maxCommit; pgno++ {
		if pgno == lockPgno {
			continue
		}
		if !allPages[pgno] {
			missing = append(missing, pgno)
		}
	}

	if len(missing) > 0 {
		// Show first few missing pages
		show := missing
		if len(show) > 10 {
			show = show[:10]
		}
		t.Fatalf("FAIL: %d pages missing from L0 files (commit=%d, have %d pages). "+
			"Pages lost between pre-checkpoint sync and checkpoint execution. "+
			"First missing: %v",
			len(missing), maxCommit, len(allPages), show)
	}

	t.Logf("all %d pages present across L0 files (commit=%d)", len(allPages), maxCommit)

	replicaDir := t.TempDir()
	replicaL0Dir := filepath.Join(replicaDir, "l0")
	if err := os.Mkdir(replicaL0Dir, 0o755); err != nil {
		t.Fatal(err)
	}
	l0Entries, err := os.ReadDir(db.LTXLevelDir(0))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range l0Entries {
		srcPath := filepath.Join(db.LTXLevelDir(0), entry.Name())
		dstPath := filepath.Join(replicaL0Dir, entry.Name())

		src, err := os.Open(srcPath)
		if err != nil {
			t.Fatal(err)
		}
		dst, err := os.Create(dstPath)
		if err != nil {
			_ = src.Close()
			t.Fatal(err)
		}
		if _, err := io.Copy(dst, src); err != nil {
			_ = src.Close()
			_ = dst.Close()
			t.Fatal(err)
		}
		if err := src.Close(); err != nil {
			_ = dst.Close()
			t.Fatal(err)
		}
		if err := dst.Close(); err != nil {
			t.Fatal(err)
		}
	}

	restorePath := filepath.Join(dir, "restored.db")
	restoreDB := NewDB(restorePath)
	restoreReplica := NewReplica(restoreDB)
	restoreReplica.Client = &testReplicaClient{dir: replicaDir}

	restoreOpt := NewRestoreOptions()
	restoreOpt.OutputPath = restorePath
	restoreOpt.IntegrityCheck = IntegrityCheckFull

	if err := restoreReplica.Restore(ctx, restoreOpt); err != nil {
		t.Fatalf("restore from local ltx chain: %v", err)
	}
}

// TestDB_Sync_InitErrorMetrics verifies that sync error counter is incremented
// when db.init() fails. Regression test for issue #1128.
func TestDB_Sync_InitErrorMetrics(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")

	// Create a directory at the DB path so init() will fail when trying to
	// open it as a SQLite database.
	if err := os.Mkdir(dbPath, 0o755); err != nil {
		t.Fatal(err)
	}

	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = NewReplica(db)
	db.Replica.Client = &testReplicaClient{dir: t.TempDir()}
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close(context.Background())
	}()

	baselineErrors := testutil.ToFloat64(syncErrorNCounterVec.WithLabelValues(db.Path()))

	err := db.Sync(context.Background())
	if err == nil {
		t.Fatal("expected Sync to return error when init fails, got nil")
	}

	syncErrorValue := testutil.ToFloat64(syncErrorNCounterVec.WithLabelValues(db.Path()))
	if syncErrorValue <= baselineErrors {
		t.Fatalf("litestream_sync_error_count=%v, want > %v (init error should be counted)", syncErrorValue, baselineErrors)
	}
}

func TestDB_Pos_OpenErrorReturnsLTXError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("test requires non-root (chmod 000 has no effect as root)")
	}

	db := NewDB(filepath.Join(t.TempDir(), "test.db"))

	ltxDir := db.LTXLevelDir(0)
	if err := os.MkdirAll(ltxDir, 0o755); err != nil {
		t.Fatal(err)
	}

	ltxPath := db.LTXPath(0, 1, 1)
	if err := os.WriteFile(ltxPath, []byte("dummy"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(ltxPath, 0o644) })

	_, err := db.Pos()
	if err == nil {
		t.Fatal("expected error")
	}

	var ltxErr *LTXError
	if !errors.As(err, &ltxErr) {
		t.Fatalf("expected *LTXError, got %T: %v", err, err)
	}
	if ltxErr.Op != "open" {
		t.Fatalf("expected op=open, got %q", ltxErr.Op)
	}
	if ltxErr.IsAutoRecoverable() {
		t.Fatal("permission-denied error should not be auto-recoverable")
	}
}

func TestDB_Pos_VerifyErrorReturnsLTXError(t *testing.T) {
	db := NewDB(filepath.Join(t.TempDir(), "test.db"))

	ltxDir := db.LTXLevelDir(0)
	if err := os.MkdirAll(ltxDir, 0o755); err != nil {
		t.Fatal(err)
	}

	ltxPath := db.LTXPath(0, 1, 1)
	if err := os.WriteFile(ltxPath, []byte("not a valid ltx file"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := db.Pos()
	if err == nil {
		t.Fatal("expected error")
	}

	var ltxErr *LTXError
	if !errors.As(err, &ltxErr) {
		t.Fatalf("expected *LTXError, got %T: %v", err, err)
	}
	if ltxErr.Op != "verify" {
		t.Fatalf("expected op=verify, got %q", ltxErr.Op)
	}
	if !errors.Is(err, ErrLTXCorrupted) {
		t.Fatal("verify error should wrap ErrLTXCorrupted")
	}
	if !ltxErr.IsAutoRecoverable() {
		t.Fatal("corruption error should be auto-recoverable")
	}
}

func TestApplySyncResult(t *testing.T) {
	db := NewDB(filepath.Join(t.TempDir(), "test.db"))

	t.Run("WALState", func(t *testing.T) {
		db.mu.Lock()
		defer db.mu.Unlock()

		db.applySyncResult(&db.syncState, syncResult{newWALSize: 12345, syncedToWALEnd: true})
		if got := db.syncState.lastSyncedWALOffset; got != 12345 {
			t.Fatalf("lastSyncedWALOffset=%d, want 12345", got)
		}
		if !db.syncState.syncedToWALEnd {
			t.Fatal("syncedToWALEnd=false, want true")
		}
	})

	t.Run("Pos", func(t *testing.T) {
		db.mu.Lock()
		defer db.mu.Unlock()

		pos := ltx.Pos{TXID: 42}
		db.applySyncResult(&db.syncState, syncResult{pos: &pos})

		db.pos.Lock()
		got := db.pos.value
		db.pos.Unlock()
		if got == nil || got.TXID != 42 {
			t.Fatalf("pos=%v, want TXID=42", got)
		}
	})

	t.Run("NilPosPreservesExisting", func(t *testing.T) {
		db.mu.Lock()
		defer db.mu.Unlock()

		existing := ltx.Pos{TXID: 99}
		db.pos.Lock()
		db.pos.value = &existing
		db.pos.Unlock()

		db.applySyncResult(&db.syncState, syncResult{})

		db.pos.Lock()
		got := db.pos.value
		db.pos.Unlock()
		if got == nil || got.TXID != 99 {
			t.Fatalf("pos=%v, want TXID=99", got)
		}
	})

	t.Run("L0FileInfo", func(t *testing.T) {
		db.mu.Lock()
		defer db.mu.Unlock()

		info := &ltx.FileInfo{Level: 0, MinTXID: 1, MaxTXID: 1}
		db.applySyncResult(&db.syncState, syncResult{l0FileInfo: info})

		db.maxLTXFileInfos.Lock()
		got := db.maxLTXFileInfos.m[0]
		db.maxLTXFileInfos.Unlock()
		if got != info {
			t.Fatalf("l0FileInfo=%v, want %v", got, info)
		}
	})
}

func TestVerifyAndSync_DelaysStateMutationUntilApply(t *testing.T) {
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
	if _, err := sqldb.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (1, 'before')`); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := sqldb.Exec(`INSERT INTO t VALUES (2, 'after')`); err != nil {
		t.Fatal(err)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	oldWALOffset := db.syncState.lastSyncedWALOffset
	oldSyncedToWALEnd := db.syncState.syncedToWALEnd

	db.pos.Lock()
	if db.pos.value == nil {
		db.pos.Unlock()
		t.Fatal("cached pos is nil after initial sync")
	}
	oldPos := *db.pos.value
	db.pos.Unlock()

	db.maxLTXFileInfos.Lock()
	oldL0 := db.maxLTXFileInfos.m[0]
	db.maxLTXFileInfos.Unlock()
	if oldL0 == nil {
		t.Fatal("cached l0 file info is nil after initial sync")
	}

	result, err := db.verifyAndSync(ctx, false, &db.syncState)
	if err != nil {
		t.Fatal(err)
	}
	if !result.synced {
		t.Fatal("verifyAndSync did not report a sync after new writes")
	}
	if result.newWALSize == oldWALOffset {
		t.Fatalf("newWALSize=%d, want change from %d", result.newWALSize, oldWALOffset)
	}
	if result.pos == nil || result.pos.TXID <= oldPos.TXID {
		t.Fatalf("result.pos=%v, want TXID > %d", result.pos, oldPos.TXID)
	}
	if result.l0FileInfo == nil || result.l0FileInfo.MaxTXID <= oldL0.MaxTXID {
		t.Fatalf("result.l0FileInfo=%v, want MaxTXID > %d", result.l0FileInfo, oldL0.MaxTXID)
	}

	if db.syncState.lastSyncedWALOffset != oldWALOffset {
		t.Fatalf("lastSyncedWALOffset mutated early: got %d, want %d", db.syncState.lastSyncedWALOffset, oldWALOffset)
	}
	if db.syncState.syncedToWALEnd != oldSyncedToWALEnd {
		t.Fatalf("syncedToWALEnd mutated early: got %t, want %t", db.syncState.syncedToWALEnd, oldSyncedToWALEnd)
	}

	db.pos.Lock()
	gotPosBeforeApply := db.pos.value
	db.pos.Unlock()
	if gotPosBeforeApply == nil || gotPosBeforeApply.TXID != oldPos.TXID {
		t.Fatalf("cached pos mutated early: got %v, want TXID=%d", gotPosBeforeApply, oldPos.TXID)
	}

	db.maxLTXFileInfos.Lock()
	gotL0BeforeApply := db.maxLTXFileInfos.m[0]
	db.maxLTXFileInfos.Unlock()
	if gotL0BeforeApply == nil || gotL0BeforeApply.MaxTXID != oldL0.MaxTXID {
		t.Fatalf("cached l0 file info mutated early: got %v, want MaxTXID=%d", gotL0BeforeApply, oldL0.MaxTXID)
	}

	db.applySyncResult(&db.syncState, result)

	if db.syncState.lastSyncedWALOffset != result.newWALSize {
		t.Fatalf("lastSyncedWALOffset=%d, want %d", db.syncState.lastSyncedWALOffset, result.newWALSize)
	}
	if db.syncState.syncedToWALEnd != result.syncedToWALEnd {
		t.Fatalf("syncedToWALEnd=%t, want %t", db.syncState.syncedToWALEnd, result.syncedToWALEnd)
	}

	db.pos.Lock()
	gotPosAfterApply := db.pos.value
	db.pos.Unlock()
	if gotPosAfterApply == nil || gotPosAfterApply.TXID != result.pos.TXID {
		t.Fatalf("cached pos=%v, want TXID=%d", gotPosAfterApply, result.pos.TXID)
	}

	db.maxLTXFileInfos.Lock()
	gotL0AfterApply := db.maxLTXFileInfos.m[0]
	db.maxLTXFileInfos.Unlock()
	if gotL0AfterApply == nil || gotL0AfterApply.MaxTXID != result.l0FileInfo.MaxTXID {
		t.Fatalf("cached l0 file info=%v, want MaxTXID=%d", gotL0AfterApply, result.l0FileInfo.MaxTXID)
	}
}

func TestVerifyAndSync_DelaysExpectedTruncationStateMutationUntilApply(t *testing.T) {
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
	if _, err := sqldb.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.Exec(`INSERT INTO t VALUES (1, 'before')`); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if !db.syncState.syncedToWALEnd {
		t.Fatal("syncedToWALEnd=false, want true after sync")
	}
	oldSyncedToWALEnd := db.syncState.syncedToWALEnd

	if err := os.Truncate(db.WALPath(), WALHeaderSize); err != nil {
		t.Fatal(err)
	}

	result, err := db.verifyAndSync(ctx, false, &db.syncState)
	if err != nil {
		t.Fatal(err)
	}
	if result.synced {
		t.Fatal("verifyAndSync reported a sync, want no sync after truncation without new writes")
	}
	if result.syncedToWALEnd {
		t.Fatal("result.syncedToWALEnd=true, want false")
	}
	if db.syncState.syncedToWALEnd != oldSyncedToWALEnd {
		t.Fatalf("syncedToWALEnd mutated early: got %t, want %t", db.syncState.syncedToWALEnd, oldSyncedToWALEnd)
	}

	db.applySyncResult(&db.syncState, result)

	if db.syncState.syncedToWALEnd {
		t.Fatal("syncedToWALEnd=true after apply, want false")
	}
}
