package litestream

import (
	"context"
	"database/sql"
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
}
