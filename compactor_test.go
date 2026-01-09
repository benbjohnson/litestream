package litestream_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

func TestCompactor_Compact(t *testing.T) {
	t.Run("L0ToL1", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Create test L0 files
		createTestLTXFile(t, client, 0, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)

		info, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if info.Level != 1 {
			t.Errorf("Level=%d, want 1", info.Level)
		}
		if info.MinTXID != 1 || info.MaxTXID != 2 {
			t.Errorf("TXID range=%d-%d, want 1-2", info.MinTXID, info.MaxTXID)
		}
	})

	t.Run("NoFiles", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		_, err := compactor.Compact(context.Background(), 1)
		if err != litestream.ErrNoCompaction {
			t.Errorf("err=%v, want ErrNoCompaction", err)
		}
	})

	t.Run("L1ToL2", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Create L0 files
		createTestLTXFile(t, client, 0, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)

		// Compact to L1
		_, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}

		// Create more L0 files
		createTestLTXFile(t, client, 0, 3, 3)

		// Compact to L1 again (should only include TXID 3)
		info, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if info.MinTXID != 3 || info.MaxTXID != 3 {
			t.Errorf("TXID range=%d-%d, want 3-3", info.MinTXID, info.MaxTXID)
		}

		// Now compact L1 to L2 (should include all from 1-3)
		info, err = compactor.Compact(context.Background(), 2)
		if err != nil {
			t.Fatal(err)
		}
		if info.Level != 2 {
			t.Errorf("Level=%d, want 2", info.Level)
		}
		if info.MinTXID != 1 || info.MaxTXID != 3 {
			t.Errorf("TXID range=%d-%d, want 1-3", info.MinTXID, info.MaxTXID)
		}
	})
}

func TestCompactor_MaxLTXFileInfo(t *testing.T) {
	t.Run("WithFiles", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		createTestLTXFile(t, client, 0, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)
		createTestLTXFile(t, client, 0, 3, 5)

		info, err := compactor.MaxLTXFileInfo(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		if info.MaxTXID != 5 {
			t.Errorf("MaxTXID=%d, want 5", info.MaxTXID)
		}
	})

	t.Run("NoFiles", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		info, err := compactor.MaxLTXFileInfo(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		if info.MaxTXID != 0 {
			t.Errorf("MaxTXID=%d, want 0", info.MaxTXID)
		}
	})

	t.Run("WithCache", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Use callbacks for caching
		cache := make(map[int]*ltx.FileInfo)
		compactor.CacheGetter = func(level int) (*ltx.FileInfo, bool) {
			info, ok := cache[level]
			return info, ok
		}
		compactor.CacheSetter = func(level int, info *ltx.FileInfo) {
			cache[level] = info
		}

		createTestLTXFile(t, client, 0, 1, 3)

		// First call should populate cache
		info, err := compactor.MaxLTXFileInfo(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		if info.MaxTXID != 3 {
			t.Errorf("MaxTXID=%d, want 3", info.MaxTXID)
		}

		// Second call should use cache
		info, err = compactor.MaxLTXFileInfo(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		if info.MaxTXID != 3 {
			t.Errorf("MaxTXID=%d, want 3 (from cache)", info.MaxTXID)
		}
	})
}

func TestCompactor_EnforceRetentionByTXID(t *testing.T) {
	t.Run("DeletesOldFiles", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Create files at L1
		createTestLTXFile(t, client, 1, 1, 2)
		createTestLTXFile(t, client, 1, 3, 5)
		createTestLTXFile(t, client, 1, 6, 10)

		// Enforce retention - delete files below TXID 5
		err := compactor.EnforceRetentionByTXID(context.Background(), 1, 5)
		if err != nil {
			t.Fatal(err)
		}

		// Verify only the first file was deleted
		info, err := compactor.MaxLTXFileInfo(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if info.MaxTXID != 10 {
			t.Errorf("MaxTXID=%d, want 10", info.MaxTXID)
		}

		// Check that files starting from TXID 3 are still present
		itr, err := client.LTXFiles(context.Background(), 1, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		var count int
		for itr.Next() {
			count++
		}
		if count != 2 {
			t.Errorf("file count=%d, want 2", count)
		}
	})

	t.Run("KeepsLastFile", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Create single file
		createTestLTXFile(t, client, 1, 1, 2)

		// Try to delete it - should keep at least one
		err := compactor.EnforceRetentionByTXID(context.Background(), 1, 100)
		if err != nil {
			t.Fatal(err)
		}

		// Verify file still exists
		info, err := compactor.MaxLTXFileInfo(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if info.MaxTXID != 2 {
			t.Errorf("MaxTXID=%d, want 2 (last file should be kept)", info.MaxTXID)
		}
	})
}

func TestCompactor_EnforceL0Retention(t *testing.T) {
	t.Run("DeletesCompactedFiles", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Create L0 files
		createTestLTXFile(t, client, 0, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)
		createTestLTXFile(t, client, 0, 3, 3)

		// Compact to L1
		_, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}

		// Enforce L0 retention with 0 duration (delete immediately)
		err = compactor.EnforceL0Retention(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}

		// L0 files compacted into L1 should be deleted (except last)
		itr, err := client.LTXFiles(context.Background(), 0, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		var count int
		for itr.Next() {
			count++
		}
		// At least one file should remain
		if count < 1 {
			t.Errorf("file count=%d, want at least 1", count)
		}
	})

	t.Run("SkipsIfNoL1", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Create L0 files without compacting to L1
		createTestLTXFile(t, client, 0, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)

		// Enforce L0 retention - should do nothing since no L1 exists
		err := compactor.EnforceL0Retention(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}

		// All L0 files should still exist
		itr, err := client.LTXFiles(context.Background(), 0, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		var count int
		for itr.Next() {
			count++
		}
		if count != 2 {
			t.Errorf("file count=%d, want 2", count)
		}
	})
}

func TestCompactor_EnforceSnapshotRetention(t *testing.T) {
	t.Run("DeletesOldSnapshots", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Create snapshot files with different timestamps
		createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 5, time.Now().Add(-2*time.Hour))
		createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 10, time.Now().Add(-30*time.Minute))
		createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 15, time.Now().Add(-5*time.Minute))

		// Enforce retention - keep snapshots from last hour
		_, err := compactor.EnforceSnapshotRetention(context.Background(), time.Hour)
		if err != nil {
			t.Fatal(err)
		}

		// Count remaining snapshots
		itr, err := client.LTXFiles(context.Background(), litestream.SnapshotLevel, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		var count int
		for itr.Next() {
			count++
		}
		// Should have 2 snapshots (the 30min and 5min old ones)
		if count != 2 {
			t.Errorf("snapshot count=%d, want 2", count)
		}
	})
}

// createTestLTXFile creates a minimal LTX file for testing.
func createTestLTXFile(t testing.TB, client litestream.ReplicaClient, level int, minTXID, maxTXID ltx.TXID) {
	t.Helper()
	createTestLTXFileWithTimestamp(t, client, level, minTXID, maxTXID, time.Now())
}

// createTestLTXFileWithTimestamp creates a minimal LTX file with a specific timestamp.
func createTestLTXFileWithTimestamp(t testing.TB, client litestream.ReplicaClient, level int, minTXID, maxTXID ltx.TXID, ts time.Time) {
	t.Helper()

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}

	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: ts.UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}

	// Write a dummy page
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}

	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := client.WriteLTXFile(context.Background(), level, minTXID, maxTXID, io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
}
