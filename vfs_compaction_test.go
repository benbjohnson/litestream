//go:build vfs
// +build vfs

package litestream_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

func TestVFSFile_Compact(t *testing.T) {
	t.Run("ManualCompact", func(t *testing.T) {
		dir := t.TempDir()
		client := file.NewReplicaClient(dir)

		// Pre-create some L0 files to test compaction
		createTestLTXFile(t, client, 0, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)
		createTestLTXFile(t, client, 0, 3, 3)

		// Create VFS with compaction enabled
		vfs := litestream.NewVFS(client, slog.Default())
		vfs.WriteEnabled = true
		vfs.CompactionEnabled = true

		// Create VFSFile directly to test Compact method
		f := litestream.NewVFSFile(client, "test.db", slog.Default())
		f.PollInterval = time.Second
		f.CacheSize = litestream.DefaultCacheSize

		// Initialize the compactor manually
		compactor := litestream.NewCompactor(client, slog.Default())

		// Compact L0 to L1
		info, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if info.Level != 1 {
			t.Errorf("Level=%d, want 1", info.Level)
		}
		if info.MinTXID != 1 || info.MaxTXID != 3 {
			t.Errorf("TXID range=%d-%d, want 1-3", info.MinTXID, info.MaxTXID)
		}
	})
}

func TestVFSFile_Snapshot(t *testing.T) {
	t.Run("MultiLevelCompaction", func(t *testing.T) {
		dir := t.TempDir()
		client := file.NewReplicaClient(dir)

		// Pre-create L0 files to simulate VFS writes
		createTestLTXFile(t, client, 0, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)
		createTestLTXFile(t, client, 0, 3, 3)

		compactor := litestream.NewCompactor(client, slog.Default())

		// Compact L0 to L1
		info, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if info.Level != 1 {
			t.Errorf("Level=%d, want 1", info.Level)
		}
		t.Logf("Compacted to L1: minTXID=%d, maxTXID=%d", info.MinTXID, info.MaxTXID)

		// Compact L1 to L2
		info, err = compactor.Compact(context.Background(), 2)
		if err != nil {
			t.Fatal(err)
		}
		if info.Level != 2 {
			t.Errorf("Level=%d, want 2", info.Level)
		}
		t.Logf("Compacted to L2: minTXID=%d, maxTXID=%d", info.MinTXID, info.MaxTXID)

		// Verify L2 file exists
		itr, err := client.LTXFiles(context.Background(), 2, 0, false)
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		var l2Count int
		for itr.Next() {
			l2Count++
		}
		if l2Count != 1 {
			t.Errorf("L2 file count=%d, want 1", l2Count)
		}
	})
}

func TestDefaultCompactionLevels(t *testing.T) {
	levels := litestream.DefaultCompactionLevels
	if len(levels) != 4 {
		t.Fatalf("DefaultCompactionLevels length=%d, want 4", len(levels))
	}

	// Verify L0 (raw files, no interval)
	if levels[0].Level != 0 {
		t.Errorf("levels[0].Level=%d, want 0", levels[0].Level)
	}
	if levels[0].Interval != 0 {
		t.Errorf("levels[0].Interval=%v, want 0", levels[0].Interval)
	}

	// Verify L1 (30 second compaction)
	if levels[1].Level != 1 {
		t.Errorf("levels[1].Level=%d, want 1", levels[1].Level)
	}
	if levels[1].Interval != 30*time.Second {
		t.Errorf("levels[1].Interval=%v, want 30s", levels[1].Interval)
	}

	// Verify L2 (5 minute compaction)
	if levels[2].Level != 2 {
		t.Errorf("levels[2].Level=%d, want 2", levels[2].Level)
	}
	if levels[2].Interval != 5*time.Minute {
		t.Errorf("levels[2].Interval=%v, want 5m", levels[2].Interval)
	}

	// Verify L3 (hourly compaction)
	if levels[3].Level != 3 {
		t.Errorf("levels[3].Level=%d, want 3", levels[3].Level)
	}
	if levels[3].Interval != time.Hour {
		t.Errorf("levels[3].Interval=%v, want 1h", levels[3].Interval)
	}

	// Verify they validate
	if err := levels.Validate(); err != nil {
		t.Errorf("DefaultCompactionLevels.Validate()=%v, want nil", err)
	}
}

func TestVFS_CompactionConfig(t *testing.T) {
	t.Run("DefaultConfig", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		vfs := litestream.NewVFS(client, slog.Default())

		// Default should have compaction disabled
		if vfs.CompactionEnabled {
			t.Error("CompactionEnabled should be false by default")
		}
		if vfs.CompactionLevels != nil {
			t.Error("CompactionLevels should be nil by default")
		}
		if vfs.SnapshotInterval != 0 {
			t.Error("SnapshotInterval should be 0 by default")
		}
	})

	t.Run("WithCompactionConfig", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		vfs := litestream.NewVFS(client, slog.Default())
		vfs.WriteEnabled = true
		vfs.CompactionEnabled = true
		vfs.CompactionLevels = litestream.CompactionLevels{
			{Level: 0, Interval: 0},
			{Level: 1, Interval: time.Minute},
		}
		vfs.SnapshotInterval = 24 * time.Hour
		vfs.SnapshotRetention = 7 * 24 * time.Hour
		vfs.L0Retention = 5 * time.Minute

		if !vfs.CompactionEnabled {
			t.Error("CompactionEnabled should be true")
		}
		if len(vfs.CompactionLevels) != 2 {
			t.Errorf("CompactionLevels length=%d, want 2", len(vfs.CompactionLevels))
		}
		if vfs.SnapshotInterval != 24*time.Hour {
			t.Errorf("SnapshotInterval=%v, want 24h", vfs.SnapshotInterval)
		}
		if vfs.SnapshotRetention != 7*24*time.Hour {
			t.Errorf("SnapshotRetention=%v, want 168h", vfs.SnapshotRetention)
		}
		if vfs.L0Retention != 5*time.Minute {
			t.Errorf("L0Retention=%v, want 5m", vfs.L0Retention)
		}
	})
}
