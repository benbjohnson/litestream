package litestream_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
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

		// The base is captured in the snapshot at TXID 1; L0 holds increments
		// from TXID 2 onward. Compaction into the empty L1 seeks from snapMax+1.
		createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)
		createTestLTXFile(t, client, 0, 3, 3)

		info, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if info.Level != 1 {
			t.Errorf("Level=%d, want 1", info.Level)
		}
		if info.MinTXID != 2 || info.MaxTXID != 3 {
			t.Errorf("TXID range=%d-%d, want 2-3", info.MinTXID, info.MaxTXID)
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

		// Base in the snapshot at TXID 1; L0 increments from TXID 2.
		createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)
		createTestLTXFile(t, client, 0, 3, 3)

		// Compact to L1 (seeks from snapMax+1 = 2)
		_, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}

		// Create more L0 files
		createTestLTXFile(t, client, 0, 4, 4)

		// Compact to L1 again (should only include TXID 4)
		info, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if info.MinTXID != 4 || info.MaxTXID != 4 {
			t.Errorf("TXID range=%d-%d, want 4-4", info.MinTXID, info.MaxTXID)
		}

		// Now compact L1 to L2 (empty L2 seeks from snapMax+1 = 2, pulling all L1)
		info, err = compactor.Compact(context.Background(), 2)
		if err != nil {
			t.Fatal(err)
		}
		if info.Level != 2 {
			t.Errorf("Level=%d, want 2", info.Level)
		}
		if info.MinTXID != 2 || info.MaxTXID != 4 {
			t.Errorf("TXID range=%d-%d, want 2-4", info.MinTXID, info.MaxTXID)
		}
	})
}

func TestCompactor_Compact_SeeksFromSnapshot(t *testing.T) {
	t.Run("EmptyDestinationSkipsSnapshottedRange", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// A snapshot already covers TXID 1-5.
		createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 5)

		// L0 has the snapshotted range plus new increments.
		createTestLTXFile(t, client, 0, 1, 5)
		createTestLTXFile(t, client, 0, 6, 6)
		createTestLTXFile(t, client, 0, 7, 7)

		// L1 has never been compacted (empty destination): should seek from
		// snapMax+1 rather than TXID 1, skipping the file already captured
		// in the snapshot.
		info, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if info.MinTXID != 6 || info.MaxTXID != 7 {
			t.Errorf("TXID range=%d-%d, want 6-7", info.MinTXID, info.MaxTXID)
		}
	})

	t.Run("NoSnapshotDefers", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// No snapshot exists yet. An empty destination must DEFER (ErrNoCompaction)
		// rather than seek from TXID 1, which would pull the DB-sized base into
		// the ladder's first compaction. The snapshot and ladder levels run in
		// independent monitors with no ordering guarantee, so a ladder level can
		// reach Compact before the snapshot lands; it should wait.
		createTestLTXFile(t, client, 0, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)

		if _, err := compactor.Compact(context.Background(), 1); err != litestream.ErrNoCompaction {
			t.Errorf("err=%v, want ErrNoCompaction", err)
		}
	})

	t.Run("DefersThenProceedsOnceSnapshotLands", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// L0 base + increment, but no snapshot yet: the first compaction defers.
		createTestLTXFile(t, client, 0, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)
		if _, err := compactor.Compact(context.Background(), 1); err != litestream.ErrNoCompaction {
			t.Fatalf("err=%v, want ErrNoCompaction before snapshot exists", err)
		}

		// Once the snapshot lands (covering the base at TXID 1), the same empty
		// destination proceeds, seeking from snapMax+1 and skipping the base.
		createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 1)
		info, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if info.MinTXID != 2 || info.MaxTXID != 2 {
			t.Errorf("TXID range=%d-%d, want 2-2 (base skipped, seek from snapMax+1)", info.MinTXID, info.MaxTXID)
		}
	})

	t.Run("NonEmptyDestinationSkipsPastLeapfroggingSnapshot", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// A base snapshot must exist for the empty-destination first compaction to
		// proceed (an empty destination defers until the snapshot lands).
		createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 1)

		// L1 already has files up through TXID 3 (seeded from snapMax+1 = 2).
		createTestLTXFile(t, client, 0, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)
		createTestLTXFile(t, client, 0, 3, 3)
		if _, err := compactor.Compact(context.Background(), 1); err != nil {
			t.Fatal(err)
		}

		// A later snapshot leapfrogs past the current L1 head, covering [1,10].
		createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 10)

		// New L0 increments 4..10 are now redundant (subsumed by the snapshot),
		// plus fresh increments past the snapshot at 11.
		for txID := ltx.TXID(4); txID <= 10; txID++ {
			createTestLTXFile(t, client, 0, txID, txID)
		}
		createTestLTXFile(t, client, 0, 11, 11)

		// The seek is max(dstMax, snapMax)+1 = 11: the ladder skips the
		// snapshot-covered range 4..10 instead of re-writing it, and picks up at
		// TXID 11. This is the whole point of the policy -- snapshot-covered data
		// is never dragged back through the ladder.
		info, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if info.MinTXID != 11 || info.MaxTXID != 11 {
			t.Errorf("TXID range=%d-%d, want 11-11 (ladder seeks past the leapfrogging snapshot)", info.MinTXID, info.MaxTXID)
		}

		// L1 is now {[2,3],[11,11]}: the gap [4,10] is spanned by the snapshot
		// [1,10], which VerifyLevelConsistency must accept.
		if err := compactor.VerifyLevelConsistency(context.Background(), 1); err != nil {
			t.Errorf("expected snapshot-covered gap to verify, got: %v", err)
		}
	})

	t.Run("LaggingDestinationDoesNotStraddleSnapshotHole", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Base snapshot covers TXID 1.
		createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 1)

		// L1 has only caught up through TXID 3 (it lags). With the base snapshot at
		// TXID 1, the first compaction seeks from 2 and produces L1 [2,3].
		createTestLTXFile(t, client, 0, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)
		createTestLTXFile(t, client, 0, 3, 3)
		if info, err := compactor.Compact(context.Background(), 1); err != nil {
			t.Fatal(err)
		} else if info.MinTXID != 2 || info.MaxTXID != 3 {
			t.Fatalf("L1 head=%d-%d, want 2-3", info.MinTXID, info.MaxTXID)
		}

		// A boundary snapshot lands at TXID 5, leaving a HOLE in L0 (no [5,5]
		// file: that txID went straight to the snapshot level). L0 has a file at
		// TXID 4 that L1 has not yet consumed, sitting below the snapshot.
		createTestLTXFile(t, client, 0, 4, 4)
		createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 5)
		createTestLTXFile(t, client, 0, 6, 6)
		createTestLTXFile(t, client, 0, 7, 7)

		// L1 (head=3) lags the snapshot (max=5). A naive seek of dstMax+1 = 4
		// would read L0 files on BOTH sides of the hole -- {[4,4],[6,6],[7,7]} --
		// a non-contiguous input that ltx.Compactor rejects. The policy seeks from
		// max(3,5)+1 = 6 instead, skipping the redundant [4,4] and the hole.
		info, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatalf("straddle should be avoided by seeking past the snapshot, got: %v", err)
		}
		if info.MinTXID != 6 || info.MaxTXID != 7 {
			t.Errorf("TXID range=%d-%d, want 6-7 (seek past the boundary-snapshot hole)", info.MinTXID, info.MaxTXID)
		}

		// L1 is {[2,3],[6,7]}: the gap [4,5] is spanned by the snapshot [1,5].
		if err := compactor.VerifyLevelConsistency(context.Background(), 1); err != nil {
			t.Errorf("expected snapshot-covered gap to verify, got: %v", err)
		}
	})
}

func TestCompactor_CompactClosesPipeOnWriteError(t *testing.T) {
	client := newEarlyReturnCompactionClient(t.TempDir())
	compactor := litestream.NewCompactor(client, slog.Default())

	createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 1)
	createTestLTXFile(t, client, 0, 2, 2)
	createTestLTXFile(t, client, 0, 3, 3)
	client.failWrites = true

	before := countCompactorPipeWriters()
	if _, err := compactor.Compact(context.Background(), 1); err == nil {
		t.Fatal("expected error")
	} else if !strings.Contains(err.Error(), "write ltx file: early write failure") {
		t.Fatalf("unexpected error: %v", err)
	}

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if got := countCompactorPipeWriters(); got <= before {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("compactor goroutine leaked: got %d, want <= %d", countCompactorPipeWriters(), before)
		case <-ticker.C:
		}
	}
}

func TestCompactor_CompactResumesRemoteSourceAfterDisconnect(t *testing.T) {
	client := newDisconnectingCompactionClient(t.TempDir(), 16)
	compactor := litestream.NewCompactor(client, slog.Default())

	createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 1)
	createTestLTXFile(t, client, 0, 2, 2)

	info, err := compactor.Compact(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if info.Level != 1 {
		t.Errorf("Level=%d, want 1", info.Level)
	}
	if info.MinTXID != 2 || info.MaxTXID != 2 {
		t.Errorf("TXID range=%d-%d, want 2-2", info.MinTXID, info.MaxTXID)
	}

	var resumed bool
	for _, offset := range client.openOffsets[1:] {
		if offset > 0 {
			resumed = true
			break
		}
	}
	if !resumed {
		t.Fatalf("OpenLTXFile offsets=%v, want reopen at non-zero offset", client.openOffsets)
	}
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

		// Base in the snapshot at TXID 1; L0 increments from TXID 2.
		createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)
		createTestLTXFile(t, client, 0, 3, 3)
		createTestLTXFile(t, client, 0, 4, 4)

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

func TestCompactor_EnforceSnapshotRetention_RetentionDisabled(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	compactor := litestream.NewCompactor(client, slog.Default())
	compactor.RetentionEnabled = false

	var localDeleted []ltx.TXID
	compactor.LocalFileDeleter = func(level int, minTXID, maxTXID ltx.TXID) error {
		localDeleted = append(localDeleted, maxTXID)
		return nil
	}

	createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 5, time.Now().Add(-2*time.Hour))
	createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 10, time.Now().Add(-30*time.Minute))
	createTestLTXFileWithTimestamp(t, client, litestream.SnapshotLevel, 1, 15, time.Now().Add(-5*time.Minute))

	_, err := compactor.EnforceSnapshotRetention(context.Background(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	// Remote files should all still exist (skip remote deletion).
	itr, err := client.LTXFiles(context.Background(), litestream.SnapshotLevel, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer itr.Close()

	var count int
	for itr.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("remote file count=%d, want 3 (no remote deletion)", count)
	}

	// Local file deleter should still have been called.
	if len(localDeleted) != 1 {
		t.Errorf("local deleted count=%d, want 1", len(localDeleted))
	}
}

func TestCompactor_EnforceRetentionByTXID_RetentionDisabled(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	compactor := litestream.NewCompactor(client, slog.Default())
	compactor.RetentionEnabled = false

	var localDeleted []ltx.TXID
	compactor.LocalFileDeleter = func(level int, minTXID, maxTXID ltx.TXID) error {
		localDeleted = append(localDeleted, maxTXID)
		return nil
	}

	createTestLTXFile(t, client, 1, 1, 2)
	createTestLTXFile(t, client, 1, 3, 5)
	createTestLTXFile(t, client, 1, 6, 10)

	err := compactor.EnforceRetentionByTXID(context.Background(), 1, 5)
	if err != nil {
		t.Fatal(err)
	}

	// Remote files should all still exist.
	itr, err := client.LTXFiles(context.Background(), 1, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer itr.Close()

	var count int
	for itr.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("remote file count=%d, want 3 (no remote deletion)", count)
	}

	// Local file deleter should still have been called for the file below TXID 5.
	if len(localDeleted) != 1 {
		t.Errorf("local deleted count=%d, want 1", len(localDeleted))
	}
}

func TestCompactor_EnforceL0Retention_RetentionDisabled(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	compactor := litestream.NewCompactor(client, slog.Default())
	compactor.RetentionEnabled = false

	var localDeleted []ltx.TXID
	compactor.LocalFileDeleter = func(level int, minTXID, maxTXID ltx.TXID) error {
		localDeleted = append(localDeleted, maxTXID)
		return nil
	}

	// Base in the snapshot at TXID 1; L0 increments (old timestamps so they're
	// eligible for deletion) from TXID 2.
	createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 1)
	oldTime := time.Now().Add(-1 * time.Hour)
	createTestLTXFileWithTimestamp(t, client, 0, 2, 2, oldTime)
	createTestLTXFileWithTimestamp(t, client, 0, 3, 3, oldTime)
	createTestLTXFileWithTimestamp(t, client, 0, 4, 4, oldTime)

	// Compact to L1 first.
	_, err := compactor.Compact(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	// Use a real retention duration so the check doesn't return early.
	err = compactor.EnforceL0Retention(context.Background(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// Remote L0 files should all still exist.
	itr, err := client.LTXFiles(context.Background(), 0, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer itr.Close()

	var count int
	for itr.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("remote file count=%d, want 3 (no remote deletion)", count)
	}

	// Local file deleter should still have been called for compacted files.
	if len(localDeleted) < 1 {
		t.Errorf("local deleted count=%d, want at least 1", len(localDeleted))
	}
}

func TestCompactor_VerifyLevelConsistency(t *testing.T) {
	t.Run("ContiguousFiles", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Create contiguous files
		createTestLTXFile(t, client, 1, 1, 2)
		createTestLTXFile(t, client, 1, 3, 5)
		createTestLTXFile(t, client, 1, 6, 10)

		// Should pass verification
		err := compactor.VerifyLevelConsistency(context.Background(), 1)
		if err != nil {
			t.Errorf("expected nil error for contiguous files, got: %v", err)
		}
	})

	t.Run("GapDetected", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Create files with a gap (missing TXID 3-4)
		createTestLTXFile(t, client, 1, 1, 2)
		createTestLTXFile(t, client, 1, 5, 7) // gap: expected MinTXID=3, got 5

		err := compactor.VerifyLevelConsistency(context.Background(), 1)
		if err == nil {
			t.Error("expected error for gap in files, got nil")
		}
		if err != nil && !containsString(err.Error(), "gap") {
			t.Errorf("expected gap error, got: %v", err)
		}
	})

	t.Run("OverlapDetected", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Create overlapping files
		createTestLTXFile(t, client, 1, 1, 5)
		createTestLTXFile(t, client, 1, 3, 7) // overlap: expected MinTXID=6, got 3

		err := compactor.VerifyLevelConsistency(context.Background(), 1)
		if err == nil {
			t.Error("expected error for overlapping files, got nil")
		}
		if err != nil && !containsString(err.Error(), "overlap") {
			t.Errorf("expected overlap error, got: %v", err)
		}
	})

	t.Run("SingleFile", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Create single file - should pass
		createTestLTXFile(t, client, 1, 1, 5)

		err := compactor.VerifyLevelConsistency(context.Background(), 1)
		if err != nil {
			t.Errorf("expected nil error for single file, got: %v", err)
		}
	})

	t.Run("EmptyLevel", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())

		// Empty level - should pass
		err := compactor.VerifyLevelConsistency(context.Background(), 1)
		if err != nil {
			t.Errorf("expected nil error for empty level, got: %v", err)
		}
	})
}

func TestCompactor_CompactWithVerification(t *testing.T) {
	t.Run("VerificationEnabled", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		compactor := litestream.NewCompactor(client, slog.Default())
		compactor.VerifyCompaction = true

		// Base in the snapshot at TXID 1; contiguous L0 increments from TXID 2.
		createTestLTXFile(t, client, litestream.SnapshotLevel, 1, 1)
		createTestLTXFile(t, client, 0, 2, 2)
		createTestLTXFile(t, client, 0, 3, 3)
		createTestLTXFile(t, client, 0, 4, 4)

		// Compact to L1 - should succeed with verification
		info, err := compactor.Compact(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if info.Level != 1 {
			t.Errorf("Level=%d, want 1", info.Level)
		}
		if info.MinTXID != 2 || info.MaxTXID != 4 {
			t.Errorf("TXID range=%d-%d, want 2-4", info.MinTXID, info.MaxTXID)
		}
	})
}

// containsString checks if s contains substr.
func containsString(s, substr string) bool {
	return bytes.Contains([]byte(s), []byte(substr))
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

type earlyReturnCompactionClient struct {
	litestream.ReplicaClient
	failWrites bool
}

func newEarlyReturnCompactionClient(path string) *earlyReturnCompactionClient {
	return &earlyReturnCompactionClient{ReplicaClient: file.NewReplicaClient(path)}
}

func (c *earlyReturnCompactionClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if c.failWrites {
		return nil, fmt.Errorf("early write failure")
	}
	return c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

type disconnectingCompactionClient struct {
	litestream.ReplicaClient
	dropAfter   int64
	dropped     bool
	openOffsets []int64
}

func newDisconnectingCompactionClient(path string, dropAfter int64) *disconnectingCompactionClient {
	return &disconnectingCompactionClient{
		ReplicaClient: file.NewReplicaClient(path),
		dropAfter:     dropAfter,
	}
}

func (c *disconnectingCompactionClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.openOffsets = append(c.openOffsets, offset)

	rc, err := c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
	if err != nil {
		return nil, err
	}
	if !c.dropped && offset == 0 {
		c.dropped = true
		return &disconnectingReadCloser{ReadCloser: rc, remaining: c.dropAfter}, nil
	}
	return rc, nil
}

type disconnectingReadCloser struct {
	io.ReadCloser
	remaining int64
}

func (r *disconnectingReadCloser) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:int(r.remaining)]
	}

	n, err := r.ReadCloser.Read(p)
	r.remaining -= int64(n)
	if err != nil {
		return n, err
	}
	if r.remaining <= 0 {
		return n, io.EOF
	}
	return n, nil
}

func countCompactorPipeWriters() int {
	buf := make([]byte, 2<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "github.com/benbjohnson/litestream.(*Compactor).Compact.func")
}
