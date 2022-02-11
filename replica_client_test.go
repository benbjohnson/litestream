package litestream_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/mock"
)

func TestFindSnapshotForIndex(t *testing.T) {
	t.Run("BeforeIndex", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "find-snapshot-for-index", "ok"))
		if snapshotIndex, err := litestream.FindSnapshotForIndex(context.Background(), client, "0000000000000000", 0x000007d0); err != nil {
			t.Fatal(err)
		} else if got, want := snapshotIndex, 0x000003e8; got != want {
			t.Fatalf("index=%s, want %s", litestream.FormatIndex(got), litestream.FormatIndex(want))
		}
	})

	t.Run("AtIndex", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "find-snapshot-for-index", "ok"))
		if snapshotIndex, err := litestream.FindSnapshotForIndex(context.Background(), client, "0000000000000000", 0x000003e8); err != nil {
			t.Fatal(err)
		} else if got, want := snapshotIndex, 0x000003e8; got != want {
			t.Fatalf("index=%s, want %s", litestream.FormatIndex(got), litestream.FormatIndex(want))
		}
	})

	t.Run("ErrNoSnapshotsBeforeIndex", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "find-snapshot-for-index", "no-snapshots-before-index"))
		_, err := litestream.FindSnapshotForIndex(context.Background(), client, "0000000000000000", 0x000003e8)
		if err == nil || err.Error() != `no snapshots available at or before index 00000000000003e8` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrNoSnapshots", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "find-snapshot-for-index", "no-snapshots"))
		_, err := litestream.FindSnapshotForIndex(context.Background(), client, "0000000000000000", 0x000003e8)
		if err != litestream.ErrNoSnapshots {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrSnapshots", func(t *testing.T) {
		var client mock.ReplicaClient
		client.SnapshotsFunc = func(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
			return nil, fmt.Errorf("marker")
		}
		_, err := litestream.FindSnapshotForIndex(context.Background(), &client, "0000000000000000", 0x000003e8)
		if err == nil || err.Error() != `snapshots: marker` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrSnapshotIterator", func(t *testing.T) {
		var itr mock.SnapshotIterator
		itr.NextFunc = func() bool { return false }
		itr.CloseFunc = func() error { return fmt.Errorf("marker") }

		var client mock.ReplicaClient
		client.SnapshotsFunc = func(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
			return &itr, nil
		}

		_, err := litestream.FindSnapshotForIndex(context.Background(), &client, "0000000000000000", 0x000003e8)
		if err == nil || err.Error() != `snapshot iteration: marker` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

func TestSnapshotTimeBounds(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "snapshot-time-bounds", "ok"))
		if min, max, err := litestream.SnapshotTimeBounds(context.Background(), client, "0000000000000000"); err != nil {
			t.Fatal(err)
		} else if got, want := min, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("min=%s, want %s", got, want)
		} else if got, want := max, time.Date(2000, 1, 3, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("max=%s, want %s", got, want)
		}
	})

	t.Run("ErrNoSnapshots", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "snapshot-time-bounds", "no-snapshots"))
		if _, _, err := litestream.SnapshotTimeBounds(context.Background(), client, "0000000000000000"); err != litestream.ErrNoSnapshots {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrSnapshots", func(t *testing.T) {
		var client mock.ReplicaClient
		client.SnapshotsFunc = func(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
			return nil, fmt.Errorf("marker")
		}

		_, _, err := litestream.SnapshotTimeBounds(context.Background(), &client, "0000000000000000")
		if err == nil || err.Error() != `snapshots: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshotIterator", func(t *testing.T) {
		var itr mock.SnapshotIterator
		itr.NextFunc = func() bool { return false }
		itr.CloseFunc = func() error { return fmt.Errorf("marker") }

		var client mock.ReplicaClient
		client.SnapshotsFunc = func(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
			return &itr, nil
		}

		_, _, err := litestream.SnapshotTimeBounds(context.Background(), &client, "0000000000000000")
		if err == nil || err.Error() != `snapshot iteration: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestWALTimeBounds(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "wal-time-bounds", "ok"))
		if min, max, err := litestream.WALTimeBounds(context.Background(), client, "0000000000000000"); err != nil {
			t.Fatal(err)
		} else if got, want := min, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("min=%s, want %s", got, want)
		} else if got, want := max, time.Date(2000, 1, 3, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("max=%s, want %s", got, want)
		}
	})

	t.Run("ErrNoWALSegments", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "wal-time-bounds", "no-wal-segments"))
		if _, _, err := litestream.WALTimeBounds(context.Background(), client, "0000000000000000"); err != litestream.ErrNoWALSegments {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrWALSegments", func(t *testing.T) {
		var client mock.ReplicaClient
		client.WALSegmentsFunc = func(ctx context.Context, generation string) (litestream.WALSegmentIterator, error) {
			return nil, fmt.Errorf("marker")
		}

		_, _, err := litestream.WALTimeBounds(context.Background(), &client, "0000000000000000")
		if err == nil || err.Error() != `wal segments: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrWALSegmentIterator", func(t *testing.T) {
		var itr mock.WALSegmentIterator
		itr.NextFunc = func() bool { return false }
		itr.CloseFunc = func() error { return fmt.Errorf("marker") }

		var client mock.ReplicaClient
		client.WALSegmentsFunc = func(ctx context.Context, generation string) (litestream.WALSegmentIterator, error) {
			return &itr, nil
		}

		_, _, err := litestream.WALTimeBounds(context.Background(), &client, "0000000000000000")
		if err == nil || err.Error() != `wal segment iteration: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestGenerationTimeBounds(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "generation-time-bounds", "ok"))
		if min, max, err := litestream.GenerationTimeBounds(context.Background(), client, "0000000000000000"); err != nil {
			t.Fatal(err)
		} else if got, want := min, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("min=%s, want %s", got, want)
		} else if got, want := max, time.Date(2000, 1, 3, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("max=%s, want %s", got, want)
		}
	})

	t.Run("SnapshotsOnly", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "generation-time-bounds", "snapshots-only"))
		if min, max, err := litestream.GenerationTimeBounds(context.Background(), client, "0000000000000000"); err != nil {
			t.Fatal(err)
		} else if got, want := min, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("min=%s, want %s", got, want)
		} else if got, want := max, time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("max=%s, want %s", got, want)
		}
	})

	t.Run("ErrNoSnapshots", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "generation-time-bounds", "no-snapshots"))
		if _, _, err := litestream.GenerationTimeBounds(context.Background(), client, "0000000000000000"); err != litestream.ErrNoSnapshots {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrWALSegments", func(t *testing.T) {
		var snapshotN int
		var itr mock.SnapshotIterator
		itr.NextFunc = func() bool {
			snapshotN++
			return snapshotN == 1
		}
		itr.SnapshotFunc = func() litestream.SnapshotInfo {
			return litestream.SnapshotInfo{CreatedAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)}
		}
		itr.CloseFunc = func() error { return nil }

		var client mock.ReplicaClient
		client.SnapshotsFunc = func(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
			return &itr, nil
		}
		client.WALSegmentsFunc = func(ctx context.Context, generation string) (litestream.WALSegmentIterator, error) {
			return nil, fmt.Errorf("marker")
		}

		_, _, err := litestream.GenerationTimeBounds(context.Background(), &client, "0000000000000000")
		if err == nil || err.Error() != `wal segments: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestFindLatestGeneration(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "find-latest-generation", "ok"))
		if generation, err := litestream.FindLatestGeneration(context.Background(), client); err != nil {
			t.Fatal(err)
		} else if got, want := generation, "0000000000000001"; got != want {
			t.Fatalf("generation=%s, want %s", got, want)
		}
	})

	t.Run("ErrNoSnapshots", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "find-latest-generation", "no-generations"))
		if generation, err := litestream.FindLatestGeneration(context.Background(), client); err != litestream.ErrNoGeneration {
			t.Fatalf("unexpected error: %s", err)
		} else if got, want := generation, ""; got != want {
			t.Fatalf("generation=%s, want %s", got, want)
		}
	})

	t.Run("ErrGenerations", func(t *testing.T) {
		var client mock.ReplicaClient
		client.GenerationsFunc = func(ctx context.Context) ([]string, error) {
			return nil, fmt.Errorf("marker")
		}

		_, err := litestream.FindLatestGeneration(context.Background(), &client)
		if err == nil || err.Error() != `generations: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshots", func(t *testing.T) {
		var client mock.ReplicaClient
		client.GenerationsFunc = func(ctx context.Context) ([]string, error) {
			return []string{"0000000000000000"}, nil
		}
		client.SnapshotsFunc = func(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
			return nil, fmt.Errorf("marker")
		}

		_, err := litestream.FindLatestGeneration(context.Background(), &client)
		if err == nil || err.Error() != `generation time bounds: snapshots: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestReplicaClientTimeBounds(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "find-latest-generation", "ok"))
		if min, max, err := litestream.ReplicaClientTimeBounds(context.Background(), client); err != nil {
			t.Fatal(err)
		} else if got, want := min, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("min=%s, want %s", got, want)
		} else if got, want := max, time.Date(2000, 1, 3, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Fatalf("max=%s, want %s", got, want)
		}
	})

	t.Run("ErrNoGeneration", func(t *testing.T) {
		var client mock.ReplicaClient
		client.GenerationsFunc = func(ctx context.Context) ([]string, error) {
			return nil, nil
		}

		_, _, err := litestream.ReplicaClientTimeBounds(context.Background(), &client)
		if err != litestream.ErrNoGeneration {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrGenerations", func(t *testing.T) {
		var client mock.ReplicaClient
		client.GenerationsFunc = func(ctx context.Context) ([]string, error) {
			return nil, fmt.Errorf("marker")
		}

		_, _, err := litestream.ReplicaClientTimeBounds(context.Background(), &client)
		if err == nil || err.Error() != `generations: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshots", func(t *testing.T) {
		var client mock.ReplicaClient
		client.GenerationsFunc = func(ctx context.Context) ([]string, error) {
			return []string{"0000000000000000"}, nil
		}
		client.SnapshotsFunc = func(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
			return nil, fmt.Errorf("marker")
		}

		_, _, err := litestream.ReplicaClientTimeBounds(context.Background(), &client)
		if err == nil || err.Error() != `generation time bounds: snapshots: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestFindMaxSnapshotIndexByGeneration(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "max-snapshot-index", "ok"))
		if index, err := litestream.FindMaxSnapshotIndexByGeneration(context.Background(), client, "0000000000000000"); err != nil {
			t.Fatal(err)
		} else if got, want := index, 0x000007d0; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		}
	})

	t.Run("ErrNoSnapshots", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "max-snapshot-index", "no-snapshots"))

		_, err := litestream.FindMaxSnapshotIndexByGeneration(context.Background(), client, "0000000000000000")
		if err != litestream.ErrNoSnapshots {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshots", func(t *testing.T) {
		var client mock.ReplicaClient
		client.SnapshotsFunc = func(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
			return nil, fmt.Errorf("marker")
		}

		_, err := litestream.FindMaxSnapshotIndexByGeneration(context.Background(), &client, "0000000000000000")
		if err == nil || err.Error() != `snapshots: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshotIteration", func(t *testing.T) {
		var itr mock.SnapshotIterator
		itr.NextFunc = func() bool { return false }
		itr.CloseFunc = func() error { return fmt.Errorf("marker") }

		var client mock.ReplicaClient
		client.SnapshotsFunc = func(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
			return &itr, nil
		}

		_, err := litestream.FindMaxSnapshotIndexByGeneration(context.Background(), &client, "0000000000000000")
		if err == nil || err.Error() != `snapshot iteration: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestFindMaxWALIndexByGeneration(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "max-wal-index", "ok"))
		if index, err := litestream.FindMaxWALIndexByGeneration(context.Background(), client, "0000000000000000"); err != nil {
			t.Fatal(err)
		} else if got, want := index, 1; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		}
	})

	t.Run("ErrNoWALSegments", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "max-wal-index", "no-wal"))

		_, err := litestream.FindMaxWALIndexByGeneration(context.Background(), client, "0000000000000000")
		if err != litestream.ErrNoWALSegments {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrWALSegments", func(t *testing.T) {
		var client mock.ReplicaClient
		client.WALSegmentsFunc = func(ctx context.Context, generation string) (litestream.WALSegmentIterator, error) {
			return nil, fmt.Errorf("marker")
		}

		_, err := litestream.FindMaxWALIndexByGeneration(context.Background(), &client, "0000000000000000")
		if err == nil || err.Error() != `wal segments: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrWALSegmentIteration", func(t *testing.T) {
		var itr mock.WALSegmentIterator
		itr.NextFunc = func() bool { return false }
		itr.CloseFunc = func() error { return fmt.Errorf("marker") }

		var client mock.ReplicaClient
		client.WALSegmentsFunc = func(ctx context.Context, generation string) (litestream.WALSegmentIterator, error) {
			return &itr, nil
		}

		_, err := litestream.FindMaxWALIndexByGeneration(context.Background(), &client, "0000000000000000")
		if err == nil || err.Error() != `wal segment iteration: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestFindMaxIndexByGeneration(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "max-index", "ok"))
		if index, err := litestream.FindMaxIndexByGeneration(context.Background(), client, "0000000000000000"); err != nil {
			t.Fatal(err)
		} else if got, want := index, 0x00000002; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		}
	})

	t.Run("NoWAL", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "max-index", "no-wal"))
		if index, err := litestream.FindMaxIndexByGeneration(context.Background(), client, "0000000000000000"); err != nil {
			t.Fatal(err)
		} else if got, want := index, 0x00000001; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		}
	})

	t.Run("SnapshotLaterThanWAL", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "max-index", "snapshot-later-than-wal"))
		if index, err := litestream.FindMaxIndexByGeneration(context.Background(), client, "0000000000000000"); err != nil {
			t.Fatal(err)
		} else if got, want := index, 0x00000001; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		}
	})

	t.Run("ErrNoSnapshots", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "max-index", "no-snapshots"))

		_, err := litestream.FindMaxIndexByGeneration(context.Background(), client, "0000000000000000")
		if err != litestream.ErrNoSnapshots {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSnapshots", func(t *testing.T) {
		var client mock.ReplicaClient
		client.SnapshotsFunc = func(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
			return nil, fmt.Errorf("marker")
		}

		_, err := litestream.FindMaxIndexByGeneration(context.Background(), &client, "0000000000000000")
		if err == nil || err.Error() != `max snapshot index: snapshots: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrWALSegments", func(t *testing.T) {
		var client mock.ReplicaClient
		client.SnapshotsFunc = func(ctx context.Context, generation string) (litestream.SnapshotIterator, error) {
			return litestream.NewSnapshotInfoSliceIterator([]litestream.SnapshotInfo{{Index: 0x00000001}}), nil
		}
		client.WALSegmentsFunc = func(ctx context.Context, generation string) (litestream.WALSegmentIterator, error) {
			return nil, fmt.Errorf("marker")
		}

		_, err := litestream.FindMaxIndexByGeneration(context.Background(), &client, "0000000000000000")
		if err == nil || err.Error() != `max wal index: wal segments: marker` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestRestore(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "ok")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		if err := litestream.Restore(context.Background(), client, filepath.Join(tempDir, "db"), "0000000000000000", 0, 2, litestream.NewRestoreOptions()); err != nil {
			t.Fatal(err)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000002.db"), filepath.Join(tempDir, "db")) {
			t.Fatalf("file mismatch")
		}
	})

	t.Run("SnapshotOnly", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "snapshot-only")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		if err := litestream.Restore(context.Background(), client, filepath.Join(tempDir, "db"), "0000000000000000", 0, 0, litestream.NewRestoreOptions()); err != nil {
			t.Fatal(err)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000000.db"), filepath.Join(tempDir, "db")) {
			t.Fatalf("file mismatch")
		}
	})

	t.Run("DefaultParallelism", func(t *testing.T) {
		testDir := filepath.Join("testdata", "restore", "ok")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		opt := litestream.NewRestoreOptions()
		opt.Parallelism = 0
		if err := litestream.Restore(context.Background(), client, filepath.Join(tempDir, "db"), "0000000000000000", 0, 2, opt); err != nil {
			t.Fatal(err)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000002.db"), filepath.Join(tempDir, "db")) {
			t.Fatalf("file mismatch")
		}
	})

	t.Run("ErrPathRequired", func(t *testing.T) {
		var client mock.ReplicaClient
		if err := litestream.Restore(context.Background(), &client, "", "0000000000000000", 0, 0, litestream.NewRestoreOptions()); err == nil || err.Error() != `restore path required` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrGenerationRequired", func(t *testing.T) {
		var client mock.ReplicaClient
		if err := litestream.Restore(context.Background(), &client, t.TempDir(), "", 0, 0, litestream.NewRestoreOptions()); err == nil || err.Error() != `generation required` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrSnapshotIndexRequired", func(t *testing.T) {
		var client mock.ReplicaClient
		if err := litestream.Restore(context.Background(), &client, t.TempDir(), "0000000000000000", -1, 0, litestream.NewRestoreOptions()); err == nil || err.Error() != `snapshot index required` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrTargetIndexRequired", func(t *testing.T) {
		var client mock.ReplicaClient
		if err := litestream.Restore(context.Background(), &client, t.TempDir(), "0000000000000000", 0, -1, litestream.NewRestoreOptions()); err == nil || err.Error() != `target index required` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrPathExists", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "db")
		if err := os.WriteFile(filename, []byte("foo"), 0600); err != nil {
			t.Fatal(err)
		}
		var client mock.ReplicaClient
		if err := litestream.Restore(context.Background(), &client, filename, "0000000000000000", 0, 0, litestream.NewRestoreOptions()); err == nil || !strings.Contains(err.Error(), `cannot restore, output path already exists`) {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrPathPermissions", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0000); err != nil {
			t.Fatal(err)
		}

		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "restore", "bad-permissions"))
		if err := litestream.Restore(context.Background(), client, filepath.Join(dir, "db"), "0000000000000000", 0, 0, litestream.NewRestoreOptions()); err == nil || !strings.Contains(err.Error(), `permission denied`) {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}
