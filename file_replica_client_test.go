package litestream_test

import (
	"reflect"
	"testing"

	"github.com/benbjohnson/litestream"
)

func TestReplicaClient_Path(t *testing.T) {
	c := litestream.NewFileReplicaClient("/foo/bar")
	if got, want := c.Path(), "/foo/bar"; got != want {
		t.Fatalf("Path()=%v, want %v", got, want)
	}
}

func TestReplicaClient_Type(t *testing.T) {
	if got, want := litestream.NewFileReplicaClient("").Type(), "file"; got != want {
		t.Fatalf("Type()=%v, want %v", got, want)
	}
}

func TestReplicaClient_GenerationsDir(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if got, err := litestream.NewFileReplicaClient("/foo").GenerationsDir(); err != nil {
			t.Fatal(err)
		} else if want := "/foo/generations"; got != want {
			t.Fatalf("GenerationsDir()=%v, want %v", got, want)
		}
	})
	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := litestream.NewFileReplicaClient("").GenerationsDir(); err == nil || err.Error() != `file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_GenerationDir(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if got, err := litestream.NewFileReplicaClient("/foo").GenerationDir("0123456701234567"); err != nil {
			t.Fatal(err)
		} else if want := "/foo/generations/0123456701234567"; got != want {
			t.Fatalf("GenerationDir()=%v, want %v", got, want)
		}
	})
	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := litestream.NewFileReplicaClient("").GenerationDir("0123456701234567"); err == nil || err.Error() != `file replica path required` {
			t.Fatalf("expected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := litestream.NewFileReplicaClient("/foo").GenerationDir(""); err == nil || err.Error() != `generation required` {
			t.Fatalf("expected error: %v", err)
		}
	})
}

func TestReplicaClient_SnapshotsDir(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if got, err := litestream.NewFileReplicaClient("/foo").SnapshotsDir("0123456701234567"); err != nil {
			t.Fatal(err)
		} else if want := "/foo/generations/0123456701234567/snapshots"; got != want {
			t.Fatalf("SnapshotsDir()=%v, want %v", got, want)
		}
	})
	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := litestream.NewFileReplicaClient("").SnapshotsDir("0123456701234567"); err == nil || err.Error() != `file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := litestream.NewFileReplicaClient("/foo").SnapshotsDir(""); err == nil || err.Error() != `generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_SnapshotPath(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if got, err := litestream.NewFileReplicaClient("/foo").SnapshotPath("0123456701234567", 1000); err != nil {
			t.Fatal(err)
		} else if want := "/foo/generations/0123456701234567/snapshots/00000000000003e8.snapshot.lz4"; got != want {
			t.Fatalf("SnapshotPath()=%v, want %v", got, want)
		}
	})
	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := litestream.NewFileReplicaClient("").SnapshotPath("0123456701234567", 1000); err == nil || err.Error() != `file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := litestream.NewFileReplicaClient("/foo").SnapshotPath("", 1000); err == nil || err.Error() != `generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WALDir(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if got, err := litestream.NewFileReplicaClient("/foo").WALDir("0123456701234567"); err != nil {
			t.Fatal(err)
		} else if want := "/foo/generations/0123456701234567/wal"; got != want {
			t.Fatalf("WALDir()=%v, want %v", got, want)
		}
	})
	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := litestream.NewFileReplicaClient("").WALDir("0123456701234567"); err == nil || err.Error() != `file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := litestream.NewFileReplicaClient("/foo").WALDir(""); err == nil || err.Error() != `generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WALSegmentPath(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if got, err := litestream.NewFileReplicaClient("/foo").WALSegmentPath("0123456701234567", 1000, 1001); err != nil {
			t.Fatal(err)
		} else if want := "/foo/generations/0123456701234567/wal/00000000000003e8/00000000000003e9.wal.lz4"; got != want {
			t.Fatalf("WALPath()=%v, want %v", got, want)
		}
	})
	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := litestream.NewFileReplicaClient("").WALSegmentPath("0123456701234567", 1000, 0); err == nil || err.Error() != `file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := litestream.NewFileReplicaClient("/foo").WALSegmentPath("", 1000, 0); err == nil || err.Error() != `generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestFileWALSegmentIterator_Append(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		itr := litestream.NewFileWALSegmentIterator(t.TempDir(), "0123456789abcdef", nil)
		if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 0}); err != nil {
			t.Fatal(err)
		}

		select {
		case <-itr.NotifyCh():
		default:
			t.Fatal("expected notification")
		}

		if !itr.Next() {
			t.Fatal("expected next")
		} else if got, want := itr.WALSegment(), (litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 0}); got != want {
			t.Fatalf("info=%#v, want %#v", got, want)
		}
	})

	t.Run("MultiOffset", func(t *testing.T) {
		itr := litestream.NewFileWALSegmentIterator(t.TempDir(), "0123456789abcdef", nil)
		if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 0}); err != nil {
			t.Fatal(err)
		} else if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 1}); err != nil {
			t.Fatal(err)
		}

		select {
		case <-itr.NotifyCh():
		default:
			t.Fatal("expected notification")
		}

		if !itr.Next() {
			t.Fatal("expected next")
		} else if got, want := itr.WALSegment(), (litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 0}); got != want {
			t.Fatalf("info=%#v, want %#v", got, want)
		}

		if !itr.Next() {
			t.Fatal("expected next")
		} else if got, want := itr.WALSegment(), (litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 1}); got != want {
			t.Fatalf("info=%#v, want %#v", got, want)
		}
	})

	t.Run("MultiIndex", func(t *testing.T) {
		itr := litestream.NewFileWALSegmentIterator(t.TempDir(), "0123456789abcdef", nil)
		if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 0}); err != nil {
			t.Fatal(err)
		} else if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 1, Offset: 0}); err != nil {
			t.Fatal(err)
		} else if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 1, Offset: 1}); err != nil {
			t.Fatal(err)
		} else if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 2, Offset: 0}); err != nil {
			t.Fatal(err)
		}

		if got, want := itr.Indexes(), []int{1, 2}; !reflect.DeepEqual(got, want) {
			t.Fatalf("indexes=%v, want %v", got, want)
		}
	})

	t.Run("ErrGenerationMismatch", func(t *testing.T) {
		itr := litestream.NewFileWALSegmentIterator(t.TempDir(), "0000000000000000", nil)
		if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 0}); err == nil || err.Error() != `generation mismatch` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrBelowMaxIndex", func(t *testing.T) {
		itr := litestream.NewFileWALSegmentIterator(t.TempDir(), "0123456789abcdef", nil)
		if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 0}); err != nil {
			t.Fatal(err)
		} else if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 1, Offset: 0}); err != nil {
			t.Fatal(err)
		} else if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 0}); err == nil || err.Error() != `appended index "0000000000000000" below max index "0000000000000001"` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrAboveMaxIndex", func(t *testing.T) {
		itr := litestream.NewFileWALSegmentIterator(t.TempDir(), "0123456789abcdef", nil)
		if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 0}); err != nil {
			t.Fatal(err)
		} else if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 1, Offset: 0}); err != nil {
			t.Fatal(err)
		} else if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 3, Offset: 0}); err == nil || err.Error() != `appended index "0000000000000003" skips index "0000000000000002"` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrBelowCurrentIndex", func(t *testing.T) {
		itr := litestream.NewFileWALSegmentIterator(t.TempDir(), "0123456789abcdef", nil)
		if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 1, Offset: 0}); err != nil {
			t.Fatal(err)
		} else if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 0}); err == nil || err.Error() != `appended index "0000000000000000" below current index "0000000000000001"` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrSkipsNextIndex", func(t *testing.T) {
		itr := litestream.NewFileWALSegmentIterator(t.TempDir(), "0123456789abcdef", nil)
		if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 0}); err != nil {
			t.Fatal(err)
		} else if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 2, Offset: 0}); err == nil || err.Error() != `appended index "0000000000000002" skips next index "0000000000000001"` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrBelowOffset", func(t *testing.T) {
		itr := litestream.NewFileWALSegmentIterator(t.TempDir(), "0123456789abcdef", nil)
		if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 5}); err != nil {
			t.Fatal(err)
		} else if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 4}); err == nil || err.Error() != `appended offset 0000000000000000/0000000000000004 before last offset 0000000000000000/0000000000000005` {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrDuplicateOffset", func(t *testing.T) {
		itr := litestream.NewFileWALSegmentIterator(t.TempDir(), "0123456789abcdef", nil)
		if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 5}); err != nil {
			t.Fatal(err)
		} else if err := itr.Append(litestream.WALSegmentInfo{Generation: "0123456789abcdef", Index: 0, Offset: 5}); err == nil || err.Error() != `duplicate offset 0000000000000000/0000000000000005 appended` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}
