package litestream_test

import (
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
