package file_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/pierrec/lz4/v4"
)

func TestReplicaClient_Path(t *testing.T) {
	c := file.NewReplicaClient("/foo/bar")
	if got, want := c.Path(), "/foo/bar"; got != want {
		t.Fatalf("Path()=%v, want %v", got, want)
	}
}

func TestReplicaClient_Type(t *testing.T) {
	if got, want := file.NewReplicaClient("").Type(), "file"; got != want {
		t.Fatalf("Type()=%v, want %v", got, want)
	}
}

func TestReplicaClient_GenerationsDir(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if got, err := file.NewReplicaClient("/foo").GenerationsDir(); err != nil {
			t.Fatal(err)
		} else if want := "/foo/generations"; got != want {
			t.Fatalf("GenerationsDir()=%v, want %v", got, want)
		}
	})
	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := file.NewReplicaClient("").GenerationsDir(); err == nil || err.Error() != `file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_GenerationDir(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if got, err := file.NewReplicaClient("/foo").GenerationDir("0123456701234567"); err != nil {
			t.Fatal(err)
		} else if want := "/foo/generations/0123456701234567"; got != want {
			t.Fatalf("GenerationDir()=%v, want %v", got, want)
		}
	})
	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := file.NewReplicaClient("").GenerationDir("0123456701234567"); err == nil || err.Error() != `file replica path required` {
			t.Fatalf("expected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := file.NewReplicaClient("/foo").GenerationDir(""); err == nil || err.Error() != `generation required` {
			t.Fatalf("expected error: %v", err)
		}
	})
}

func TestReplicaClient_SnapshotsDir(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if got, err := file.NewReplicaClient("/foo").SnapshotsDir("0123456701234567"); err != nil {
			t.Fatal(err)
		} else if want := "/foo/generations/0123456701234567/snapshots"; got != want {
			t.Fatalf("SnapshotsDir()=%v, want %v", got, want)
		}
	})
	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := file.NewReplicaClient("").SnapshotsDir("0123456701234567"); err == nil || err.Error() != `file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := file.NewReplicaClient("/foo").SnapshotsDir(""); err == nil || err.Error() != `generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_SnapshotPath(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if got, err := file.NewReplicaClient("/foo").SnapshotPath("0123456701234567", 1000); err != nil {
			t.Fatal(err)
		} else if want := "/foo/generations/0123456701234567/snapshots/000003e8.snapshot.lz4"; got != want {
			t.Fatalf("SnapshotPath()=%v, want %v", got, want)
		}
	})
	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := file.NewReplicaClient("").SnapshotPath("0123456701234567", 1000); err == nil || err.Error() != `file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := file.NewReplicaClient("/foo").SnapshotPath("", 1000); err == nil || err.Error() != `generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WALDir(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if got, err := file.NewReplicaClient("/foo").WALDir("0123456701234567"); err != nil {
			t.Fatal(err)
		} else if want := "/foo/generations/0123456701234567/wal"; got != want {
			t.Fatalf("WALDir()=%v, want %v", got, want)
		}
	})
	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := file.NewReplicaClient("").WALDir("0123456701234567"); err == nil || err.Error() != `file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := file.NewReplicaClient("/foo").WALDir(""); err == nil || err.Error() != `generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WALSegmentPath(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if got, err := file.NewReplicaClient("/foo").WALSegmentPath("0123456701234567", 1000, 1001); err != nil {
			t.Fatal(err)
		} else if want := "/foo/generations/0123456701234567/wal/000003e8_000003e9.wal.lz4"; got != want {
			t.Fatalf("WALPath()=%v, want %v", got, want)
		}
	})
	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := file.NewReplicaClient("").WALSegmentPath("0123456701234567", 1000, 0); err == nil || err.Error() != `file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := file.NewReplicaClient("/foo").WALSegmentPath("", 1000, 0); err == nil || err.Error() != `generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_Generations(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "generations", "5efbd8d042012dca"), 0777); err != nil {
			t.Fatal(err)
		} else if err := os.MkdirAll(filepath.Join(dir, "generations", "b16ddcf5c697540f"), 0777); err != nil {
			t.Fatal(err)
		} else if err := os.MkdirAll(filepath.Join(dir, "generations", "155fe292f8333c72"), 0777); err != nil {
			t.Fatal(err)
		}

		c := file.NewReplicaClient(dir)
		if got, err := c.Generations(context.Background()); err != nil {
			t.Fatal(err)
		} else if want := []string{"155fe292f8333c72", "5efbd8d042012dca", "b16ddcf5c697540f"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Generations()=%v, want %v", got, want)
		}
	})

	t.Run("WithInvalidEntries", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "generations", "5efbd8d042012dca"), 0777); err != nil {
			t.Fatal(err)
		} else if err := os.MkdirAll(filepath.Join(dir, "generations", "not_a_generation"), 0777); err != nil {
			t.Fatal(err)
		} else if err := ioutil.WriteFile(filepath.Join(dir, "generations", "0000000000000000"), nil, 0666); err != nil {
			t.Fatal(err)
		}

		c := file.NewReplicaClient(dir)
		if got, err := c.Generations(context.Background()); err != nil {
			t.Fatal(err)
		} else if want := []string{"5efbd8d042012dca"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("Generations()=%v, want %v", got, want)
		}
	})

	t.Run("NoGenerationsDir", func(t *testing.T) {
		c := file.NewReplicaClient(t.TempDir())
		if generations, err := c.Generations(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := len(generations), 0; got != want {
			t.Fatalf("len(Generations())=%v, want %v", got, want)
		}
	})

	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := file.NewReplicaClient("").Generations(context.Background()); err == nil || err.Error() != `cannot determine generations path: file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_Snapshots(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "generations", "5efbd8d042012dca", "snapshots"), 0777); err != nil {
			t.Fatal(err)
		} else if err := ioutil.WriteFile(filepath.Join(dir, "generations", "5efbd8d042012dca", "snapshots", "00000001.snapshot.lz4"), nil, 0666); err != nil {
			t.Fatal(err)
		}

		if err := os.MkdirAll(filepath.Join(dir, "generations", "b16ddcf5c697540f", "snapshots"), 0777); err != nil {
			t.Fatal(err)
		} else if err := ioutil.WriteFile(filepath.Join(dir, "generations", "b16ddcf5c697540f", "snapshots", "00000005.snapshot.lz4"), []byte("x"), 0666); err != nil {
			t.Fatal(err)
		} else if err := ioutil.WriteFile(filepath.Join(dir, "generations", "b16ddcf5c697540f", "snapshots", "0000000a.snapshot.lz4"), []byte("xyz"), 0666); err != nil {
			t.Fatal(err)
		} else if err := ioutil.WriteFile(filepath.Join(dir, "generations", "b16ddcf5c697540f", "snapshots", "not_a_snapshot.snapshot.lz4"), nil, 0666); err != nil {
			t.Fatal(err)
		}

		c := file.NewReplicaClient(dir)
		itr, err := c.Snapshots(context.Background(), "b16ddcf5c697540f")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		// Read all snapshots into a slice so they can be sorted.
		a, err := litestream.SliceSnapshotIterator(itr)
		if err != nil {
			t.Fatal(err)
		} else if got, want := len(a), 2; got != want {
			t.Fatalf("len=%v, want %v", got, want)
		}
		sort.Sort(litestream.SnapshotInfoSlice(a))

		// Verify first snapshot metadata.
		if got, want := a[0].Generation, "b16ddcf5c697540f"; got != want {
			t.Fatalf("Generation=%v, want %v", got, want)
		} else if got, want := a[0].Index, 5; got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		} else if got, want := a[0].Size, int64(1); got != want {
			t.Fatalf("Size=%v, want %v", got, want)
		} else if a[0].CreatedAt.IsZero() {
			t.Fatalf("expected CreatedAt")
		}

		// Verify second snapshot metadata.
		if got, want := a[1].Generation, "b16ddcf5c697540f"; got != want {
			t.Fatalf("Generation=%v, want %v", got, want)
		} else if got, want := a[1].Index, 0xA; got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		} else if got, want := a[1].Size, int64(3); got != want {
			t.Fatalf("Size=%v, want %v", got, want)
		} else if a[1].CreatedAt.IsZero() {
			t.Fatalf("expected CreatedAt")
		}

		// Ensure close is clean.
		if err := itr.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("NoGenerationDir", func(t *testing.T) {
		c := file.NewReplicaClient(t.TempDir())
		itr, err := c.Snapshots(context.Background(), "5efbd8d042012dca")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatal("expected no snapshots")
		}
	})

	t.Run("NoSnapshots", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "generations", "5efbd8d042012dca", "snapshots"), 0777); err != nil {
			t.Fatal(err)
		}

		c := file.NewReplicaClient(dir)
		itr, err := c.Snapshots(context.Background(), "5efbd8d042012dca")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatal("expected no snapshots")
		}
	})

	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := file.NewReplicaClient("").Snapshots(context.Background(), "b16ddcf5c697540f"); err == nil || err.Error() != `cannot determine snapshot directory: file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := file.NewReplicaClient(t.TempDir()).Snapshots(context.Background(), ""); err == nil || err.Error() != `cannot determine snapshot directory: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WriteSnapshot(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		data := []byte("foobar")

		c := file.NewReplicaClient(t.TempDir())
		if _, err := c.WriteSnapshot(context.Background(), "b16ddcf5c697540f", 1000, bytes.NewReader(compress(t, data))); err != nil {
			t.Fatal(err)
		}

		if r, err := c.SnapshotReader(context.Background(), "b16ddcf5c697540f", 1000); err != nil {
			t.Fatal(err)
		} else if buf, err := ioutil.ReadAll(lz4.NewReader(r)); err != nil {
			t.Fatal(err)
		} else if err := r.Close(); err != nil {
			t.Fatal(err)
		} else if got, want := buf, data; !bytes.Equal(got, want) {
			t.Fatalf("data=%q, want %q", got, want)
		}
	})

	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := file.NewReplicaClient("").WriteSnapshot(context.Background(), "b16ddcf5c697540f", 0, nil); err == nil || err.Error() != `cannot determine snapshot path: file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := file.NewReplicaClient(t.TempDir()).WriteSnapshot(context.Background(), "", 0, nil); err == nil || err.Error() != `cannot determine snapshot path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_SnapshotReader(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "generations", "5efbd8d042012dca", "snapshots"), 0777); err != nil {
			t.Fatal(err)
		} else if err := ioutil.WriteFile(filepath.Join(dir, "generations", "5efbd8d042012dca", "snapshots", "0000000a.snapshot.lz4"), compress(t, []byte("foo")), 0666); err != nil {
			t.Fatal(err)
		}

		c := file.NewReplicaClient(dir)
		r, err := c.SnapshotReader(context.Background(), "5efbd8d042012dca", 10)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if buf, err := ioutil.ReadAll(lz4.NewReader(r)); err != nil {
			t.Fatal(err)
		} else if got, want := buf, []byte("foo"); !bytes.Equal(got, want) {
			t.Fatalf("ReadAll=%v, want %v", got, want)
		}
	})

	t.Run("ErrNotFound", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "generations", "5efbd8d042012dca", "snapshots"), 0777); err != nil {
			t.Fatal(err)
		}

		c := file.NewReplicaClient(dir)
		if _, err := c.SnapshotReader(context.Background(), "5efbd8d042012dca", 1); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
	})

	t.Run("ErrNoPath", func(t *testing.T) {
		c := file.NewReplicaClient("")
		if _, err := c.SnapshotReader(context.Background(), "5efbd8d042012dca", 1); err == nil || err.Error() != `cannot determine snapshot path: file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ErrGeneration", func(t *testing.T) {
		dir := t.TempDir()
		c := file.NewReplicaClient(dir)
		if _, err := c.SnapshotReader(context.Background(), "", 1); err == nil || err.Error() != `cannot determine snapshot path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WALs(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "generations", "5efbd8d042012dca", "wal"), 0777); err != nil {
			t.Fatal(err)
		} else if err := ioutil.WriteFile(filepath.Join(dir, "generations", "5efbd8d042012dca", "wal", "00000001_00000000.wal.lz4"), nil, 0666); err != nil {
			t.Fatal(err)
		}

		if err := os.MkdirAll(filepath.Join(dir, "generations", "b16ddcf5c697540f", "wal"), 0777); err != nil {
			t.Fatal(err)
		} else if err := ioutil.WriteFile(filepath.Join(dir, "generations", "b16ddcf5c697540f", "wal", "00000002_00000000.wal.lz4"), []byte("12345"), 0666); err != nil {
			t.Fatal(err)
		} else if err := ioutil.WriteFile(filepath.Join(dir, "generations", "b16ddcf5c697540f", "wal", "00000002_00000005.wal.lz4"), []byte("67"), 0666); err != nil {
			t.Fatal(err)
		} else if err := ioutil.WriteFile(filepath.Join(dir, "generations", "b16ddcf5c697540f", "wal", "00000003_00000000.wal.lz4"), []byte("xyz"), 0666); err != nil {
			t.Fatal(err)
		}

		c := file.NewReplicaClient(dir)
		itr, err := c.WALSegments(context.Background(), "b16ddcf5c697540f")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		// Read all WAL segment files into a slice so they can be sorted.
		a, err := litestream.SliceWALSegmentIterator(itr)
		if err != nil {
			t.Fatal(err)
		} else if got, want := len(a), 3; got != want {
			t.Fatalf("len=%v, want %v", got, want)
		}
		sort.Sort(litestream.WALSegmentInfoSlice(a))

		// Verify first WAL segment metadata.
		if got, want := a[0].Generation, "b16ddcf5c697540f"; got != want {
			t.Fatalf("Generation=%v, want %v", got, want)
		} else if got, want := a[0].Index, 2; got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		} else if got, want := a[0].Offset, int64(0); got != want {
			t.Fatalf("Offset=%v, want %v", got, want)
		} else if got, want := a[0].Size, int64(5); got != want {
			t.Fatalf("Size=%v, want %v", got, want)
		} else if a[0].CreatedAt.IsZero() {
			t.Fatalf("expected CreatedAt")
		}

		// Verify first WAL segment metadata.
		if got, want := a[1].Generation, "b16ddcf5c697540f"; got != want {
			t.Fatalf("Generation=%v, want %v", got, want)
		} else if got, want := a[1].Index, 2; got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		} else if got, want := a[1].Offset, int64(5); got != want {
			t.Fatalf("Offset=%v, want %v", got, want)
		} else if got, want := a[1].Size, int64(2); got != want {
			t.Fatalf("Size=%v, want %v", got, want)
		} else if a[1].CreatedAt.IsZero() {
			t.Fatalf("expected CreatedAt")
		}

		// Verify third WAL segment metadata.
		if got, want := a[2].Generation, "b16ddcf5c697540f"; got != want {
			t.Fatalf("Generation=%v, want %v", got, want)
		} else if got, want := a[2].Index, 3; got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		} else if got, want := a[2].Offset, int64(0); got != want {
			t.Fatalf("Offset=%v, want %v", got, want)
		} else if got, want := a[2].Size, int64(3); got != want {
			t.Fatalf("Size=%v, want %v", got, want)
		} else if a[1].CreatedAt.IsZero() {
			t.Fatalf("expected CreatedAt")
		}

		// Ensure close is clean.
		if err := itr.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("NoGenerationDir", func(t *testing.T) {
		c := file.NewReplicaClient(t.TempDir())
		itr, err := c.WALSegments(context.Background(), "5efbd8d042012dca")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatal("expected no wal files")
		}
	})

	t.Run("NoWALs", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "generations", "5efbd8d042012dca", "wals"), 0777); err != nil {
			t.Fatal(err)
		}

		c := file.NewReplicaClient(dir)
		itr, err := c.WALSegments(context.Background(), "5efbd8d042012dca")
		if err != nil {
			t.Fatal(err)
		}
		defer itr.Close()

		if itr.Next() {
			t.Fatal("expected no wal files")
		}
	})

	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := file.NewReplicaClient("").WALSegments(context.Background(), "b16ddcf5c697540f"); err == nil || err.Error() != `cannot determine wal directory: file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := file.NewReplicaClient(t.TempDir()).WALSegments(context.Background(), ""); err == nil || err.Error() != `cannot determine wal directory: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WriteWALSegment(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		data := []byte("foobar")

		c := file.NewReplicaClient(t.TempDir())
		if _, err := c.WriteWALSegment(context.Background(), litestream.Pos{Generation: "b16ddcf5c697540f", Index: 1000, Offset: 2000}, bytes.NewReader(compress(t, data))); err != nil {
			t.Fatal(err)
		}

		if r, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: "b16ddcf5c697540f", Index: 1000, Offset: 2000}); err != nil {
			t.Fatal(err)
		} else if buf, err := ioutil.ReadAll(lz4.NewReader(r)); err != nil {
			t.Fatal(err)
		} else if err := r.Close(); err != nil {
			t.Fatal(err)
		} else if got, want := buf, data; !bytes.Equal(got, want) {
			t.Fatalf("data=%q, want %q", got, want)
		}
	})

	t.Run("ErrNoPath", func(t *testing.T) {
		if _, err := file.NewReplicaClient("").WriteWALSegment(context.Background(), litestream.Pos{Generation: "b16ddcf5c697540f", Index: 0, Offset: 0}, nil); err == nil || err.Error() != `cannot determine wal segment path: file replica path required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ErrNoGeneration", func(t *testing.T) {
		if _, err := file.NewReplicaClient(t.TempDir()).WriteWALSegment(context.Background(), litestream.Pos{Generation: "", Index: 0, Offset: 0}, nil); err == nil || err.Error() != `cannot determine wal segment path: generation required` {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestReplicaClient_WALReader(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "generations", "5efbd8d042012dca", "wal"), 0777); err != nil {
		t.Fatal(err)
	} else if err := ioutil.WriteFile(filepath.Join(dir, "generations", "5efbd8d042012dca", "wal", "0000000a_00000005.wal.lz4"), compress(t, []byte("foobar")), 0666); err != nil {
		t.Fatal(err)
	}

	c := file.NewReplicaClient(dir)

	t.Run("OK", func(t *testing.T) {
		r, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: "5efbd8d042012dca", Index: 10, Offset: 5})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		if buf, err := ioutil.ReadAll(lz4.NewReader(r)); err != nil {
			t.Fatal(err)
		} else if got, want := buf, []byte("foobar"); !bytes.Equal(got, want) {
			t.Fatalf("ReadAll=%v, want %v", got, want)
		}
	})

	t.Run("ErrNotFound", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "generations", "5efbd8d042012dca", "wal"), 0777); err != nil {
			t.Fatal(err)
		}

		c := file.NewReplicaClient(dir)
		if _, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: "5efbd8d042012dca", Index: 1, Offset: 0}); !os.IsNotExist(err) {
			t.Fatalf("expected not exist, got %#v", err)
		}
	})
}

/*
func TestReplica_Sync(t *testing.T) {
	// Ensure replica can successfully sync after DB has sync'd.
	t.Run("InitialSync", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		r := litestream.NewReplica(db, "", file.NewReplicaClient(t.TempDir()))
		r.MonitorEnabled = false
		db.Replicas = []*litestream.Replica{r}

		// Sync database & then sync replica.
		if err := db.Sync(context.Background()); err != nil {
			t.Fatal(err)
		} else if err := r.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Ensure posistions match.
		if want, err := db.Pos(); err != nil {
			t.Fatal(err)
		} else if got, err := r.Pos(context.Background()); err != nil {
			t.Fatal(err)
		} else if got != want {
			t.Fatalf("Pos()=%v, want %v", got, want)
		}
	})

	// Ensure replica can successfully sync multiple times.
	t.Run("MultiSync", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		r := litestream.NewReplica(db, "", file.NewReplicaClient(t.TempDir()))
		r.MonitorEnabled = false
		db.Replicas = []*litestream.Replica{r}

		if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
			t.Fatal(err)
		}

		// Write to the database multiple times and sync after each write.
		for i, n := 0, db.MinCheckpointPageN*2; i < n; i++ {
			if _, err := sqldb.Exec(`INSERT INTO foo (bar) VALUES ('baz')`); err != nil {
				t.Fatal(err)
			}

			// Sync periodically.
			if i%100 == 0 || i == n-1 {
				if err := db.Sync(context.Background()); err != nil {
					t.Fatal(err)
				} else if err := r.Sync(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		}

		// Ensure posistions match.
		pos, err := db.Pos()
		if err != nil {
			t.Fatal(err)
		} else if got, want := pos.Index, 2; got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		}

		if want, err := r.Pos(context.Background()); err != nil {
			t.Fatal(err)
		} else if got := pos; got != want {
			t.Fatalf("Pos()=%v, want %v", got, want)
		}
	})

	// Ensure replica returns an error if there is no generation available from the DB.
	t.Run("ErrNoGeneration", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)

		r := litestream.NewReplica(db, "", file.NewReplicaClient(t.TempDir()))
		r.MonitorEnabled = false
		db.Replicas = []*litestream.Replica{r}

		if err := r.Sync(context.Background()); err == nil || err.Error() != `no generation, waiting for data` {
			t.Fatal(err)
		}
	})
}
*/

// compress compresses b using LZ4.
func compress(tb testing.TB, b []byte) []byte {
	tb.Helper()

	var buf bytes.Buffer
	zw := lz4.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		tb.Fatal(err)
	} else if err := zw.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}
