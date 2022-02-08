package litestream_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
	"github.com/benbjohnson/litestream/mock"
)

// TestWALDownloader runs downloader tests against different levels of parallelism.
func TestWALDownloader(t *testing.T) {
	for _, parallelism := range []int{1, 8, 1024} {
		t.Run(fmt.Sprint(parallelism), func(t *testing.T) {
			testWALDownloader(t, parallelism)
		})
	}
}

func testWALDownloader(t *testing.T, parallelism int) {
	// Ensure WAL files can be downloaded from file replica on disk.
	t.Run("OK", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "ok")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 0, 2)
		defer d.Close()

		if index, filename, err := d.Next(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := index, 0; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000000.wal"), filename) {
			t.Fatalf("output file mismatch: %s", filename)
		}

		if index, filename, err := d.Next(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := index, 1; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000001.wal"), filename) {
			t.Fatalf("output file mismatch: %s", filename)
		}

		if index, filename, err := d.Next(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := index, 2; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000002.wal"), filename) {
			t.Fatalf("output file mismatch: %s", filename)
		}

		if _, _, err := d.Next(context.Background()); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		} else if got, want := d.N(), 3; got != want {
			t.Fatalf("N=%d, want %d", got, want)
		}

		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure a single WAL index can be downloaded.
	t.Run("One", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "one")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 0, 0)
		defer d.Close()

		if index, filename, err := d.Next(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := index, 0; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000000.wal"), filename) {
			t.Fatalf("output file mismatch: %s", filename)
		}

		if _, _, err := d.Next(context.Background()); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		} else if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure a subset of WAL indexes can be downloaded.
	t.Run("Slice", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "ok")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 1, 1)
		defer d.Close()

		if index, filename, err := d.Next(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := index, 1; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000001.wal"), filename) {
			t.Fatalf("output file mismatch: %s", filename)
		}

		if _, _, err := d.Next(context.Background()); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		} else if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure a subset of WAL indexes can be downloaded starting from zero.
	t.Run("SliceLeft", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "ok")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 0, 1)
		defer d.Close()

		if index, filename, err := d.Next(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := index, 0; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000000.wal"), filename) {
			t.Fatalf("output file mismatch: %s", filename)
		}

		if index, filename, err := d.Next(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := index, 1; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000001.wal"), filename) {
			t.Fatalf("output file mismatch: %s", filename)
		}

		if _, _, err := d.Next(context.Background()); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		} else if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure a subset of WAL indexes can be downloaded ending at the last index.
	t.Run("SliceRight", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "ok")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 1, 2)
		defer d.Close()

		if index, filename, err := d.Next(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := index, 1; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000001.wal"), filename) {
			t.Fatalf("output file mismatch: %s", filename)
		}

		if index, filename, err := d.Next(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := index, 2; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000002.wal"), filename) {
			t.Fatalf("output file mismatch: %s", filename)
		}

		if _, _, err := d.Next(context.Background()); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		} else if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure a large, generated set of WAL files can be downloaded in the correct order.
	t.Run("Large", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short mode, skipping")
		}

		// Generate WAL files.
		const n = 1000
		tempDir := t.TempDir()
		for i := 0; i < n; i++ {
			filename := filepath.Join(tempDir, "generations", "0000000000000000", "wal", litestream.FormatIndex(i), "0000000000000000.wal.lz4")
			if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil {
				t.Fatal(err)
			} else if err := os.WriteFile(filename, testingutil.CompressLZ4(t, []byte(fmt.Sprint(i))), 0666); err != nil {
				t.Fatal(err)
			}
		}

		client := litestream.NewFileReplicaClient(tempDir)
		d := litestream.NewWALDownloader(client, filepath.Join(t.TempDir(), "wal"), "0000000000000000", 0, n-1)
		d.Parallelism = parallelism
		defer d.Close()

		for i := 0; i < n; i++ {
			if index, filename, err := d.Next(context.Background()); err != nil {
				t.Fatal(err)
			} else if got, want := index, i; got != want {
				t.Fatalf("index[%d]=%d, want %d", i, got, want)
			} else if buf, err := os.ReadFile(filename); err != nil {
				t.Fatal(err)
			} else if got, want := fmt.Sprint(i), string(buf); got != want {
				t.Fatalf("file[%d]=%q, want %q", i, got, want)
			}
		}

		if _, _, err := d.Next(context.Background()); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		} else if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure a non-existent WAL directory returns error.
	t.Run("ErrEmptyGenerationDir", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "empty-generation-dir")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 0, 0)
		defer d.Close()

		var e *litestream.WALNotFoundError
		if _, _, err := d.Next(context.Background()); !errors.As(err, &e) {
			t.Fatalf("unexpected error type: %#v", err)
		} else if *e != (litestream.WALNotFoundError{Generation: "0000000000000000", Index: 0}) {
			t.Fatalf("unexpected error: %#v", err)
		} else if got, want := d.N(), 0; got != want {
			t.Fatalf("N=%d, want %d", got, want)
		}

		// Reinvoking Next() should return the same error.
		if _, _, err := d.Next(context.Background()); !errors.As(err, &e) {
			t.Fatalf("unexpected error type: %#v", err)
		} else if *e != (litestream.WALNotFoundError{Generation: "0000000000000000", Index: 0}) {
			t.Fatalf("unexpected error: %#v", err)
		}

		// Close should return the same error.
		if err := d.Close(); !errors.As(err, &e) {
			t.Fatalf("unexpected error type: %#v", err)
		} else if *e != (litestream.WALNotFoundError{Generation: "0000000000000000", Index: 0}) {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	// Ensure an empty WAL directory returns error.
	t.Run("EmptyWALDir", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "empty-wal-dir")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 0, 0)
		defer d.Close()

		var e *litestream.WALNotFoundError
		if _, _, err := d.Next(context.Background()); !errors.As(err, &e) {
			t.Fatalf("unexpected error type: %#v", err)
		} else if *e != (litestream.WALNotFoundError{Generation: "0000000000000000", Index: 0}) {
			t.Fatalf("unexpected error: %#v", err)
		} else if got, want := d.N(), 0; got != want {
			t.Fatalf("N=%d, want %d", got, want)
		}
	})

	// Ensure an empty WAL index directory returns EOF.
	t.Run("EmptyWALIndexDir", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "empty-wal-index-dir")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 0, 0)
		defer d.Close()

		var e *litestream.WALNotFoundError
		if _, _, err := d.Next(context.Background()); !errors.As(err, &e) {
			t.Fatalf("unexpected error type: %#v", err)
		} else if *e != (litestream.WALNotFoundError{Generation: "0000000000000000", Index: 0}) {
			t.Fatalf("unexpected error: %#v", err)
		} else if got, want := d.N(), 0; got != want {
			t.Fatalf("N=%d, want %d", got, want)
		}
	})

	// Ensure closing downloader before calling Next() does not panic.
	t.Run("CloseWithoutNext", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(t.TempDir())
		d := litestream.NewWALDownloader(client, filepath.Join(t.TempDir(), "wal"), "0000000000000000", 0, 2)
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure downloader closes successfully if invoked after Next() but before last index.
	t.Run("CloseEarly", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "ok")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 0, 2)
		defer d.Close()

		if index, filename, err := d.Next(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := index, 0; got != want {
			t.Fatalf("index=%d, want %d", got, want)
		} else if !fileEqual(t, filepath.Join(testDir, "0000000000000000.wal"), filename) {
			t.Fatalf("output file mismatch: %s", filename)
		}

		if err := d.Close(); err != nil {
			t.Fatal(err)
		}

		if _, _, err := d.Next(context.Background()); err == nil {
			t.Fatal("expected error")
		}
	})

	// Ensure downloader without a minimum index returns an error.
	t.Run("ErrMinIndexRequired", func(t *testing.T) {
		d := litestream.NewWALDownloader(litestream.NewFileReplicaClient(t.TempDir()), t.TempDir(), "0000000000000000", -1, 2)
		defer d.Close()
		if _, _, err := d.Next(context.Background()); err == nil || err.Error() != `minimum index required` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	// Ensure downloader without a maximum index returns an error.
	t.Run("ErrMinIndexRequired", func(t *testing.T) {
		d := litestream.NewWALDownloader(litestream.NewFileReplicaClient(t.TempDir()), t.TempDir(), "0000000000000000", 1, -1)
		defer d.Close()
		if _, _, err := d.Next(context.Background()); err == nil || err.Error() != `maximum index required` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	// Ensure downloader with invalid min/max indexes returns an error.
	t.Run("ErrMinIndexTooLarge", func(t *testing.T) {
		d := litestream.NewWALDownloader(litestream.NewFileReplicaClient(t.TempDir()), t.TempDir(), "0000000000000000", 2, 1)
		defer d.Close()
		if _, _, err := d.Next(context.Background()); err == nil || err.Error() != `minimum index cannot be larger than maximum index` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	// Ensure downloader returns error if parallelism field is invalid.
	t.Run("ErrParallelismRequired", func(t *testing.T) {
		d := litestream.NewWALDownloader(litestream.NewFileReplicaClient(t.TempDir()), t.TempDir(), "0000000000000000", 0, 0)
		d.Parallelism = -1
		defer d.Close()
		if _, _, err := d.Next(context.Background()); err == nil || err.Error() != `parallelism must be at least one` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	// Ensure a missing index at the beginning returns an error.
	t.Run("ErrMissingInitialIndex", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "missing-initial-index")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 0, 2)
		defer d.Close()

		var e *litestream.WALNotFoundError
		if _, _, err := d.Next(context.Background()); !errors.As(err, &e) {
			t.Fatalf("unexpected error type: %#v", err)
		} else if *e != (litestream.WALNotFoundError{Generation: "0000000000000000", Index: 0}) {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	// Ensure a gap in indicies returns an error.
	t.Run("ErrMissingMiddleIndex", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "missing-middle-index")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 0, 2)
		defer d.Close()

		var e *litestream.WALNotFoundError
		if _, _, err := d.Next(context.Background()); !errors.As(err, &e) {
			t.Fatalf("unexpected error type: %#v", err)
		} else if *e != (litestream.WALNotFoundError{Generation: "0000000000000000", Index: 1}) {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	// Ensure a missing index at the end returns an error.
	t.Run("ErrMissingEndingIndex", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "missing-ending-index")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 0, 2)
		defer d.Close()

		var e *litestream.WALNotFoundError
		if _, _, err := d.Next(context.Background()); !errors.As(err, &e) {
			t.Fatalf("unexpected error type: %#v", err)
		} else if *e != (litestream.WALNotFoundError{Generation: "0000000000000000", Index: 2}) {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	// Ensure downloader returns error WAL segment iterator creation returns error.
	t.Run("ErrWALSegments", func(t *testing.T) {
		var client mock.ReplicaClient
		client.WALSegmentsFunc = func(ctx context.Context, generation string) (litestream.WALSegmentIterator, error) {
			return nil, errors.New("marker")
		}

		d := litestream.NewWALDownloader(&client, filepath.Join(t.TempDir(), "wal"), "0000000000000000", 0, 2)
		defer d.Close()

		if _, _, err := d.Next(context.Background()); err == nil || err.Error() != `wal segments: marker` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	// Ensure downloader returns error if WAL segments have a gap in offsets.
	t.Run("ErrMissingOffset", func(t *testing.T) {
		testDir := filepath.Join("testdata", "wal-downloader", "missing-offset")
		tempDir := t.TempDir()

		client := litestream.NewFileReplicaClient(testDir)
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "wal"), "0000000000000000", 0, 0)
		defer d.Close()

		if _, _, err := d.Next(context.Background()); err == nil || err.Error() != `missing WAL offset: generation=0000000000000000 index=0000000000000000 offset=0000000000002050` {
			t.Fatal(err)
		} else if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure downloader returns error if context is canceled.
	t.Run("ErrContextCanceled", func(t *testing.T) {
		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "wal-downloader", "ok"))
		d := litestream.NewWALDownloader(client, filepath.Join(t.TempDir(), "wal"), "0000000000000000", 0, 2)
		defer d.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if _, _, err := d.Next(ctx); err != context.Canceled {
			t.Fatal(err)
		} else if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure downloader returns error if error occurs while writing WAL to disk.
	t.Run("ErrWriteWAL", func(t *testing.T) {
		// Create a subdirectory that is not writable.
		tempDir := t.TempDir()
		if err := os.Mkdir(filepath.Join(tempDir, "nowrite"), 0000); err != nil {
			t.Fatal(err)
		}

		client := litestream.NewFileReplicaClient(filepath.Join("testdata", "wal-downloader", "err-write-wal"))
		d := litestream.NewWALDownloader(client, filepath.Join(tempDir, "nowrite", "wal"), "0000000000000000", 0, 0)
		defer d.Close()

		if _, _, err := d.Next(context.Background()); err == nil || !strings.Contains(err.Error(), `permission denied`) {
			t.Fatal(err)
		} else if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure downloader returns error if error occurs while downloading WAL.
	t.Run("ErrDownloadWAL", func(t *testing.T) {
		fileClient := litestream.NewFileReplicaClient(filepath.Join("testdata", "wal-downloader", "err-download-wal"))

		var client mock.ReplicaClient
		client.WALSegmentsFunc = fileClient.WALSegments
		client.WALSegmentReaderFunc = func(ctx context.Context, pos litestream.Pos) (io.ReadCloser, error) {
			return nil, fmt.Errorf("marker")
		}

		d := litestream.NewWALDownloader(&client, filepath.Join(t.TempDir(), "wal"), "0000000000000000", 0, 0)
		defer d.Close()

		if _, _, err := d.Next(context.Background()); err == nil || err.Error() != `read WAL segment: marker` {
			t.Fatalf("unexpected error: %#v", err)
		} else if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure downloader returns error if reading the segment fails.
	t.Run("ErrReadWALSegment", func(t *testing.T) {
		fileClient := litestream.NewFileReplicaClient(filepath.Join("testdata", "wal-downloader", "err-read-wal-segment"))

		var client mock.ReplicaClient
		client.WALSegmentsFunc = fileClient.WALSegments
		client.WALSegmentReaderFunc = func(ctx context.Context, pos litestream.Pos) (io.ReadCloser, error) {
			var rc mock.ReadCloser
			rc.ReadFunc = func([]byte) (int, error) { return 0, errors.New("marker") }
			rc.CloseFunc = func() error { return nil }
			return &rc, nil
		}

		d := litestream.NewWALDownloader(&client, filepath.Join(t.TempDir(), "wal"), "0000000000000000", 0, 0)
		defer d.Close()

		if _, _, err := d.Next(context.Background()); err == nil || err.Error() != `copy WAL segment: marker` {
			t.Fatalf("unexpected error: %#v", err)
		} else if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestWALNotFoundError(t *testing.T) {
	err := &litestream.WALNotFoundError{Generation: "0123456789abcdef", Index: 1000}
	if got, want := err.Error(), `wal not found: generation=0123456789abcdef index=00000000000003e8`; got != want {
		t.Fatalf("Error()=%q, want %q", got, want)
	}
}
