package file_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream/file"
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

// TestReplicaClient_WriteLTXFile_ErrorCleanup verifies temp files are cleaned up on errors
func TestReplicaClient_WriteLTXFile_ErrorCleanup(t *testing.T) {
	t.Run("DiskFull", func(t *testing.T) {
		tmpDir := t.TempDir()
		c := file.NewReplicaClient(tmpDir)

		// Create a reader that fails after 50 bytes to simulate disk full
		failReader := &failAfterReader{
			data: createLTXHeader(1, 2),
			n:    50,
			err:  fmt.Errorf("no space left on device"),
		}

		_, err := c.WriteLTXFile(context.Background(), 0, 1, 2, failReader)
		if err == nil {
			t.Fatal("expected error from failReader")
		}
		if !strings.Contains(err.Error(), "no space left on device") {
			t.Fatalf("expected disk full error, got: %v", err)
		}

		// Verify no .tmp files remain
		tmpFiles := findTmpFiles(t, tmpDir)
		if len(tmpFiles) > 0 {
			t.Fatalf("found %d .tmp files after error: %v", len(tmpFiles), tmpFiles)
		}
	})

	t.Run("SuccessNoLeaks", func(t *testing.T) {
		tmpDir := t.TempDir()
		c := file.NewReplicaClient(tmpDir)

		ltxData := createLTXData(1, 2, []byte("test data"))
		info, err := c.WriteLTXFile(context.Background(), 0, 1, 2, bytes.NewReader(ltxData))
		if err != nil {
			t.Fatal(err)
		}
		if info == nil {
			t.Fatal("expected FileInfo")
		}

		// Verify no .tmp files remain
		tmpFiles := findTmpFiles(t, tmpDir)
		if len(tmpFiles) > 0 {
			t.Fatalf("found %d .tmp files after successful write: %v", len(tmpFiles), tmpFiles)
		}

		// Verify final file exists
		finalPath := c.LTXFilePath(0, 1, 2)
		if _, err := os.Stat(finalPath); err != nil {
			t.Fatalf("final file missing: %v", err)
		}
	})

	t.Run("MultipleErrors", func(t *testing.T) {
		tmpDir := t.TempDir()
		c := file.NewReplicaClient(tmpDir)

		// Simulate multiple failed writes
		for i := 0; i < 5; i++ {
			failReader := &failAfterReader{
				data: createLTXHeader(ltx.TXID(i+1), ltx.TXID(i+1)),
				n:    30,
				err:  fmt.Errorf("write error %d", i),
			}

			_, err := c.WriteLTXFile(context.Background(), 0, ltx.TXID(i+1), ltx.TXID(i+1), failReader)
			if err == nil {
				t.Fatalf("iteration %d: expected error from failReader", i)
			}
		}

		// Verify no .tmp files accumulated
		tmpFiles := findTmpFiles(t, tmpDir)
		if len(tmpFiles) > 0 {
			t.Fatalf("found %d .tmp files after multiple errors: %v", len(tmpFiles), tmpFiles)
		}
	})
}

// failAfterReader simulates io.Copy failure after reading n bytes
type failAfterReader struct {
	data []byte
	n    int // fail after n bytes
	pos  int
	err  error
}

func (r *failAfterReader) Read(p []byte) (n int, err error) {
	if r.pos >= r.n {
		return 0, r.err
	}
	remaining := r.n - r.pos
	toRead := len(p)
	if toRead > remaining {
		toRead = remaining
	}
	if toRead > len(r.data)-r.pos {
		toRead = len(r.data) - r.pos
	}
	if toRead == 0 {
		return 0, r.err
	}
	n = copy(p, r.data[r.pos:r.pos+toRead])
	r.pos += n
	return n, nil
}

// findTmpFiles recursively finds all .tmp files in the directory
func findTmpFiles(t *testing.T, root string) []string {
	t.Helper()
	var tmpFiles []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && strings.HasSuffix(path, ".tmp") {
			tmpFiles = append(tmpFiles, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk error: %v", err)
	}
	return tmpFiles
}

// createLTXData creates a minimal valid LTX file with a header for testing
func createLTXData(minTXID, maxTXID ltx.TXID, data []byte) []byte {
	hdr := ltx.Header{
		Version:   ltx.Version,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: time.Now().UnixMilli(),
	}
	if minTXID == 1 {
		hdr.PreApplyChecksum = 0
	} else {
		hdr.PreApplyChecksum = ltx.ChecksumFlag
	}
	headerBytes, _ := hdr.MarshalBinary()
	return append(headerBytes, data...)
}

// createLTXHeader creates minimal LTX header for testing
func createLTXHeader(minTXID, maxTXID ltx.TXID) []byte {
	return createLTXData(minTXID, maxTXID, nil)
}

func TestReplicaClient_GenerationsV3(t *testing.T) {
	t.Run("NoGenerationsDir", func(t *testing.T) {
		tmpDir := t.TempDir()
		c := file.NewReplicaClient(tmpDir)

		gens, err := c.GenerationsV3(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(gens) != 0 {
			t.Errorf("expected empty slice, got %v", gens)
		}
	})

	t.Run("EmptyGenerationsDir", func(t *testing.T) {
		tmpDir := t.TempDir()
		if err := os.Mkdir(filepath.Join(tmpDir, "generations"), 0755); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		gens, err := c.GenerationsV3(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(gens) != 0 {
			t.Errorf("expected empty slice, got %v", gens)
		}
	})

	t.Run("InvalidGenerationIDsOnly", func(t *testing.T) {
		tmpDir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations", "not-valid"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations", "also-invalid"), 0755); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		gens, err := c.GenerationsV3(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(gens) != 0 {
			t.Errorf("expected empty slice for invalid IDs, got %v", gens)
		}
	})

	t.Run("SingleGeneration", func(t *testing.T) {
		tmpDir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations", "0123456789abcdef"), 0755); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		gens, err := c.GenerationsV3(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(gens) != 1 || gens[0] != "0123456789abcdef" {
			t.Errorf("expected [0123456789abcdef], got %v", gens)
		}
	})

	t.Run("MultipleGenerationsSorted", func(t *testing.T) {
		tmpDir := t.TempDir()
		// Create in non-sorted order
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations", "ffffffffffffffff"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations", "0000000000000000"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations", "abcdef0123456789"), 0755); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		gens, err := c.GenerationsV3(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"0000000000000000", "abcdef0123456789", "ffffffffffffffff"}
		if !reflect.DeepEqual(gens, want) {
			t.Errorf("expected %v, got %v", want, gens)
		}
	})

	t.Run("MixedValidInvalid", func(t *testing.T) {
		tmpDir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations", "invalid"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations", "abcdef0123456789"), 0755); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		gens, err := c.GenerationsV3(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(gens) != 1 || gens[0] != "abcdef0123456789" {
			t.Errorf("expected [abcdef0123456789], got %v", gens)
		}
	})
}

func TestReplicaClient_SnapshotsV3(t *testing.T) {
	t.Run("NoSnapshotsDir", func(t *testing.T) {
		tmpDir := t.TempDir()
		c := file.NewReplicaClient(tmpDir)

		snapshots, err := c.SnapshotsV3(context.Background(), "0123456789abcdef")
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) != 0 {
			t.Errorf("expected empty slice, got %v", snapshots)
		}
	})

	t.Run("EmptySnapshotsDir", func(t *testing.T) {
		tmpDir := t.TempDir()
		gen := "0123456789abcdef"
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations", gen, "snapshots"), 0755); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		snapshots, err := c.SnapshotsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) != 0 {
			t.Errorf("expected empty slice, got %v", snapshots)
		}
	})

	t.Run("SingleSnapshot", func(t *testing.T) {
		tmpDir := t.TempDir()
		gen := "0123456789abcdef"
		snapshotsDir := filepath.Join(tmpDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(snapshotsDir, "00000001.snapshot.lz4"), []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		snapshots, err := c.SnapshotsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) != 1 {
			t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
		}
		if snapshots[0].Generation != gen || snapshots[0].Index != 1 || snapshots[0].Size != 4 {
			t.Errorf("unexpected snapshot: %+v", snapshots[0])
		}
	})

	t.Run("MultipleSnapshotsSorted", func(t *testing.T) {
		tmpDir := t.TempDir()
		gen := "0123456789abcdef"
		snapshotsDir := filepath.Join(tmpDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Create in non-sorted order
		if err := os.WriteFile(filepath.Join(snapshotsDir, "00000003.snapshot.lz4"), []byte("c"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(snapshotsDir, "00000001.snapshot.lz4"), []byte("a"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(snapshotsDir, "00000002.snapshot.lz4"), []byte("bb"), 0644); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		snapshots, err := c.SnapshotsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) != 3 {
			t.Fatalf("expected 3 snapshots, got %d", len(snapshots))
		}
		if snapshots[0].Index != 1 || snapshots[1].Index != 2 || snapshots[2].Index != 3 {
			t.Errorf("snapshots not sorted by index: %+v", snapshots)
		}
	})

	t.Run("SkipsInvalidFilenames", func(t *testing.T) {
		tmpDir := t.TempDir()
		gen := "0123456789abcdef"
		snapshotsDir := filepath.Join(tmpDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(snapshotsDir, "00000001.snapshot.lz4"), []byte("a"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(snapshotsDir, "invalid.txt"), []byte("b"), 0644); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		snapshots, err := c.SnapshotsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) != 1 || snapshots[0].Index != 1 {
			t.Errorf("expected single valid snapshot, got %+v", snapshots)
		}
	})
}

func TestReplicaClient_WALSegmentsV3(t *testing.T) {
	t.Run("NoWALDir", func(t *testing.T) {
		tmpDir := t.TempDir()
		c := file.NewReplicaClient(tmpDir)

		segments, err := c.WALSegmentsV3(context.Background(), "0123456789abcdef")
		if err != nil {
			t.Fatal(err)
		}
		if len(segments) != 0 {
			t.Errorf("expected empty slice, got %v", segments)
		}
	})

	t.Run("EmptyWALDir", func(t *testing.T) {
		tmpDir := t.TempDir()
		gen := "0123456789abcdef"
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations", gen, "wal"), 0755); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		segments, err := c.WALSegmentsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(segments) != 0 {
			t.Errorf("expected empty slice, got %v", segments)
		}
	})

	t.Run("SingleSegment", func(t *testing.T) {
		tmpDir := t.TempDir()
		gen := "0123456789abcdef"
		walDir := filepath.Join(tmpDir, "generations", gen, "wal")
		if err := os.MkdirAll(walDir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(walDir, "00000001-0000000000001000.wal.lz4"), []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		segments, err := c.WALSegmentsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(segments) != 1 {
			t.Fatalf("expected 1 segment, got %d", len(segments))
		}
		if segments[0].Generation != gen || segments[0].Index != 1 || segments[0].Offset != 4096 || segments[0].Size != 4 {
			t.Errorf("unexpected segment: %+v", segments[0])
		}
	})

	t.Run("MultipleSortedByIndexThenOffset", func(t *testing.T) {
		tmpDir := t.TempDir()
		gen := "0123456789abcdef"
		walDir := filepath.Join(tmpDir, "generations", gen, "wal")
		if err := os.MkdirAll(walDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Create in non-sorted order
		if err := os.WriteFile(filepath.Join(walDir, "00000002-0000000000000000.wal.lz4"), []byte("d"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(walDir, "00000001-0000000000002000.wal.lz4"), []byte("b"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(walDir, "00000001-0000000000001000.wal.lz4"), []byte("a"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(walDir, "00000001-0000000000003000.wal.lz4"), []byte("c"), 0644); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		segments, err := c.WALSegmentsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(segments) != 4 {
			t.Fatalf("expected 4 segments, got %d", len(segments))
		}
		// Should be sorted by index, then offset
		expected := []struct {
			index  int
			offset int64
		}{
			{1, 0x1000},
			{1, 0x2000},
			{1, 0x3000},
			{2, 0x0000},
		}
		for i, exp := range expected {
			if segments[i].Index != exp.index || segments[i].Offset != exp.offset {
				t.Errorf("segment %d: expected (index=%d, offset=%x), got (index=%d, offset=%x)",
					i, exp.index, exp.offset, segments[i].Index, segments[i].Offset)
			}
		}
	})

	t.Run("SkipsInvalidFilenames", func(t *testing.T) {
		tmpDir := t.TempDir()
		gen := "0123456789abcdef"
		walDir := filepath.Join(tmpDir, "generations", gen, "wal")
		if err := os.MkdirAll(walDir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(walDir, "00000001-0000000000001000.wal.lz4"), []byte("a"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(walDir, "invalid.wal"), []byte("b"), 0644); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		segments, err := c.WALSegmentsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(segments) != 1 || segments[0].Index != 1 {
			t.Errorf("expected single valid segment, got %+v", segments)
		}
	})
}

func TestReplicaClient_OpenSnapshotV3(t *testing.T) {
	t.Run("NotFound", func(t *testing.T) {
		tmpDir := t.TempDir()
		c := file.NewReplicaClient(tmpDir)

		_, err := c.OpenSnapshotV3(context.Background(), "0123456789abcdef", 0)
		if !os.IsNotExist(err) {
			t.Errorf("expected not exist error, got %v", err)
		}
	})

	t.Run("ReadDecompressed", func(t *testing.T) {
		tmpDir := t.TempDir()
		gen := "0123456789abcdef"
		snapshotsDir := filepath.Join(tmpDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}

		// Create LZ4-compressed test data
		original := []byte("test snapshot data for decompression")
		var buf bytes.Buffer
		w := lz4.NewWriter(&buf)
		if _, err := w.Write(original); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// Write compressed file
		path := filepath.Join(snapshotsDir, "00000000.snapshot.lz4")
		if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
			t.Fatal(err)
		}

		c := file.NewReplicaClient(tmpDir)
		r, err := c.OpenSnapshotV3(context.Background(), gen, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, original) {
			t.Errorf("decompressed data mismatch: got %q, want %q", data, original)
		}
	})
}

func TestReplicaClient_OpenWALSegmentV3(t *testing.T) {
	t.Run("NotFound", func(t *testing.T) {
		tmpDir := t.TempDir()
		c := file.NewReplicaClient(tmpDir)

		_, err := c.OpenWALSegmentV3(context.Background(), "0123456789abcdef", 0, 0)
		if !os.IsNotExist(err) {
			t.Errorf("expected not exist error, got %v", err)
		}
	})

	t.Run("ReadDecompressed", func(t *testing.T) {
		tmpDir := t.TempDir()
		gen := "0123456789abcdef"
		walDir := filepath.Join(tmpDir, "generations", gen, "wal")
		if err := os.MkdirAll(walDir, 0755); err != nil {
			t.Fatal(err)
		}

		// Create LZ4-compressed test data
		original := []byte("test WAL segment data")
		var buf bytes.Buffer
		w := lz4.NewWriter(&buf)
		if _, err := w.Write(original); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// Write compressed file
		path := filepath.Join(walDir, "00000001-0000000000001000.wal.lz4")
		if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
			t.Fatal(err)
		}

		c := file.NewReplicaClient(tmpDir)
		r, err := c.OpenWALSegmentV3(context.Background(), gen, 1, 4096)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, original) {
			t.Errorf("decompressed data mismatch: got %q, want %q", data, original)
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
