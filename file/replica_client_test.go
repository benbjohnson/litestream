package file_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

		generations, err := c.GenerationsV3(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(generations) != 0 {
			t.Fatalf("expected no generations, got %v", generations)
		}
	})

	t.Run("EmptyGenerationsDir", func(t *testing.T) {
		tmpDir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations"), 0755); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		generations, err := c.GenerationsV3(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(generations) != 0 {
			t.Fatalf("expected no generations, got %v", generations)
		}
	})

	t.Run("MultipleGenerationsSorted", func(t *testing.T) {
		tmpDir := t.TempDir()
		genDir := filepath.Join(tmpDir, "generations")
		// Create in non-sorted order
		for _, gen := range []string{"ffffffffffffffff", "0000000000000001", "aaaaaaaaaaaaaaaa"} {
			if err := os.MkdirAll(filepath.Join(genDir, gen), 0755); err != nil {
				t.Fatal(err)
			}
		}
		c := file.NewReplicaClient(tmpDir)

		generations, err := c.GenerationsV3(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"0000000000000001", "aaaaaaaaaaaaaaaa", "ffffffffffffffff"}
		if len(generations) != len(want) {
			t.Fatalf("got %d generations, want %d", len(generations), len(want))
		}
		for i, g := range generations {
			if g != want[i] {
				t.Errorf("generations[%d] = %q, want %q", i, g, want[i])
			}
		}
	})

	t.Run("SkipsInvalidIDs", func(t *testing.T) {
		tmpDir := t.TempDir()
		genDir := filepath.Join(tmpDir, "generations")
		// Create mix of valid and invalid
		for _, name := range []string{
			"0000000000000001", // valid
			"invalid",          // invalid - not hex
			"0123456789abcde",  // invalid - 15 chars
			"0123456789ABCDEF", // invalid - uppercase
			"aaaaaaaaaaaaaaaa", // valid
		} {
			if err := os.MkdirAll(filepath.Join(genDir, name), 0755); err != nil {
				t.Fatal(err)
			}
		}
		c := file.NewReplicaClient(tmpDir)

		generations, err := c.GenerationsV3(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"0000000000000001", "aaaaaaaaaaaaaaaa"}
		if len(generations) != len(want) {
			t.Fatalf("got %d generations, want %d: %v", len(generations), len(want), generations)
		}
		for i, g := range generations {
			if g != want[i] {
				t.Errorf("generations[%d] = %q, want %q", i, g, want[i])
			}
		}
	})

	t.Run("SkipsFiles", func(t *testing.T) {
		tmpDir := t.TempDir()
		genDir := filepath.Join(tmpDir, "generations")
		if err := os.MkdirAll(genDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Create a file with valid generation name (should be skipped)
		if err := os.WriteFile(filepath.Join(genDir, "0000000000000001"), []byte("test"), 0644); err != nil {
			t.Fatal(err)
		}
		// Create a valid directory
		if err := os.MkdirAll(filepath.Join(genDir, "aaaaaaaaaaaaaaaa"), 0755); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		generations, err := c.GenerationsV3(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(generations) != 1 || generations[0] != "aaaaaaaaaaaaaaaa" {
			t.Fatalf("expected only valid directory, got %v", generations)
		}
	})
}

func TestReplicaClient_SnapshotsV3(t *testing.T) {
	gen := "0123456789abcdef"

	t.Run("NoSnapshotsDir", func(t *testing.T) {
		tmpDir := t.TempDir()
		c := file.NewReplicaClient(tmpDir)

		snapshots, err := c.SnapshotsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) != 0 {
			t.Fatalf("expected no snapshots, got %v", snapshots)
		}
	})

	t.Run("EmptySnapshotsDir", func(t *testing.T) {
		tmpDir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations", gen, "snapshots"), 0755); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		snapshots, err := c.SnapshotsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) != 0 {
			t.Fatalf("expected no snapshots, got %v", snapshots)
		}
	})

	t.Run("SingleSnapshot", func(t *testing.T) {
		tmpDir := t.TempDir()
		snapshotsDir := filepath.Join(tmpDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Create snapshot file
		path := filepath.Join(snapshotsDir, "00000001.snapshot.lz4")
		if err := os.WriteFile(path, []byte("test data"), 0644); err != nil {
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
		if snapshots[0].Generation != gen {
			t.Errorf("Generation = %q, want %q", snapshots[0].Generation, gen)
		}
		if snapshots[0].Index != 1 {
			t.Errorf("Index = %d, want 1", snapshots[0].Index)
		}
		if snapshots[0].Size != 9 { // len("test data")
			t.Errorf("Size = %d, want 9", snapshots[0].Size)
		}
	})

	t.Run("MultipleSnapshotsSorted", func(t *testing.T) {
		tmpDir := t.TempDir()
		snapshotsDir := filepath.Join(tmpDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Create snapshots in non-sorted order
		for _, idx := range []int{0xff, 0x01, 0x10} {
			filename := fmt.Sprintf("%08x.snapshot.lz4", idx)
			if err := os.WriteFile(filepath.Join(snapshotsDir, filename), []byte("x"), 0644); err != nil {
				t.Fatal(err)
			}
		}
		c := file.NewReplicaClient(tmpDir)

		snapshots, err := c.SnapshotsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) != 3 {
			t.Fatalf("expected 3 snapshots, got %d", len(snapshots))
		}
		wantIndices := []int{0x01, 0x10, 0xff}
		for i, s := range snapshots {
			if s.Index != wantIndices[i] {
				t.Errorf("snapshots[%d].Index = %d, want %d", i, s.Index, wantIndices[i])
			}
		}
	})

	t.Run("SkipsInvalidFilenames", func(t *testing.T) {
		tmpDir := t.TempDir()
		snapshotsDir := filepath.Join(tmpDir, "generations", gen, "snapshots")
		if err := os.MkdirAll(snapshotsDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Create mix of valid and invalid
		files := map[string]bool{
			"00000001.snapshot.lz4": true,  // valid
			"invalid.snapshot.lz4":  false, // invalid
			"00000002.snapshot":     false, // missing .lz4
			"00000003.wal.lz4":      false, // wrong type
			"00000004.snapshot.lz4": true,  // valid
		}
		for name := range files {
			if err := os.WriteFile(filepath.Join(snapshotsDir, name), []byte("x"), 0644); err != nil {
				t.Fatal(err)
			}
		}
		c := file.NewReplicaClient(tmpDir)

		snapshots, err := c.SnapshotsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) != 2 {
			t.Fatalf("expected 2 valid snapshots, got %d", len(snapshots))
		}
	})
}

func TestReplicaClient_WALSegmentsV3(t *testing.T) {
	gen := "0123456789abcdef"

	t.Run("NoWALDir", func(t *testing.T) {
		tmpDir := t.TempDir()
		c := file.NewReplicaClient(tmpDir)

		segments, err := c.WALSegmentsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(segments) != 0 {
			t.Fatalf("expected no segments, got %v", segments)
		}
	})

	t.Run("EmptyWALDir", func(t *testing.T) {
		tmpDir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(tmpDir, "generations", gen, "wal"), 0755); err != nil {
			t.Fatal(err)
		}
		c := file.NewReplicaClient(tmpDir)

		segments, err := c.WALSegmentsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(segments) != 0 {
			t.Fatalf("expected no segments, got %v", segments)
		}
	})

	t.Run("SingleSegment", func(t *testing.T) {
		tmpDir := t.TempDir()
		walDir := filepath.Join(tmpDir, "generations", gen, "wal")
		if err := os.MkdirAll(walDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Create WAL segment file
		path := filepath.Join(walDir, "00000001-0000000000001000.wal.lz4")
		if err := os.WriteFile(path, []byte("wal data"), 0644); err != nil {
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
		if segments[0].Generation != gen {
			t.Errorf("Generation = %q, want %q", segments[0].Generation, gen)
		}
		if segments[0].Index != 1 {
			t.Errorf("Index = %d, want 1", segments[0].Index)
		}
		if segments[0].Offset != 0x1000 {
			t.Errorf("Offset = %d, want %d", segments[0].Offset, 0x1000)
		}
		if segments[0].Size != 8 { // len("wal data")
			t.Errorf("Size = %d, want 8", segments[0].Size)
		}
	})

	t.Run("MultipleSortedByIndexThenOffset", func(t *testing.T) {
		tmpDir := t.TempDir()
		walDir := filepath.Join(tmpDir, "generations", gen, "wal")
		if err := os.MkdirAll(walDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Create WAL segments in non-sorted order
		segments := []struct {
			index  int
			offset int64
		}{
			{2, 0x2000},
			{1, 0x1000},
			{1, 0x0000},
			{2, 0x0000},
		}
		for _, s := range segments {
			filename := fmt.Sprintf("%08x-%016x.wal.lz4", s.index, s.offset)
			if err := os.WriteFile(filepath.Join(walDir, filename), []byte("x"), 0644); err != nil {
				t.Fatal(err)
			}
		}
		c := file.NewReplicaClient(tmpDir)

		result, err := c.WALSegmentsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != 4 {
			t.Fatalf("expected 4 segments, got %d", len(result))
		}
		// Verify sorted order: index 1 offset 0, index 1 offset 0x1000, index 2 offset 0, index 2 offset 0x2000
		expected := []struct {
			index  int
			offset int64
		}{
			{1, 0x0000},
			{1, 0x1000},
			{2, 0x0000},
			{2, 0x2000},
		}
		for i, s := range result {
			if s.Index != expected[i].index || s.Offset != expected[i].offset {
				t.Errorf("segments[%d] = (%d, %d), want (%d, %d)",
					i, s.Index, s.Offset, expected[i].index, expected[i].offset)
			}
		}
	})

	t.Run("SkipsInvalidFilenames", func(t *testing.T) {
		tmpDir := t.TempDir()
		walDir := filepath.Join(tmpDir, "generations", gen, "wal")
		if err := os.MkdirAll(walDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Create mix of valid and invalid
		files := []string{
			"00000001-0000000000000000.wal.lz4", // valid
			"invalid.wal.lz4",                   // invalid
			"00000002.wal.lz4",                  // missing offset
			"00000003-0000000000001000.wal",     // missing .lz4
			"00000004-0000000000002000.wal.lz4", // valid
		}
		for _, name := range files {
			if err := os.WriteFile(filepath.Join(walDir, name), []byte("x"), 0644); err != nil {
				t.Fatal(err)
			}
		}
		c := file.NewReplicaClient(tmpDir)

		result, err := c.WALSegmentsV3(context.Background(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 valid segments, got %d", len(result))
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
