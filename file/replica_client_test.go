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
