package litestream_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/mock"
	"github.com/pierrec/lz4/v4"
)

func nextIndex(pos litestream.Pos) litestream.Pos {
	return litestream.Pos{
		Index: pos.Index + 1,
	}
}

func TestReplica_Name(t *testing.T) {
	t.Run("WithName", func(t *testing.T) {
		if got, want := litestream.NewReplica(nil, "NAME").Name(), "NAME"; got != want {
			t.Fatalf("Name()=%v, want %v", got, want)
		}
	})
	t.Run("WithoutName", func(t *testing.T) {
		r := litestream.NewReplica(nil, "")
		r.Client = &mock.ReplicaClient{}
		if got, want := r.Name(), "mock"; got != want {
			t.Fatalf("Name()=%v, want %v", got, want)
		}
	})
}

func TestReplica_Sync(t *testing.T) {
	db, sqldb := MustOpenDBs(t)
	defer MustCloseDBs(t, db, sqldb)

	// Issue initial database sync.
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Fetch current database position.
	dpos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}

	c := file.NewReplicaClient(t.TempDir())
	r := litestream.NewReplica(db, "")
	c.Replica, r.Client = r, c

	if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify we synced checkpoint page to WAL.
	if r, err := c.WALSegmentReader(context.Background(), nextIndex(dpos)); err != nil {
		t.Fatal(err)
	} else if b, err := io.ReadAll(lz4.NewReader(r)); err != nil {
		t.Fatal(err)
	} else if err := r.Close(); err != nil {
		t.Fatal(err)
	} else if len(b) == db.PageSize() {
		t.Fatalf("wal mismatch: len(%d), len(%d)", len(b), db.PageSize())
	}

	// Reset WAL so the next write will only write out the segment we are checking.
	if err := db.Checkpoint(context.Background(), litestream.CheckpointModeTruncate); err != nil {
		t.Fatal(err)
	}

	// Execute a query to write something into the truncated WAL.
	if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
		t.Fatal(err)
	}

	// Sync database to catch up the shadow WAL.
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Save position after sync, it should be after our write.
	dpos, err = db.Pos()
	if err != nil {
		t.Fatal(err)
	}

	// Sync WAL segment out to replica.
	if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify WAL matches replica WAL.
	if b0, err := os.ReadFile(db.Path() + "-wal"); err != nil {
		t.Fatal(err)
	} else if r, err := c.WALSegmentReader(context.Background(), dpos.Truncate()); err != nil {
		t.Fatal(err)
	} else if b1, err := io.ReadAll(lz4.NewReader(r)); err != nil {
		t.Fatal(err)
	} else if err := r.Close(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b0, b1) {
		t.Fatalf("wal mismatch: len(%d), len(%d)", len(b0), len(b1))
	}
}
