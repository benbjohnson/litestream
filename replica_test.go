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
		Generation: pos.Generation,
		Index:      pos.Index + 1,
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

	// Issue initial database sync to setup generation.
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

	// Verify client generation matches database.
	generations, err := c.Generations(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if got, want := len(generations), 1; got != want {
		t.Fatalf("len(generations)=%v, want %v", got, want)
	} else if got, want := generations[0], dpos.Generation; got != want {
		t.Fatalf("generations[0]=%v, want %v", got, want)
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

func TestReplica_Snapshot(t *testing.T) {
	db, sqldb := MustOpenDBs(t)
	defer MustCloseDBs(t, db, sqldb)

	c := file.NewReplicaClient(t.TempDir())
	r := litestream.NewReplica(db, "")
	r.Client = c

	// Execute a query to force a write to the WAL.
	if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
		t.Fatal(err)
	} else if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	} else if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Fetch current database position & snapshot.
	pos0, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	} else if info, err := r.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	} else if got, want := info.Pos(), nextIndex(pos0); got != want {
		t.Fatalf("pos=%s, want %s", got, want)
	}

	// Sync database and then replica.
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	} else if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Execute a query to force a write to the WAL & truncate to start new index.
	if _, err := sqldb.Exec(`INSERT INTO foo (bar) VALUES ('baz');`); err != nil {
		t.Fatal(err)
	} else if err := db.Checkpoint(context.Background(), litestream.CheckpointModeTruncate); err != nil {
		t.Fatal(err)
	}

	// Fetch current database position & snapshot.
	pos1, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	} else if info, err := r.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	} else if got, want := info.Pos(), nextIndex(pos1); got != want {
		t.Fatalf("pos=%v, want %v", got, want)
	}

	// Verify three snapshots exist.
	if infos, err := r.Snapshots(context.Background()); err != nil {
		t.Fatal(err)
	} else if got, want := len(infos), 3; got != want {
		t.Fatalf("len=%v, want %v", got, want)
	} else if got, want := infos[0].Pos(), pos0.Truncate(); got != want {
		t.Fatalf("info[0]=%s, want %s", got, want)
	} else if got, want := infos[1].Pos(), nextIndex(pos0); got != want {
		t.Fatalf("info[1]=%s, want %s", got, want)
	} else if got, want := infos[2].Pos(), nextIndex(pos1); got != want {
		t.Fatalf("info[2]=%s, want %s", got, want)
	}
}
