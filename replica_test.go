package litestream_test

import (
	"context"
	"testing"

	"github.com/benbjohnson/litestream"
)

func TestFileReplica_Sync(t *testing.T) {
	// Ensure replica can successfully sync after DB has sync'd.
	t.Run("InitialSync", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)
		r := NewTestFileReplica(t, db)

		// Sync database & then sync replica.
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		} else if err := r.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Ensure posistions match.
		if pos, err := db.Pos(); err != nil {
			t.Fatal(err)
		} else if got, want := r.LastPos(), pos; got != want {
			t.Fatalf("LastPos()=%v, want %v", got, want)
		}
	})

	// Ensure replica can successfully sync multiple times.
	t.Run("MultiSync", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)
		r := NewTestFileReplica(t, db)

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
				if err := db.Sync(); err != nil {
					t.Fatal(err)
				} else if err := r.Sync(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		}

		// Ensure posistions match.
		if pos, err := db.Pos(); err != nil {
			t.Fatal(err)
		} else if got, want := pos.Index, 2; got != want {
			t.Fatalf("Index=%v, want %v", got, want)
		} else if calcPos, err := r.CalcPos(pos.Generation); err != nil {
			t.Fatal(err)
		} else if got, want := calcPos, pos; got != want {
			t.Fatalf("CalcPos()=%v, want %v", got, want)
		} else if got, want := r.LastPos(), pos; got != want {
			t.Fatalf("LastPos()=%v, want %v", got, want)
		}
	})

	// Ensure replica returns an error if there is no generation available from the DB.
	t.Run("ErrNoGeneration", func(t *testing.T) {
		db, sqldb := MustOpenDBs(t)
		defer MustCloseDBs(t, db, sqldb)
		r := NewTestFileReplica(t, db)

		if err := r.Sync(context.Background()); err == nil || err.Error() != `no generation, waiting for data` {
			t.Fatal(err)
		}
	})
}

// NewTestFileReplica returns a new replica using a temp directory & with monitoring disabled.
func NewTestFileReplica(tb testing.TB, db *litestream.DB) *litestream.FileReplica {
	r := litestream.NewFileReplica(db, "", tb.TempDir())
	r.MonitorEnabled = false
	db.Replicas = []litestream.Replica{r}
	return r
}
