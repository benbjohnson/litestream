package file_test

import (
	"testing"

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
