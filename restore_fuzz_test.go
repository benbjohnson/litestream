package litestream_test

import (
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func FuzzRestoreWithMissingCompactedFile(f *testing.F) {
	f.Add(int64(1))
	f.Add(int64(2))
	f.Add(int64(3))

	f.Fuzz(func(t *testing.T, seed int64) {
		if testing.Short() {
			t.Skip("skipping fuzz test in short mode")
		}

		rng := rand.New(rand.NewSource(seed))
		ctx := t.Context()

		db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
		db.MonitorInterval = 20 * time.Millisecond
		db.Replica = litestream.NewReplica(db)
		db.Replica.SyncInterval = 20 * time.Millisecond
		client := file.NewReplicaClient(t.TempDir())
		db.Replica.Client = client

		if err := db.Open(); err != nil {
			t.Fatal(err)
		}
		sqldb := testingutil.MustOpenSQLDB(t, db.Path())
		defer testingutil.MustCloseDBs(t, db, sqldb)

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{
			{Level: 0},
			{Level: 1, Interval: 50 * time.Millisecond},
			{Level: 2, Interval: 150 * time.Millisecond},
		})
		store.SnapshotInterval = 200 * time.Millisecond
		if err := store.Open(ctx); err != nil {
			t.Fatal(err)
		}
		defer store.Close(ctx)

		if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT);`); err != nil {
			t.Fatal(err)
		}

		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-done:
					return
				case <-ticker.C:
					if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (val) VALUES (?);`, time.Now().String()); err != nil {
						return
					}
				}
			}
		}()

		runDuration := 800*time.Millisecond + time.Duration(rng.Intn(400))*time.Millisecond
		time.Sleep(runDuration)
		close(done)
		wg.Wait()

		// Allow compaction/snapshotting to catch up.
		time.Sleep(200 * time.Millisecond)

		var candidates []*ltx.FileInfo
		for _, level := range []int{1, 2} {
			itr, err := client.LTXFiles(ctx, level, 0, false)
			if err != nil {
				t.Fatal(err)
			}
			for itr.Next() {
				candidates = append(candidates, itr.Item())
			}
			if err := itr.Close(); err != nil {
				t.Fatal(err)
			}
		}

		if len(candidates) == 0 {
			t.Skip("no compacted files available to delete")
		}

		toDelete := candidates[rng.Intn(len(candidates))]
		if err := client.DeleteLTXFiles(ctx, []*ltx.FileInfo{toDelete}); err != nil {
			t.Fatal(err)
		}

		outputPath := filepath.Join(t.TempDir(), "restore.db")
		if err := db.Replica.Restore(ctx, litestream.RestoreOptions{
			OutputPath: outputPath,
		}); err != nil {
			t.Fatalf("restore failed after deleting L%d %s: %v", toDelete.Level, ltx.FormatFilename(toDelete.MinTXID, toDelete.MaxTXID), err)
		}

		restoreDB := testingutil.MustOpenSQLDB(t, outputPath)
		defer testingutil.MustCloseSQLDB(t, restoreDB)

		var result string
		if err := restoreDB.QueryRowContext(ctx, `PRAGMA integrity_check;`).Scan(&result); err != nil {
			t.Fatal(err)
		} else if result != "ok" {
			t.Fatalf("integrity check failed: %s", result)
		}

		var count int
		if err := restoreDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&count); err != nil {
			t.Fatal(err)
		} else if count == 0 {
			t.Fatal("no records found in restored database")
		}
	})
}
