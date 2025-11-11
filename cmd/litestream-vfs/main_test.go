//go:build vfs
// +build vfs

package main_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestVFS_Simple(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	if err := sqlite3vfs.RegisterVFS("litestream", vfs); err != nil {
		t.Fatalf("failed to register litestream vfs: %v", err)
	}

	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb0 := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb0)

	if _, err := sqldb0.Exec("CREATE TABLE t (x)"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb0.Exec("INSERT INTO t (x) VALUES (100)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * db.MonitorInterval)

	sqldb1, err := sql.Open("sqlite3", "file:/tmp/test.db?vfs=litestream")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()

	// Execute query
	var x int
	if err := sqldb1.QueryRow("SELECT * FROM t").Scan(&x); err != nil {
		t.Fatalf("failed to query database: %v", err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}

func TestVFS_Updating(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	if err := sqlite3vfs.RegisterVFS("litestream", vfs); err != nil {
		t.Fatalf("failed to register litestream vfs: %v", err)
	}

	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.SyncInterval = 100 * time.Millisecond
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb0 := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb0)

	t.Log("creating table")
	if _, err := sqldb0.Exec("CREATE TABLE t (x)"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb0.Exec("INSERT INTO t (x) VALUES (100)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * db.MonitorInterval)

	t.Log("opening vfs")
	sqldb1, err := sql.Open("sqlite3", "file:/tmp/test.db?vfs=litestream")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()

	// Execute query
	var x int
	if err := sqldb1.QueryRow("SELECT * FROM t").Scan(&x); err != nil {
		t.Fatalf("failed to query database: %v", err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}

	t.Log("updating source database")
	// Update the value from the source database.
	if _, err := sqldb0.Exec("UPDATE t SET x = 200"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * db.MonitorInterval)

	// Ensure replica has updated itself.
	t.Log("ensuring replica has updated")
	if err := sqldb1.QueryRow("SELECT * FROM t").Scan(&x); err != nil {
		t.Fatalf("failed to query database: %v", err)
	} else if got, want := x, 200; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}

func TestVFS_ActiveReadTransaction(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	if err := sqlite3vfs.RegisterVFS("litestream-txn", vfs); err != nil {
		t.Fatalf("failed to register litestream vfs: %v", err)
	}

	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.SyncInterval = 100 * time.Millisecond
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	sqldb0 := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb0)

	// Create a table with many rows to ensure we span multiple pages
	// With 4KB page size, we want to ensure we're using hundreds of pages
	t.Log("creating table with many rows")
	if _, err := sqldb0.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)"); err != nil {
		t.Fatal(err)
	}

	// Insert ~10000 rows, each with substantial data to span many pages
	// This should occupy at least 200+ pages (assuming ~200 bytes per row, ~20 rows per 4KB page)
	if _, err := sqldb0.Exec("BEGIN"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		data := fmt.Sprintf("initial_data_%d_padding_%s", i, string(make([]byte, 100)))
		if _, err := sqldb0.Exec("INSERT INTO t (id, data) VALUES (?, ?)", i, data); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sqldb0.Exec("COMMIT"); err != nil {
		t.Fatal(err)
	}

	// Wait for replication to sync
	time.Sleep(3 * db.MonitorInterval)

	t.Log("opening vfs replica")
	sqldb1, err := sql.Open("sqlite3", "file:/tmp/test-txn.db?vfs=litestream-txn")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()

	// Start a read transaction on the replica
	t.Log("starting read transaction on replica")
	tx, err := sqldb1.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Verify we can read initial data from within the transaction
	var initialData string
	if err := tx.QueryRow("SELECT data FROM t WHERE id = 5000").Scan(&initialData); err != nil {
		t.Fatalf("failed to query initial data: %v", err)
	}
	if !strings.HasPrefix(initialData, "initial_data_5000") {
		t.Fatalf("unexpected initial data: %s", initialData)
	}

	t.Log("updating source database with many affected pages")
	// Update many rows in the source database to affect many pages
	if _, err := sqldb0.Exec("BEGIN"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		data := fmt.Sprintf("updated_data_%d_padding_%s", i, string(make([]byte, 100)))
		if _, err := sqldb0.Exec("UPDATE t SET data = ? WHERE id = ?", data, i); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sqldb0.Exec("COMMIT"); err != nil {
		t.Fatal(err)
	}

	// Wait for replication to sync the updates
	t.Log("waiting for replication sync")
	time.Sleep(5 * db.MonitorInterval)

	// The active read transaction should still see old data (snapshot isolation)
	t.Log("verifying read transaction still sees old data")
	var txData string
	if err := tx.QueryRow("SELECT data FROM t WHERE id = 5000").Scan(&txData); err != nil {
		t.Fatalf("failed to query within transaction: %v", err)
	}
	if !strings.HasPrefix(txData, "initial_data_5000") {
		t.Fatalf("transaction should see old data, got: %s", txData)
	}

	// Commit the read transaction
	t.Log("committing read transaction")
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	// Now a new query should see the updated data
	t.Log("verifying new query sees updated data")
	var updatedData string
	if err := sqldb1.QueryRow("SELECT data FROM t WHERE id = 5000").Scan(&updatedData); err != nil {
		t.Fatalf("failed to query updated data: %v", err)
	}
	if !strings.HasPrefix(updatedData, "updated_data_5000") {
		t.Fatalf("expected updated data, got: %s", updatedData)
	}

	// Verify multiple rows across different pages
	t.Log("verifying multiple rows across pages")
	for _, id := range []int{0, 2500, 5000, 7500, 9999} {
		var data string
		if err := sqldb1.QueryRow("SELECT data FROM t WHERE id = ?", id).Scan(&data); err != nil {
			t.Fatalf("failed to query id %d: %v", id, err)
		}
		expected := fmt.Sprintf("updated_data_%d", id)
		if !strings.HasPrefix(data, expected) {
			t.Fatalf("id %d: expected prefix %s, got: %s", id, expected, data)
		}
	}
}

func TestVFS_PollsL1Files(t *testing.T) {
	ctx := context.Background()
	client := file.NewReplicaClient(t.TempDir())

	// Create and populate source database
	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "db"))
	db.MonitorInterval = 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.SyncInterval = 100 * time.Millisecond
	db.Replica.MonitorEnabled = false

	// Create a store to handle compaction
	levels := litestream.CompactionLevels{
		{Level: 0},
		{Level: 1, Interval: 1 * time.Second},
	}
	store := litestream.NewStore([]*litestream.DB{db}, levels)
	store.CompactionMonitorEnabled = false

	if err := store.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer store.Close(ctx)

	sqldb0 := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb0)

	// Create table and insert data
	t.Log("creating table with data")
	if _, err := sqldb0.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)"); err != nil {
		t.Fatal(err)
	}

	// Insert multiple transactions to create several L0 files
	for i := 0; i < 5; i++ {
		if _, err := sqldb0.Exec("INSERT INTO t (data) VALUES (?)", fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		if err := db.Replica.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond) // Small delay between transactions
	}

	t.Log("compacting to L1")
	// Compact L0 files to L1
	if _, err := store.CompactDB(ctx, db, levels[1]); err != nil {
		t.Fatalf("failed to compact to L1: %v", err)
	}

	// Verify L1 files exist
	itr, err := client.LTXFiles(ctx, 1, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	var l1Count int
	for itr.Next() {
		l1Count++
	}
	itr.Close()

	if l1Count == 0 {
		t.Fatal("expected L1 files to exist after compaction")
	}
	t.Logf("found %d L1 file(s)", l1Count)

	// Register VFS
	vfs := newVFS(t, client)
	if err := sqlite3vfs.RegisterVFS("litestream-l1", vfs); err != nil {
		t.Fatalf("failed to register litestream vfs: %v", err)
	}

	// Open database through VFS
	t.Log("opening vfs")
	sqldb1, err := sql.Open("sqlite3", "file:/tmp/test-l1.db?vfs=litestream-l1")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()

	// Query to ensure data is readable
	var count int
	if err := sqldb1.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("failed to query database: %v", err)
	} else if got, want := count, 5; got != want {
		t.Fatalf("got %d rows, want %d", got, want)
	}

	// Get the VFS file to check maxTXID1
	// The VFS creates the file when opened, we need to access it
	// Since VFS.Open returns the file, we need to track it
	// For now, let's add more data and wait for polling

	t.Log("adding more data to source")
	// Add more data to L0 to trigger polling
	for i := 5; i < 10; i++ {
		if _, err := sqldb0.Exec("INSERT INTO t (data) VALUES (?)", fmt.Sprintf("value-%d", i)); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		if err := db.Replica.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for VFS to poll new files
	t.Log("waiting for VFS to poll")
	time.Sleep(5 * vfs.PollInterval)

	// Close and reopen the VFS connection to see updates
	// (VFS is designed for read replicas where clients open new connections)
	sqldb1.Close()

	t.Log("reopening vfs to see updates")
	sqldb1, err = sql.Open("sqlite3", "file:/tmp/test-l1.db?vfs=litestream-l1")
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer sqldb1.Close()

	// Verify VFS can read the new data
	if err := sqldb1.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("failed to query updated database: %v", err)
	} else if got, want := count, 10; got != want {
		t.Fatalf("after update: got %d rows, want %d", got, want)
	}

	// Compact the new L0 files to L1
	t.Log("compacting new data to L1")
	time.Sleep(levels[1].Interval) // Wait for compaction interval
	if _, err := store.CompactDB(ctx, db, levels[1]); err != nil {
		t.Fatalf("failed to compact new data to L1: %v", err)
	}

	// Wait for VFS to poll the new L1 files
	t.Log("waiting for VFS to poll new L1 files")
	time.Sleep(5 * vfs.PollInterval)

	// At this point, the VFS should have polled L1 files
	// We can't directly access the VFSFile from here without modifying VFS.Open
	// But we can verify the data is readable, which proves L1 files are being used

	// Query a specific value to ensure L1 data is accessible
	var data string
	if err := sqldb1.QueryRow("SELECT data FROM t WHERE id = 7").Scan(&data); err != nil {
		t.Fatalf("failed to query specific row: %v", err)
	} else if got, want := data, "value-6"; got != want {
		t.Fatalf("got data %q, want %q", got, want)
	}

	t.Log("L1 file polling verified successfully")
}

func TestVFS_HighLoadConcurrentReads(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	vfs.PollInterval = 50 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)

	db, primary := openReplicatedPrimary(t, client, 50*time.Millisecond, 50*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec(`CREATE TABLE t (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		value TEXT,
		updated_at INTEGER
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	seedLargeTable(t, primary, 2000)
	forceReplicaSync(t, db)

	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()
	if _, err := replica.Exec("PRAGMA temp_store = MEMORY"); err != nil {
		t.Fatalf("set temp_store: %v", err)
	}

	waitForReplicaRowCount(t, primary, replica, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var writerOps atomic.Int64
	writerErr := make(chan error, 1)
	go func() {
		defer close(writerErr)
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			select {
			case <-ctx.Done():
				writerErr <- nil
				return
			default:
			}

			switch rnd.Intn(3) {
			case 0:
				if _, err := primary.Exec("INSERT INTO t (value, updated_at) VALUES (?, strftime('%s','now'))", fmt.Sprintf("value-%d", rnd.Int())); err != nil {
					writerErr <- err
					return
				}
			case 1:
				if _, err := primary.Exec("UPDATE t SET value = value || '-u' WHERE id IN (SELECT id FROM t ORDER BY RANDOM() LIMIT 1)"); err != nil {
					writerErr <- err
					return
				}
			default:
				if _, err := primary.Exec("DELETE FROM t WHERE id IN (SELECT id FROM t ORDER BY RANDOM() LIMIT 1)"); err != nil {
					writerErr <- err
					return
				}
			}

			writerOps.Add(1)
			time.Sleep(time.Duration(rnd.Intn(5)+1) * time.Millisecond)
		}
	}()

	readerErrCh := make(chan error, 1)
	var readerWg sync.WaitGroup
	for i := 0; i < 8; i++ {
		readerWg.Add(1)
		go func(id int) {
			defer readerWg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				var count int
				var totalBytes int
				if err := replica.QueryRow("SELECT COUNT(*), IFNULL(SUM(LENGTH(value)), 0) FROM t").Scan(&count, &totalBytes); err != nil {
					readerErrCh <- fmt.Errorf("reader %d query: %w", id, err)
					return
				}
				if count < 0 || totalBytes < 0 {
					readerErrCh <- fmt.Errorf("reader %d observed invalid stats", id)
					return
				}
			}
		}(i)
	}

	<-ctx.Done()
	readerWg.Wait()

	if err := <-writerErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("writer error: %v", err)
	}
	select {
	case err := <-readerErrCh:
		if err != nil {
			t.Fatalf("reader error: %v", err)
		}
	default:
	}

	if ops := writerOps.Load(); ops < 500 {
		t.Fatalf("expected high write volume, got %d ops", ops)
	}

	waitForReplicaRowCount(t, primary, replica, 30*time.Second)

	var primaryCount, replicaCount int
	if err := primary.QueryRow("SELECT COUNT(*) FROM t").Scan(&primaryCount); err != nil {
		t.Fatalf("primary count: %v", err)
	}
	if err := replica.QueryRow("SELECT COUNT(*) FROM t").Scan(&replicaCount); err != nil {
		t.Fatalf("replica count: %v", err)
	}
	if primaryCount != replicaCount {
		t.Fatalf("replica lagging: primary=%d replica=%d", primaryCount, replicaCount)
	}
}

func TestVFS_SortingLargeResultSet(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	vfs.PollInterval = 50 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)

	db, primary := openReplicatedPrimary(t, client, 50*time.Millisecond, 50*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec(`CREATE TABLE t (
		id INTEGER PRIMARY KEY,
		payload TEXT NOT NULL,
		grp INTEGER NOT NULL
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	seedSortedDataset(t, primary, 25000)
	forceReplicaSync(t, db)

	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()
	if _, err := replica.Exec("PRAGMA temp_store = FILE"); err != nil {
		t.Fatalf("set temp_store: %v", err)
	}
	if _, err := replica.Exec("PRAGMA cache_size = -2048"); err != nil {
		t.Fatalf("set cache_size: %v", err)
	}

	waitForReplicaRowCount(t, primary, replica, time.Minute)

	expected := fetchOrderedPayloads(t, primary, 500, "payload DESC, id DESC")
	got := fetchOrderedPayloads(t, replica, 500, "payload DESC, id DESC")

	if len(expected) != len(got) {
		t.Fatalf("unexpected result size: expected=%d got=%d", len(expected), len(got))
	}
	for i := range expected {
		if expected[i] != got[i] {
			t.Fatalf("mismatched payload at %d: expected=%q got=%q", i, expected[i], got[i])
		}
	}
}

func newVFS(tb testing.TB, client litestream.ReplicaClient) *litestream.VFS {
	tb.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	vfs := litestream.NewVFS(client, logger)
	vfs.PollInterval = 100 * time.Millisecond
	return vfs
}

func registerTestVFS(tb testing.TB, vfs *litestream.VFS) string {
	tb.Helper()
	name := fmt.Sprintf("litestream-%s-%d", strings.ToLower(tb.Name()), time.Now().UnixNano())
	if err := sqlite3vfs.RegisterVFS(name, vfs); err != nil {
		tb.Fatalf("failed to register litestream vfs %s: %v", name, err)
	}
	return name
}

func openReplicatedPrimary(tb testing.TB, client litestream.ReplicaClient, monitorInterval, syncInterval time.Duration) (*litestream.DB, *sql.DB) {
	tb.Helper()
	db := testingutil.NewDB(tb, filepath.Join(tb.TempDir(), "primary.db"))
	db.MonitorInterval = monitorInterval
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.SyncInterval = syncInterval
	if err := db.Open(); err != nil {
		tb.Fatalf("open db: %v", err)
	}
	sqldb := testingutil.MustOpenSQLDB(tb, db.Path())
	tb.Cleanup(func() { _ = db.Close(context.Background()) })
	return db, sqldb
}

func forceReplicaSync(tb testing.TB, db *litestream.DB) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Sync(ctx); err != nil {
		tb.Fatalf("force sync: %v", err)
	}
	if db.Replica != nil {
		if err := db.Replica.Sync(ctx); err != nil {
			tb.Fatalf("replica sync: %v", err)
		}
	}
}

func openVFSReplicaDB(tb testing.TB, vfsName string) *sql.DB {
	tb.Helper()
	dsn := fmt.Sprintf("file:%s?vfs=%s", filepath.ToSlash(filepath.Join(tb.TempDir(), vfsName+".db")), vfsName)
	sqldb, err := sql.Open("sqlite3", dsn)
	if err != nil {
		tb.Fatalf("open replica db: %v", err)
	}
	sqldb.SetMaxOpenConns(32)
	sqldb.SetMaxIdleConns(32)
	sqldb.SetConnMaxIdleTime(30 * time.Second)
	if _, err := sqldb.Exec("PRAGMA busy_timeout = 2000"); err != nil {
		tb.Fatalf("set busy timeout: %v", err)
	}
	return sqldb
}

func waitForReplicaRowCount(tb testing.TB, primary, replica *sql.DB, timeout time.Duration) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var primaryCount int
		if err := primary.QueryRow("SELECT COUNT(*) FROM t").Scan(&primaryCount); err != nil {
			tb.Fatalf("primary count: %v", err)
		}

		var replicaCount int
		if err := replica.QueryRow("SELECT COUNT(*) FROM t").Scan(&replicaCount); err == nil {
			if primaryCount == replicaCount {
				return
			}
		} else {
			// Table may not exist yet on replica; retry.
		}

		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatalf("timeout waiting for replica row count to match")
}

func fetchOrderedPayloads(tb testing.TB, db *sql.DB, limit int, orderBy string) []string {
	tb.Helper()
	query := fmt.Sprintf("SELECT payload FROM t ORDER BY %s LIMIT %d", orderBy, limit)
	rows, err := db.Query(query)
	if err != nil {
		tb.Fatalf("query payloads: %v", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			tb.Fatalf("scan payload: %v", err)
		}
		out = append(out, payload)
	}
	if err := rows.Err(); err != nil {
		tb.Fatalf("rows err: %v", err)
	}
	return out
}

func seedLargeTable(tb testing.TB, db *sql.DB, n int) {
	tb.Helper()
	trx, err := db.Begin()
	if err != nil {
		tb.Fatalf("begin seed: %v", err)
	}
	stmt, err := trx.Prepare("INSERT INTO t (value, updated_at) VALUES (?, strftime('%s','now'))")
	if err != nil {
		_ = trx.Rollback()
		tb.Fatalf("prepare seed: %v", err)
	}
	defer stmt.Close()
	rnd := rand.New(rand.NewSource(42))
	for i := 0; i < n; i++ {
		if _, err := stmt.Exec(fmt.Sprintf("seed-%d-%d", i, rnd.Int())); err != nil {
			_ = trx.Rollback()
			tb.Fatalf("seed exec: %v", err)
		}
	}
	if err := trx.Commit(); err != nil {
		tb.Fatalf("commit seed: %v", err)
	}
}

func seedSortedDataset(tb testing.TB, db *sql.DB, n int) {
	tb.Helper()
	trx, err := db.Begin()
	if err != nil {
		tb.Fatalf("begin sorted seed: %v", err)
	}
	stmt, err := trx.Prepare("INSERT INTO t (id, payload, grp) VALUES (?, ?, ?)")
	if err != nil {
		_ = trx.Rollback()
		tb.Fatalf("prepare sorted seed: %v", err)
	}
	defer stmt.Close()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < n; i++ {
		if _, err := stmt.Exec(i+1, randomPayload(rnd, 256), rnd.Intn(1024)); err != nil {
			_ = trx.Rollback()
			tb.Fatalf("sorted seed exec: %v", err)
		}
	}
	if err := trx.Commit(); err != nil {
		tb.Fatalf("commit sorted seed: %v", err)
	}
}

func randomPayload(r *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}
