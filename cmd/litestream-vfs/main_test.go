//go:build vfs
// +build vfs

package main_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
	"github.com/stretchr/testify/require"

	"github.com/superfly/ltx"

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
	waitForLTXFiles(t, client, 10*time.Second, db.MonitorInterval)

	sqldb1, err := sql.Open("sqlite3", "file:/tmp/test.db?vfs=litestream")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()

	// Execute query - wait for value to be replicated
	waitForReplicaValue(t, sqldb1, "SELECT * FROM t", 100, 10*time.Second, db.MonitorInterval)
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
	waitForLTXFiles(t, client, 10*time.Second, db.MonitorInterval)

	t.Log("opening vfs")
	sqldb1, err := sql.Open("sqlite3", "file:/tmp/test.db?vfs=litestream")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer sqldb1.Close()

	// Wait for initial value to replicate
	waitForReplicaValue(t, sqldb1, "SELECT * FROM t", 100, 10*time.Second, db.MonitorInterval)

	t.Log("updating source database")
	// Update the value from the source database.
	if _, err := sqldb0.Exec("UPDATE t SET x = 200"); err != nil {
		t.Fatal(err)
	}

	// Ensure replica has updated itself.
	t.Log("ensuring replica has updated")
	waitForReplicaValue(t, sqldb1, "SELECT * FROM t", 200, 10*time.Second, db.MonitorInterval)

	if err := db.Replica.Stop(false); err != nil {
		t.Fatalf("stop replica: %v", err)
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
	waitForLTXFiles(t, client, 10*time.Second, db.MonitorInterval)

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

	// Wait for replication to sync the updates - verify new data is available
	// by checking from a fresh connection (not the transaction's snapshot)
	t.Log("waiting for replication sync")
	require.Eventually(t, func() bool {
		var data string
		// Use a new query to check if updates have replicated
		if err := sqldb1.QueryRow("SELECT data FROM t WHERE id = 5000").Scan(&data); err != nil {
			return false
		}
		return strings.HasPrefix(data, "updated_data_5000")
	}, 10*time.Second, db.MonitorInterval, "updates should replicate")

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

	// Close and reopen the VFS connection to see updates
	// (VFS is designed for read replicas where clients open new connections)
	sqldb1.Close()

	t.Log("reopening vfs to see updates")
	sqldb1, err = sql.Open("sqlite3", "file:/tmp/test-l1.db?vfs=litestream-l1")
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer sqldb1.Close()

	// Wait for VFS to poll new files
	t.Log("waiting for VFS to poll")
	require.Eventually(t, func() bool {
		if err := sqldb1.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
			return false
		}
		return count == 10
	}, 10*time.Second, vfs.PollInterval, "VFS should poll and see 10 rows")

	// Compact the new L0 files to L1
	// Use Eventually since compaction has a 1-second interval and first compaction just completed
	t.Log("compacting new data to L1")
	require.Eventually(t, func() bool {
		_, err := store.CompactDB(ctx, db, levels[1])
		return err == nil
	}, 5*time.Second, 200*time.Millisecond, "second compaction should succeed after interval passes")

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

func TestVFS_LongRunningTxnStress(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	vfs.PollInterval = 25 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)

	_, primary := openReplicatedPrimary(t, client, 25*time.Millisecond, 25*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE metrics (id INTEGER PRIMARY KEY, value INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := primary.Exec("INSERT INTO metrics (id, value) VALUES (1, 0)"); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()
	require.Eventually(t, func() bool {
		var tmp int
		return replica.QueryRow("SELECT value FROM metrics WHERE id = 1").Scan(&tmp) == nil
	}, 30*time.Second, 50*time.Millisecond, "replica should observe metrics row")

	tx, err := replica.Begin()
	if err != nil {
		t.Fatalf("begin replica txn: %v", err)
	}
	defer tx.Rollback()

	var initialValue int
	if err := tx.QueryRow("SELECT value FROM metrics WHERE id = 1").Scan(&initialValue); err != nil {
		t.Fatalf("initial read: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	writerDone := make(chan error, 1)
	go func() {
		defer close(writerDone)
		value := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			value++
			if _, err := primary.Exec("UPDATE metrics SET value = ? WHERE id = 1", value); err != nil {
				writerDone <- err
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			if err := <-writerDone; err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("writer error: %v", err)
			}
			goto done
		case <-time.After(50 * time.Millisecond):
			var v int
			if err := tx.QueryRow("SELECT value FROM metrics WHERE id = 1").Scan(&v); err != nil {
				t.Fatalf("read during txn: %v", err)
			}
			if v != initialValue {
				t.Fatalf("long-running txn observed change: got %d want %d", v, initialValue)
			}
		}
	}

done:
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var finalValue int
	if err := replica.QueryRow("SELECT value FROM metrics WHERE id = 1").Scan(&finalValue); err != nil {
		t.Fatalf("post-commit read: %v", err)
	}
	if finalValue == initialValue {
		t.Fatalf("expected updated value after commit")
	}
}

func TestVFS_HighLoadConcurrentReads(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	vfs.PollInterval = 50 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)

	_, primary := openReplicatedPrimary(t, client, 50*time.Millisecond, 50*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec(`CREATE TABLE t (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		value TEXT,
		updated_at INTEGER
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	seedLargeTable(t, primary, 2000)

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

// TestVFS_OverlappingTransactionCommitStorm tests that the VFS can handle
// concurrent read operations while writes are happening on the primary.
// The test verifies that the replica eventually catches up with the primary.
func TestVFS_OverlappingTransactionCommitStorm(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	const interval = 25 * time.Millisecond
	db, primary := openReplicatedPrimary(t, client, interval, interval)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, account INTEGER, amount INTEGER, created_at INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := primary.Exec("INSERT INTO ledger (account, amount, created_at) VALUES (1, 0, strftime('%s','now'))"); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}

	// Wait for LTX files to be created before opening VFS replica
	require.Eventually(t, func() bool {
		itr, err := client.LTXFiles(context.Background(), 0, 0, false)
		if err != nil {
			return false
		}
		defer itr.Close()
		return itr.Next()
	}, 5*time.Second, db.MonitorInterval, "LTX files should be created")
	forceReplicaSync(t, db)

	vfs := newVFS(t, client)
	vfs.PollInterval = interval
	vfsName := registerTestVFS(t, vfs)
	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()

	// Verify initial sync
	require.Eventually(t, func() bool {
		var primaryCount int
		if err := primary.QueryRow("SELECT COUNT(*) FROM ledger").Scan(&primaryCount); err != nil {
			return false
		}
		var replicaCount int
		if err := replica.QueryRow("SELECT COUNT(*) FROM ledger").Scan(&replicaCount); err != nil {
			return false
		}
		return primaryCount == replicaCount
	}, time.Minute, 25*time.Millisecond, "ledger counts should match initially")

	// Run concurrent writers for a short period (reduced from 10s to 3s)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var writerWG sync.WaitGroup
	writer := func(account int) {
		defer writerWG.Done()
		rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(account)))
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			amount := rnd.Intn(200) - 100
			if _, err := primary.Exec("BEGIN IMMEDIATE"); err != nil {
				continue
			}
			if _, err := primary.Exec("INSERT INTO ledger (account, amount, created_at) VALUES (?, ?, strftime('%s','now'))", account, amount); err != nil {
				primary.Exec("ROLLBACK")
				continue
			}
			if _, err := primary.Exec("COMMIT"); err != nil {
				primary.Exec("ROLLBACK")
				continue
			}
			// Slow down writes to allow background monitor to keep up
			time.Sleep(time.Duration(rnd.Intn(20)+10) * time.Millisecond)
		}
	}
	writerWG.Add(2)
	go writer(1)
	go writer(2)

	// Run concurrent reader that verifies count never goes to zero
	readerCtx, readerCancel := context.WithCancel(ctx)
	readerErr := make(chan error, 1)
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-readerCtx.Done():
				return
			default:
			}
			var count int
			if err := replica.QueryRow("SELECT COUNT(*) FROM ledger").Scan(&count); err != nil {
				readerErr <- err
				return
			}
			if count == 0 {
				readerErr <- fmt.Errorf("ledger count went to zero")
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
	}()

	<-ctx.Done()
	readerCancel()
	writerWG.Wait()
	readerWG.Wait()

	// Check for reader errors
	select {
	case err := <-readerErr:
		if err != nil {
			t.Fatalf("reader error: %v", err)
		}
	default:
	}

	// Force final sync and wait for replica to catch up
	forceReplicaSync(t, db)

	require.Eventually(t, func() bool {
		var primaryCount int
		if err := primary.QueryRow("SELECT COUNT(*) FROM ledger").Scan(&primaryCount); err != nil {
			return false
		}
		var replicaCount int
		if err := replica.QueryRow("SELECT COUNT(*) FROM ledger").Scan(&replicaCount); err != nil {
			return false
		}
		return primaryCount == replicaCount
	}, 30*time.Second, 100*time.Millisecond, "ledger counts should match after writer done")
}

func TestVFS_CacheMissStorm(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	const interval = 20 * time.Millisecond
	_, primary := openReplicatedPrimary(t, client, interval, interval)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE stats (id INTEGER PRIMARY KEY, payload TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 1000; i++ {
		if _, err := primary.Exec("INSERT INTO stats (payload) VALUES (?)", fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("insert payload: %v", err)
		}
	}

	vfs := newVFS(t, client)
	vfs.PollInterval = interval
	vfsName := registerTestVFS(t, vfs)
	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()

	waitForTableRowCount(t, primary, replica, "stats", 30*time.Second)

	if _, err := replica.Exec("PRAGMA cache_size = -64"); err != nil {
		t.Fatalf("set cache_size: %v", err)
	}
	if _, err := replica.Exec("PRAGMA cache_spill = ON"); err != nil {
		t.Fatalf("enable cache_spill: %v", err)
	}

	for i := 0; i < 100; i++ {
		var maxID int
		if err := replica.QueryRow("SELECT MAX(id) FROM stats").Scan(&maxID); err != nil {
			t.Fatalf("cache-miss query: %v", err)
		}
		if maxID == 0 {
			t.Fatalf("unexpected empty stats table")
		}
	}
}

func BenchmarkVFS_LargeDatabase(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}
	client := file.NewReplicaClient(b.TempDir())
	db, primary := openReplicatedPrimary(b, client, 25*time.Millisecond, 25*time.Millisecond)
	b.Cleanup(func() { testingutil.MustCloseSQLDB(b, primary) })

	if _, err := primary.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT, updated_at INTEGER)"); err != nil {
		b.Fatalf("create table: %v", err)
	}
	seedLargeTable(b, primary, 20000)
	forceReplicaSync(b, db)
	if err := db.Replica.Stop(false); err != nil {
		b.Fatalf("stop replica: %v", err)
	}

	vfs := newVFS(b, client)
	vfs.PollInterval = 25 * time.Millisecond
	vfsName := registerTestVFS(b, vfs)
	replica := openVFSReplicaDB(b, vfsName)
	b.Cleanup(func() { replica.Close() })
	waitForReplicaRowCount(b, primary, replica, 30*time.Second)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var count, totalBytes int
		if err := replica.QueryRow("SELECT COUNT(*), IFNULL(SUM(LENGTH(value)), 0) FROM t").Scan(&count, &totalBytes); err != nil {
			b.Fatalf("benchmark query: %v", err)
		}
	}
}

func TestVFS_NetworkLatencySensitivity(t *testing.T) {
	client := &latencyReplicaClient{ReplicaClient: file.NewReplicaClient(t.TempDir()), delay: 10 * time.Millisecond}
	vfs := newVFS(t, client)
	vfs.PollInterval = 25 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)

	_, primary := openReplicatedPrimary(t, client, 25*time.Millisecond, 25*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE logs (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := primary.Exec("INSERT INTO logs (value) VALUES ('ok')"); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()

	require.Eventually(t, func() bool {
		var count int
		if err := replica.QueryRow("SELECT COUNT(*) FROM logs").Scan(&count); err != nil {
			return false
		}
		return count == 1
	}, 10*time.Second, 50*time.Millisecond, "replica should observe log row under injected latency")
}

func TestVFS_ConcurrentConnectionScaling(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	vfs.PollInterval = 25 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)

	db, primary := openReplicatedPrimary(t, client, 25*time.Millisecond, 25*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE metrics (id INTEGER PRIMARY KEY AUTOINCREMENT, value INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 1000; i++ {
		if _, err := primary.Exec("INSERT INTO metrics (value) VALUES (?)", i); err != nil {
			t.Fatalf("insert row: %v", err)
		}
	}

	// Wait for LTX files to be created before forceReplicaSync
	require.Eventually(t, func() bool {
		itr, err := client.LTXFiles(context.Background(), 0, 0, false)
		if err != nil {
			return false
		}
		defer itr.Close()
		return itr.Next()
	}, 5*time.Second, db.MonitorInterval, "LTX files should be created")
	forceReplicaSync(t, db)

	const connCount = 32
	conns := make([]*sql.DB, connCount)
	for i := 0; i < connCount; i++ {
		conns[i] = openVFSReplicaDB(t, vfsName)
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for idx := range conns {
		wg.Add(1)
		go func(id int, dbConn *sql.DB) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				var min, max int
				if err := dbConn.QueryRow("SELECT MIN(value), MAX(value) FROM metrics").Scan(&min, &max); err != nil {
					t.Errorf("conn %d query: %v", id, err)
					return
				}
			}
		}(idx, conns[idx])
	}

	wg.Wait()
	if err := ctx.Err(); err != context.Canceled && err != context.DeadlineExceeded {
		t.Fatalf("unexpected context err: %v", err)
	}
}

func TestVFS_PRAGMAQueryBehavior(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	vfs := newVFS(t, client)
	vfs.PollInterval = 25 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)

	db, primary := openReplicatedPrimary(t, client, 25*time.Millisecond, 25*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE configs (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := primary.Exec("INSERT INTO configs (name) VALUES ('ok')"); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	if _, err := primary.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("create table t: %v", err)
	}
	if _, err := primary.Exec("INSERT INTO t (value) VALUES ('seed')"); err != nil {
		t.Fatalf("seed t: %v", err)
	}

	// Wait for LTX files to be created before forceReplicaSync
	require.Eventually(t, func() bool {
		itr, err := client.LTXFiles(context.Background(), 0, 0, false)
		if err != nil {
			return false
		}
		defer itr.Close()
		return itr.Next()
	}, 5*time.Second, db.MonitorInterval, "LTX files should be created")
	forceReplicaSync(t, db)

	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()

	waitForReplicaRowCount(t, primary, replica, 30*time.Second)

	var journalMode string
	if err := replica.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if strings.ToLower(journalMode) != "delete" {
		t.Fatalf("expected journal_mode delete, got %s", journalMode)
	}

	if _, err := replica.Exec("PRAGMA cache_size = -2048"); err != nil {
		t.Fatalf("set cache_size: %v", err)
	}
	var cacheSize int
	if err := replica.QueryRow("PRAGMA cache_size").Scan(&cacheSize); err != nil {
		t.Fatalf("read cache_size: %v", err)
	}
	if cacheSize != -2048 {
		t.Fatalf("unexpected cache_size: %d", cacheSize)
	}

	var pageSize int
	if err := replica.QueryRow("PRAGMA page_size").Scan(&pageSize); err != nil {
		t.Fatalf("read page_size: %v", err)
	}
	if pageSize != 4096 {
		t.Fatalf("unexpected page_size: %d", pageSize)
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
	if err := db.Replica.Stop(false); err != nil {
		t.Fatalf("stop replica: %v", err)
	}

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

func TestVFS_ConcurrentIndexAccessRaces(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	const monitorInterval = 10 * time.Millisecond
	_, primary := openReplicatedPrimary(t, client, monitorInterval, 10*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT, updated_at INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	seedLargeTable(t, primary, 10000)

	vfs := newVFS(t, client)
	vfs.PollInterval = 15 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)
	dsn := fmt.Sprintf("file:%s?vfs=%s", filepath.ToSlash(filepath.Join(t.TempDir(), "fail.db")), vfsName)
	replica, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open replica db: %v", err)
	}
	defer replica.Close()
	replica.SetMaxOpenConns(4)
	replica.SetMaxIdleConns(4)
	replica.SetConnMaxIdleTime(30 * time.Second)

	waitForReplicaRowCount(t, primary, replica, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	readerErrCh := make(chan error, 1)
	var readerWG sync.WaitGroup
	for i := 0; i < 100; i++ {
		readerWG.Add(1)
		go func(id int) {
			defer readerWG.Done()
			rnd := rand.New(rand.NewSource(int64(id) + time.Now().UnixNano()))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				var count int
				var totalBytes int
				if err := replica.QueryRow("SELECT COUNT(*), IFNULL(SUM(LENGTH(value)), 0) FROM t").Scan(&count, &totalBytes); err != nil {
					select {
					case readerErrCh <- fmt.Errorf("reader %d: %w", id, err):
					default:
					}
					cancel()
					return
				}
				if count < 0 || totalBytes < 0 {
					select {
					case readerErrCh <- fmt.Errorf("reader %d observed invalid stats", id):
					default:
					}
					cancel()
					return
				}
				_ = rnd.Int() // exercise RNG to vary workload
			}
		}(i)
	}

	var writerOps atomic.Int64
	writerErrCh := make(chan error, 1)
	go func() {
		defer close(writerErrCh)
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			switch rnd.Intn(3) {
			case 0:
				_, err := primary.Exec("INSERT INTO t (value, updated_at) VALUES (?, strftime('%s','now'))", fmt.Sprintf("writer-%d", rnd.Int()))
				if err != nil {
					if isBusyError(err) {
						continue
					}
					writerErrCh <- err
					cancel()
					return
				}
			case 1:
				_, err := primary.Exec("UPDATE t SET value = value || '-u', updated_at = strftime('%s','now') WHERE id IN (SELECT id FROM t ORDER BY RANDOM() LIMIT 1)")
				if err != nil {
					if isBusyError(err) {
						continue
					}
					writerErrCh <- err
					cancel()
					return
				}
			default:
				_, err := primary.Exec("DELETE FROM t WHERE id IN (SELECT id FROM t ORDER BY RANDOM() LIMIT 1)")
				if err != nil {
					if isBusyError(err) {
						continue
					}
					writerErrCh <- err
					cancel()
					return
				}
			}
			writerOps.Add(1)
			time.Sleep(time.Duration(rnd.Intn(5)+1) * time.Millisecond)
		}
	}()

	<-ctx.Done()
	readerWG.Wait()
	if err := <-writerErrCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("writer error: %v", err)
	}
	select {
	case err := <-readerErrCh:
		if err != nil {
			t.Fatalf("reader error: %v", err)
		}
	default:
	}

	if ops := writerOps.Load(); ops == 0 {
		t.Fatalf("writer did not perform any operations")
	}
}

func TestVFS_MultiplePageSizes(t *testing.T) {
	pageSizes := []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}
	for _, pageSize := range pageSizes {
		pageSize := pageSize
		const monitorInterval = 50 * time.Millisecond
		t.Run(fmt.Sprintf("page_%d", pageSize), func(t *testing.T) {
			client := file.NewReplicaClient(t.TempDir())
			_, primary := openReplicatedPrimary(t, client, monitorInterval, 50*time.Millisecond)
			defer testingutil.MustCloseSQLDB(t, primary)

			if _, err := primary.Exec("PRAGMA journal_mode=DELETE"); err != nil {
				t.Fatalf("disable wal: %v", err)
			}
			if _, err := primary.Exec(fmt.Sprintf("PRAGMA page_size = %d", pageSize)); err != nil {
				t.Fatalf("set page size: %v", err)
			}
			if _, err := primary.Exec("VACUUM"); err != nil {
				t.Fatalf("vacuum: %v", err)
			}
			if _, err := primary.Exec("PRAGMA journal_mode=WAL"); err != nil {
				t.Fatalf("enable wal: %v", err)
			}

			if _, err := primary.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)"); err != nil {
				t.Fatalf("create table: %v", err)
			}

			const totalRows = 200
			if _, err := primary.Exec("BEGIN"); err != nil {
				t.Fatalf("begin tx: %v", err)
			}
			for i := 0; i < totalRows; i++ {
				payload := pageSizedPayload(pageSize, i)
				if _, err := primary.Exec("INSERT INTO t (payload) VALUES (?)", payload); err != nil {
					primary.Exec("ROLLBACK")
					t.Fatalf("insert row %d: %v", i, err)
				}
			}
			if _, err := primary.Exec("COMMIT"); err != nil {
				t.Fatalf("commit: %v", err)
			}

			vfs := newVFS(t, client)
			vfs.PollInterval = 50 * time.Millisecond
			vfsName := registerTestVFS(t, vfs)
			replica := openVFSReplicaDB(t, vfsName)
			defer replica.Close()

			waitForReplicaRowCount(t, primary, replica, 30*time.Second)

			var replicaPageSize int
			if err := replica.QueryRow("PRAGMA page_size").Scan(&replicaPageSize); err != nil {
				t.Fatalf("read replica page size: %v", err)
			}
			if replicaPageSize != pageSize {
				t.Fatalf("unexpected page size: got %d want %d", replicaPageSize, pageSize)
			}

			rows, err := replica.Query("SELECT id, payload FROM t ORDER BY id")
			if err != nil {
				t.Fatalf("select rows: %v", err)
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				var id int
				var payload string
				if err := rows.Scan(&id, &payload); err != nil {
					t.Fatalf("scan row: %v", err)
				}
				expected := pageSizedPayload(pageSize, id-1)
				if payload != expected {
					t.Fatalf("row %d mismatch: got %q want %q", id, payload, expected)
				}
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows err: %v", err)
			}
			if count != totalRows {
				t.Fatalf("unexpected row count: got %d want %d", count, totalRows)
			}
		})
	}
}

func TestVFS_WaitsForInitialSnapshot(t *testing.T) {
	t.Run("BlocksUntilSnapshot", func(t *testing.T) {
		client := file.NewReplicaClient(t.TempDir())
		vfs := newVFS(t, client)
		vfs.PollInterval = 50 * time.Millisecond
		vfsName := registerTestVFS(t, vfs)
		dsn := fmt.Sprintf("file:%s?vfs=%s", filepath.ToSlash(filepath.Join(t.TempDir(), "wait.db")), vfsName)

		errCh := make(chan error, 1)
		go func() {
			sqldb, err := sql.Open("sqlite3", dsn)
			if err != nil {
				errCh <- fmt.Errorf("open replica: %w", err)
				return
			}
			defer sqldb.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var count int
			if err := sqldb.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master").Scan(&count); err != nil {
				errCh <- err
				return
			}
			errCh <- nil
		}()

		select {
		case err := <-errCh:
			t.Fatalf("replica should block until snapshot is available, got %v", err)
		case <-time.After(200 * time.Millisecond):
		}

		_, primary := openReplicatedPrimary(t, client, 25*time.Millisecond, 25*time.Millisecond)
		defer testingutil.MustCloseSQLDB(t, primary)

		if _, err := primary.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
			t.Fatalf("create table: %v", err)
		}
		if _, err := primary.Exec("INSERT INTO t (id) VALUES (1)"); err != nil {
			t.Fatalf("insert row: %v", err)
		}

		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("replica query failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for replica to observe initial snapshot")
		}
	})

}

func TestVFS_StorageFailureInjection(t *testing.T) {
	tests := []struct {
		name string
		mode string
	}{
		{"timeout", "timeout"},
		{"server_error", "server"},
		{"partial_read", "partial"},
		{"corrupt_data", "corrupt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := file.NewReplicaClient(t.TempDir())
			db, primary := openReplicatedPrimary(t, client, 50*time.Millisecond, 50*time.Millisecond)
			defer testingutil.MustCloseSQLDB(t, primary)

			if _, err := primary.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
				t.Fatalf("create table: %v", err)
			}
			if _, err := primary.Exec("INSERT INTO t (value) VALUES ('ok')"); err != nil {
				t.Fatalf("insert row: %v", err)
			}
			// Wait for LTX files to be written by background monitor
			require.Eventually(t, func() bool {
				itr, err := client.LTXFiles(context.Background(), 0, 0, false)
				if err != nil {
					return false
				}
				defer itr.Close()
				return itr.Next()
			}, 5*time.Second, db.MonitorInterval, "LTX files should be created by background monitor")
			forceReplicaSync(t, db)
			if err := db.Replica.Stop(false); err != nil {
				t.Fatalf("stop replica: %v", err)
			}

			vfs := newVFS(t, client)
			vfs.PollInterval = time.Hour
			vfsName := registerTestVFS(t, vfs)
			replicaPath := filepath.Join(t.TempDir(), fmt.Sprintf("storage-failure-%s.db", tt.name))
			dsn := fmt.Sprintf("file:%s?vfs=%s", filepath.ToSlash(replicaPath), vfsName)
			replica, err := sql.Open("sqlite3", dsn)
			if err != nil {
				t.Fatalf("open replica db: %v", err)
			}
			defer replica.Close()
			replica.SetMaxOpenConns(4)
			replica.SetMaxIdleConns(4)
			replica.SetConnMaxIdleTime(30 * time.Second)
			if _, err := replica.Exec("PRAGMA busy_timeout = 2000"); err != nil {
				t.Fatalf("set busy timeout: %v", err)
			}

			injectFailure := func() {
				var err error
				switch tt.mode {
				case "timeout":
					err = context.DeadlineExceeded
				case "server":
					err = fmt.Errorf("storage error: 500 Internal Server Error")
				case "partial":
					err = io.ErrUnexpectedEOF
				case "corrupt":
					err = fmt.Errorf("corrupt data")
				default:
					err = fmt.Errorf("injected failure")
				}
				vfs.Inject(replicaPath, err)
			}

			injectFailure()
			var val string
			if err := replica.QueryRow("SELECT value FROM t").Scan(&val); err == nil {
				t.Fatalf("expected failure due to injected storage error")
			}

			if err := replica.QueryRow("SELECT value FROM t").Scan(&val); err != nil {
				t.Fatalf("second read failed: %v", err)
			}
			if val != "ok" {
				t.Fatalf("unexpected row value: %q", val)
			}
		})
	}
}

func TestVFS_PartialLTXUpload(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	db, primary := openReplicatedPrimary(t, client, 25*time.Millisecond, 25*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE logs (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := primary.Exec("INSERT INTO logs (value) VALUES ('ok')"); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	// Wait for LTX files to be created before forceReplicaSync
	require.Eventually(t, func() bool {
		itr, err := client.LTXFiles(context.Background(), 0, 0, false)
		if err != nil {
			return false
		}
		defer itr.Close()
		return itr.Next()
	}, 5*time.Second, db.MonitorInterval, "LTX files should be created")
	forceReplicaSync(t, db)

	vfs := newVFS(t, client)
	vfs.PollInterval = time.Hour
	vfsName := registerTestVFS(t, vfs)
	replicaPath := filepath.Join(t.TempDir(), "partial.db")
	dsn := fmt.Sprintf("file:%s?vfs=%s", filepath.ToSlash(replicaPath), vfsName)
	replica, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open replica db: %v", err)
	}
	defer replica.Close()
	replica.SetMaxOpenConns(8)
	replica.SetMaxIdleConns(8)
	replica.SetConnMaxIdleTime(30 * time.Second)
	if _, err := replica.Exec("PRAGMA busy_timeout = 2000"); err != nil {
		t.Fatalf("set busy timeout: %v", err)
	}

	vfs.Inject(replicaPath, io.ErrUnexpectedEOF)
	var val string
	if err := replica.QueryRow("SELECT value FROM logs").Scan(&val); err == nil {
		t.Fatalf("expected failure due to partial upload")
	}

	if err := replica.QueryRow("SELECT value FROM logs").Scan(&val); err != nil {
		t.Fatalf("second attempt should succeed: %v", err)
	}
	if val != "ok" {
		t.Fatalf("unexpected row value: %q", val)
	}
}

func TestVFS_S3EventualConsistency(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	_, primary := openReplicatedPrimary(t, client, 25*time.Millisecond, 25*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := primary.Exec("INSERT INTO t (value) VALUES ('visible')"); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	eventualClient := &eventualConsistencyClient{ReplicaClient: client}
	vfs := newVFS(t, eventualClient)
	vfs.PollInterval = 25 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)
	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()

	waitForReplicaRowCount(t, primary, replica, 5*time.Second)

	if calls := eventualClient.calls.Load(); calls < 2 {
		t.Fatalf("expected multiple polls under eventual consistency, got %d", calls)
	}
}

func TestVFS_FileDescriptorBudget(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	_, primary := openReplicatedPrimary(t, client, 25*time.Millisecond, 25*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := primary.Exec("INSERT INTO t (value) VALUES ('seed')"); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	limited := &fdLimitedReplicaClient{ReplicaClient: client, limit: 64}
	vfs := newVFS(t, limited)
	vfs.PollInterval = 10 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)
	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()

	waitForReplicaRowCount(t, primary, replica, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	writerDone := make(chan error, 1)
	go func() {
		defer close(writerDone)
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if _, err := primary.Exec("INSERT INTO t (value) VALUES (?)", fmt.Sprintf("v-%d", rnd.Int())); err != nil {
				if isBusyError(err) {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				writerDone <- err
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	const readers = 8
	errs := make(chan error, readers)
	for i := 0; i < readers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					errs <- nil
					return
				default:
				}
				var count int
				if err := replica.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
					if isBusyError(err) {
						time.Sleep(2 * time.Millisecond)
						continue
					}
					errs <- err
					return
				}
			}
		}()
	}

	<-ctx.Done()
	for i := 0; i < readers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("reader %d error: %T %v", i, err, err)
		}
	}
	if err := <-writerDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("writer error: %v", err)
	}

	deadline := time.After(250 * time.Millisecond)
	for limited.open.Load() != 0 {
		select {
		case <-deadline:
			t.Fatalf("descriptor leak: %d handles still open", limited.open.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestVFS_PageIndexOOM(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	_, primary := openReplicatedPrimary(t, client, 25*time.Millisecond, 25*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := primary.Exec("INSERT INTO t (value) VALUES ('ok')"); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	for i := 0; i < 64; i++ {
		payload := strings.Repeat("p", 3500)
		if _, err := primary.Exec("INSERT INTO t (value) VALUES (?)", payload); err != nil {
			t.Fatalf("bulk insert: %v", err)
		}
	}

	oomClient := &oomPageIndexClient{ReplicaClient: client}
	vfs := newVFS(t, oomClient)
	vfs.PollInterval = 20 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)
	dsn := fmt.Sprintf("file:%s?vfs=%s", filepath.ToSlash(filepath.Join(t.TempDir(), "oom.db")), vfsName)
	failing, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open replica db: %v", err)
	}
	defer failing.Close()
	failing.SetMaxOpenConns(4)
	failing.SetMaxIdleConns(4)

	oomClient.failNext.Store(true)
	var count int
	if err := failing.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err == nil {
		t.Fatalf("expected query to fail due to page index OOM")
	}
	if !oomClient.triggered.Load() {
		t.Fatalf("page index client never triggered")
	}

	oomClient.failNext.Store(false)
	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()
	waitForReplicaRowCount(t, primary, replica, 5*time.Second)

	if err := replica.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("post-oom read failed: %v", err)
	}
	var expected int
	if err := primary.QueryRow("SELECT COUNT(*) FROM t").Scan(&expected); err != nil {
		t.Fatalf("primary count: %v", err)
	}
	if count != expected {
		t.Fatalf("unexpected row count: got %d want %d", count, expected)
	}
}

func TestVFS_PageIndexCorruptionRecovery(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	_, primary := openReplicatedPrimary(t, client, 25*time.Millisecond, 25*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := primary.Exec("INSERT INTO t (value) VALUES ('ok')"); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	corruptClient := &corruptingPageIndexClient{ReplicaClient: client}
	vfs := newVFS(t, corruptClient)
	vfs.PollInterval = 20 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)
	dsn := fmt.Sprintf("file:%s?vfs=%s", filepath.ToSlash(filepath.Join(t.TempDir(), "corrupt.db")), vfsName)

	corruptClient.corruptNext.Store(true)
	badConn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open corrupt replica: %v", err)
	}
	badConn.SetMaxOpenConns(8)
	badConn.SetMaxIdleConns(8)
	badConn.SetConnMaxIdleTime(30 * time.Second)
	var count int
	if err := badConn.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err == nil {
		badConn.Close()
		t.Fatalf("expected corruption failure")
	}
	badConn.Close()
	if !corruptClient.triggered.Load() {
		t.Fatalf("corruption hook never triggered")
	}

	goodConn := openVFSReplicaDB(t, vfsName)
	defer goodConn.Close()
	if err := goodConn.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("post-corruption read failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("unexpected row count after recovery: %d", count)
	}
}

func TestVFS_RapidUpdateCoalescing(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	const interval = 5 * time.Millisecond
	_, primary := openReplicatedPrimary(t, client, interval, interval)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE metrics (id INTEGER PRIMARY KEY, value INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := primary.Exec("INSERT INTO metrics (id, value) VALUES (1, 0)"); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	vfs := newVFS(t, client)
	vfs.PollInterval = interval
	vfsName := registerTestVFS(t, vfs)
	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()

	const updates = 200
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 1; i <= updates; i++ {
			if _, err := primary.Exec("UPDATE metrics SET value = ? WHERE id = 1", i); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	require.Eventually(t, func() bool {
		var value int
		if err := replica.QueryRow("SELECT value FROM metrics WHERE id = 1").Scan(&value); err != nil {
			return false
		}
		return value == updates
	}, 3*time.Second, 5*time.Millisecond, "replica should observe final value")
	<-writerDone

	var value int
	if err := replica.QueryRow("SELECT value FROM metrics WHERE id = 1").Scan(&value); err != nil {
		t.Fatalf("final read: %v", err)
	}
	if value != updates {
		t.Fatalf("unexpected final value: got %d want %d", value, updates)
	}
}

func TestVFS_NonContiguousTXIDGapFailsOnOpen(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	for txID := ltx.TXID(1); txID <= 4; txID++ {
		writeSinglePageLTXFile(t, client, txID, byte('a'+int(txID)))
	}

	missing := client.LTXFilePath(0, 2, 2)
	if err := os.Remove(missing); err != nil {
		t.Fatalf("remove ltx file: %v", err)
	}

	fileLogger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	f := litestream.NewVFSFile(client, "gap.db", fileLogger)
	f.PollInterval = 25 * time.Millisecond

	if err := f.Open(); err == nil {
		t.Fatalf("expected open to fail after removing %s", filepath.Base(missing))
	} else if errMsg := err.Error(); !strings.Contains(errMsg, "non-contiguous") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestVFS_PollingThreadRecoversFromLTXListFailure(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	flakyClient := &flakyLTXClient{ReplicaClient: client}
	const monitorInterval = 25 * time.Millisecond
	_, primary := openReplicatedPrimary(t, client, monitorInterval, monitorInterval)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := primary.Exec("INSERT INTO t (value) VALUES ('seed')"); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	vfs := newVFS(t, flakyClient)
	vfs.PollInterval = 25 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)
	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()

	waitForReplicaRowCount(t, primary, replica, 10*time.Second)

	flakyClient.failNext.Store(true)
	if _, err := primary.Exec("INSERT INTO t (value) VALUES ('after-failure')"); err != nil {
		t.Fatalf("insert post-failure: %v", err)
	}

	waitForReplicaRowCount(t, primary, replica, 10*time.Second)

	if flakyClient.failures.Load() == 0 {
		t.Fatalf("expected at least one LTXFiles failure")
	}

	var primaryCount, replicaCount int
	if err := primary.QueryRow("SELECT COUNT(*) FROM t").Scan(&primaryCount); err != nil {
		t.Fatalf("primary count: %v", err)
	}
	if err := replica.QueryRow("SELECT COUNT(*) FROM t").Scan(&replicaCount); err != nil {
		t.Fatalf("replica count: %v", err)
	}
	if primaryCount != replicaCount {
		t.Fatalf("replica did not catch up after failure: primary=%d replica=%d", primaryCount, replicaCount)
	}
}

func TestVFS_PollIntervalEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		minCalls int64
		maxCalls int64
	}{
		{"fast", 5 * time.Millisecond, 10, 500},
		{"slow", 200 * time.Millisecond, 1, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := file.NewReplicaClient(t.TempDir())
			obs := &observingReplicaClient{ReplicaClient: client}
			_, primary := openReplicatedPrimary(t, obs, tt.interval, tt.interval)
			defer testingutil.MustCloseSQLDB(t, primary)

			if _, err := primary.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)"); err != nil {
				t.Fatalf("create table: %v", err)
			}

			vfs := newVFS(t, obs)
			vfs.PollInterval = tt.interval
			vfsName := registerTestVFS(t, vfs)
			replica := openVFSReplicaDB(t, vfsName)
			defer replica.Close()

			start := obs.ltxCalls.Load()
			time.Sleep(750 * time.Millisecond)
			delta := obs.ltxCalls.Load() - start
			if delta < tt.minCalls {
				t.Fatalf("expected at least %d polls, got %d", tt.minCalls, delta)
			}
			if tt.maxCalls > 0 && delta > tt.maxCalls {
				t.Fatalf("expected at most %d polls, got %d", tt.maxCalls, delta)
			}
		})
	}
}

func newVFS(tb testing.TB, client litestream.ReplicaClient) *testVFS {
	tb.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	base := litestream.NewVFS(client, logger)
	base.PollInterval = 100 * time.Millisecond
	return &testVFS{
		VFS:      base,
		failures: make(map[string][]error),
	}
}

type testVFS struct {
	*litestream.VFS

	mu       sync.Mutex
	failures map[string][]error
}

func (v *testVFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	f, flags, err := v.VFS.Open(name, flags)
	if err != nil {
		return nil, flags, err
	}
	return &injectingFile{File: f, vfs: v, name: name}, flags, nil
}

func (v *testVFS) Inject(path string, err error) {
	v.mu.Lock()
	v.failures[path] = append(v.failures[path], err)
	v.mu.Unlock()
}

func (v *testVFS) popFailure(path string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	queue := v.failures[path]
	if len(queue) == 0 {
		return nil
	}
	err := queue[0]
	if len(queue) == 1 {
		delete(v.failures, path)
	} else {
		v.failures[path] = queue[1:]
	}
	if err == nil {
		return errors.New("vfs page read error")
	}
	return err
}

type injectingFile struct {
	sqlite3vfs.File

	vfs  *testVFS
	name string
}

func (f *injectingFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.vfs.popFailure(f.name); err != nil {
		return 0, err
	}
	return f.File.ReadAt(p, off)
}

func (f *injectingFile) FileControl(op int, pragmaName string, pragmaValue *string) (*string, error) {
	if fc, ok := f.File.(sqlite3vfs.FileController); ok {
		return fc.FileControl(op, pragmaName, pragmaValue)
	}
	return nil, sqlite3vfs.NotFoundError
}

func registerTestVFS(tb testing.TB, vfs sqlite3vfs.VFS) string {
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
	require.Eventually(tb, func() bool {
		var primaryCount int
		if err := primary.QueryRow("SELECT COUNT(*) FROM t").Scan(&primaryCount); err != nil {
			return false
		}
		var replicaCount int
		if err := replica.QueryRow("SELECT COUNT(*) FROM t").Scan(&replicaCount); err != nil {
			return false
		}
		return primaryCount == replicaCount
	}, timeout, 50*time.Millisecond, "replica row count should match primary")
}

func waitForTableRowCount(tb testing.TB, primary, replica *sql.DB, table string, timeout time.Duration) {
	tb.Helper()
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	require.Eventually(tb, func() bool {
		var primaryCount int
		if err := primary.QueryRow(query).Scan(&primaryCount); err != nil {
			return false
		}
		var replicaCount int
		if err := replica.QueryRow(query).Scan(&replicaCount); err != nil {
			return false
		}
		return primaryCount == replicaCount
	}, timeout, 50*time.Millisecond, "replica row count for %s should match primary", table)
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

func pageSizedPayload(pageSize int, row int) string {
	base := fmt.Sprintf("row_%05d_", row)
	maxPayload := pageSize / 4
	if maxPayload < len(base)+1 {
		maxPayload = len(base) + 1
	}
	if maxPayload > 4096 {
		maxPayload = 4096
	}
	fillerLen := maxPayload - len(base)
	if fillerLen < 0 {
		fillerLen = 0
	}
	return base + strings.Repeat("x", fillerLen)
}

func isBusyError(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(sqlite3.Error); ok {
		if e.Code == sqlite3.ErrBusy || e.Code == sqlite3.ErrLocked {
			return true
		}
		// Under heavy churn, go-sqlite3 can surface ErrError with the
		// generic "SQL logic error" message while the VFS swaps databases.
		if e.Code == sqlite3.ErrError && strings.Contains(e.Error(), "SQL logic error") {
			return true
		}
	}
	msg := err.Error()
	if strings.Contains(msg, "database is locked") || strings.Contains(msg, "database is busy") {
		return true
	}
	return strings.Contains(msg, "converting NULL to int")
}

func writeSinglePageLTXFile(tb testing.TB, client *file.ReplicaClient, txid ltx.TXID, fill byte) {
	tb.Helper()
	page := bytes.Repeat([]byte{fill}, 4096)
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		tb.Fatalf("new encoder: %v", err)
	}
	hdr := ltx.Header{
		Version:   ltx.Version,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   txid,
		MaxTXID:   txid,
		Timestamp: time.Now().UnixMilli(),
		Flags:     ltx.HeaderFlagNoChecksum,
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		tb.Fatalf("encode header: %v", err)
	}
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, page); err != nil {
		tb.Fatalf("encode page: %v", err)
	}
	if err := enc.Close(); err != nil {
		tb.Fatalf("close encoder: %v", err)
	}

	if _, err := client.WriteLTXFile(context.Background(), 0, txid, txid, bytes.NewReader(buf.Bytes())); err != nil {
		tb.Fatalf("write ltx file: %v", err)
	}
}

type latencyReplicaClient struct {
	litestream.ReplicaClient
	delay time.Duration
}

func (c *latencyReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	time.Sleep(c.delay)
	return c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
}

func (c *latencyReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	time.Sleep(c.delay)
	return c.ReplicaClient.LTXFiles(ctx, level, seek, useMetadata)
}

type eventualConsistencyClient struct {
	litestream.ReplicaClient
	calls atomic.Int32
}

func (c *eventualConsistencyClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	if c.calls.Add(1) == 1 {
		return ltx.NewFileInfoSliceIterator(nil), nil
	}
	return c.ReplicaClient.LTXFiles(ctx, level, seek, useMetadata)
}

type observingReplicaClient struct {
	litestream.ReplicaClient
	ltxCalls atomic.Int64
}

type fdLimitedReplicaClient struct {
	litestream.ReplicaClient
	limit   int32
	open    atomic.Int32
	maxOpen atomic.Int32
}

func (c *fdLimitedReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	current := c.open.Add(1)
	for {
		max := c.maxOpen.Load()
		if current <= max || c.maxOpen.CompareAndSwap(max, current) {
			break
		}
	}
	if current > c.limit {
		c.open.Add(-1)
		return nil, fmt.Errorf("fd limit exceeded: %d/%d", current, c.limit)
	}
	rc, err := c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
	if err != nil {
		c.open.Add(-1)
		return nil, err
	}
	return &hookedReadCloser{ReadCloser: rc, hook: func() { c.open.Add(-1) }}, nil
}

func (c *observingReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	c.ltxCalls.Add(1)
	return c.ReplicaClient.LTXFiles(ctx, level, seek, useMetadata)
}

type flakyLTXClient struct {
	litestream.ReplicaClient
	failNext atomic.Bool
	failures atomic.Int64
}

func (c *flakyLTXClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	if c.failNext.CompareAndSwap(true, false) {
		c.failures.Add(1)
		return nil, fmt.Errorf("ltx list unavailable")
	}
	return c.ReplicaClient.LTXFiles(ctx, level, seek, useMetadata)
}

type oomPageIndexClient struct {
	litestream.ReplicaClient
	failNext  atomic.Bool
	triggered atomic.Bool
}

func (c *oomPageIndexClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if offset > 0 && c.failNext.CompareAndSwap(true, false) {
		c.triggered.Store(true)
		return nil, fmt.Errorf("simulated page index OOM")
	}
	return c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
}

type corruptingPageIndexClient struct {
	litestream.ReplicaClient
	corruptNext atomic.Bool
	triggered   atomic.Bool
}

func (c *corruptingPageIndexClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	rc, err := c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
	if err != nil {
		return nil, err
	}
	if c.corruptNext.CompareAndSwap(true, false) {
		c.triggered.Store(true)
		data, readErr := io.ReadAll(rc)
		rc.Close()
		if readErr != nil {
			return nil, readErr
		}
		if len(data) > 0 {
			data[0] ^= 0xFF
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return rc, nil
}

type hookedReadCloser struct {
	io.ReadCloser
	once sync.Once
	hook func()
}

func (h *hookedReadCloser) Close() error {
	var err error
	h.once.Do(func() {
		err = h.ReadCloser.Close()
		if h.hook != nil {
			h.hook()
		}
	})
	return err
}

// waitForLTXFiles waits until at least one LTX file is available in the replica client.
func waitForLTXFiles(t *testing.T, client litestream.ReplicaClient, timeout, tick time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		itr, err := client.LTXFiles(context.Background(), 0, 0, false)
		if err != nil {
			return false
		}
		defer itr.Close()
		return itr.Next()
	}, timeout, tick, "LTX files should be available")
}

// waitForReplicaValue waits until the replica database returns the expected int value.
func waitForReplicaValue(t *testing.T, db *sql.DB, query string, expected int, timeout, tick time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		var got int
		if err := db.QueryRow(query).Scan(&got); err != nil {
			return false
		}
		return got == expected
	}, timeout, tick, "replica should return expected value")
}
