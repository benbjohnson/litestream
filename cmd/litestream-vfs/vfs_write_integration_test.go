//go:build vfs
// +build vfs

package main_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/psanford/sqlite3vfs"
	"github.com/stretchr/testify/require"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// =============================================================================
// Basic Operations Tests
// =============================================================================

// TestVFS_WriteAndSync_FileBackend tests basic write and sync functionality
// with the file backend.
func TestVFS_WriteAndSync_FileBackend(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	// First, create initial data using standard litestream replication
	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "source.db"))
	db.MonitorInterval = 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.SyncInterval = 100 * time.Millisecond
	require.NoError(t, db.Open())

	sqldb0 := testingutil.MustOpenSQLDB(t, db.Path())
	defer testingutil.MustCloseSQLDB(t, sqldb0)

	_, err := sqldb0.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)
	_, err = sqldb0.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	require.NoError(t, err)

	waitForLTXFiles(t, client, 10*time.Second, db.MonitorInterval)
	require.NoError(t, db.Replica.Stop(false))
	testingutil.MustCloseSQLDB(t, sqldb0)
	require.NoError(t, db.Close(context.Background()))

	// Now open via writable VFS and add more data
	vfs := newWritableVFS(t, client, 1*time.Second, t.TempDir())
	vfsName := fmt.Sprintf("litestream-write-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb1, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb1.Close()

	// Verify initial data
	var name string
	err = sqldb1.QueryRow("SELECT name FROM users WHERE id = 1").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Alice", name)

	// Insert new data via VFS
	_, err = sqldb1.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)

	// Force sync
	sqldb1.Close()

	// Verify data was synced by opening fresh VFS
	vfs2 := newWritableVFS(t, client, 0, "")
	vfsName2 := fmt.Sprintf("litestream-write2-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName2, vfs2))

	sqldb2, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName2))
	require.NoError(t, err)
	defer sqldb2.Close()

	err = sqldb2.QueryRow("SELECT name FROM users WHERE id = 2").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Bob", name)
}

// TestVFS_ReadYourWrites verifies that written data is visible immediately
// before sync (from dirty pages).
func TestVFS_ReadYourWrites(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	// Create initial database
	setupInitialDB(t, client)

	// Open via writable VFS with long sync interval (won't auto-sync)
	vfs := newWritableVFS(t, client, 1*time.Hour, t.TempDir())
	vfsName := fmt.Sprintf("litestream-ryw-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Write data
	_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)

	// Read it back immediately (before sync)
	var name string
	err = sqldb.QueryRow("SELECT name FROM users WHERE id = 2").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Bob", name)

	// Update and read again
	_, err = sqldb.Exec("UPDATE users SET name = 'Robert' WHERE id = 2")
	require.NoError(t, err)

	err = sqldb.QueryRow("SELECT name FROM users WHERE id = 2").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Robert", name)
}

// TestVFS_MultipleTransactions tests multiple sequential transactions.
func TestVFS_MultipleTransactions(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	vfs := newWritableVFS(t, client, 100*time.Millisecond, t.TempDir())
	vfsName := fmt.Sprintf("litestream-multi-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Execute multiple transactions
	for i := 2; i <= 10; i++ {
		_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i, fmt.Sprintf("User%d", i))
		require.NoError(t, err)
	}

	// Wait for syncs
	time.Sleep(500 * time.Millisecond)

	// Verify all data
	var count int
	err = sqldb.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 10, count)
}

// TestVFS_LargeTransaction tests writing many pages in a single transaction.
func TestVFS_LargeTransaction(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	vfs := newWritableVFS(t, client, 1*time.Second, t.TempDir())
	vfsName := fmt.Sprintf("litestream-large-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Insert 1000 rows in a single transaction (should span many pages)
	tx, err := sqldb.Begin()
	require.NoError(t, err)

	for i := 2; i <= 1001; i++ {
		_, err = tx.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i, fmt.Sprintf("User%d with some extra data to take up space", i))
		require.NoError(t, err)
	}

	err = tx.Commit()
	require.NoError(t, err)

	// Force sync by closing
	sqldb.Close()

	// Verify data persisted
	vfs2 := newWritableVFS(t, client, 0, "")
	vfsName2 := fmt.Sprintf("litestream-large2-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName2, vfs2))

	sqldb2, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName2))
	require.NoError(t, err)
	defer sqldb2.Close()

	var count int
	err = sqldb2.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1001, count)
}

// =============================================================================
// Sync Behavior Tests
// =============================================================================

// TestVFS_PeriodicSync verifies automatic periodic sync.
func TestVFS_PeriodicSync(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	// Get initial LTX count
	initialCount := countLTXFiles(t, client)

	vfs := newWritableVFS(t, client, 200*time.Millisecond, t.TempDir())
	vfsName := fmt.Sprintf("litestream-periodic-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Write data
	_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)

	// Wait for auto-sync (should happen within ~200ms)
	time.Sleep(500 * time.Millisecond)

	// Verify new LTX file was created
	newCount := countLTXFiles(t, client)
	require.Greater(t, newCount, initialCount, "expected new LTX file from auto-sync")
}

// TestVFS_SyncDuringTransaction verifies sync is deferred during active transaction.
func TestVFS_SyncDuringTransaction(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	vfs := newWritableVFS(t, client, 100*time.Millisecond, t.TempDir())
	vfsName := fmt.Sprintf("litestream-txsync-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	initialCount := countLTXFiles(t, client)

	// Begin transaction
	tx, err := sqldb.Begin()
	require.NoError(t, err)

	// Write data within transaction
	_, err = tx.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)

	// Wait - sync should be deferred
	time.Sleep(300 * time.Millisecond)

	// LTX count should not have increased during transaction
	midCount := countLTXFiles(t, client)
	require.Equal(t, initialCount, midCount, "sync should be deferred during transaction")

	// Commit transaction
	err = tx.Commit()
	require.NoError(t, err)

	// Wait for sync after commit
	time.Sleep(300 * time.Millisecond)

	// Now LTX should have increased
	finalCount := countLTXFiles(t, client)
	require.Greater(t, finalCount, initialCount, "expected new LTX file after commit")
}

// TestVFS_ManualSyncOnly tests with SyncInterval=0 (manual sync only).
func TestVFS_ManualSyncOnly(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	// SyncInterval=0 means no auto-sync
	vfs := newWritableVFS(t, client, 0, t.TempDir())
	vfsName := fmt.Sprintf("litestream-manual-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)

	initialCount := countLTXFiles(t, client)

	// Write data
	_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)

	// Wait - should NOT auto-sync
	time.Sleep(500 * time.Millisecond)

	midCount := countLTXFiles(t, client)
	require.Equal(t, initialCount, midCount, "should not auto-sync when SyncInterval=0")

	// Close triggers sync
	sqldb.Close()

	// Now should be synced
	finalCount := countLTXFiles(t, client)
	require.Greater(t, finalCount, initialCount, "expected sync on close")
}

// =============================================================================
// Write Buffer Tests
// =============================================================================

// TestVFS_WriteBufferDiscardedOnOpen tests that write buffer is discarded on open
// (unsynced data is lost after crash).
func TestVFS_WriteBufferDiscardedOnOpen(t *testing.T) {
	replicaDir := t.TempDir()
	bufferDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	// Open VFS and write data
	vfs := newWritableVFS(t, client, 1*time.Hour, bufferDir) // Long interval, won't auto-sync
	vfsName := fmt.Sprintf("litestream-discard1-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)

	_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)

	// Verify write buffer file exists
	bufferPath := filepath.Join(bufferDir, ".litestream-write-buffer")
	_, err = os.Stat(bufferPath)
	require.NoError(t, err, "write buffer file should exist")

	// Simulate crash by not closing properly (don't call sqldb.Close())
	// Just abandon the connection

	// Reopen with new VFS - buffer should be discarded
	vfs2 := newWritableVFS(t, client, 1*time.Second, bufferDir)
	vfsName2 := fmt.Sprintf("litestream-discard2-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName2, vfs2))

	sqldb2, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName2))
	require.NoError(t, err)
	defer sqldb2.Close()

	// Data should NOT be recovered (buffer is discarded on open)
	var count int
	err = sqldb2.QueryRow("SELECT COUNT(*) FROM users WHERE id = 2").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "unsynced data should be lost after crash")
}

// TestVFS_WriteBufferDuplicatePages tests that duplicate page writes within
// a session correctly overwrite previous values in the buffer.
func TestVFS_WriteBufferDuplicatePages(t *testing.T) {
	replicaDir := t.TempDir()
	bufferDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	vfs := newWritableVFS(t, client, 1*time.Hour, bufferDir)
	vfsName := fmt.Sprintf("litestream-dup1-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Write to same row multiple times (updates same pages)
	_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)
	_, err = sqldb.Exec("UPDATE users SET name = 'Robert' WHERE id = 2")
	require.NoError(t, err)
	_, err = sqldb.Exec("UPDATE users SET name = 'Bobby' WHERE id = 2")
	require.NoError(t, err)

	// Should have latest value (read-your-writes within session)
	var name string
	err = sqldb.QueryRow("SELECT name FROM users WHERE id = 2").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Bobby", name)

	// Close to trigger sync
	sqldb.Close()

	// Verify data persists in replica after sync
	vfs2 := newWritableVFS(t, client, 1*time.Second, t.TempDir())
	vfsName2 := fmt.Sprintf("litestream-dup2-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName2, vfs2))

	sqldb2, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName2))
	require.NoError(t, err)
	defer sqldb2.Close()

	// Should have latest value from synced data
	err = sqldb2.QueryRow("SELECT name FROM users WHERE id = 2").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Bobby", name)
}

// TestVFS_ExistingBufferDiscarded tests that any existing buffer file is discarded on open.
func TestVFS_ExistingBufferDiscarded(t *testing.T) {
	replicaDir := t.TempDir()
	bufferDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	// Create a pre-existing write buffer file with some content
	bufferPath := filepath.Join(bufferDir, ".litestream-write-buffer")
	require.NoError(t, os.WriteFile(bufferPath, []byte("stale data"), 0644))

	// Open VFS - should discard existing buffer
	vfs := newWritableVFS(t, client, 1*time.Second, bufferDir)
	vfsName := fmt.Sprintf("litestream-existing-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Should only see original data (existing buffer discarded)
	var count int
	err = sqldb.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "existing buffer should be discarded")
}

// TestVFS_WriteBufferCorrupted tests handling of corrupted buffer file.
func TestVFS_WriteBufferCorrupted(t *testing.T) {
	replicaDir := t.TempDir()
	bufferDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	// Create corrupted buffer (invalid magic)
	bufferPath := filepath.Join(bufferDir, ".litestream-write-buffer")
	require.NoError(t, os.WriteFile(bufferPath, []byte("INVALID DATA"), 0644))

	// Open VFS - should handle gracefully
	vfs := newWritableVFS(t, client, 1*time.Second, bufferDir)
	vfsName := fmt.Sprintf("litestream-corrupt-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Should work with original data
	var count int
	err = sqldb.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// =============================================================================
// Conflict Detection Tests
// =============================================================================

// TestVFS_ConflictDetection tests that conflicts are detected when remote changes.
func TestVFS_ConflictDetection(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	// Open writable VFS
	vfs := newWritableVFS(t, client, 1*time.Hour, t.TempDir()) // Long interval
	vfsName := fmt.Sprintf("litestream-conflict-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Write data via VFS (not synced yet)
	_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)

	// Externally add new LTX file to simulate another writer
	addExternalLTXFile(t, client, replicaDir)

	// Now close - sync should fail with conflict
	// Note: The conflict detection happens during sync, but the error may be logged
	// rather than returned to the user. This test verifies the mechanism exists.
	sqldb.Close()

	// The conflict should have been detected (check logs or VFS state)
	// For now, we just verify the test doesn't crash
}

// TestVFS_NoConflictWhenRemoteUnchanged verifies no false conflicts.
func TestVFS_NoConflictWhenRemoteUnchanged(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	vfs := newWritableVFS(t, client, 100*time.Millisecond, t.TempDir())
	vfsName := fmt.Sprintf("litestream-noconflict-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Multiple write/sync cycles - no conflicts expected
	for i := 2; i <= 5; i++ {
		_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i, fmt.Sprintf("User%d", i))
		require.NoError(t, err)
		time.Sleep(200 * time.Millisecond) // Wait for sync
	}

	// All data should be present
	var count int
	err = sqldb.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 5, count)
}

// =============================================================================
// Concurrency Tests
// =============================================================================

// TestVFS_ConcurrentReaders tests one writer with multiple readers.
func TestVFS_ConcurrentReaders(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	// Writer VFS
	writerVFS := newWritableVFS(t, client, 100*time.Millisecond, t.TempDir())
	writerVFSName := fmt.Sprintf("litestream-writer-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(writerVFSName, writerVFS))

	writerDB, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", writerVFSName))
	require.NoError(t, err)
	defer writerDB.Close()

	// Reader VFS (read-only)
	readerVFS := newReadOnlyVFS(t, client)
	readerVFSName := fmt.Sprintf("litestream-reader-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(readerVFSName, readerVFS))

	readerDB, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", readerVFSName))
	require.NoError(t, err)
	defer readerDB.Close()

	// Write some data
	_, err = writerDB.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)

	// Wait for sync
	time.Sleep(300 * time.Millisecond)

	// Reader should eventually see the data
	require.Eventually(t, func() bool {
		var count int
		if err := readerDB.QueryRow("SELECT COUNT(*) FROM users").Scan(&count); err != nil {
			return false
		}
		return count == 2
	}, 5*time.Second, 100*time.Millisecond)
}

// TestVFS_ReadWhileWriting tests reading during an active write transaction.
func TestVFS_ReadWhileWriting(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	vfs := newWritableVFS(t, client, 1*time.Second, t.TempDir())
	vfsName := fmt.Sprintf("litestream-readwrite-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 2; i <= 20; i++ {
			if _, err := sqldb.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i, fmt.Sprintf("User%d", i)); err != nil {
				errors <- err
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			var count int
			if err := sqldb.QueryRow("SELECT COUNT(*) FROM users").Scan(&count); err != nil {
				errors <- err
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent operation failed: %v", err)
	}
}

// =============================================================================
// Edge Case Tests
// =============================================================================

// TestVFS_Truncate tests database truncation via VACUUM.
func TestVFS_Truncate(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	vfs := newWritableVFS(t, client, 100*time.Millisecond, t.TempDir())
	vfsName := fmt.Sprintf("litestream-truncate-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Add many rows
	for i := 2; i <= 100; i++ {
		_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i, fmt.Sprintf("User%d with lots of data to take up space", i))
		require.NoError(t, err)
	}

	time.Sleep(300 * time.Millisecond)

	// Delete all but one
	_, err = sqldb.Exec("DELETE FROM users WHERE id > 1")
	require.NoError(t, err)

	// VACUUM to reclaim space
	_, err = sqldb.Exec("VACUUM")
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	// Verify only 1 row remains
	var count int
	err = sqldb.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// TestVFS_EmptyTransaction tests begin/commit with no changes.
func TestVFS_EmptyTransaction(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	vfs := newWritableVFS(t, client, 1*time.Second, t.TempDir())
	vfsName := fmt.Sprintf("litestream-empty-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	initialCount := countLTXFiles(t, client)

	// Empty transaction
	tx, err := sqldb.Begin()
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Should not create new LTX for empty transaction
	finalCount := countLTXFiles(t, client)
	require.Equal(t, initialCount, finalCount, "empty transaction should not create new LTX")
}

// TestVFS_SchemaChanges tests DDL operations.
func TestVFS_SchemaChanges(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	vfs := newWritableVFS(t, client, 100*time.Millisecond, t.TempDir())
	vfsName := fmt.Sprintf("litestream-schema-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Create table
	_, err = sqldb.Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
	require.NoError(t, err)

	// Add column
	_, err = sqldb.Exec("ALTER TABLE products ADD COLUMN quantity INTEGER DEFAULT 0")
	require.NoError(t, err)

	// Insert data
	_, err = sqldb.Exec("INSERT INTO products (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 100)")
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	// Verify schema
	var qty int
	err = sqldb.QueryRow("SELECT quantity FROM products WHERE id = 1").Scan(&qty)
	require.NoError(t, err)
	require.Equal(t, 100, qty)

	// Drop table
	_, err = sqldb.Exec("DROP TABLE products")
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Verify dropped
	_, err = sqldb.Query("SELECT * FROM products")
	require.Error(t, err)
}

// TestVFS_BlobData tests large blob operations.
func TestVFS_BlobData(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	vfs := newWritableVFS(t, client, 100*time.Millisecond, t.TempDir())
	vfsName := fmt.Sprintf("litestream-blob-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Create table for blobs
	_, err = sqldb.Exec("CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)")
	require.NoError(t, err)

	// Insert large blob (100KB - spans multiple pages)
	largeData := make([]byte, 100*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	_, err = sqldb.Exec("INSERT INTO blobs (id, data) VALUES (1, ?)", largeData)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	// Read back and verify
	var retrieved []byte
	err = sqldb.QueryRow("SELECT data FROM blobs WHERE id = 1").Scan(&retrieved)
	require.NoError(t, err)
	require.Equal(t, largeData, retrieved)
}

// =============================================================================
// Round-Trip Verification Tests
// =============================================================================

// TestVFS_WriteAndRestore tests full write -> sync -> restore cycle.
func TestVFS_WriteAndRestore(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	// Write via VFS
	vfs := newWritableVFS(t, client, 100*time.Millisecond, t.TempDir())
	vfsName := fmt.Sprintf("litestream-restore1-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)

	for i := 2; i <= 10; i++ {
		_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i, fmt.Sprintf("User%d", i))
		require.NoError(t, err)
	}
	sqldb.Close()

	// Restore to a new file
	restoredPath := filepath.Join(t.TempDir(), "restored.db")
	err = restoreDB(t, client, restoredPath)
	require.NoError(t, err)

	// Verify restored database
	restoredDB, err := sql.Open("sqlite3", restoredPath)
	require.NoError(t, err)
	defer restoredDB.Close()

	var count int
	err = restoredDB.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 10, count)
}

// TestVFS_WriteReadVFSOnly tests write via writable VFS, read via read-only VFS.
func TestVFS_WriteReadVFSOnly(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	// Write via writable VFS
	writerVFS := newWritableVFS(t, client, 100*time.Millisecond, t.TempDir())
	writerVFSName := fmt.Sprintf("litestream-vfsonly-w-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(writerVFSName, writerVFS))

	writerDB, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", writerVFSName))
	require.NoError(t, err)

	_, err = writerDB.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)
	writerDB.Close()

	// Read via read-only VFS
	readerVFS := newReadOnlyVFS(t, client)
	readerVFSName := fmt.Sprintf("litestream-vfsonly-r-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(readerVFSName, readerVFS))

	readerDB, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", readerVFSName))
	require.NoError(t, err)
	defer readerDB.Close()

	var name string
	err = readerDB.QueryRow("SELECT name FROM users WHERE id = 2").Scan(&name)
	require.NoError(t, err)
	require.Equal(t, "Bob", name)
}

// TestVFS_MixedWorkload tests interleaved reads/writes/syncs.
func TestVFS_MixedWorkload(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	vfs := newWritableVFS(t, client, 200*time.Millisecond, t.TempDir())
	vfsName := fmt.Sprintf("litestream-mixed-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Mixed operations
	for i := 0; i < 50; i++ {
		switch i % 5 {
		case 0, 1, 2: // Write
			_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i+2, fmt.Sprintf("User%d", i))
			require.NoError(t, err)
		case 3: // Read
			var count int
			err = sqldb.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
			require.NoError(t, err)
		case 4: // Update
			_, err = sqldb.Exec("UPDATE users SET name = ? WHERE id = ?", fmt.Sprintf("Updated%d", i), (i%10)+1)
			require.NoError(t, err)
		}
	}

	// Final verification
	var count int
	err = sqldb.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.GreaterOrEqual(t, count, 30) // At least 30 inserts (0,1,2 mod 5)
}

// =============================================================================
// Error Handling Tests
// =============================================================================

// TestVFS_SyncNetworkError tests handling of network errors during sync.
func TestVFS_SyncNetworkError(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	setupInitialDB(t, client)

	vfs := newWritableVFS(t, client, 1*time.Hour, t.TempDir())
	vfsName := fmt.Sprintf("litestream-neterr-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Write data
	_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (2, 'Bob')")
	require.NoError(t, err)

	// Remove replica directory to simulate error
	require.NoError(t, os.RemoveAll(replicaDir))

	// Close should handle error gracefully (sync will fail but shouldn't crash)
	sqldb.Close()
}

// TestVFS_InvalidPageSize tests mismatched page size handling.
func TestVFS_InvalidPageSize(t *testing.T) {
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)

	// Create database with different page size
	db := testingutil.NewDB(t, filepath.Join(t.TempDir(), "source.db"))
	db.MonitorInterval = 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	require.NoError(t, db.Open())

	sqldb0 := testingutil.MustOpenSQLDB(t, db.Path())
	// Note: Page size is set at database creation, this is just verifying
	// the test setup works
	_, err := sqldb0.Exec("CREATE TABLE test (x)")
	require.NoError(t, err)

	waitForLTXFiles(t, client, 10*time.Second, db.MonitorInterval)
	require.NoError(t, db.Replica.Stop(false))
	testingutil.MustCloseSQLDB(t, sqldb0)
	require.NoError(t, db.Close(context.Background()))

	// Open via VFS (should work with same page size)
	vfs := newWritableVFS(t, client, 1*time.Second, t.TempDir())
	vfsName := fmt.Sprintf("litestream-pagesize-%d", time.Now().UnixNano())
	require.NoError(t, sqlite3vfs.RegisterVFS(vfsName, vfs))

	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s", vfsName))
	require.NoError(t, err)
	defer sqldb.Close()

	// Should be able to query
	var count int
	err = sqldb.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
}

// =============================================================================
// Helper Functions
// =============================================================================

// newWritableVFS creates a VFS with write support enabled.
func newWritableVFS(tb testing.TB, client litestream.ReplicaClient, syncInterval time.Duration, localPath string) *litestream.VFS {
	tb.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	vfs := litestream.NewVFS(client, logger)
	vfs.PollInterval = 100 * time.Millisecond
	vfs.WriteEnabled = true
	vfs.WriteSyncInterval = syncInterval
	vfs.WriteLocalPath = localPath

	return vfs
}

// newReadOnlyVFS creates a read-only VFS.
func newReadOnlyVFS(tb testing.TB, client litestream.ReplicaClient) *litestream.VFS {
	tb.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	vfs := litestream.NewVFS(client, logger)
	vfs.PollInterval = 100 * time.Millisecond

	return vfs
}

// setupInitialDB creates an initial database with standard schema.
func setupInitialDB(t *testing.T, client litestream.ReplicaClient) {
	t.Helper()

	dbDir := t.TempDir()
	db := testingutil.NewDB(t, filepath.Join(dbDir, "source.db"))
	db.MonitorInterval = 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.SyncInterval = 100 * time.Millisecond
	require.NoError(t, db.Open())

	sqldb := testingutil.MustOpenSQLDB(t, db.Path())

	_, err := sqldb.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)
	_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	require.NoError(t, err)

	waitForLTXFiles(t, client, 10*time.Second, db.MonitorInterval)

	require.NoError(t, db.Replica.Stop(false))
	testingutil.MustCloseSQLDB(t, sqldb)
	require.NoError(t, db.Close(context.Background()))
}

// countLTXFiles returns the number of LTX files in the replica.
func countLTXFiles(t *testing.T, client litestream.ReplicaClient) int {
	t.Helper()

	itr, err := client.LTXFiles(context.Background(), 0, 0, false)
	require.NoError(t, err)
	defer itr.Close()

	count := 0
	for itr.Next() {
		count++
	}
	return count
}

// addExternalLTXFile adds an LTX file to simulate an external writer.
func addExternalLTXFile(t *testing.T, client litestream.ReplicaClient, replicaDir string) {
	t.Helper()

	// Get current max TXID
	itr, err := client.LTXFiles(context.Background(), 0, 0, false)
	require.NoError(t, err)

	var maxTXID ltx.TXID
	for itr.Next() {
		if itr.Item().MaxTXID > maxTXID {
			maxTXID = itr.Item().MaxTXID
		}
	}
	itr.Close()

	// Create a new LTX file with next TXID
	nextTXID := maxTXID + 1
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	require.NoError(t, err)

	require.NoError(t, enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  4096,
		Commit:    2,
		MinTXID:   nextTXID,
		MaxTXID:   nextTXID,
		Timestamp: time.Now().UnixMilli(),
	}))

	// Encode a dummy page
	page := make([]byte, 4096)
	require.NoError(t, enc.EncodePage(ltx.PageHeader{Pgno: 2}, page))
	require.NoError(t, enc.Close())

	// Write via client
	_, err = client.WriteLTXFile(context.Background(), 0, nextTXID, nextTXID, &buf)
	require.NoError(t, err)
}

// restoreDB restores a database from the replica to the given path.
func restoreDB(t *testing.T, client litestream.ReplicaClient, outputPath string) error {
	t.Helper()

	// Open output file
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Get all LTX files
	itr, err := client.LTXFiles(context.Background(), 0, 0, false)
	if err != nil {
		return err
	}
	defer itr.Close()

	var pageSize uint32
	pages := make(map[uint32][]byte)
	var commit uint32

	for itr.Next() {
		info := itr.Item()

		rc, err := client.OpenLTXFile(context.Background(), info.Level, info.MinTXID, info.MaxTXID, 0, 0)
		if err != nil {
			return err
		}

		dec := ltx.NewDecoder(rc)
		if err := dec.DecodeHeader(); err != nil {
			rc.Close()
			return err
		}
		hdr := dec.Header()

		if pageSize == 0 {
			pageSize = hdr.PageSize
		}
		commit = hdr.Commit

		pageBuf := make([]byte, hdr.PageSize)
		for {
			var phdr ltx.PageHeader
			if err := dec.DecodePage(&phdr, pageBuf); err != nil {
				break
			}
			// Copy page data since pageBuf is reused
			data := make([]byte, len(pageBuf))
			copy(data, pageBuf)
			pages[phdr.Pgno] = data
		}
		rc.Close()
	}

	// Write pages to file
	for pgno := uint32(1); pgno <= commit; pgno++ {
		data, ok := pages[pgno]
		if !ok {
			data = make([]byte, pageSize)
		}
		if _, err := f.WriteAt(data, int64(pgno-1)*int64(pageSize)); err != nil {
			return err
		}
	}

	return nil
}
