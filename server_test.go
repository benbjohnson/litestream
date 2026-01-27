package litestream_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

var testSocketCounter uint64

func testSocketPath(t *testing.T) string {
	t.Helper()
	n := atomic.AddUint64(&testSocketCounter, 1)
	path := fmt.Sprintf("/tmp/ls-test-%d.sock", n)
	t.Cleanup(func() { os.Remove(path) })
	return path
}

func TestServer_HandleInfo(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
	store.CompactionMonitorEnabled = false
	require.NoError(t, store.Open(t.Context()))
	defer store.Close(t.Context())

	server := litestream.NewServer(store)
	server.SocketPath = testSocketPath(t)
	server.Version = "v1.0.0-test"
	require.NoError(t, server.Start())
	defer server.Close()

	client := newSocketClient(t, server.SocketPath)
	resp, err := client.Get("http://localhost/info")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result litestream.InfoResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))

	require.Equal(t, "v1.0.0-test", result.Version)
	require.Greater(t, result.PID, 0)
	require.Equal(t, 1, result.DatabaseCount)
	require.False(t, result.StartedAt.IsZero())
	require.GreaterOrEqual(t, result.UptimeSeconds, int64(0))
}

func TestServer_HandleList(t *testing.T) {
	t.Run("EmptyStore", func(t *testing.T) {
		store := litestream.NewStore(nil, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		resp, err := client.Get("http://localhost/list")
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.ListResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Empty(t, result.Databases)
	})

	t.Run("WithDatabases", func(t *testing.T) {
		db1, sqldb1 := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db1, sqldb1)

		db2, sqldb2 := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db2, sqldb2)

		store := litestream.NewStore([]*litestream.DB{db1, db2}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		resp, err := client.Get("http://localhost/list")
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.ListResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Databases, 2)

		// Verify both databases are listed (order may vary).
		paths := make(map[string]string)
		for _, db := range result.Databases {
			paths[db.Path] = db.Status
		}
		require.Contains(t, paths, db1.Path())
		require.Contains(t, paths, db2.Path())
	})

	t.Run("StatusOpenWhenMonitorDisabled", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		// MonitorEnabled is false by default in test helper.
		require.False(t, db.Replica.MonitorEnabled)

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		resp, err := client.Get("http://localhost/list")
		require.NoError(t, err)
		defer resp.Body.Close()

		var result litestream.ListResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Databases, 1)

		// Since MonitorEnabled is false, status should be "open" not "replicating".
		require.Equal(t, "open", result.Databases[0].Status)
	})

	t.Run("StatusReplicatingWhenMonitorEnabled", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		// Enable the monitor to simulate active replication.
		db.Replica.MonitorEnabled = true

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		resp, err := client.Get("http://localhost/list")
		require.NoError(t, err)
		defer resp.Body.Close()

		var result litestream.ListResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Databases, 1)

		// Since MonitorEnabled is true, status should be "replicating".
		require.Equal(t, "replicating", result.Databases[0].Status)
	})

	t.Run("IncludesLastSyncAt", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		// Create some data and sync.
		_, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`)
		require.NoError(t, err)
		_, err = sqldb.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (1)`)
		require.NoError(t, err)
		require.NoError(t, db.Sync(t.Context()))
		require.NoError(t, db.Replica.Sync(t.Context()))

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		resp, err := client.Get("http://localhost/list")
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.ListResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Databases, 1)
		require.NotNil(t, result.Databases[0].LastSyncAt, "LastSyncAt should be set after sync")
	})
}

func TestServer_HandleStart(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
	store.CompactionMonitorEnabled = false
	require.NoError(t, store.Open(t.Context()))
	defer store.Close(t.Context())

	server := litestream.NewServer(store)
	server.SocketPath = testSocketPath(t)
	require.NoError(t, server.Start())
	defer server.Close()

	t.Run("MissingPath", func(t *testing.T) {
		client := newSocketClient(t, server.SocketPath)
		resp, err := client.Post("http://localhost/start", "application/json", nil)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("DatabaseNotFound", func(t *testing.T) {
		client := newSocketClient(t, server.SocketPath)
		body := `{"path": "/nonexistent/db"}`
		resp, err := client.Post("http://localhost/start", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})
}

func TestServer_HandleStop(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
	store.CompactionMonitorEnabled = false
	require.NoError(t, store.Open(t.Context()))
	defer store.Close(t.Context())

	server := litestream.NewServer(store)
	server.SocketPath = testSocketPath(t)
	require.NoError(t, server.Start())
	defer server.Close()

	t.Run("MissingPath", func(t *testing.T) {
		client := newSocketClient(t, server.SocketPath)
		resp, err := client.Post("http://localhost/stop", "application/json", nil)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}

func TestServer_HandleRegister(t *testing.T) {
	t.Run("MissingPath", func(t *testing.T) {
		store := litestream.NewStore(nil, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		body := `{"replica_url": "file:///tmp/backup"}`
		resp, err := client.Post("http://localhost/register", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result litestream.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "path required", result.Error)
	})

	t.Run("MissingReplicaURL", func(t *testing.T) {
		store := litestream.NewStore(nil, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		body := `{"path": "/tmp/test.db"}`
		resp, err := client.Post("http://localhost/register", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result litestream.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "replica_url required", result.Error)
	})

	t.Run("InvalidReplicaURL", func(t *testing.T) {
		store := litestream.NewStore(nil, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		body := `{"path": "/tmp/test.db", "replica_url": "invalid://badscheme"}`
		resp, err := client.Post("http://localhost/register", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result litestream.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Contains(t, result.Error, "invalid replica url")
	})

	t.Run("Success", func(t *testing.T) {
		store := litestream.NewStore(nil, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		// Create a temporary database file.
		db, sqldb := testingutil.MustOpenDBs(t)
		testingutil.MustCloseDBs(t, db, sqldb)
		dbPath := db.Path()

		// Create a temp directory for backup.
		backupDir := t.TempDir()

		client := newSocketClient(t, server.SocketPath)
		body := fmt.Sprintf(`{"path": %q, "replica_url": "file://%s"}`, dbPath, backupDir)
		resp, err := client.Post("http://localhost/register", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.RegisterDatabaseResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "registered", result.Status)
		require.Equal(t, dbPath, result.Path)

		// Verify database was registered with store.
		require.Len(t, store.DBs(), 1)
		require.Equal(t, dbPath, store.DBs()[0].Path())
	})

	t.Run("AlreadyExists", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		// Try to register the same database again.
		backupDir := t.TempDir()
		client := newSocketClient(t, server.SocketPath)
		body := fmt.Sprintf(`{"path": %q, "replica_url": "file://%s"}`, db.Path(), backupDir)
		resp, err := client.Post("http://localhost/register", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.RegisterDatabaseResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "already_exists", result.Status)
	})
}

func TestServer_HandleUnregister(t *testing.T) {
	t.Run("MissingPath", func(t *testing.T) {
		store := litestream.NewStore(nil, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		body := `{}`
		resp, err := client.Post("http://localhost/unregister", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result litestream.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "path required", result.Error)
	})

	t.Run("NotFoundIsIdempotent", func(t *testing.T) {
		store := litestream.NewStore(nil, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		body := `{"path": "/nonexistent/db"}`
		resp, err := client.Post("http://localhost/unregister", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		// UnregisterDB is idempotent - returns success even if DB not found.
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.UnregisterDatabaseResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "not_registered", result.Status)
		require.Zero(t, result.TXID)
	})

	t.Run("Success", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)
		dbPath := db.Path()

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		require.Len(t, store.DBs(), 1)

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		body := fmt.Sprintf(`{"path": %q}`, dbPath)
		resp, err := client.Post("http://localhost/unregister", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.UnregisterDatabaseResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "unregistered", result.Status)
		require.Equal(t, dbPath, result.Path)

		// Verify database was unregistered from store.
		require.Empty(t, store.DBs())
	})
}

func TestServer_HandleSync(t *testing.T) {
	t.Run("MissingPath", func(t *testing.T) {
		store := litestream.NewStore(nil, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		body := `{}`
		resp, err := client.Post("http://localhost/sync", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusBadRequest, resp.StatusCode)

		var result litestream.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "path required", result.Error)
	})

	t.Run("DatabaseNotFound", func(t *testing.T) {
		store := litestream.NewStore(nil, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		body := `{"path": "/nonexistent/db"}`
		resp, err := client.Post("http://localhost/sync", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("Success", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		_, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`)
		require.NoError(t, err)

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		body := fmt.Sprintf(`{"path": %q}`, db.Path())
		resp, err := client.Post("http://localhost/sync", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.SyncResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "synced_local", result.Status)
		require.Equal(t, db.Path(), result.Path)
		require.Greater(t, result.TXID, uint64(0))
	})

	t.Run("SuccessWithWait", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		_, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`)
		require.NoError(t, err)
		_, err = sqldb.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (1)`)
		require.NoError(t, err)

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		body := fmt.Sprintf(`{"path": %q, "wait": true, "timeout": 30}`, db.Path())
		resp, err := client.Post("http://localhost/sync", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.SyncResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "synced", result.Status)
		require.Equal(t, db.Path(), result.Path)
		require.Greater(t, result.TXID, uint64(0))
		require.Greater(t, result.ReplicatedTXID, uint64(0))
	})

	t.Run("NoChange", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		_, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`)
		require.NoError(t, err)

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		body := fmt.Sprintf(`{"path": %q}`, db.Path())

		// First sync should pick up the CREATE TABLE.
		resp1, err := client.Post("http://localhost/sync", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp1.Body.Close()
		require.Equal(t, http.StatusOK, resp1.StatusCode)

		var result1 litestream.SyncResponse
		require.NoError(t, json.NewDecoder(resp1.Body).Decode(&result1))
		require.Equal(t, "synced_local", result1.Status)
		require.Greater(t, result1.TXID, uint64(0))

		// Second sync with no new writes should return no_change.
		resp2, err := client.Post("http://localhost/sync", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp2.Body.Close()
		require.Equal(t, http.StatusOK, resp2.StatusCode)

		var result2 litestream.SyncResponse
		require.NoError(t, json.NewDecoder(resp2.Body).Decode(&result2))
		require.Equal(t, "no_change", result2.Status)
		require.Equal(t, result1.TXID, result2.TXID)
	})
}

func TestServer_HandleSyncStatus(t *testing.T) {
	t.Run("AllDatabases", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		resp, err := client.Get("http://localhost/debug/sync-status")
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.SyncDiagnosticsResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Len(t, result.Databases, 1)
		require.Equal(t, db.Path(), result.Databases[0].Path)
		require.False(t, result.Databases[0].Active)
	})

	t.Run("DatabaseNotFound", func(t *testing.T) {
		store := litestream.NewStore(nil, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		resp, err := client.Get("http://localhost/debug/sync-status?path=/nonexistent/db")
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}

func newSocketClient(t *testing.T, socketPath string) *http.Client {
	t.Helper()
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.DialTimeout("unix", socketPath, 10*time.Second)
			},
		},
	}
}

type stringReaderType struct {
	s string
	i int
}

func stringReader(s string) *stringReaderType {
	return &stringReaderType{s: s}
}

func (r *stringReaderType) Read(p []byte) (n int, err error) {
	if r.i >= len(r.s) {
		return 0, io.EOF
	}
	n = copy(p, r.s[r.i:])
	r.i += n
	return n, nil
}

// shortSocketPath returns a short socket path that won't exceed Unix socket limits.
// Unix sockets have a max path length of ~104 chars on macOS.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ls")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func TestServer_HandleMonitor(t *testing.T) {
	t.Run("StreamsInitialFullStatus", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore([]*litestream.DB{db}, levels)
		store.CompactionMonitorEnabled = false

		if err := store.Open(t.Context()); err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer store.Close(t.Context())

		// Create and sync data
		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := db.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Create server with socket (use short path to avoid Unix socket length limits)
		socketPath := shortSocketPath(t)
		server := litestream.NewServer(store)
		server.SocketPath = socketPath

		if err := server.Start(); err != nil {
			t.Fatalf("start server: %v", err)
		}
		defer server.Close()

		// Connect via Unix socket
		client := &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", socketPath)
				},
			},
		}

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost/monitor", nil)
		if err != nil {
			t.Fatalf("create request: %v", err)
		}

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", resp.StatusCode)
		}

		contentType := resp.Header.Get("Content-Type")
		if contentType != "application/x-ndjson" {
			t.Errorf("expected Content-Type 'application/x-ndjson', got %q", contentType)
		}

		// Read initial full status event (one event per database)
		scanner := bufio.NewScanner(resp.Body)
		if !scanner.Scan() {
			t.Fatal("expected to read first event")
		}

		var event litestream.StatusEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}

		if event.Type != "full" {
			t.Errorf("expected event type 'full', got %q", event.Type)
		}
		if event.Database == nil {
			t.Fatal("expected non-nil database in full event")
		}
		if event.Database.Path != db.Path() {
			t.Errorf("expected path %q, got %q", db.Path(), event.Database.Path)
		}
	})

	t.Run("StreamsSyncEvents", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore([]*litestream.DB{db}, levels)
		store.CompactionMonitorEnabled = false

		if err := store.Open(t.Context()); err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer store.Close(t.Context())

		// Create server with socket (use short path to avoid Unix socket length limits)
		socketPath := shortSocketPath(t)
		server := litestream.NewServer(store)
		server.SocketPath = socketPath

		if err := server.Start(); err != nil {
			t.Fatalf("start server: %v", err)
		}
		defer server.Close()

		// Wire up AfterSync callback
		db.Replica.AfterSync = func(pos ltx.Pos) {
			server.StatusMonitor.NotifySync(db, pos)
		}

		// Connect via Unix socket
		client := &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", socketPath)
				},
			},
		}

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", "http://localhost/monitor", nil)
		if err != nil {
			t.Fatalf("create request: %v", err)
		}

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		scanner := bufio.NewScanner(resp.Body)

		// Skip initial full status
		if !scanner.Scan() {
			t.Fatal("expected to read first event")
		}

		// Now trigger a sync
		if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INT)`); err != nil {
			t.Fatal(err)
		}
		if err := db.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := db.Replica.Sync(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Read sync event
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				t.Fatalf("scanner error: %v", err)
			}
			t.Fatal("expected to read sync event")
		}

		var event litestream.StatusEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}

		if event.Type != "sync" {
			t.Errorf("expected event type 'sync', got %q", event.Type)
		}
		if event.Database == nil {
			t.Fatal("expected non-nil database in sync event")
		}
		if event.Database.ReplicaTXID == "" {
			t.Error("expected non-empty replica_txid in sync event")
		}
	})

	t.Run("ClientDisconnect", func(t *testing.T) {
		levels := litestream.CompactionLevels{{Level: 0}}
		store := litestream.NewStore(nil, levels)

		socketPath := shortSocketPath(t)
		server := litestream.NewServer(store)
		server.SocketPath = socketPath

		if err := server.Start(); err != nil {
			t.Fatalf("start server: %v", err)
		}
		defer server.Close()

		// Connect and immediately disconnect
		conn, err := net.Dial("unix", socketPath)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}

		// Send HTTP request
		_, err = conn.Write([]byte("GET /monitor HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		if err != nil {
			t.Fatalf("write: %v", err)
		}

		// Close connection
		conn.Close()

		// Server should handle this gracefully (test passes if no panic)
	})
}

func TestServer_SocketPermissions(t *testing.T) {
	levels := litestream.CompactionLevels{{Level: 0}}
	store := litestream.NewStore(nil, levels)

	socketPath := shortSocketPath(t)
	server := litestream.NewServer(store)
	server.SocketPath = socketPath
	server.SocketPerms = 0600

	if err := server.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer server.Close()

	info, err := os.Stat(socketPath)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Errorf("expected socket permissions 0600, got %o", perm)
	}
}
