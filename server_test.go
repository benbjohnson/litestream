package litestream_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

func TestServer_HandleAdd(t *testing.T) {
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
		resp, err := client.Post("http://localhost/add", "application/json", io.NopCloser(stringReader(body)))
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
		resp, err := client.Post("http://localhost/add", "application/json", io.NopCloser(stringReader(body)))
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
		resp, err := client.Post("http://localhost/add", "application/json", io.NopCloser(stringReader(body)))
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
		resp, err := client.Post("http://localhost/add", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.AddDatabaseResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "added", result.Status)
		require.Equal(t, dbPath, result.Path)

		// Verify database was added to store.
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

		// Try to add the same database again.
		backupDir := t.TempDir()
		client := newSocketClient(t, server.SocketPath)
		body := fmt.Sprintf(`{"path": %q, "replica_url": "file://%s"}`, db.Path(), backupDir)
		resp, err := client.Post("http://localhost/add", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.AddDatabaseResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "already_exists", result.Status)
	})
}

func TestServer_HandleRemove(t *testing.T) {
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
		resp, err := client.Post("http://localhost/remove", "application/json", io.NopCloser(stringReader(body)))
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
		resp, err := client.Post("http://localhost/remove", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		// RemoveDB is idempotent - returns success even if DB not found.
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.RemoveDatabaseResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "removed", result.Status)
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
		resp, err := client.Post("http://localhost/remove", "application/json", io.NopCloser(stringReader(body)))
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.RemoveDatabaseResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "removed", result.Status)
		require.Equal(t, dbPath, result.Path)

		// Verify database was removed from store.
		require.Empty(t, store.DBs())
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
