package litestream_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
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

	t.Run("StatusReflectsMonitorState", func(t *testing.T) {
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
}

func TestServer_HandleStatus(t *testing.T) {
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
		resp, err := client.Get("http://localhost/status")
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
		resp, err := client.Get("http://localhost/status?path=/nonexistent/db")
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusNotFound, resp.StatusCode)

		var result litestream.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Contains(t, result.Error, "database not found")
	})

	t.Run("DatabaseFound", func(t *testing.T) {
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
		reqURL := "http://localhost/status?path=" + url.QueryEscape(db.Path())
		resp, err := client.Get(reqURL)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.StatusResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))

		require.Equal(t, db.Path(), result.Path)
		// Status should be "open" since MonitorEnabled is false in test helper.
		require.Equal(t, "open", result.Status)
		require.Greater(t, result.PageSize, 0)
		require.NotNil(t, result.Position)
		require.NotEmpty(t, result.Position.TXID)
		require.NotNil(t, result.LastSyncAt)
		require.Len(t, result.Replicas, 1)
		require.Equal(t, "file", result.Replicas[0].Type)
	})

	t.Run("ReplicatingStatus", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		// Enable monitor to get "replicating" status.
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
		reqURL := "http://localhost/status?path=" + url.QueryEscape(db.Path())
		resp, err := client.Get(reqURL)
		require.NoError(t, err)
		defer resp.Body.Close()

		var result litestream.StatusResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "replicating", result.Status)
	})

	t.Run("StoppedStatus", func(t *testing.T) {
		db := testingutil.MustOpenDB(t)
		// Close DB to get "stopped" status.
		require.NoError(t, db.Close(context.Background()))

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		// Don't open store - it would try to open the closed DB.

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		reqURL := "http://localhost/status?path=" + url.QueryEscape(db.Path())
		resp, err := client.Get(reqURL)
		require.NoError(t, err)
		defer resp.Body.Close()

		var result litestream.StatusResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, "stopped", result.Status)
	})

	t.Run("PathExpansion", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		require.NoError(t, store.Open(t.Context()))
		defer store.Close(t.Context())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		// Set path expander that adds a prefix.
		server.PathExpander = func(path string) (string, error) {
			if path == "/alias/db" {
				return db.Path(), nil
			}
			return path, nil
		}
		require.NoError(t, server.Start())
		defer server.Close()

		client := newSocketClient(t, server.SocketPath)
		reqURL := "http://localhost/status?path=" + url.QueryEscape("/alias/db")
		resp, err := client.Get(reqURL)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result litestream.StatusResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		require.Equal(t, db.Path(), result.Path)
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
