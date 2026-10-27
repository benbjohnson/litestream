package litestream

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServer_HandleSync_QueuedCancellation(t *testing.T) {
	for _, wait := range []bool{false, true} {
		for _, source := range []string{"client", "deadline", "shutdown", "timeout"} {
			t.Run(fmt.Sprintf("wait=%t/%s", wait, source), func(t *testing.T) {
				db := NewDB(filepath.Join(t.TempDir(), "db"))
				db.MonitorInterval = 0
				db.Replica = NewReplica(db)
				db.Replica.Client = &testReplicaClient{dir: t.TempDir()}
				db.Replica.MonitorEnabled = false
				require.NoError(t, db.Open())
				t.Cleanup(func() { require.NoError(t, db.Close(context.Background())) })
				sqldb, err := sql.Open("sqlite", db.Path())
				require.NoError(t, err)
				defer func() { require.NoError(t, sqldb.Close()) }()
				_, err = sqldb.Exec("PRAGMA journal_mode = wal; CREATE TABLE t (id INTEGER)")
				require.NoError(t, err)
				require.NoError(t, db.Sync(context.Background()))
				_, before, err := db.MaxLTX()
				require.NoError(t, err)
				_, err = sqldb.Exec("INSERT INTO t VALUES (1)")
				require.NoError(t, err)

				server := NewServer(NewStore([]*DB{db}, CompactionLevels{{Level: 0}}))
				socketDir, err := os.MkdirTemp("/tmp", "ls-sync-")
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, os.RemoveAll(socketDir)) })
				server.SocketPath = filepath.Join(socketDir, "s")
				require.NoError(t, server.Start())
				t.Cleanup(func() { require.NoError(t, server.Close()) })
				transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", server.SocketPath)
				}}
				defer transport.CloseIdleConnections()
				client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if source == "deadline" {
					var deadlineCancel context.CancelFunc
					ctx, deadlineCancel = context.WithTimeout(ctx, time.Second)
					defer deadlineCancel()
				}
				timeout := 30
				switch source {
				case "timeout":
					timeout = 1
				case "client":
					timeout = 0
				}
				body := fmt.Sprintf(`{"path":%q,"wait":%t,"timeout":%d}`, db.Path(), wait, timeout)
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/sync", strings.NewReader(body))
				require.NoError(t, err)
				require.NoError(t, db.execSem.Acquire(t.Context(), 1))
				locked := true
				defer func() {
					if locked {
						db.execSem.Release(1)
					}
				}()
				done := make(chan error, 1)
				go func() {
					resp, err := client.Do(req)
					if err == nil {
						var result ErrorResponse
						err = json.NewDecoder(resp.Body).Decode(&result)
						if err == nil && (resp.StatusCode != http.StatusInternalServerError || !strings.Contains(result.Error, "context")) {
							err = fmt.Errorf("unexpected cancellation response: %d %+v", resp.StatusCode, result)
						}
						err = errors.Join(err, resp.Body.Close())
					}
					done <- err
				}()
				require.Eventually(t, func() bool { return db.SyncDiagnostic().ExecutorWaiterCount == 1 }, 5*time.Second, time.Millisecond)
				switch source {
				case "client":
					cancel()
				case "shutdown":
					require.NoError(t, server.Close())
				}
				select {
				case err := <-done:
					switch source {
					case "client":
						require.ErrorIs(t, err, context.Canceled)
					case "deadline":
						require.ErrorIs(t, err, context.DeadlineExceeded)
					default:
						require.NoError(t, err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("request did not finish")
				}
				require.Eventually(t, func() bool { return db.SyncDiagnostic().ExecutorWaiterCount == 0 }, time.Second, time.Millisecond, "abandoned sync remains queued")
				db.execSem.Release(1)
				locked = false
				_, after, err := db.MaxLTX()
				require.NoError(t, err)
				require.Equal(t, before, after)
				if source != "shutdown" {
					resp, err := client.Post("http://localhost/sync", "application/json", strings.NewReader(body))
					require.NoError(t, err)
					defer func() { require.NoError(t, resp.Body.Close()) }()
					require.Equal(t, http.StatusOK, resp.StatusCode)
					var result SyncResponse
					require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
					require.Greater(t, result.TXID, uint64(before))
				}
			})
		}
	}
}
