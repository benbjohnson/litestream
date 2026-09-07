//go:build vfs
// +build vfs

package main_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// TestVFS_LitestreamTimeAdvancesOnPoll verifies that litestream_time() advances
// on a long-lived connection as the backup progresses.
func TestVFS_LitestreamTimeAdvancesOnPoll(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	primaryDB, primary := openReplicatedPrimary(t, client, 50*time.Millisecond, 50*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec("CREATE TABLE t (x)"); err != nil {
		t.Fatal(err)
	}
	if _, err := primary.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	waitForLTXFiles(t, client, 10*time.Second, 50*time.Millisecond)

	vfs := newVFS(t, client)
	vfs.PollInterval = 50 * time.Millisecond
	name := registerTestVFS(t, vfs)
	replica := openVFSReplicaDB(t, name)
	defer replica.Close()
	// One connection => one VFSFile, so we observe the polling file's time.
	replica.SetMaxOpenConns(1)

	// Initial data visible, and the initial litestream_time on the held-open
	// connection (set at open by buildIndex).
	waitForReplicaValue(t, replica, "SELECT x FROM t", 1, 10*time.Second, 50*time.Millisecond)
	t0 := readLitestreamTime(t, replica)

	// Ensure the next LTX file gets a strictly-later CreatedAt than the first.
	time.Sleep(1100 * time.Millisecond)

	// Advance the backup. The VFS poll should apply the new data AND advance
	// litestream_time — all on the same, never-reset connection.
	if _, err := primary.Exec("INSERT INTO t VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	forceReplicaSync(t, primaryDB)
	waitForReplicaValue(t, replica, "SELECT max(x) FROM t", 2, 10*time.Second, 50*time.Millisecond)

	t1 := readLitestreamTime(t, replica)

	require.Truef(t, t1.After(t0),
		"litestream_time should advance on poll without reset: t0=%s t1=%s", t0, t1)
}

func readLitestreamTime(t *testing.T, db *sql.DB) time.Time {
	t.Helper()
	var s string
	require.NoError(t, db.QueryRow("PRAGMA litestream_time").Scan(&s))
	ts, err := time.Parse(time.RFC3339Nano, s)
	require.NoErrorf(t, err, "parse litestream_time %q", s)
	return ts
}
