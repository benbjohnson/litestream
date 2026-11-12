package litestream_test

import (
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
	"github.com/benbjohnson/litestream/mock"
)

func TestStore_SyncDB_ConcurrentCapture(t *testing.T) {
	db, sqlDB := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqlDB)
	ctx := t.Context()
	_, err := sqlDB.ExecContext(ctx, "CREATE TABLE sync_target (value INTEGER)")
	require.NoError(t, err)
	require.NoError(t, db.SyncAndWait(ctx))
	_, err = sqlDB.ExecContext(ctx, "INSERT INTO sync_target VALUES (1)")
	require.NoError(t, err)
	var uploadedThrough ltx.TXID
	client := db.Replica.Client
	db.Replica.Client = &mock.ReplicaClient{
		LTXFilesFunc:    client.LTXFiles,
		OpenLTXFileFunc: client.OpenLTXFile,
		WriteLTXFileFunc: func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, reader io.Reader) (*ltx.FileInfo, error) {
			info, err := client.WriteLTXFile(ctx, level, minTXID, maxTXID, reader)
			if err != nil || uploadedThrough != 0 {
				return info, err
			}
			uploadedThrough = maxTXID
			if _, err := sqlDB.ExecContext(ctx, "INSERT INTO sync_target VALUES (2)"); err != nil {
				return nil, err
			}
			if err := db.Sync(ctx); err != nil {
				return nil, err
			}
			return info, nil
		},
	}
	store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
	result, err := store.SyncDB(ctx, db.Path(), true)
	require.NoError(t, err)
	local, err := db.Pos()
	require.NoError(t, err)
	t.Logf("uploaded=%d, local=%d, returned target=%d, returned replica=%d", uploadedThrough, local.TXID, result.TXID, result.ReplicatedTXID)
	require.NotZero(t, uploadedThrough)
	require.Equal(t, uint64(uploadedThrough), result.TXID)
	require.GreaterOrEqual(t, result.ReplicatedTXID, result.TXID)
	require.True(t, result.Changed)
	require.Greater(t, uint64(local.TXID), result.TXID, "later writes belong to a subsequent sync")
	outputPath := filepath.Join(t.TempDir(), "restored.db")
	require.NoError(t, db.Replica.Restore(ctx, litestream.RestoreOptions{OutputPath: outputPath, TXID: ltx.TXID(result.TXID)}))
	restoredDB, err := sql.Open("sqlite", outputPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, restoredDB.Close()) }()
	var values []int
	rows, err := restoredDB.QueryContext(ctx, "SELECT value FROM sync_target ORDER BY value")
	require.NoError(t, err)
	for rows.Next() {
		var value int
		require.NoError(t, rows.Scan(&value))
		values = append(values, value)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, []int{1}, values)
	next, err := store.SyncDB(ctx, db.Path(), true)
	require.NoError(t, err)
	require.Equal(t, uint64(local.TXID), next.TXID)
	require.GreaterOrEqual(t, next.ReplicatedTXID, next.TXID)
}
