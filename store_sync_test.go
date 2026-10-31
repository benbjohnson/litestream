package litestream_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// Inject a WAL capture during an upload deterministically, without sleeps or
// network access. This reproduces the same interleaving as the WAL monitor.
type syncTargetUploadHook struct {
	litestream.ReplicaClient
	afterUpload func() error
}

func (c *syncTargetUploadHook) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, reader io.Reader) (*ltx.FileInfo, error) {
	info, err := c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, reader)
	if err == nil && c.afterUpload != nil {
		hook := c.afterUpload
		c.afterUpload = nil
		err = hook()
	}
	return info, err
}

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
	db.Replica.Client = &syncTargetUploadHook{
		ReplicaClient: db.Replica.Client,
		afterUpload: func() error {
			position, err := db.Pos()
			if err != nil {
				return err
			}
			uploadedThrough = position.TXID
			if _, err := sqlDB.ExecContext(ctx, "INSERT INTO sync_target VALUES (2)"); err != nil {
				return err
			}
			return db.Sync(ctx)
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
	next, err := store.SyncDB(ctx, db.Path(), true)
	require.NoError(t, err)
	require.Equal(t, uint64(local.TXID), next.TXID)
	require.GreaterOrEqual(t, next.ReplicatedTXID, next.TXID)
}

func TestStore_SyncDB_NoWaitAndIdle(t *testing.T) {
	db, sqlDB := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqlDB)
	_, err := sqlDB.ExecContext(t.Context(), "CREATE TABLE sync_target (value INTEGER)")
	require.NoError(t, err)
	store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
	local, err := store.SyncDB(t.Context(), db.Path(), false)
	require.NoError(t, err)
	require.NotZero(t, local.TXID)
	require.Zero(t, local.ReplicatedTXID, "no-wait must not force an upload")
	require.True(t, local.Changed)
	remote, err := store.SyncDB(t.Context(), db.Path(), true)
	require.NoError(t, err)
	require.Equal(t, local.TXID, remote.TXID)
	require.GreaterOrEqual(t, remote.ReplicatedTXID, remote.TXID)
	require.False(t, remote.Changed)
}

func TestStore_SyncDB_UploadFailure(t *testing.T) {
	for _, cause := range []error{errors.New("injected upload failure"), context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			db, sqlDB := testingutil.MustOpenDBs(t)
			defer testingutil.MustCloseDBs(t, db, sqlDB)
			_, err := sqlDB.ExecContext(t.Context(), "CREATE TABLE sync_target (value INTEGER)")
			require.NoError(t, err)
			db.Replica.Client = &syncTargetUploadHook{ReplicaClient: db.Replica.Client, afterUpload: func() error { return cause }}
			store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
			result, err := store.SyncDB(t.Context(), db.Path(), true)
			require.ErrorIs(t, err, cause)
			require.Zero(t, result, "never return successful capture evidence on an upload failure")
		})
	}
}

func TestStore_SyncDB_NoReplica(t *testing.T) {
	db, sqlDB := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqlDB)
	replica := db.Replica
	db.Replica = nil
	defer func() { db.Replica = replica }()
	store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
	result, err := store.SyncDB(t.Context(), db.Path(), true)
	require.EqualError(t, err, "sync database: no replica configured")
	require.Zero(t, result)
}
