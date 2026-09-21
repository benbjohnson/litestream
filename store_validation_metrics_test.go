package litestream

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/superfly/ltx"
)

func TestStore_ValidateMetrics(t *testing.T) {
	listErr := errors.New("cannot list files")
	closeErr := errors.New("cannot finish listing files")
	cases := []struct {
		name     string
		files    []*ltx.FileInfo
		err      error
		closeErr error
		outcome  string
	}{
		{name: "Empty", outcome: "success"},
		{name: "Contiguous", files: []*ltx.FileInfo{{MinTXID: 1, MaxTXID: 1}, {MinTXID: 2, MaxTXID: 2}}, outcome: "success"},
		{name: "MissingTransactions", files: []*ltx.FileInfo{{MinTXID: 1, MaxTXID: 1}, {MinTXID: 3, MaxTXID: 3}}, outcome: "invalid"},
		{name: "OverlappingTransactions", files: []*ltx.FileInfo{{MinTXID: 1, MaxTXID: 3}, {MinTXID: 2, MaxTXID: 4}}, outcome: "invalid"},
		{name: "Unsorted", files: []*ltx.FileInfo{{MinTXID: 3, MaxTXID: 3}, {MinTXID: 1, MaxTXID: 1}}, outcome: "invalid"},
		{name: "MultipleIssues", files: []*ltx.FileInfo{{MinTXID: 1, MaxTXID: 1}, {MinTXID: 3, MaxTXID: 3}, {MinTXID: 5, MaxTXID: 5}}, outcome: "invalid"},
		{name: "ListError", err: listErr, outcome: "error"},
		{name: "IteratorError", closeErr: closeErr, outcome: "error"},
		{name: "Canceled", err: context.Canceled, outcome: "error"},
		{name: "DeadlineExceeded", err: context.DeadlineExceeded, outcome: "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &validationMetricsReplicaClient{files: tc.files, err: tc.err, closeErr: tc.closeErr}
			store, db := newValidationMetricsStore(t, client)
			started := float64(time.Now().Unix())
			result, err := store.Validate(t.Context())
			if tc.outcome == "error" {
				require.Nil(t, result)
				wantErr := tc.err
				if wantErr == nil {
					wantErr = tc.closeErr
				}
				require.ErrorIs(t, err, wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.outcome == "success", result.Valid)
			}

			for _, outcome := range []string{"success", "invalid", "error"} {
				want := float64(0)
				if outcome == tc.outcome {
					want = 1
				}
				require.Equal(t, want, testutil.ToFloat64(validationChecksCounterVec.WithLabelValues(db.Path(), "0", outcome)))
			}
			success := testutil.ToFloat64(validationSuccessGaugeVec.WithLabelValues(db.Path(), "0"))
			lastSuccess := testutil.ToFloat64(validationLastSuccessGaugeVec.WithLabelValues(db.Path(), "0"))
			if tc.outcome == "success" {
				require.Equal(t, float64(1), success)
				require.GreaterOrEqual(t, lastSuccess, started)
				require.LessOrEqual(t, lastSuccess, float64(time.Now().Unix()+1))
			} else {
				require.Zero(t, success)
				require.Zero(t, lastSuccess)
			}
		})
	}
}

func TestStore_ValidateMetricsRecovery(t *testing.T) {
	client := &validationMetricsReplicaClient{}
	store, db := newValidationMetricsStore(t, client)
	lastSuccess := validationLastSuccessGaugeVec.WithLabelValues(db.Path(), "0")
	success := validationSuccessGaugeVec.WithLabelValues(db.Path(), "0")
	lastSuccess.Set(123)

	client.files = []*ltx.FileInfo{{MinTXID: 1, MaxTXID: 1}, {MinTXID: 3, MaxTXID: 3}}
	result, err := store.Validate(t.Context())
	require.NoError(t, err)
	require.False(t, result.Valid)
	require.Zero(t, testutil.ToFloat64(success))
	require.Equal(t, float64(123), testutil.ToFloat64(lastSuccess))

	client.err = errors.New("cannot list files")
	_, err = store.Validate(t.Context())
	require.Error(t, err)
	require.Zero(t, testutil.ToFloat64(success))
	require.Equal(t, float64(123), testutil.ToFloat64(lastSuccess))

	client.err = nil
	client.files = []*ltx.FileInfo{{MinTXID: 1, MaxTXID: 3}}
	result, err = store.Validate(t.Context())
	require.NoError(t, err)
	require.True(t, result.Valid)
	require.Equal(t, float64(1), testutil.ToFloat64(success))
	require.Greater(t, testutil.ToFloat64(lastSuccess), float64(123))
	for _, outcome := range []string{"success", "invalid", "error"} {
		require.Equal(t, float64(1), testutil.ToFloat64(validationChecksCounterVec.WithLabelValues(db.Path(), "0", outcome)))
	}
}

func TestStore_ValidateMetricsAttribution(t *testing.T) {
	store, db := newValidationMetricsStore(t, &validationMetricsReplicaClient{})
	store.levels = CompactionLevels{{Level: 0}, {Level: 1}}
	otherDB := NewDB(filepath.Join(t.TempDir(), "other.db"))
	otherDB.Replica = NewReplicaWithClient(otherDB, &validationMetricsReplicaClient{
		files: []*ltx.FileInfo{{MinTXID: 1, MaxTXID: 1}, {MinTXID: 3, MaxTXID: 3}},
	})
	store.dbs = append(store.dbs, otherDB)
	result, err := store.Validate(t.Context())
	require.NoError(t, err)
	require.False(t, result.Valid)
	for _, level := range []string{"0", "1"} {
		require.Equal(t, float64(1), testutil.ToFloat64(validationSuccessGaugeVec.WithLabelValues(db.Path(), level)))
		require.Zero(t, testutil.ToFloat64(validationSuccessGaugeVec.WithLabelValues(otherDB.Path(), level)))
		require.Equal(t, float64(1), testutil.ToFloat64(validationChecksCounterVec.WithLabelValues(db.Path(), level, "success")))
		require.Equal(t, float64(1), testutil.ToFloat64(validationChecksCounterVec.WithLabelValues(otherDB.Path(), level, "invalid")))
	}
}

func TestStore_ValidationMonitorMetrics(t *testing.T) {
	store, db := newValidationMetricsStore(t, &validationMetricsReplicaClient{})
	store.ValidationInterval = time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		store.monitorValidation(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(validationChecksCounterVec.WithLabelValues(db.Path(), "0", "success")) >= 2 &&
			testutil.ToFloat64(validationLastSuccessGaugeVec.WithLabelValues(db.Path(), "0")) > 0
	}, time.Second, time.Millisecond)
}

func newValidationMetricsStore(t *testing.T, client ReplicaClient) (*Store, *DB) {
	t.Helper()
	db := NewDB(filepath.Join(t.TempDir(), "db"))
	db.Replica = NewReplicaWithClient(db, client)
	return NewStore([]*DB{db}, CompactionLevels{{Level: 0}}), db
}

type validationMetricsReplicaClient struct {
	ReplicaClient
	files    []*ltx.FileInfo
	err      error
	closeErr error
}

func (c *validationMetricsReplicaClient) SetLogger(*slog.Logger) {}

func (c *validationMetricsReplicaClient) Type() string { return "test" }

func (c *validationMetricsReplicaClient) LTXFiles(context.Context, int, ltx.TXID, bool) (ltx.FileIterator, error) {
	if c.err != nil {
		return nil, c.err
	}
	return &validationMetricsIterator{FileIterator: ltx.NewFileInfoSliceIterator(c.files), err: c.closeErr}, nil
}

type validationMetricsIterator struct {
	ltx.FileIterator
	err error
}

func (i *validationMetricsIterator) Close() error { return i.err }
