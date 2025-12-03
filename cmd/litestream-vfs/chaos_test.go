//go:build vfs && chaos
// +build vfs,chaos

package main_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestVFS_ChaosEngineering(t *testing.T) {
	client := file.NewReplicaClient(t.TempDir())
	db, primary := openReplicatedPrimary(t, client, 15*time.Millisecond, 15*time.Millisecond)
	defer testingutil.MustCloseSQLDB(t, primary)

	if _, err := primary.Exec(`CREATE TABLE chaos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        value TEXT,
        grp INTEGER
    )`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 0; i < 64; i++ {
		if _, err := primary.Exec("INSERT INTO chaos (value, grp) VALUES (?, ?)", randomPayload(rand.New(rand.NewSource(int64(i))), 48), i%8); err != nil {
			t.Fatalf("seed chaos: %v", err)
		}
	}

	chaosClient := newChaosReplicaClient(client)
	vfs := newVFS(t, chaosClient)
	vfs.PollInterval = 15 * time.Millisecond
	vfsName := registerTestVFS(t, vfs)

	replica := openVFSReplicaDB(t, vfsName)
	defer replica.Close()

	waitForTableRowCount(t, primary, replica, "chaos", 5*time.Second)
	chaosClient.active.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	writerDone := make(chan error, 1)
	go func() {
		rnd := rand.New(rand.NewSource(42))
		for {
			select {
			case <-ctx.Done():
				writerDone <- nil
				return
			default:
			}
			switch rnd.Intn(3) {
			case 0:
				if _, err := primary.Exec("INSERT INTO chaos (value, grp) VALUES (?, ?)", randomPayload(rnd, 32), rnd.Intn(8)); err != nil && !isBusyError(err) {
					writerDone <- err
					return
				}
			case 1:
				if _, err := primary.Exec("UPDATE chaos SET value = ? WHERE id = (ABS(random()) % 64) + 1", randomPayload(rnd, 24)); err != nil && !isBusyError(err) {
					writerDone <- err
					return
				}
			case 2:
				if _, err := primary.Exec("DELETE FROM chaos WHERE id IN (SELECT id FROM chaos ORDER BY RANDOM() LIMIT 1)"); err != nil && !isBusyError(err) {
					writerDone <- err
					return
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	const readers = 16
	readerErrs := make(chan error, readers)
	for i := 0; i < readers; i++ {
		go func() {
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				select {
				case <-ctx.Done():
					readerErrs <- nil
					return
				default:
				}
				var count int
				switch rnd.Intn(3) {
				case 0:
					err := replica.QueryRow("SELECT COUNT(*) FROM chaos WHERE grp = ?", rnd.Intn(8)).Scan(&count)
					if err != nil {
						if isBusyError(err) {
							continue
						}
						readerErrs <- err
						return
					}
				case 1:
					rows, err := replica.Query("SELECT id, value FROM chaos ORDER BY id DESC LIMIT 5 OFFSET ?", rnd.Intn(10))
					if err != nil {
						if isBusyError(err) {
							continue
						}
						readerErrs <- err
						return
					}
					retryRows := false
					for rows.Next() {
						var id int
						var value string
						if err := rows.Scan(&id, &value); err != nil {
							rows.Close()
							if isBusyError(err) {
								retryRows = true
								break
							}
							readerErrs <- err
							return
						}
					}
					if retryRows {
						continue
					}
					if err := rows.Err(); err != nil {
						rows.Close()
						if isBusyError(err) {
							continue
						}
						readerErrs <- err
						return
					}
					rows.Close()
				case 2:
					err := replica.QueryRow("SELECT SUM(LENGTH(value)) FROM chaos WHERE id BETWEEN ? AND ?",
						rnd.Intn(32)+1, rnd.Intn(32)+33).Scan(&count)
					if err != nil {
						if isBusyError(err) {
							continue
						}
						readerErrs <- err
						return
					}
				}
			}
		}()
	}

	<-ctx.Done()
	for i := 0; i < readers; i++ {
		if err := <-readerErrs; err != nil {
			t.Fatalf("reader error: %v", err)
		}
	}
	if err := <-writerDone; err != nil {
		t.Fatalf("writer error: %v", err)
	}

	waitForTableRowCount(t, primary, replica, "chaos", 5*time.Second)
	if chaosClient.failures.Load() == 0 {
		t.Fatalf("expected injected failures")
	}
}

func newChaosReplicaClient(base litestream.ReplicaClient) *chaosReplicaClient {
	return &chaosReplicaClient{
		ReplicaClient: base,
		rnd:           rand.New(rand.NewSource(99)),
	}
}

type chaosReplicaClient struct {
	litestream.ReplicaClient
	rnd      *rand.Rand
	failures atomic.Int32
	active   atomic.Bool
}

func (c *chaosReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	if !c.active.Load() {
		return c.ReplicaClient.LTXFiles(ctx, level, seek, useMetadata)
	}
	if c.rnd.Float64() < 0.05 {
		c.failures.Add(1)
		return nil, context.DeadlineExceeded
	}
	return c.ReplicaClient.LTXFiles(ctx, level, seek, useMetadata)
}

func (c *chaosReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if !c.active.Load() {
		return c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
	}
	delay := time.Duration(c.rnd.Intn(5)) * time.Millisecond
	if delay > 0 {
		time.Sleep(delay)
	}
	if c.rnd.Float64() < 0.05 {
		c.failures.Add(1)
		return nil, context.DeadlineExceeded
	}
	rc, err := c.ReplicaClient.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
	if err != nil {
		return nil, err
	}
	if c.rnd.Float64() < 0.05 && size > 0 {
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, err
		}
		if len(data) > 32 {
			data = data[:len(data)/2]
		}
		c.failures.Add(1)
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return rc, nil
}
