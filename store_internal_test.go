package litestream

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/superfly/ltx"
)

func TestStore_MonitorCompactionLevelBusyRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs lockedLogBuffer
		logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		dbs := []*DB{NewDB("busy"), NewDB("ready")}
		levels := CompactionLevels{{Level: 0}, {Level: 1, Interval: time.Minute}}
		s := NewStore(dbs, levels)
		s.Logger = logger
		defer s.cancel()
		for _, db := range dbs {
			db.SetLogger(logger.With("db", db.Path()))
			db.opened = true
			db.pageSize = 4096
			db.maxLTXFileInfos.m[0] = &ltx.FileInfo{}
			db.maxLTXFileInfos.m[1] = &ltx.FileInfo{}
		}
		dbs[0].maintenanceMu.Lock()
		locked := true
		defer func() {
			if locked {
				dbs[0].maintenanceMu.Unlock()
			}
		}()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go s.monitorCompactionLevel(ctx, levels[1])
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if got := strings.Count(logs.String(), "maintenance busy"); got != 1 {
			t.Fatalf("initial busy attempts=%d", got)
		}
		for i, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second} {
			time.Sleep(delay - time.Nanosecond)
			synctest.Wait()
			if got := strings.Count(logs.String(), "maintenance busy"); got != i+1 {
				t.Fatalf("retry fired early: attempts=%d want=%d", got, i+1)
			}
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			if got := strings.Count(logs.String(), "maintenance busy"); got != i+2 {
				t.Fatalf("retry missing after %s: attempts=%d want=%d", delay, got, i+2)
			}
		}
		if got := strings.Count(logs.String(), "no compaction"); got != 1 {
			t.Fatalf("ready database repeated during early retries: %d", got)
		}
		dbs[0].maintenanceMu.Unlock()
		locked = false
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := strings.Count(logs.String(), "no compaction"); got != 2 {
			t.Fatalf("released database was not retried: %d", got)
		}
		time.Sleep(time.Until(levels[1].NextCompactionAt(time.Now())))
		synctest.Wait()
		if got := strings.Count(logs.String(), "no compaction"); got != 4 {
			t.Fatalf("regular schedule drifted: attempts=%d", got)
		}
		cancel()
		synctest.Wait()
	})
}

func TestSnapshotReadCloser_CloseJoinsProducer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := NewDB("snapshot")
		db.chkMu.RLock()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		pr, pw := io.Pipe()
		defer func() {
			if err := pw.Close(); err != nil {
				t.Error(err)
			}
		}()
		done, release := make(chan struct{}), make(chan struct{})
		r := &snapshotReadCloser{PipeReader: pr, pos: &snapshotReadPosition{db: db}, cancel: cancel, done: done}
		go func() { <-ctx.Done(); <-release; close(done) }()
		closed := make(chan error, 1)
		go func() { closed <- r.Close() }()
		synctest.Wait()
		if ctx.Err() == nil {
			t.Error("Close did not cancel producer")
		}
		returned := false
		select {
		case <-closed:
			returned = true
			t.Error("Close returned before producer exited")
		default:
		}
		cancel()
		close(release)
		synctest.Wait()
		if !returned {
			if err := <-closed; err != nil {
				t.Fatal(err)
			}
		}
		if !db.chkMu.TryLock() {
			t.Fatal("checkpoint remains blocked after Close")
		}
		db.chkMu.Unlock()
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
