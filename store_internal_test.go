package litestream

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/superfly/ltx"
)

func TestStore_MonitorCompactionRetriesBusyDB(t *testing.T) {
	db := NewDB(filepath.Join(t.TempDir(), "db"))
	db.opened = true
	db.pageSize = 4096
	db.maxLTXFileInfos.m[0] = &ltx.FileInfo{}
	db.maxLTXFileInfos.m[1] = &ltx.FileInfo{}
	pr, pw := io.Pipe()
	events := make(chan string, 16)
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		scanner := bufio.NewScanner(pr)
		for scanner.Scan() {
			select {
			case events <- scanner.Text():
			default:
			}
		}
	}()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	s := NewStore([]*DB{db}, CompactionLevels{{Level: 0}, {Level: 1, Interval: 24 * time.Hour}})
	db.Logger = slog.New(slog.NewTextHandler(pw, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s.MaintenanceBusyRetryInterval = 10 * time.Millisecond
	db.maintenanceMu.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			db.maintenanceMu.Unlock()
		}
		cancel()
		<-done
		if err := pw.Close(); err != nil {
			t.Error(err)
		}
		<-scanDone
		if err := pr.Close(); err != nil {
			t.Error(err)
		}
	})
	go func() { defer close(done); s.monitorCompactionLevel(ctx, s.levels[1]) }()
	for _, message := range []string{"maintenance busy", "no compaction"} {
		deadline := time.NewTimer(2 * time.Second)
		found := false
		for !found {
			select {
			case event := <-events:
				found = strings.Contains(event, message)
			case <-deadline.C:
				t.Fatalf("monitor did not report %q", message)
			}
		}
		deadline.Stop()
		if locked {
			db.maintenanceMu.Unlock()
			locked = false
		}
	}
}

func TestStore_MonitorCompactionLevelBusyRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var busyLogs, readyLogs lockedLogBuffer
		busy, ready := NewDB("busy"), NewDB("ready")
		level := &CompactionLevel{Level: 1, Interval: time.Minute}
		s := NewStore([]*DB{busy, ready}, CompactionLevels{{Level: 0}, level})
		defer s.cancel()
		opts := &slog.HandlerOptions{Level: slog.LevelDebug}
		busy.SetLogger(slog.New(slog.NewTextHandler(&busyLogs, opts)))
		ready.SetLogger(slog.New(slog.NewTextHandler(&readyLogs, opts)))
		s.Logger = slog.New(slog.NewTextHandler(io.Discard, opts))
		for _, db := range []*DB{busy, ready} {
			db.opened = true
			db.pageSize = 4096
			db.maxLTXFileInfos.m[0] = &ltx.FileInfo{}
			db.maxLTXFileInfos.m[1] = &ltx.FileInfo{}
		}
		busy.maintenanceMu.Lock()
		release := sync.OnceFunc(busy.maintenanceMu.Unlock)
		defer release()

		go s.monitorCompactionLevel(s.ctx, level)
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		nextRegular := level.NextCompactionAt(time.Now())
		if got := strings.Count(busyLogs.String(), "maintenance busy"); got != 1 {
			t.Fatalf("initial busy attempts = %d, want 1", got)
		}
		if got := strings.Count(readyLogs.String(), "no compaction"); got != 1 {
			t.Fatalf("initial ready database attempts = %d, want 1", got)
		}
		for i, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second} {
			time.Sleep(delay - time.Nanosecond)
			synctest.Wait()
			if got := strings.Count(busyLogs.String(), "maintenance busy"); got != i+1 {
				t.Fatalf("retry fired early: attempts = %d, want %d", got, i+1)
			}
			time.Sleep(time.Nanosecond)
			synctest.Wait()
			if got := strings.Count(busyLogs.String(), "maintenance busy"); got != i+2 {
				t.Fatalf("retry missing after %s: attempts = %d, want %d", delay, got, i+2)
			}
		}

		release()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := strings.Count(busyLogs.String(), "no compaction"); got != 1 {
			t.Fatalf("released database attempts before regular pass = %d, want 1", got)
		}
		time.Sleep(time.Until(nextRegular) - time.Nanosecond)
		synctest.Wait()
		if got := strings.Count(readyLogs.String(), "no compaction"); got != 1 {
			t.Fatalf("ready database attempts before regular pass = %d, want 1", got)
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		for _, logs := range []*lockedLogBuffer{&busyLogs, &readyLogs} {
			if got := strings.Count(logs.String(), "no compaction"); got != 2 {
				t.Fatalf("attempts after regular pass = %d, want 2; logs:\n%s", got, logs.String())
			}
		}
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
		want := errors.New("snapshot spill cleanup failed")
		var cleanupErr error
		r := &snapshotReadCloser{
			PipeReader: pr,
			pos:        &snapshotReadPosition{db: db},
			cancel:     cancel,
			done:       done,
			cleanupErr: &cleanupErr,
		}
		go func() {
			<-ctx.Done()
			<-release
			cleanupErr = want
			close(done)
		}()
		closed := make(chan struct{})
		var closeErr error
		go func() {
			closeErr = r.Close()
			close(closed)
		}()
		synctest.Wait()
		if ctx.Err() == nil {
			t.Error("Close did not cancel producer")
		}
		select {
		case <-closed:
			t.Error("Close returned before producer exited")
		default:
		}
		if db.chkMu.TryLock() {
			db.chkMu.Unlock()
			t.Error("checkpoint lock released before producer exited")
		}
		cancel()
		close(release)
		<-closed
		if !errors.Is(closeErr, want) {
			t.Errorf("Close() error = %v, want %v", closeErr, want)
		}
		if !db.chkMu.TryLock() {
			t.Fatal("checkpoint remains blocked after Close")
		}
		db.chkMu.Unlock()
		if err := r.Close(); !errors.Is(err, want) {
			t.Errorf("second Close() error = %v, want %v", err, want)
		}
	})
}
