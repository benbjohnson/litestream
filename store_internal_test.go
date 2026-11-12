package litestream

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
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
