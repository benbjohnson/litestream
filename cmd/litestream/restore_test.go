package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	litestream "github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestRestoreCommand_FollowIntervalFlag(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantVal time.Duration
		wantErr bool
	}{
		{
			name:    "Default",
			args:    []string{"/tmp/db"},
			wantVal: time.Second,
		},
		{
			name:    "CustomValue",
			args:    []string{"-follow-interval", "500ms", "/tmp/db"},
			wantVal: 500 * time.Millisecond,
		},
		{
			name:    "LongerInterval",
			args:    []string{"-follow-interval", "5s", "/tmp/db"},
			wantVal: 5 * time.Second,
		},
		{
			name:    "InvalidDuration",
			args:    []string{"-follow-interval", "notaduration", "/tmp/db"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := litestream.NewRestoreOptions()
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.DurationVar(&opt.FollowInterval, "follow-interval", opt.FollowInterval, "polling interval for follow mode")

			err := fs.Parse(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if opt.FollowInterval != tt.wantVal {
				t.Fatalf("FollowInterval=%v, want %v", opt.FollowInterval, tt.wantVal)
			}
		})
	}
}

func TestRestoreCommand_ExecImpliesFollow(t *testing.T) {
	t.Run("ExecWithTXID", func(t *testing.T) {
		cmd := &RestoreCommand{}
		err := cmd.Run(context.Background(), []string{"-exec", "echo test", "-txid", "0000000000000001", "/tmp/db"})
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "cannot use follow mode with -txid") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ExecWithTimestamp", func(t *testing.T) {
		cmd := &RestoreCommand{}
		err := cmd.Run(context.Background(), []string{"-exec", "echo test", "-timestamp", "2020-01-01T00:00:00Z", "/tmp/db"})
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "cannot use follow mode with -timestamp") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ExecEmptyCommand", func(t *testing.T) {
		cmd := &RestoreCommand{}
		err := cmd.Run(context.Background(), []string{"-exec", "   ", "/tmp/db"})
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "exec command is empty") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ExecParseFailure", func(t *testing.T) {
		cmd := &RestoreCommand{}
		err := cmd.Run(context.Background(), []string{"-exec", "'unterminated", "/tmp/db"})
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "cannot parse exec command") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestRestoreCommand_Exec_SignalForwarding(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires shell signal handling")
	}

	replicaURL, workDir := createRestoreExecFixture(t)
	outputPath := filepath.Join(workDir, "follower.db")
	startMarker := filepath.Join(workDir, "started")
	stopMarker := filepath.Join(workDir, "stopped")
	scriptPath := filepath.Join(workDir, "trap.sh")
	script := fmt.Sprintf("#!/bin/sh\ntouch %q\ntrap 'touch %q; exit 0' INT TERM\nwhile true; do\n  sleep 1\ndone\n", startMarker, stopMarker)
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := &RestoreCommand{}
	sigCh := make(chan os.Signal, 2)
	cmd.signalChanFn = func() <-chan os.Signal { return sigCh }
	execArg := fmt.Sprintf("sh %q", scriptPath)
	errCh := make(chan error, 1)
	go func() {
		errCh <- cmd.Run(context.Background(), []string{"-exec", execArg, "-o", outputPath, replicaURL})
	}()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(startMarker); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(startMarker); err != nil {
		t.Fatalf("exec command did not start: %v", err)
	}

	sigCh <- syscall.SIGINT

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("restore command returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("restore command did not shut down after interrupt")
	}

	if _, err := os.Stat(stopMarker); err != nil {
		t.Fatalf("exec command did not receive forwarded signal: %v", err)
	}
}

func TestRestoreCommand_Exec_CleanExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires shell")
	}

	replicaURL, workDir := createRestoreExecFixture(t)
	outputPath := filepath.Join(workDir, "follower.db")
	startMarker := filepath.Join(workDir, "started")
	scriptPath := filepath.Join(workDir, "exit0.sh")
	script := fmt.Sprintf("#!/bin/sh\ntouch %q\nexit 0\n", startMarker)
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := &RestoreCommand{}
	execArg := fmt.Sprintf("sh %q", scriptPath)
	if err := cmd.Run(context.Background(), []string{"-exec", execArg, "-o", outputPath, replicaURL}); err != nil {
		t.Fatalf("restore command returned error: %v", err)
	}

	if _, err := os.Stat(startMarker); err != nil {
		t.Fatalf("exec command did not run: %v", err)
	}
}

func TestRestoreCommand_Exec_NonZeroExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires shell")
	}

	replicaURL, workDir := createRestoreExecFixture(t)
	outputPath := filepath.Join(workDir, "follower.db")
	startMarker := filepath.Join(workDir, "started")
	scriptPath := filepath.Join(workDir, "exit23.sh")
	script := fmt.Sprintf("#!/bin/sh\ntouch %q\nexit 23\n", startMarker)
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := &RestoreCommand{}
	execArg := fmt.Sprintf("sh %q", scriptPath)
	err := cmd.Run(context.Background(), []string{"-exec", execArg, "-o", outputPath, replicaURL})
	if err == nil {
		t.Fatal("expected non-zero exec exit error")
	}
	if !strings.Contains(err.Error(), "exit status 23") {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, statErr := os.Stat(startMarker); statErr != nil {
		t.Fatalf("exec command did not run: %v", statErr)
	}
}

func TestRestoreCommand_Exec_StartFailure(t *testing.T) {
	replicaURL, workDir := createRestoreExecFixture(t)
	outputPath := filepath.Join(workDir, "follower.db")

	cmd := &RestoreCommand{}
	err := cmd.Run(context.Background(), []string{"-exec", "command-that-does-not-exist-xyz", "-o", outputPath, replicaURL})
	if err == nil {
		t.Fatal("expected exec start failure")
	}
	if !strings.Contains(err.Error(), "cannot start exec command") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRestoreCommand_Exec_SignalExitWithoutInterrupt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires shell signal handling")
	}

	replicaURL, workDir := createRestoreExecFixture(t)
	outputPath := filepath.Join(workDir, "follower.db")
	startMarker := filepath.Join(workDir, "started")
	scriptPath := filepath.Join(workDir, "sigterm-self.sh")
	script := fmt.Sprintf("#!/bin/sh\ntouch %q\nkill -TERM $$\n", startMarker)
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := &RestoreCommand{}
	execArg := fmt.Sprintf("sh %q", scriptPath)
	err := cmd.Run(context.Background(), []string{"-exec", execArg, "-o", outputPath, replicaURL})
	if err == nil {
		t.Fatal("expected signal exit error")
	}
	if !strings.Contains(err.Error(), "signal:") {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, statErr := os.Stat(startMarker); statErr != nil {
		t.Fatalf("exec command did not run: %v", statErr)
	}
}

func TestRestoreCommand_Exec_NotStartedOnSkip(t *testing.T) {
	t.Run("IfDBNotExistsSkip", func(t *testing.T) {
		replicaURL, workDir := createRestoreExecFixture(t)
		outputPath := filepath.Join(workDir, "existing.db")
		markerPath := filepath.Join(workDir, "started")
		scriptPath := filepath.Join(workDir, "start.sh")
		script := fmt.Sprintf("#!/bin/sh\ntouch %q\nexit 0\n", markerPath)
		if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(outputPath, []byte("existing"), 0o644); err != nil {
			t.Fatal(err)
		}

		cmd := &RestoreCommand{}
		execArg := fmt.Sprintf("sh %q", scriptPath)
		if err := cmd.Run(context.Background(), []string{"-exec", execArg, "-if-db-not-exists", "-o", outputPath, replicaURL}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
			t.Fatalf("exec command should not have started, stat err=%v", err)
		}
	})

	t.Run("IfReplicaExistsSkip", func(t *testing.T) {
		replicaDir := t.TempDir()
		replicaURL := "file://" + replicaDir
		workDir := t.TempDir()
		outputPath := filepath.Join(workDir, "follower.db")
		markerPath := filepath.Join(workDir, "started")
		scriptPath := filepath.Join(workDir, "start.sh")
		script := fmt.Sprintf("#!/bin/sh\ntouch %q\nexit 0\n", markerPath)
		if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}

		cmd := &RestoreCommand{}
		execArg := fmt.Sprintf("sh %q", scriptPath)
		if err := cmd.Run(context.Background(), []string{"-exec", execArg, "-if-replica-exists", "-o", outputPath, replicaURL}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
			t.Fatalf("exec command should not have started, stat err=%v", err)
		}
	})
}

func TestRestoreCommand_Exec_SecondSignalForcesShutdown(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires shell signal handling")
	}

	replicaURL, workDir := createRestoreExecFixture(t)
	outputPath := filepath.Join(workDir, "follower.db")
	startMarker := filepath.Join(workDir, "started")
	scriptPath := filepath.Join(workDir, "ignore.sh")
	script := fmt.Sprintf("#!/bin/sh\ntouch %q\ntrap '' INT TERM\nwhile true; do\n  sleep 1\ndone\n", startMarker)
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := &RestoreCommand{}
	sigCh := make(chan os.Signal, 2)
	cmd.signalChanFn = func() <-chan os.Signal { return sigCh }
	execArg := fmt.Sprintf("sh %q", scriptPath)
	errCh := make(chan error, 1)
	go func() {
		errCh <- cmd.Run(context.Background(), []string{"-exec", execArg, "-o", outputPath, replicaURL})
	}()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(startMarker); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(startMarker); err != nil {
		t.Fatalf("exec command did not start: %v", err)
	}

	sigCh <- syscall.SIGINT
	time.Sleep(50 * time.Millisecond)
	sigCh <- syscall.SIGINT

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("restore command returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("restore command did not shut down after second interrupt")
	}
}

func TestRestoreCommand_Exec_RunsOnResumePath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires shell signal handling")
	}

	replicaURL, workDir := createRestoreExecFixture(t)
	outputPath := filepath.Join(workDir, "follower.db")

	// First run: enter follow mode and interrupt to leave a restore+txid state.
	seedCmd := &RestoreCommand{}
	seedSigCh := make(chan os.Signal, 2)
	seedCmd.signalChanFn = func() <-chan os.Signal { return seedSigCh }
	seedErrCh := make(chan error, 1)
	go func() {
		seedErrCh <- seedCmd.Run(context.Background(), []string{"-f", "-o", outputPath, replicaURL})
	}()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(outputPath + "-txid"); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(outputPath + "-txid"); err != nil {
		t.Fatalf("txid sidecar not created: %v", err)
	}

	seedSigCh <- syscall.SIGINT
	select {
	case err := <-seedErrCh:
		if err != nil {
			t.Fatalf("seed follow restore returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("seed follow restore did not stop")
	}

	// Second run: existing output should take resume path and still run -exec.
	markerPath := filepath.Join(workDir, "resume-exec-started")
	scriptPath := filepath.Join(workDir, "resume.sh")
	script := fmt.Sprintf("#!/bin/sh\ntouch %q\nexit 0\n", markerPath)
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := &RestoreCommand{}
	execArg := fmt.Sprintf("sh %q", scriptPath)
	if err := cmd.Run(context.Background(), []string{"-exec", execArg, "-o", outputPath, replicaURL}); err != nil {
		t.Fatalf("resume run returned error: %v", err)
	}

	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("exec command did not run on resume path: %v", err)
	}
}

func TestRestoreCommand_Exec_FirstSignalBeforeStart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires unix signals")
	}

	replicaURL, workDir := createRestoreExecFixture(t)
	outputPath := filepath.Join(workDir, "follower.db")
	markerPath := filepath.Join(workDir, "started")
	scriptPath := filepath.Join(workDir, "start.sh")
	script := fmt.Sprintf("#!/bin/sh\ntouch %q\nwhile true; do sleep 1; done\n", markerPath)
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	sigCh := make(chan os.Signal, 1)
	cmd := &RestoreCommand{
		signalChanFn: func() <-chan os.Signal { return sigCh },
		restoreFn: func(ctx context.Context, _ *litestream.Replica, _ litestream.RestoreOptions) error {
			<-ctx.Done()
			return nil
		},
	}

	execArg := fmt.Sprintf("sh %q", scriptPath)
	errCh := make(chan error, 1)
	go func() {
		errCh <- cmd.Run(context.Background(), []string{"-exec", execArg, "-o", outputPath, replicaURL})
	}()

	sigCh <- syscall.SIGINT

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("restore command returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("restore command did not stop after interrupt before start")
	}

	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("exec command should not have started, stat err=%v", err)
	}
}

func TestRestoreCommand_Exec_RestoreErrorAfterStart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires shell signal handling")
	}

	replicaURL, workDir := createRestoreExecFixture(t)
	outputPath := filepath.Join(workDir, "follower.db")
	scriptPath := filepath.Join(workDir, "trap.sh")
	script := "#!/bin/sh\nwhile true; do\n  sleep 1\ndone\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("forced restore error")
	cmd := &RestoreCommand{
		restoreFn: func(_ context.Context, _ *litestream.Replica, opt litestream.RestoreOptions) error {
			if opt.OnRestored == nil {
				t.Fatal("expected OnRestored callback")
			}
			if err := opt.OnRestored(); err != nil {
				t.Fatalf("OnRestored failed: %v", err)
			}
			return wantErr
		},
	}

	execArg := fmt.Sprintf("sh %q", scriptPath)
	err := cmd.Run(context.Background(), []string{"-exec", execArg, "-o", outputPath, replicaURL})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected restore error, got %v", err)
	}
}

func createRestoreExecFixture(t *testing.T) (replicaURL, workDir string) {
	t.Helper()

	ctx := context.Background()
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE test(id INTEGER PRIMARY KEY, value TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.ExecContext(ctx, `INSERT INTO test VALUES (1, 'initial')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	replicaDir := t.TempDir()
	replicaClient := file.NewReplicaClient(replicaDir)
	replica := litestream.NewReplicaWithClient(db, replicaClient)
	if err := replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	return "file://" + replicaDir, t.TempDir()
}
