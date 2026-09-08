//go:build windows

package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"slices"
	"testing"
	"time"

	"golang.org/x/sys/windows/svc"
)

func TestWindowsServiceExecuteStopCloseError(t *testing.T) {
	closeErr := errors.New("test close error")
	command := NewReplicateCommand()
	command.Config = DefaultConfig()
	service := &windowsService{
		ctx:     t.Context(),
		command: command,
		closeCommand: func(ctx context.Context, command *ReplicateCommand) error {
			return errors.Join(closeErr, command.Close(ctx))
		},
	}
	requestCh := make(chan svc.ChangeRequest, 1)
	requestCh <- svc.ChangeRequest{Cmd: svc.Stop}
	statusCh := make(chan svc.Status, 3)

	serviceSpecific, exitCode := service.Execute(nil, requestCh, statusCh)
	if !serviceSpecific {
		t.Fatal("serviceSpecific=false, want true")
	}
	if exitCode != windowsServiceCloseExitCode {
		t.Fatalf("exitCode=%d, want %d", exitCode, windowsServiceCloseExitCode)
	}

	states := make([]svc.State, 0, len(statusCh))
	close(statusCh)
	for status := range statusCh {
		states = append(states, status.State)
	}
	want := []svc.State{svc.StartPending, svc.Running, svc.StopPending}
	if !slices.Equal(states, want) {
		t.Fatalf("states=%v, want %v", states, want)
	}
}

func TestWindowsServiceCloseStopsExec(t *testing.T) {
	command := NewReplicateCommand()
	command.cmd = exec.Command(os.Args[0], "-test.run=^TestWindowsServiceExecHelper$")
	command.cmd.Env = append(os.Environ(), "LITESTREAM_TEST_SERVICE_EXEC=1")
	command.execCh = make(chan error, 1)
	if err := command.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { command.execCh <- command.cmd.Wait() }()
	service := &windowsService{ctx: t.Context()}
	if err := service.close(command); err != nil {
		t.Fatal(err)
	}
	if command.cmd.ProcessState == nil || command.cmd.ProcessState.Success() {
		t.Fatal("exec child was not terminated and reaped")
	}
}

func TestWindowsServiceExecHelper(t *testing.T) {
	if os.Getenv("LITESTREAM_TEST_SERVICE_EXEC") != "1" {
		t.Skip("helper process only")
	}
	time.Sleep(time.Minute)
}
