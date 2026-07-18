//go:build windows

package main

import (
	"context"
	"errors"
	"slices"
	"testing"

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
