//go:build windows

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/eventlog"
)

const defaultConfigPath = `C:\Litestream\litestream.yml`

// serviceName is the Windows Service name.
const serviceName = "Litestream"

// isWindowsService returns true if currently executing within a Windows service.
func isWindowsService() (bool, error) {
	return svc.IsWindowsService()
}

func runWindowsService(ctx context.Context) error {
	// Attempt to install new log service. This will fail if already installed.
	// We don't log the error because we don't have anywhere to log until we open the log.
	_ = eventlog.InstallAsEventCreate(serviceName, eventlog.Error|eventlog.Warning|eventlog.Info)

	elog, err := eventlog.Open(serviceName)
	if err != nil {
		return err
	}
	defer elog.Close()

	// Set eventlog as log writer while running.
	slog.SetDefault(slog.New(slog.NewTextHandler((*eventlogWriter)(elog), nil)))
	defer slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	slog.Info("Litestream service starting")

	if err := svc.Run(serviceName, &windowsService{ctx: ctx}); err != nil {
		return errStop
	}

	slog.Info("Litestream service stopped")
	return nil
}

// windowsService is an interface adapter for svc.Handler.
type windowsService struct {
	ctx context.Context
}

func (s *windowsService) Execute(args []string, r <-chan svc.ChangeRequest, statusCh chan<- svc.Status) (svcSpecificEC bool, exitCode uint32) {
	var err error

	// Notify Windows that the service is starting up.
	statusCh <- svc.Status{State: svc.StartPending}

	// Instantiate replication command and load configuration.
	c := NewReplicateCommand()
	if c.Config, err = ReadConfigFile(DefaultConfigPath(), true); err != nil {
		slog.Error("cannot load configuration", "error", err)
		return true, 1
	}

	// Execute replication command.
	if err := c.Run(s.ctx); err != nil {
		slog.Error("cannot replicate", "error", err)
		statusCh <- svc.Status{State: svc.StopPending}
		return true, 2
	}

	// Notify Windows that the service is now running.
	statusCh <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop}

	for {
		select {
		case req := <-r:
			switch req.Cmd {
			case svc.Stop:
				c.Close(s.ctx)
				statusCh <- svc.Status{State: svc.StopPending}
				return false, windows.NO_ERROR
			case svc.Interrogate:
				statusCh <- req.CurrentStatus
			default:
				slog.Error("Litestream service received unexpected change request", "cmd", req.Cmd)
			}
		}
	}
}

// Ensure implementation implements io.Writer interface.
var _ io.Writer = (*eventlogWriter)(nil)

// eventlogWriter is an adapter for using eventlog.Log as an io.Writer.
type eventlogWriter eventlog.Log

func (w *eventlogWriter) Write(p []byte) (n int, err error) {
	elog := (*eventlog.Log)(w)
	return 0, elog.Info(1, string(p))
}

func signalChan() <-chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	return ch
}
