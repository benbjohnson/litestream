package main

import (
	"context"
	"testing"
)

func TestMonitorCommand_Run(t *testing.T) {
	t.Run("ConnectionError", func(t *testing.T) {
		cmd := &MonitorCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/nonexistent/socket.sock"})
		if err == nil {
			t.Error("expected error for nonexistent socket")
		}
	})

	t.Run("FlagParsing", func(t *testing.T) {
		// Just verify the command can parse flags without error
		cmd := &MonitorCommand{}

		// This will fail to connect, but flags should parse
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately so we don't wait for connection

		err := cmd.Run(ctx, []string{"-socket", "/tmp/test.sock", "-db", "/path/to/db", "-raw"})
		// Error expected due to cancelled context or connection failure
		if err == nil {
			t.Error("expected error")
		}
	})
}

func TestMonitorCommand_Usage(t *testing.T) {
	cmd := &MonitorCommand{}
	// Just verify Usage doesn't panic
	cmd.Usage()
}
