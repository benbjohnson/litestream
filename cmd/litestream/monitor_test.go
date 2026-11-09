package main

import (
	"context"
	"strings"
	"testing"
)

func TestMonitorCommand_Run(t *testing.T) {
	t.Run("ConnectionError", func(t *testing.T) {
		cmd := &MonitorCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/nonexistent/socket.sock"})
		if err == nil {
			t.Error("expected error for nonexistent socket")
		}
		if !strings.Contains(err.Error(), "Try: ensure litestream replicate is running with a configured socket: block") {
			t.Fatalf("expected socket hint, got %q", err.Error())
		}
	})

	t.Run("FlagParsing", func(t *testing.T) {
		// Just verify the command can parse flags without error
		cmd := &MonitorCommand{}

		// This will fail to connect, but flags should parse
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately so we don't wait for connection

		err := cmd.Run(ctx, []string{"-socket", "/tmp/test.sock", "-db", "/path/to/db", "-json"})
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
