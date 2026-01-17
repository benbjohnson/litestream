package main_test

import (
	"context"
	"testing"

	main "github.com/benbjohnson/litestream/cmd/litestream"
)

func TestListCommand_Run(t *testing.T) {
	t.Run("TooManyArguments", func(t *testing.T) {
		cmd := &main.ListCommand{}
		err := cmd.Run(context.Background(), []string{"extra-arg"})
		if err == nil {
			t.Error("expected error for too many arguments")
		}
		if err.Error() != "too many arguments" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("ConnectionError", func(t *testing.T) {
		cmd := &main.ListCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/nonexistent/socket.sock"})
		if err == nil {
			t.Error("expected error for socket connection failure")
		}
	})

	t.Run("CustomTimeout", func(t *testing.T) {
		cmd := &main.ListCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/nonexistent/socket.sock", "-timeout", "1"})
		if err == nil {
			t.Error("expected error for socket connection failure")
		}
	})
}
