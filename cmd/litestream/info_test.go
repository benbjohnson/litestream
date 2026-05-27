package main_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/benbjohnson/litestream"
	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

var testSocketCounter uint64

func testSocketPath(t *testing.T) string {
	t.Helper()
	n := atomic.AddUint64(&testSocketCounter, 1)
	path := fmt.Sprintf("/tmp/ls-cmd-test-%d.sock", n)
	t.Cleanup(func() { os.Remove(path) })
	return path
}

func TestInfoCommand_Run(t *testing.T) {
	t.Run("TooManyArguments", func(t *testing.T) {
		cmd := &main.InfoCommand{}
		err := cmd.Run(context.Background(), []string{"extra-arg"})
		if err == nil {
			t.Error("expected error for too many arguments")
		}
		if err.Error() != "too many arguments" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("ConnectionError", func(t *testing.T) {
		cmd := &main.InfoCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/nonexistent/socket.sock"})
		if err == nil {
			t.Error("expected error for socket connection failure")
		}
	})

	t.Run("CustomTimeout", func(t *testing.T) {
		cmd := &main.InfoCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", "/nonexistent/socket.sock", "-timeout", "1"})
		if err == nil {
			t.Error("expected error for socket connection failure")
		}
	})

	t.Run("InvalidTimeoutZero", func(t *testing.T) {
		cmd := &main.InfoCommand{}
		err := cmd.Run(context.Background(), []string{"-timeout", "0"})
		if err == nil {
			t.Error("expected error for zero timeout")
		}
		if err.Error() != "timeout must be greater than 0" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("InvalidTimeoutNegative", func(t *testing.T) {
		cmd := &main.InfoCommand{}
		err := cmd.Run(context.Background(), []string{"-timeout", "-1"})
		if err == nil {
			t.Error("expected error for negative timeout")
		}
		if err.Error() != "timeout must be greater than 0" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("Success", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		if err := store.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer store.Close(context.Background())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		server.Version = "v1.0.0-test"
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		defer server.Close()

		cmd := &main.InfoCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", server.SocketPath})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("JSONOutput", func(t *testing.T) {
		db, sqldb := testingutil.MustOpenDBs(t)
		defer testingutil.MustCloseDBs(t, db, sqldb)

		store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
		store.CompactionMonitorEnabled = false
		if err := store.Open(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer store.Close(context.Background())

		server := litestream.NewServer(store)
		server.SocketPath = testSocketPath(t)
		server.Version = "v1.0.0-test"
		if err := server.Start(); err != nil {
			t.Fatal(err)
		}
		defer server.Close()

		cmd := &main.InfoCommand{}
		err := cmd.Run(context.Background(), []string{"-socket", server.SocketPath, "-json"})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}
