package main_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
	main "github.com/benbjohnson/litestream/cmd/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

func TestStartCommand_RunJSONOutput(t *testing.T) {
	db, sqldb := newStartStopCommandDB(t)
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
	store.CompactionMonitorEnabled = false
	defer store.Close(t.Context())

	server := litestream.NewServer(store)
	server.SocketPath = testSocketPath(t)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	output := captureStdout(t, func() {
		cmd := &main.StartCommand{}
		if err := cmd.Run(context.Background(), []string{"-json", "-socket", server.SocketPath, db.Path()}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var got main.StartStopResult
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("failed to parse output: %v\n%s", err, output)
	}
	if got.DBPath != db.Path() {
		t.Fatalf("unexpected db path: %s", got.DBPath)
	}
	if got.State != "running" {
		t.Fatalf("unexpected state: %s", got.State)
	}
	if got.TXID == 0 {
		t.Fatal("expected non-zero txid")
	}
	if got.Socket != server.SocketPath {
		t.Fatalf("unexpected socket: %s", got.Socket)
	}
}

func TestStopCommand_RunJSONOutput(t *testing.T) {
	db, _ := newStartStopCommandDB(t)

	store := litestream.NewStore([]*litestream.DB{db}, litestream.CompactionLevels{{Level: 0}})
	store.CompactionMonitorEnabled = false
	if err := store.Open(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer store.Close(t.Context())

	server := litestream.NewServer(store)
	server.SocketPath = testSocketPath(t)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	output := captureStdout(t, func() {
		cmd := &main.StopCommand{}
		if err := cmd.Run(context.Background(), []string{"-json", "-socket", server.SocketPath, db.Path()}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	var got main.StartStopResult
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf("failed to parse output: %v\n%s", err, output)
	}
	if got.DBPath != db.Path() {
		t.Fatalf("unexpected db path: %s", got.DBPath)
	}
	if got.State != "stopped" {
		t.Fatalf("unexpected state: %s", got.State)
	}
	if got.TXID == 0 {
		t.Fatal("expected non-zero txid")
	}
	if got.Socket != server.SocketPath {
		t.Fatalf("unexpected socket: %s", got.Socket)
	}
}

func TestStartCommand_Usage(t *testing.T) {
	output := captureStdout(t, func() {
		(&main.StartCommand{}).Usage()
	})

	for _, example := range []string{
		"Examples:",
		"$ litestream start /path/to/db",
		"$ litestream start -json /path/to/db",
		"$ litestream start -socket /tmp/litestream.sock /path/to/db",
		"$ litestream start -timeout 10 /path/to/db",
	} {
		if !strings.Contains(output, example) {
			t.Fatalf("usage output missing %q:\n%s", example, output)
		}
	}
}

func TestStopCommand_Usage(t *testing.T) {
	output := captureStdout(t, func() {
		(&main.StopCommand{}).Usage()
	})

	for _, example := range []string{
		"Examples:",
		"$ litestream stop /path/to/db",
		"$ litestream stop -json /path/to/db",
		"$ litestream stop -socket /tmp/litestream.sock /path/to/db",
		"$ litestream stop -timeout 10 /path/to/db",
	} {
		if !strings.Contains(output, example) {
			t.Fatalf("usage output missing %q:\n%s", example, output)
		}
	}
}

func newStartStopCommandDB(t *testing.T) (*litestream.DB, *sql.DB) {
	t.Helper()

	db, sqldb := testingutil.MustOpenDBs(t)
	t.Cleanup(func() {
		_ = sqldb.Close()
		_ = db.Close(context.Background())
		_ = os.RemoveAll(filepath.Dir(db.Path()))
	})

	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO t (id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}

	return db, sqldb
}
