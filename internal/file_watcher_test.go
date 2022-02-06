package internal_test

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/litestream/internal"
	_ "github.com/mattn/go-sqlite3"
)

func TestFileWatcher(t *testing.T) {
	t.Run("WriteAndRemove", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "db")

		w := internal.NewFileWatcher()
		if err := w.Open(); err != nil {
			t.Fatal(err)
		}
		defer w.Close()

		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		if _, err := db.Exec(`PRAGMA journal_mode = wal`); err != nil {
			t.Fatal(err)
		} else if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}

		if err := w.Watch(dbPath + "-wal"); err != nil {
			t.Fatal(err)
		}

		// Write to the WAL file & ensure a "modified" event occurs.
		if _, err := db.Exec(`INSERT INTO t (x) VALUES (1)`); err != nil {
			t.Fatal(err)
		}

		select {
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for event")
		case event := <-w.Events():
			if got, want := event.Name, dbPath+"-wal"; got != want {
				t.Fatalf("name=%s, want %s", got, want)
			} else if got, want := event.Mask, internal.FileEventModified; got != want {
				t.Fatalf("mask=0x%02x, want 0x%02x", got, want)
			}
		}

		// Flush any duplicate events.
		drainFileEventChannel(w.Events())

		// Close database and ensure checkpointed WAL creates a "delete" event.
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		select {
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for event")
		case event := <-w.Events():
			if got, want := event.Name, dbPath+"-wal"; got != want {
				t.Fatalf("name=%s, want %s", got, want)
			} else if got, want := event.Mask, internal.FileEventDeleted; got != want {
				t.Fatalf("mask=0x%02x, want 0x%02x", got, want)
			}
		}
	})

	t.Run("LargeTx", func(t *testing.T) {
		w := internal.NewFileWatcher()
		if err := w.Open(); err != nil {
			t.Fatal(err)
		}
		defer w.Close()

		dbPath := filepath.Join(t.TempDir(), "db")
		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			t.Fatal(err)
		} else if _, err := db.Exec(`PRAGMA cache_size = 4`); err != nil {
			t.Fatal(err)
		} else if _, err := db.Exec(`PRAGMA journal_mode = wal`); err != nil {
			t.Fatal(err)
		} else if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		if err := w.Watch(dbPath + "-wal"); err != nil {
			t.Fatal(err)
		}

		// Start a transaction to ensure writing large data creates multiple write events.
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()

		// Write enough data to require a spill.
		for i := 0; i < 100; i++ {
			if _, err := tx.Exec(`INSERT INTO t (x) VALUES (?)`, strings.Repeat("x", 512)); err != nil {
				t.Fatal(err)
			}
		}

		// Ensure spill writes to disk.
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for event")
		case event := <-w.Events():
			if got, want := event.Name, dbPath+"-wal"; got != want {
				t.Fatalf("name=%s, want %s", got, want)
			} else if got, want := event.Mask, internal.FileEventModified; got != want {
				t.Fatalf("mask=0x%02x, want 0x%02x", got, want)
			}
		}

		// Flush any duplicate events.
		drainFileEventChannel(w.Events())

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		// Final commit should spill remaining pages and cause another write event.
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for event")
		case event := <-w.Events():
			if got, want := event.Name, dbPath+"-wal"; got != want {
				t.Fatalf("name=%s, want %s", got, want)
			} else if got, want := event.Mask, internal.FileEventModified; got != want {
				t.Fatalf("mask=0x%02x, want 0x%02x", got, want)
			}
		}
	})

	t.Run("WatchBeforeCreate", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "db")

		w := internal.NewFileWatcher()
		if err := w.Open(); err != nil {
			t.Fatal(err)
		}
		defer w.Close()

		if err := w.Watch(dbPath); err != nil {
			t.Fatal(err)
		} else if err := w.Watch(dbPath + "-wal"); err != nil {
			t.Fatal(err)
		}

		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}

		// Wait for main database creation event.
		waitForFileEvent(t, w.Events(), internal.FileEvent{Name: dbPath, Mask: internal.FileEventCreated})

		// Write to the WAL file & ensure a "modified" event occurs.
		if _, err := db.Exec(`PRAGMA journal_mode = wal`); err != nil {
			t.Fatal(err)
		} else if _, err := db.Exec(`INSERT INTO t (x) VALUES (1)`); err != nil {
			t.Fatal(err)
		}

		// Wait for WAL creation event.
		waitForFileEvent(t, w.Events(), internal.FileEvent{Name: dbPath + "-wal", Mask: internal.FileEventCreated})
	})
}

func drainFileEventChannel(ch <-chan internal.FileEvent) {
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			return
		case <-ch:
		}
	}
}

func waitForFileEvent(tb testing.TB, ch <-chan internal.FileEvent, want internal.FileEvent) {
	tb.Helper()

	timeout := time.After(10 * time.Second)

	for {
		select {
		case <-timeout:
			tb.Fatalf("timeout waiting for event: %#v", want)
		case got := <-ch:
			if got == want {
				return
			}
		}
	}
}
