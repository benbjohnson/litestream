package main

import (
	"testing"

	"github.com/fsnotify/fsnotify"
)

func TestDirectoryMonitor_shouldSkipPath(t *testing.T) {
	dm := &DirectoryMonitor{}

	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		// Should skip SQLite auxiliary files
		{"skip WAL file", "/path/to/db.sqlite-wal", true},
		{"skip SHM file", "/path/to/db.sqlite-shm", true},
		{"skip journal file", "/path/to/db.sqlite-journal", true},
		{"skip WAL file simple", "test.db-wal", true},
		{"skip SHM file simple", "test.db-shm", true},
		{"skip journal file simple", "test.db-journal", true},

		// Should not skip actual database files
		{"allow .db file", "/path/to/test.db", false},
		{"allow .sqlite file", "/path/to/test.sqlite", false},
		{"allow .sqlite3 file", "/path/to/test.sqlite3", false},
		{"allow arbitrary file", "/path/to/data.dat", false},

		// Edge cases
		{"allow file ending in wal (not -wal)", "/path/to/withdrawal", false},
		{"allow file ending in shm (not -shm)", "/path/to/rhythm", false},
		{"allow file ending in journal (not -journal)", "/path/to/myjournal", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dm.shouldSkipPath(tt.path)
			if got != tt.expected {
				t.Errorf("shouldSkipPath(%q) = %v, want %v", tt.path, got, tt.expected)
			}
		})
	}
}

func TestDirectoryMonitor_matchesPattern(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		path     string
		expected bool
	}{
		// *.db pattern
		{"matches .db", "*.db", "/path/to/test.db", true},
		{"no match .sqlite", "*.db", "/path/to/test.sqlite", false},
		{"no match .db.backup", "*.db", "/path/to/test.db.backup", false},

		// *.sqlite pattern
		{"matches .sqlite", "*.sqlite", "/path/to/test.sqlite", true},
		{"no match .db with sqlite pattern", "*.sqlite", "/path/to/test.db", false},

		// * (match all) pattern
		{"match all pattern", "*", "/path/to/anything.db", true},
		{"match all pattern sqlite", "*", "/path/to/anything.sqlite", true},

		// prefix patterns
		{"prefix match", "app_*.db", "/path/to/app_users.db", true},
		{"prefix no match", "app_*.db", "/path/to/users.db", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dm := &DirectoryMonitor{pattern: tt.pattern}
			got := dm.matchesPattern(tt.path)
			if got != tt.expected {
				t.Errorf("matchesPattern(%q) with pattern %q = %v, want %v", tt.path, tt.pattern, got, tt.expected)
			}
		})
	}
}

func TestDirectoryMonitor_queuePotentialDatabase(t *testing.T) {
	t.Run("coalesces multiple events", func(t *testing.T) {
		dm := &DirectoryMonitor{
			pendingEvents: make(map[string]fsnotify.Op),
		}

		// Queue multiple events for the same path
		dm.queuePotentialDatabase("/path/to/test.db", fsnotify.Create)
		dm.queuePotentialDatabase("/path/to/test.db", fsnotify.Write)
		dm.queuePotentialDatabase("/path/to/test.db", fsnotify.Write)

		dm.debounceMu.Lock()
		defer dm.debounceMu.Unlock()

		// Should have only one entry
		if len(dm.pendingEvents) != 1 {
			t.Errorf("expected 1 pending event, got %d", len(dm.pendingEvents))
		}

		// Operations should be merged
		op := dm.pendingEvents["/path/to/test.db"]
		if op&fsnotify.Create == 0 {
			t.Error("expected Create op to be set")
		}
		if op&fsnotify.Write == 0 {
			t.Error("expected Write op to be set")
		}

		// Debounce timer should be running
		if !dm.debounceRunning {
			t.Error("expected debounceRunning to be true")
		}

		// Clean up timer
		if dm.debounceTimer != nil {
			dm.debounceTimer.Stop()
		}
	})

	t.Run("queues different paths separately", func(t *testing.T) {
		dm := &DirectoryMonitor{
			pendingEvents: make(map[string]fsnotify.Op),
		}

		dm.queuePotentialDatabase("/path/to/db1.db", fsnotify.Create)
		dm.queuePotentialDatabase("/path/to/db2.db", fsnotify.Create)
		dm.queuePotentialDatabase("/path/to/db3.db", fsnotify.Write)

		dm.debounceMu.Lock()
		defer dm.debounceMu.Unlock()

		if len(dm.pendingEvents) != 3 {
			t.Errorf("expected 3 pending events, got %d", len(dm.pendingEvents))
		}

		// Clean up timer
		if dm.debounceTimer != nil {
			dm.debounceTimer.Stop()
		}
	})
}

func TestDirectoryMonitor_initSemaphore(t *testing.T) {
	t.Run("semaphore limits concurrency", func(t *testing.T) {
		// Create a semaphore with capacity 2
		initSem := make(chan struct{}, 2)

		// Fill the semaphore
		initSem <- struct{}{}
		initSem <- struct{}{}

		// Verify it's full (non-blocking check)
		select {
		case initSem <- struct{}{}:
			t.Error("expected semaphore to be full")
			<-initSem // drain if unexpectedly successful
		default:
			// Expected: semaphore is full
		}

		// Release one slot
		<-initSem

		// Now we should be able to acquire
		select {
		case initSem <- struct{}{}:
			// Expected: acquired slot
		default:
			t.Error("expected to acquire semaphore slot after release")
		}

		// Drain
		<-initSem
		<-initSem
	})
}
