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

func TestDirectoryMonitor_pendingEvents(t *testing.T) {
	t.Run("coalesces multiple events for same path", func(t *testing.T) {
		dm := &DirectoryMonitor{
			pendingEvents: make(map[string]fsnotify.Op),
		}

		dm.pendingEvents["/path/to/test.db"] |= fsnotify.Create
		dm.pendingEvents["/path/to/test.db"] |= fsnotify.Write
		dm.pendingEvents["/path/to/test.db"] |= fsnotify.Write

		if len(dm.pendingEvents) != 1 {
			t.Errorf("expected 1 pending event, got %d", len(dm.pendingEvents))
		}

		op := dm.pendingEvents["/path/to/test.db"]
		if op&fsnotify.Create == 0 {
			t.Error("expected Create op to be set")
		}
		if op&fsnotify.Write == 0 {
			t.Error("expected Write op to be set")
		}
	})

	t.Run("queues different paths separately", func(t *testing.T) {
		dm := &DirectoryMonitor{
			pendingEvents: make(map[string]fsnotify.Op),
		}

		dm.pendingEvents["/path/to/db1.db"] |= fsnotify.Create
		dm.pendingEvents["/path/to/db2.db"] |= fsnotify.Create
		dm.pendingEvents["/path/to/db3.db"] |= fsnotify.Write

		if len(dm.pendingEvents) != 3 {
			t.Errorf("expected 3 pending events, got %d", len(dm.pendingEvents))
		}
	})
}
