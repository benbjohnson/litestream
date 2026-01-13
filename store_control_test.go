package litestream_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/benbjohnson/litestream"
)

func TestStore_RegisterDB(t *testing.T) {
	t.Run("RegisterNewDB", func(t *testing.T) {
		store := litestream.NewStore(nil, nil)
		db := litestream.NewDB(t.TempDir() + "/test.db")

		if err := store.RegisterDB(db); err != nil {
			t.Fatalf("RegisterDB failed: %v", err)
		}

		if found := store.FindDB(db.Path()); found == nil {
			t.Fatal("expected to find registered database")
		}
	})

	t.Run("RegisterDuplicateDB", func(t *testing.T) {
		store := litestream.NewStore(nil, nil)
		db := litestream.NewDB(t.TempDir() + "/test.db")

		if err := store.RegisterDB(db); err != nil {
			t.Fatalf("first RegisterDB failed: %v", err)
		}

		// Registering again should not error, just return nil
		if err := store.RegisterDB(db); err != nil {
			t.Fatalf("second RegisterDB failed: %v", err)
		}

		// Should still only have one database
		dbs := store.DBs()
		if len(dbs) != 1 {
			t.Fatalf("expected 1 database, got %d", len(dbs))
		}
	})

	t.Run("RegisterNilDB", func(t *testing.T) {
		store := litestream.NewStore(nil, nil)

		if err := store.RegisterDB(nil); err == nil {
			t.Fatal("expected error when registering nil DB")
		}
	})
}

func TestStore_FindDB(t *testing.T) {
	t.Run("FindExistingDB", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		db := litestream.NewDB(dbPath)

		store := litestream.NewStore(nil, nil)
		if err := store.RegisterDB(db); err != nil {
			t.Fatalf("RegisterDB failed: %v", err)
		}

		found := store.FindDB(dbPath)
		if found == nil {
			t.Fatal("expected to find database")
		}
		if found.Path() != dbPath {
			t.Fatalf("found wrong database: expected %s, got %s", dbPath, found.Path())
		}
	})

	t.Run("FindNonexistentDB", func(t *testing.T) {
		store := litestream.NewStore(nil, nil)

		found := store.FindDB("/nonexistent/db.db")
		if found != nil {
			t.Fatal("expected nil for nonexistent database")
		}
	})
}

func TestStore_EnableDB(t *testing.T) {
	t.Run("ErrorOnNonexistentDB", func(t *testing.T) {
		store := litestream.NewStore(nil, nil)
		ctx := context.Background()

		err := store.EnableDB(ctx, "/nonexistent/db.db")
		if err == nil {
			t.Fatal("expected error when enabling nonexistent database")
		}
		if err.Error() != "database not found: /nonexistent/db.db" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ErrorWhenNoReplica", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		db := litestream.NewDB(dbPath)
		store := litestream.NewStore(nil, nil)

		if err := store.RegisterDB(db); err != nil {
			t.Fatalf("RegisterDB failed: %v", err)
		}

		ctx := context.Background()
		// EnableDB will fail because there's no replica
		err := store.EnableDB(ctx, dbPath)
		if err == nil {
			t.Fatal("expected error when enabling DB without replica")
		}
	})
}

func TestStore_DisableDB(t *testing.T) {
	t.Run("ErrorOnNonexistentDB", func(t *testing.T) {
		store := litestream.NewStore(nil, nil)
		ctx := context.Background()

		err := store.DisableDB(ctx, "/nonexistent/db.db")
		if err == nil {
			t.Fatal("expected error when disabling nonexistent database")
		}
		if err.Error() != "database not found: /nonexistent/db.db" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ErrorWhenNotOpen", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		db := litestream.NewDB(dbPath)
		store := litestream.NewStore(nil, nil)

		if err := store.RegisterDB(db); err != nil {
			t.Fatalf("RegisterDB failed: %v", err)
		}

		ctx := context.Background()
		// DisableDB will fail because database is not open
		err := store.DisableDB(ctx, dbPath)
		if err == nil {
			t.Fatal("expected error when disabling non-open database")
		}
		if err.Error() != "database already disabled: "+dbPath {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
