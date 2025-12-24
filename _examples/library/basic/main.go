// Example: Basic Litestream Library Usage
//
// This example demonstrates the simplest way to use Litestream as a Go library.
// It replicates a SQLite database to the local filesystem.
//
// Run: go run main.go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	// Paths for this example
	dbPath := "./myapp.db"
	replicaPath := "./replica"

	// 1. Create the Litestream DB wrapper
	db := litestream.NewDB(dbPath)

	// 2. Create a replica client (file-based for this example)
	client := file.NewReplicaClient(replicaPath)

	// 3. Create a replica and attach it to the database
	replica := litestream.NewReplicaWithClient(db, client)
	db.Replica = replica
	client.Replica = replica

	// 4. Create compaction levels (L0 is required, plus at least one more level)
	levels := litestream.CompactionLevels{
		{Level: 0},
		{Level: 1, Interval: 10 * time.Second},
	}

	// 5. Create a Store to manage the database and background compaction
	store := litestream.NewStore([]*litestream.DB{db}, levels)

	// 6. Open the store (opens all DBs and starts background monitors)
	if err := store.Open(ctx); err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() {
		if err := store.Close(context.Background()); err != nil {
			log.Printf("close store: %v", err)
		}
	}()

	// 7. Open your app's SQLite connection for normal database operations
	sqlDB, err := openAppDB(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("open app db: %w", err)
	}
	defer sqlDB.Close()
	if err := initSchema(ctx, sqlDB); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}

	// Insert some test data periodically
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Handle shutdown gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Writing data every 2 seconds. Press Ctrl+C to stop.")
	fmt.Printf("Database: %s\n", dbPath)
	fmt.Printf("Replica:  %s\n", replicaPath)

	for {
		select {
		case <-ticker.C:
			if err := insertRow(ctx, sqlDB); err != nil {
				log.Printf("insert row: %v", err)
			}
		case <-sigCh:
			fmt.Println("\nShutting down...")
			return nil
		}
	}
}

func openAppDB(ctx context.Context, path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode = wal;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `PRAGMA busy_timeout = 5000;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func initSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			message TEXT NOT NULL,
			created_at TEXT NOT NULL
		)
	`)
	return err
}

func insertRow(ctx context.Context, db *sql.DB) error {
	msg := fmt.Sprintf("Event at %s", time.Now().Format(time.RFC3339))
	result, err := db.ExecContext(ctx,
		`INSERT INTO events (message, created_at) VALUES (?, ?)`,
		msg, time.Now().Format(time.RFC3339))
	if err != nil {
		return err
	}
	id, _ := result.LastInsertId()
	fmt.Printf("Inserted row %d: %s\n", id, msg)
	return nil
}
