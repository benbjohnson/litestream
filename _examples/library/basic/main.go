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

	// 4. Open the database (starts background replication)
	if err := db.Open(); err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() {
		if err := db.Close(context.Background()); err != nil {
			log.Printf("close database: %v", err)
		}
	}()

	// 5. Use the underlying *sql.DB for normal database operations
	sqlDB := db.SQLDB()
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
