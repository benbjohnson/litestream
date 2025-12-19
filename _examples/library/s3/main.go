// Example: Litestream Library Usage with S3 and Restore-on-Startup
//
// This example demonstrates a production-like pattern for using Litestream:
// - Check if local database exists
// - If not, restore from S3 backup (if available)
// - Start replication to S3
// - Graceful shutdown
//
// Environment variables:
//   - AWS_ACCESS_KEY_ID: AWS access key
//   - AWS_SECRET_ACCESS_KEY: AWS secret key
//   - LITESTREAM_BUCKET: S3 bucket name (e.g., "my-backup-bucket")
//   - LITESTREAM_PATH: Path within bucket (e.g., "databases/myapp")
//   - AWS_REGION: AWS region (default: us-east-1)
//
// Run: go run main.go
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/s3"
)

const dbPath = "./myapp.db"

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	// Load configuration from environment
	bucket := os.Getenv("LITESTREAM_BUCKET")
	if bucket == "" {
		return fmt.Errorf("LITESTREAM_BUCKET environment variable required")
	}
	path := os.Getenv("LITESTREAM_PATH")
	if path == "" {
		path = "litestream"
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	// 1. Create S3 replica client
	client := s3.NewReplicaClient()
	client.Bucket = bucket
	client.Path = path
	client.Region = region
	client.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	client.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")

	// 2. Restore from S3 if local database doesn't exist
	if err := restoreIfNotExists(ctx, client, dbPath); err != nil {
		return fmt.Errorf("restore: %w", err)
	}

	// 3. Create the Litestream DB wrapper
	db := litestream.NewDB(dbPath)

	// 4. Create replica and attach to database
	replica := litestream.NewReplicaWithClient(db, client)
	db.Replica = replica

	// 5. Create compaction levels (L0 is required, plus at least one more level)
	levels := litestream.CompactionLevels{
		{Level: 0},
		{Level: 1, Interval: 10 * time.Second},
	}

	// 6. Create a Store to manage the database and background compaction
	store := litestream.NewStore([]*litestream.DB{db}, levels)

	// 7. Open store (opens all DBs and starts background monitors)
	if err := store.Open(ctx); err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() {
		log.Println("Closing store...")
		if err := store.Close(context.Background()); err != nil {
			log.Printf("close store: %v", err)
		}
	}()

	// 8. Open your app's SQLite connection for normal operations
	sqlDB, err := openAppDB(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("open app db: %w", err)
	}
	defer sqlDB.Close()
	if err := initSchema(ctx, sqlDB); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}

	// Start the application
	log.Printf("Database: %s", dbPath)
	log.Printf("Replicating to: s3://%s/%s", bucket, path)
	log.Println("Writing data every 2 seconds. Press Ctrl+C to stop.")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-ticker.C:
			if err := insertRow(ctx, sqlDB); err != nil {
				log.Printf("insert row: %v", err)
			}
		case <-sigCh:
			log.Println("Shutting down...")
			return nil
		}
	}
}

// restoreIfNotExists restores the database from S3 if it doesn't exist locally.
func restoreIfNotExists(ctx context.Context, client *s3.ReplicaClient, dbPath string) error {
	// Check if database already exists
	if _, err := os.Stat(dbPath); err == nil {
		log.Println("Local database found, skipping restore")
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	log.Println("Local database not found, attempting restore from S3...")

	// Initialize the client
	if err := client.Init(ctx); err != nil {
		return fmt.Errorf("init s3 client: %w", err)
	}

	// Create a replica (without DB) for restore
	replica := litestream.NewReplicaWithClient(nil, client)

	// Set up restore options
	opt := litestream.NewRestoreOptions()
	opt.OutputPath = dbPath

	// Attempt restore
	if err := replica.Restore(ctx, opt); err != nil {
		// If no backup exists, that's OK - we'll create a fresh database
		if errors.Is(err, litestream.ErrTxNotAvailable) || errors.Is(err, litestream.ErrNoSnapshots) {
			log.Println("No backup found in S3, will create new database")
			return nil
		}
		return err
	}

	log.Println("Database restored from S3")
	return nil
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
	log.Printf("Inserted row %d: %s", id, msg)
	return nil
}
