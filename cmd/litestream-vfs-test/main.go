package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/s3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	client := s3.NewReplicaClient()
	client.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	client.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	client.Region = "us-east-1"
	client.Bucket = "litestream-vfs"
	client.Path = "my.db"
	if err := client.Init(context.Background()); err != nil {
		return fmt.Errorf("failed to initialize litestream s3 client: %w", err)
	}

	vfs := litestream.NewVFS(client, logger)

	err := sqlite3vfs.RegisterVFS("litestream", vfs)
	if err != nil {
		return fmt.Errorf("failed to register litestream vfs: %w", err)
	}

	db, err := sql.Open("sqlite3", "file:/tmp/test.db?vfs=litestream")
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Execute query
	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		return fmt.Errorf("failed to query database: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var x int
		if err := rows.Scan(&x); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		fmt.Println(x)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate rows: %w", err)
	}

	return nil
}
