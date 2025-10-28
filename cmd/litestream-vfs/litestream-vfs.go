//go:build SQLITE3VFS_LOADABLE_EXT
// +build SQLITE3VFS_LOADABLE_EXT

package main

// import C is necessary export to the c-archive .a file

import "C"

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/psanford/sqlite3vfs"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/s3"
)

func main() {}

//export sqlite3_extension_init
func sqlite3_extension_init() {
	println("dbg/LitestreamVFSRegister")
	client, err := newReplicaClientFromEnv()
	if err != nil {
		log.Fatalf("failed to create replica client: %s", err)
	}

	var level slog.Level
	switch strings.ToUpper(os.Getenv("LITESTREAM_LOG_LEVEL")) {
	case "DEBUG":
		level = slog.LevelDebug
	default:
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	vfs := litestream.NewVFS(client, logger)

	if err := sqlite3vfs.RegisterVFS("litestream", vfs); err != nil {
		log.Fatalf("failed to register litestream vfs: %s", err)
	}
}

func newReplicaClientFromEnv() (_ litestream.ReplicaClient, err error) {
	switch os.Getenv("LITESTREAM_REPLICA_TYPE") {
	case "file":
		return newFileReplicaClientFromEnv()
	case "s3":
		return newS3ReplicaClientFromEnv()
	default:
		return nil, fmt.Errorf("unknown replica type: %s", os.Getenv("LITESTREAM_REPLICA_TYPE"))
	}
}

func newFileReplicaClientFromEnv() (_ litestream.ReplicaClient, err error) {
	path := os.Getenv("LITESTREAM_REPLICA_PATH")
	if path == "" {
		return nil, fmt.Errorf("LITESTREAM_REPLICA_PATH is required")
	}
	return file.NewReplicaClient(path), nil
}

func newS3ReplicaClientFromEnv() (_ litestream.ReplicaClient, err error) {
	client := s3.NewReplicaClient()
	client.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	client.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	client.Region = os.Getenv("LITESTREAM_S3_REGION")
	client.Bucket = os.Getenv("LITESTREAM_S3_BUCKET")
	client.Path = os.Getenv("LITESTREAM_S3_PATH")
	client.Endpoint = os.Getenv("LITESTREAM_S3_ENDPOINT")

	if v := os.Getenv("LITESTREAM_S3_FORCE_PATH_STYLE"); v != "" {
		if client.ForcePathStyle, err = strconv.ParseBool(v); err != nil {
			return nil, fmt.Errorf("failed to parse LITESTREAM_S3_FORCE_PATH_STYLE: %w", err)
		}
	}

	if v := os.Getenv("LITESTREAM_S3_SKIP_VERIFY"); v != "" {
		if client.SkipVerify, err = strconv.ParseBool(v); err != nil {
			return nil, fmt.Errorf("failed to parse LITESTREAM_S3_SKIP_VERIFY: %w", err)
		}
	}

	if err := client.Init(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize litestream s3 client: %w", err)
	}

	return client, nil
}
