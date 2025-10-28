//go:build SQLITE3VFS_LOADABLE_EXT
// +build SQLITE3VFS_LOADABLE_EXT

package main

// import C is necessary export to the c-archive .a file

/*
#cgo CFLAGS: -I${SRCDIR}/../src
#include "sqlite3.h"
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/psanford/sqlite3vfs"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/s3"
)

func main() {}

//export litestream_sqlite3_extension_init
func litestream_sqlite3_extension_init() int {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in sqlite3_extension_init: %v", r)
		}
	}()

	var level slog.Level
	switch strings.ToUpper(os.Getenv("LITESTREAM_LOG_LEVEL")) {
	case "DEBUG":
		level = slog.LevelDebug
	default:
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	// Create a lazy replica client that reads env vars on first use
	lazyClient := &LazyReplicaClient{logger: logger}
	vfs := litestream.NewVFS(lazyClient, logger)

	logger.Debug("registering litestream VFS")
	if err := sqlite3vfs.RegisterVFS("litestream", vfs); err != nil {
		log.Printf("failed to register litestream VFS: %s", err)
		return 1 // SQLITE_ERROR
	}
	logger.Debug("VFS registered successfully")
	return 0 // SQLITE_OK
}

// LazyReplicaClient wraps a ReplicaClient and defers initialization until first use.
// This allows environment variables to be set after the extension loads but before
// the VFS is actually used.
type LazyReplicaClient struct {
	mu     sync.Mutex
	client litestream.ReplicaClient
	logger *slog.Logger
	err    error
}

func (c *LazyReplicaClient) init() (litestream.ReplicaClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Return cached client or error
	if c.client != nil || c.err != nil {
		return c.client, c.err
	}

	// Initialize from environment variables
	replicaType := getenv("LITESTREAM_REPLICA_TYPE")
	replicaPath := getenv("LITESTREAM_REPLICA_PATH")
	c.logger.Debug("initializing replica client from environment variables",
		"type", replicaType,
		"path", replicaPath)

	c.client, c.err = newReplicaClientFromEnv(replicaType, replicaPath)
	return c.client, c.err
}

func (c *LazyReplicaClient) Type() string {
	client, err := c.init()
	if err != nil {
		return ""
	}
	return client.Type()
}

func (c *LazyReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	client, err := c.init()
	if err != nil {
		return nil, err
	}
	return client.LTXFiles(ctx, level, seek, useMetadata)
}

func (c *LazyReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	client, err := c.init()
	if err != nil {
		return nil, err
	}
	return client.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
}

func (c *LazyReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	client, err := c.init()
	if err != nil {
		return nil, err
	}
	return client.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

func (c *LazyReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	client, err := c.init()
	if err != nil {
		return err
	}
	return client.DeleteLTXFiles(ctx, a)
}

func (c *LazyReplicaClient) DeleteAll(ctx context.Context) error {
	client, err := c.init()
	if err != nil {
		return err
	}
	return client.DeleteAll(ctx)
}

func newReplicaClientFromEnv(replicaType, replicaPath string) (_ litestream.ReplicaClient, err error) {
	switch replicaType {
	case "file":
		return newFileReplicaClient(replicaPath)
	case "s3":
		return newS3ReplicaClient(replicaPath)
	default:
		return nil, fmt.Errorf("unknown replica type: %s", replicaType)
	}
}

func newFileReplicaClient(path string) (_ litestream.ReplicaClient, err error) {
	if path == "" {
		return nil, fmt.Errorf("LITESTREAM_REPLICA_PATH is required")
	}
	return file.NewReplicaClient(path), nil
}

func getenv(key string) string {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	if v := C.getenv(cKey); v != nil {
		return C.GoString(v)
	}
	return os.Getenv(key)
}

func newS3ReplicaClient(replicaPath string) (_ litestream.ReplicaClient, err error) {
	client := s3.NewReplicaClient()
	client.AccessKeyID = getenv("AWS_ACCESS_KEY_ID")
	client.SecretAccessKey = getenv("AWS_SECRET_ACCESS_KEY")
	client.Region = getenv("LITESTREAM_S3_REGION")
	client.Bucket = getenv("LITESTREAM_S3_BUCKET")
	if replicaPath != "" {
		client.Path = replicaPath
	} else {
		client.Path = getenv("LITESTREAM_S3_PATH")
	}
	client.Endpoint = getenv("LITESTREAM_S3_ENDPOINT")

	if v := getenv("LITESTREAM_S3_FORCE_PATH_STYLE"); v != "" {
		if client.ForcePathStyle, err = strconv.ParseBool(v); err != nil {
			return nil, fmt.Errorf("failed to parse LITESTREAM_S3_FORCE_PATH_STYLE: %w", err)
		}
	}

	if v := getenv("LITESTREAM_S3_SKIP_VERIFY"); v != "" {
		if client.SkipVerify, err = strconv.ParseBool(v); err != nil {
			return nil, fmt.Errorf("failed to parse LITESTREAM_S3_SKIP_VERIFY: %w", err)
		}
	}

	if err := client.Init(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize litestream s3 client: %w", err)
	}

	return client, nil
}
