//go:build SQLITE3VFS_LOADABLE_EXT
// +build SQLITE3VFS_LOADABLE_EXT

package main

// import C is necessary export to the c-archive .a file

import "C"

import (
	"context"
	"log"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"unsafe"

	"github.com/psanford/sqlite3vfs"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/s3"
)

func main() {}

//export LitestreamVFSRegister
func LitestreamVFSRegister() {
	var err error
	client := s3.NewReplicaClient()
	client.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	client.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	client.Region = os.Getenv("LITESTREAM_S3_REGION")
	client.Bucket = os.Getenv("LITESTREAM_S3_BUCKET")
	client.Path = os.Getenv("LITESTREAM_S3_PATH")
	client.Endpoint = os.Getenv("LITESTREAM_S3_ENDPOINT")

	if v := os.Getenv("LITESTREAM_S3_FORCE_PATH_STYLE"); v != "" {
		if client.ForcePathStyle, err = strconv.ParseBool(v); err != nil {
			log.Fatalf("failed to parse LITESTREAM_S3_FORCE_PATH_STYLE: %s", err)
		}
	}

	if v := os.Getenv("LITESTREAM_S3_SKIP_VERIFY"); v != "" {
		if client.SkipVerify, err = strconv.ParseBool(v); err != nil {
			log.Fatalf("failed to parse LITESTREAM_S3_SKIP_VERIFY: %s", err)
		}
	}

	if err := client.Init(context.Background()); err != nil {
		log.Fatalf("failed to initialize litestream s3 client: %s", err)
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

//export GoLitestreamRegisterConnection
func GoLitestreamRegisterConnection(dbPtr unsafe.Pointer, fileID C.sqlite3_uint64) *C.char {
	if err := litestream.RegisterVFSConnection(uintptr(dbPtr), uint64(fileID)); err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export GoLitestreamUnregisterConnection
func GoLitestreamUnregisterConnection(dbPtr unsafe.Pointer) *C.char {
	litestream.UnregisterVFSConnection(uintptr(dbPtr))
	return nil
}

//export GoLitestreamSetTime
func GoLitestreamSetTime(dbPtr unsafe.Pointer, timestamp *C.char) *C.char {
	if timestamp == nil {
		return C.CString("timestamp required")
	}
	if err := litestream.SetVFSConnectionTime(uintptr(dbPtr), C.GoString(timestamp)); err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export GoLitestreamResetTime
func GoLitestreamResetTime(dbPtr unsafe.Pointer) *C.char {
	if err := litestream.ResetVFSConnectionTime(uintptr(dbPtr)); err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export GoLitestreamCurrentTime
func GoLitestreamCurrentTime(dbPtr unsafe.Pointer, out **C.char) *C.char {
	value, err := litestream.CurrentVFSConnectionTime(uintptr(dbPtr))
	if err != nil {
		return C.CString(err.Error())
	}
	if out != nil {
		*out = C.CString(value)
	}
	return nil
}
