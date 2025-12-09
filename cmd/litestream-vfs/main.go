//go:build SQLITE3VFS_LOADABLE_EXT
// +build SQLITE3VFS_LOADABLE_EXT

package main

// import C is necessary export to the c-archive .a file

/*
typedef long long int sqlite3_int64;
typedef unsigned long long int sqlite3_uint64;
*/
import "C"

import (
	"context"
	"log"
	"log/slog"
	"os"
	"strings"
	"unsafe"

	"github.com/psanford/sqlite3vfs"

	"github.com/benbjohnson/litestream"

	// Import all replica backends to register their URL factories.
	_ "github.com/benbjohnson/litestream/abs"
	_ "github.com/benbjohnson/litestream/file"
	_ "github.com/benbjohnson/litestream/gs"
	_ "github.com/benbjohnson/litestream/nats"
	_ "github.com/benbjohnson/litestream/oss"
	_ "github.com/benbjohnson/litestream/s3"
	_ "github.com/benbjohnson/litestream/sftp"
	_ "github.com/benbjohnson/litestream/webdav"
)

func main() {}

//export LitestreamVFSRegister
func LitestreamVFSRegister() {
	var client litestream.ReplicaClient
	var err error

	replicaURL := os.Getenv("LITESTREAM_REPLICA_URL")
	client, err = litestream.NewReplicaClientFromURL(replicaURL)
	if err != nil {
		log.Fatalf("failed to create replica client from URL: %s", err)
	}

	// Initialize the client.
	if err := client.Init(context.Background()); err != nil {
		log.Fatalf("failed to initialize litestream replica client: %s", err)
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

//export GoLitestreamTime
func GoLitestreamTime(dbPtr unsafe.Pointer, out **C.char) *C.char {
	value, err := litestream.GetVFSConnectionTime(uintptr(dbPtr))
	if err != nil {
		return C.CString(err.Error())
	}
	if out != nil {
		*out = C.CString(value)
	}
	return nil
}

//export GoLitestreamTxid
func GoLitestreamTxid(dbPtr unsafe.Pointer, out **C.char) *C.char {
	value, err := litestream.GetVFSConnectionTXID(uintptr(dbPtr))
	if err != nil {
		return C.CString(err.Error())
	}
	if out != nil {
		*out = C.CString(value)
	}
	return nil
}

//export GoLitestreamLag
func GoLitestreamLag(dbPtr unsafe.Pointer, out *C.sqlite3_int64) *C.char {
	value, err := litestream.GetVFSConnectionLag(uintptr(dbPtr))
	if err != nil {
		return C.CString(err.Error())
	}
	if out != nil {
		*out = C.sqlite3_int64(value)
	}
	return nil
}
