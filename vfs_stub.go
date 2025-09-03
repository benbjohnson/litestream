//go:build !cgo || novfs
// +build !cgo novfs

package litestream

import (
	"log/slog"
	"time"
)

const (
	DefaultPollInterval = 1 * time.Second
)

// VFS implements the SQLite VFS interface for Litestream.
// It is intended to be used for read replicas that read directly from S3.
// This is a stub implementation for builds without CGO support.
type VFS struct {
	client ReplicaClient
	logger *slog.Logger

	// PollInterval is the interval at which to poll the replica client for new
	// LTX files. The index will be fetched for the new files automatically.
	PollInterval time.Duration
}

func NewVFS(client ReplicaClient, logger *slog.Logger) *VFS {
	panic("VFS support requires CGO and is not available in this build")
}

// VFSFile represents a single database file within the VFS.
// This is a stub implementation for builds without CGO support.
type VFSFile struct{}

func NewVFSFile(client ReplicaClient, name string, logger *slog.Logger) *VFSFile {
	panic("VFS support requires CGO and is not available in this build")
}
