//go:build vfs

package litestream

import (
	"context"
	"fmt"
	"sync"
	"time"
	_ "unsafe"

	"github.com/psanford/sqlite3vfs"
)

var (
	//go:linkname sqlite3vfsFileMap github.com/psanford/sqlite3vfs.fileMap
	sqlite3vfsFileMap map[uint64]sqlite3vfs.File

	//go:linkname sqlite3vfsFileMux github.com/psanford/sqlite3vfs.fileMux
	sqlite3vfsFileMux sync.Mutex

	vfsConnectionMap sync.Map // map[uintptr]uint64
)

// RegisterVFSConnection maps a SQLite connection handle to its VFS file ID.
func RegisterVFSConnection(dbPtr uintptr, fileID uint64) error {
	if _, ok := lookupVFSFile(fileID); !ok {
		return fmt.Errorf("vfs file not found: id=%d", fileID)
	}
	vfsConnectionMap.Store(dbPtr, fileID)
	return nil
}

// UnregisterVFSConnection removes a connection mapping.
func UnregisterVFSConnection(dbPtr uintptr) {
	vfsConnectionMap.Delete(dbPtr)
}

// SetVFSConnectionTime rebuilds the VFS index for a connection at a timestamp.
func SetVFSConnectionTime(dbPtr uintptr, timestamp string) error {
	file, err := vfsFileForConnection(dbPtr)
	if err != nil {
		return err
	}

	t, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return fmt.Errorf("parse timestamp: %w", err)
	}
	return file.SetTargetTime(context.Background(), t)
}

// ResetVFSConnectionTime rebuilds the VFS index to the latest state.
func ResetVFSConnectionTime(dbPtr uintptr) error {
	file, err := vfsFileForConnection(dbPtr)
	if err != nil {
		return err
	}
	return file.ResetTime(context.Background())
}

// CurrentVFSConnectionTime returns the human-readable target time for a connection.
func CurrentVFSConnectionTime(dbPtr uintptr) (string, error) {
	file, err := vfsFileForConnection(dbPtr)
	if err != nil {
		return "", err
	}
	if t := file.TargetTime(); t != nil {
		return t.Format(time.RFC3339Nano), nil
	}
	latestTime := file.LatestLTXTime()
	if latestTime.IsZero() {
		return "latest", nil
	}
	return latestTime.Format(time.RFC3339Nano), nil
}

func vfsFileForConnection(dbPtr uintptr) (*VFSFile, error) {
	v, ok := vfsConnectionMap.Load(dbPtr)
	if !ok {
		return nil, fmt.Errorf("connection not registered")
	}
	fileID, ok := v.(uint64)
	if !ok {
		return nil, fmt.Errorf("invalid connection mapping")
	}
	file, ok := lookupVFSFile(fileID)
	if !ok {
		return nil, fmt.Errorf("vfs file not found: id=%d", fileID)
	}
	return file, nil
}

func lookupVFSFile(fileID uint64) (*VFSFile, bool) {
	sqlite3vfsFileMux.Lock()
	defer sqlite3vfsFileMux.Unlock()

	file, ok := sqlite3vfsFileMap[fileID]
	if !ok {
		return nil, false
	}

	vfsFile, ok := file.(*VFSFile)
	return vfsFile, ok
}
