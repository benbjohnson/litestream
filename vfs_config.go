//go:build vfs
// +build vfs

package litestream

import (
	"sync"
	"time"
)

type VFSConfig struct {
	ReplicaURL       string
	WriteEnabled     *bool
	SyncInterval     *time.Duration
	BufferPath       string
	HydrationEnabled *bool
	HydrationPath    string
	PollInterval     *time.Duration
	CacheSize        *int
	LogLevel         string
}

var (
	vfsConfigs   = make(map[string]*VFSConfig)
	vfsConfigsMu sync.RWMutex
)

func SetVFSConfig(dbName string, cfg *VFSConfig) {
	vfsConfigsMu.Lock()
	defer vfsConfigsMu.Unlock()
	copied := *cfg
	vfsConfigs[dbName] = &copied
}

func GetVFSConfig(dbName string) *VFSConfig {
	vfsConfigsMu.RLock()
	defer vfsConfigsMu.RUnlock()
	orig := vfsConfigs[dbName]
	if orig == nil {
		return nil
	}
	copied := *orig
	return &copied
}

func DeleteVFSConfig(dbName string) {
	vfsConfigsMu.Lock()
	defer vfsConfigsMu.Unlock()
	delete(vfsConfigs, dbName)
}
