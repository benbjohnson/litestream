//go:build vfs
// +build vfs

package litestream

import (
	"fmt"
	"strconv"
	"strings"
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
}

var (
	vfsConfigs   = make(map[string]*VFSConfig)
	vfsConfigsMu sync.RWMutex
)

var vfsURIConfigKeys = [...]string{
	"replica_url",
	"write_enabled",
	"sync_interval",
	"buffer_path",
	"hydration_enabled",
	"hydration_path",
	"poll_interval",
	"cache_size",
}

func SetVFSConfig(dbName string, cfg *VFSConfig) {
	vfsConfigsMu.Lock()
	defer vfsConfigsMu.Unlock()
	vfsConfigs[dbName] = cloneVFSConfig(cfg)
}

func GetVFSConfig(dbName string) *VFSConfig {
	vfsConfigsMu.RLock()
	defer vfsConfigsMu.RUnlock()
	return cloneVFSConfig(vfsConfigs[dbName])
}

func DeleteVFSConfig(dbName string) {
	vfsConfigsMu.Lock()
	defer vfsConfigsMu.Unlock()
	delete(vfsConfigs, dbName)
}

func (cfg *VFSConfig) Set(key, value string) error {
	switch key {
	case "replica_url":
		cfg.ReplicaURL = value
	case "write_enabled":
		b := strings.ToLower(value) == "true" || value == "1"
		cfg.WriteEnabled = &b
	case "sync_interval":
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid sync_interval: %w", err)
		}
		cfg.SyncInterval = &d
	case "buffer_path":
		cfg.BufferPath = value
	case "hydration_enabled":
		b := strings.ToLower(value) == "true" || value == "1"
		cfg.HydrationEnabled = &b
	case "hydration_path":
		cfg.HydrationPath = value
	case "poll_interval":
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid poll_interval: %w", err)
		}
		cfg.PollInterval = &d
	case "cache_size":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid cache_size: %w", err)
		}
		cfg.CacheSize = &n
	default:
		return fmt.Errorf("unknown config key: %s", key)
	}
	return nil
}

func ParseVFSURIConfig(uriParameters map[string]string) (*VFSConfig, error) {
	if len(uriParameters) == 0 {
		return nil, nil
	}

	cfg := &VFSConfig{}
	var found bool
	for _, key := range vfsURIConfigKeys {
		value, ok := uriParameters[key]
		if !ok {
			continue
		}
		if err := cfg.Set(key, value); err != nil {
			return nil, err
		}
		found = true
	}
	if !found {
		return nil, nil
	}
	return cfg, nil
}

func MergeVFSConfig(base, override *VFSConfig) *VFSConfig {
	switch {
	case base == nil && override == nil:
		return nil
	case base == nil:
		return cloneVFSConfig(override)
	case override == nil:
		return cloneVFSConfig(base)
	}

	merged := cloneVFSConfig(base)
	if override.ReplicaURL != "" {
		merged.ReplicaURL = override.ReplicaURL
	}
	if override.WriteEnabled != nil {
		v := *override.WriteEnabled
		merged.WriteEnabled = &v
	}
	if override.SyncInterval != nil {
		v := *override.SyncInterval
		merged.SyncInterval = &v
	}
	if override.BufferPath != "" {
		merged.BufferPath = override.BufferPath
	}
	if override.HydrationEnabled != nil {
		v := *override.HydrationEnabled
		merged.HydrationEnabled = &v
	}
	if override.HydrationPath != "" {
		merged.HydrationPath = override.HydrationPath
	}
	if override.PollInterval != nil {
		v := *override.PollInterval
		merged.PollInterval = &v
	}
	if override.CacheSize != nil {
		v := *override.CacheSize
		merged.CacheSize = &v
	}
	return merged
}

func cloneVFSConfig(cfg *VFSConfig) *VFSConfig {
	if cfg == nil {
		return nil
	}
	copied := *cfg
	if cfg.WriteEnabled != nil {
		v := *cfg.WriteEnabled
		copied.WriteEnabled = &v
	}
	if cfg.SyncInterval != nil {
		v := *cfg.SyncInterval
		copied.SyncInterval = &v
	}
	if cfg.HydrationEnabled != nil {
		v := *cfg.HydrationEnabled
		copied.HydrationEnabled = &v
	}
	if cfg.PollInterval != nil {
		v := *cfg.PollInterval
		copied.PollInterval = &v
	}
	if cfg.CacheSize != nil {
		v := *cfg.CacheSize
		copied.CacheSize = &v
	}
	return &copied
}
