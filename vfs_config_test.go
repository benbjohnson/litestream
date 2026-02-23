//go:build vfs

package litestream

import (
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestVFSConfig_SetGet(t *testing.T) {
	defer clearVFSConfigs()

	cfg := &VFSConfig{ReplicaURL: "s3://bucket/path"}
	SetVFSConfig("test.db", cfg)

	got := GetVFSConfig("test.db")
	if got == nil {
		t.Fatal("expected config, got nil")
	}
	if got.ReplicaURL != "s3://bucket/path" {
		t.Fatalf("expected replica url %q, got %q", "s3://bucket/path", got.ReplicaURL)
	}

	if got := GetVFSConfig("nonexistent.db"); got != nil {
		t.Fatalf("expected nil for nonexistent db, got %+v", got)
	}

	DeleteVFSConfig("test.db")
	if got := GetVFSConfig("test.db"); got != nil {
		t.Fatalf("expected nil after delete, got %+v", got)
	}
}

func TestVFSConfig_ConcurrentAccess(t *testing.T) {
	defer clearVFSConfigs()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("db%d", i)
			cfg := &VFSConfig{ReplicaURL: "s3://bucket/" + name}
			SetVFSConfig(name, cfg)
			_ = GetVFSConfig(name)
			DeleteVFSConfig(name)
		}(i)
	}
	wg.Wait()
}

func TestVFSConfig_OverridesDefaults(t *testing.T) {
	defer clearVFSConfigs()

	poll := 5 * time.Second
	cacheSize := 20 * 1024 * 1024
	writeEnabled := true

	cfg := &VFSConfig{
		PollInterval: &poll,
		CacheSize:    &cacheSize,
		WriteEnabled: &writeEnabled,
	}
	SetVFSConfig("override.db", cfg)

	got := GetVFSConfig("override.db")
	if got == nil {
		t.Fatal("expected config, got nil")
	}
	if got.PollInterval == nil || *got.PollInterval != 5*time.Second {
		t.Fatalf("expected poll interval 5s, got %v", got.PollInterval)
	}
	if got.CacheSize == nil || *got.CacheSize != 20*1024*1024 {
		t.Fatalf("expected cache size 20MB, got %v", got.CacheSize)
	}
	if got.WriteEnabled == nil || !*got.WriteEnabled {
		t.Fatalf("expected write enabled true, got %v", got.WriteEnabled)
	}
}

func TestVFSConfig_NilOptionalFields(t *testing.T) {
	defer clearVFSConfigs()

	cfg := &VFSConfig{ReplicaURL: "s3://bucket/path"}
	SetVFSConfig("sparse.db", cfg)

	got := GetVFSConfig("sparse.db")
	if got.WriteEnabled != nil {
		t.Fatalf("expected nil WriteEnabled, got %v", got.WriteEnabled)
	}
	if got.PollInterval != nil {
		t.Fatalf("expected nil PollInterval, got %v", got.PollInterval)
	}
	if got.CacheSize != nil {
		t.Fatalf("expected nil CacheSize, got %v", got.CacheSize)
	}
}

func TestVFSConfig_PerConnectionOverrides(t *testing.T) {
	defer clearVFSConfigs()

	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))

	poll := 3 * time.Second
	cacheSize := 5 * 1024 * 1024
	SetVFSConfig("config-override.db", &VFSConfig{
		PollInterval: &poll,
		CacheSize:    &cacheSize,
	})

	vfs := NewVFS(client, slog.Default())

	f, _, err := vfs.openMainDB("config-override.db", 0x00000100) // OpenMainDB
	if err != nil {
		t.Fatalf("open main db: %v", err)
	}
	defer f.Close()

	vfsFile := f.(*VFSFile)
	if vfsFile.PollInterval != 3*time.Second {
		t.Fatalf("expected poll interval 3s, got %v", vfsFile.PollInterval)
	}
	if vfsFile.CacheSize != 5*1024*1024 {
		t.Fatalf("expected cache size 5MB, got %v", vfsFile.CacheSize)
	}
}

func TestVFSFile_PRAGMAPollInterval(t *testing.T) {
	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))

	f := NewVFSFile(client, "test.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}
	defer f.Close()

	const SQLITE_FCNTL_PRAGMA = 14

	result, err := f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_poll_interval", nil)
	if err != nil {
		t.Fatalf("get poll_interval: %v", err)
	}
	if result == nil || *result != DefaultPollInterval.String() {
		t.Fatalf("expected default poll interval, got %v", result)
	}

	newInterval := "5s"
	_, err = f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_poll_interval", &newInterval)
	if err != nil {
		t.Fatalf("set poll_interval: %v", err)
	}

	result, err = f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_poll_interval", nil)
	if err != nil {
		t.Fatalf("get poll_interval after set: %v", err)
	}
	if result == nil || *result != "5s" {
		t.Fatalf("expected 5s, got %v", result)
	}
}

func TestVFSFile_PRAGMACacheSize(t *testing.T) {
	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))

	f := NewVFSFile(client, "test.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}
	defer f.Close()

	const SQLITE_FCNTL_PRAGMA = 14

	result, err := f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_cache_size", nil)
	if err != nil {
		t.Fatalf("get cache_size: %v", err)
	}
	if result == nil || *result != strconv.Itoa(DefaultCacheSize) {
		t.Fatalf("expected default cache size, got %v", result)
	}

	newSize := "20971520"
	_, err = f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_cache_size", &newSize)
	if err != nil {
		t.Fatalf("set cache_size: %v", err)
	}

	result, err = f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_cache_size", nil)
	if err != nil {
		t.Fatalf("get cache_size after set: %v", err)
	}
	if result == nil || *result != "20971520" {
		t.Fatalf("expected 20971520, got %v", result)
	}
}

func TestVFSFile_PRAGMALogLevel(t *testing.T) {
	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))

	f := NewVFSFile(client, "test.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}
	defer f.Close()

	const SQLITE_FCNTL_PRAGMA = 14

	debugLevel := "debug"
	_, err := f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_log_level", &debugLevel)
	if err != nil {
		t.Fatalf("set log_level to debug: %v", err)
	}

	result, err := f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_log_level", nil)
	if err != nil {
		t.Fatalf("get log_level: %v", err)
	}
	if result == nil || *result != "debug" {
		t.Fatalf("expected debug, got %v", result)
	}

	infoLevel := "info"
	_, err = f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_log_level", &infoLevel)
	if err != nil {
		t.Fatalf("set log_level to info: %v", err)
	}

	result, err = f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_log_level", nil)
	if err != nil {
		t.Fatalf("get log_level: %v", err)
	}
	if result == nil || *result != "info" {
		t.Fatalf("expected info, got %v", result)
	}
}

func TestVFSFile_PRAGMAHydrationEnabled(t *testing.T) {
	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))

	f := NewVFSFile(client, "test.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}
	defer f.Close()

	const SQLITE_FCNTL_PRAGMA = 14

	result, err := f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_hydration_enabled", nil)
	if err != nil {
		t.Fatalf("get hydration_enabled: %v", err)
	}
	if result == nil || *result != "0" {
		t.Fatalf("expected 0, got %v", result)
	}

	writeVal := "1"
	_, err = f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_hydration_enabled", &writeVal)
	if err == nil {
		t.Fatal("expected error for read-only pragma, got nil")
	}
}

func TestVFSFile_PRAGMAReplicaURL(t *testing.T) {
	defer clearVFSConfigs()

	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))

	SetVFSConfig("replica-url-test.db", &VFSConfig{
		ReplicaURL: "s3://my-bucket/db",
	})

	f := NewVFSFile(client, "replica-url-test.db", slog.Default())
	if err := f.Open(); err != nil {
		t.Fatalf("open vfs file: %v", err)
	}
	defer f.Close()

	const SQLITE_FCNTL_PRAGMA = 14

	result, err := f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_replica_url", nil)
	if err != nil {
		t.Fatalf("get replica_url: %v", err)
	}
	if result == nil || *result != "s3://my-bucket/db" {
		t.Fatalf("expected s3://my-bucket/db, got %v", result)
	}

	writeVal := "s3://other/path"
	_, err = f.FileControl(SQLITE_FCNTL_PRAGMA, "litestream_replica_url", &writeVal)
	if err == nil {
		t.Fatal("expected error for read-only pragma, got nil")
	}
}

func clearVFSConfigs() {
	vfsConfigsMu.Lock()
	defer vfsConfigsMu.Unlock()
	vfsConfigs = make(map[string]*VFSConfig)
}
