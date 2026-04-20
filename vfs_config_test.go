//go:build vfs

package litestream

import (
	"fmt"
	"log/slog"
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

func TestVFSConfig_CopyOnSetAndGet(t *testing.T) {
	defer clearVFSConfigs()

	writeEnabled := true
	syncInterval := time.Second
	hydrationEnabled := true
	pollInterval := 5 * time.Second
	cacheSize := 1024
	cfg := &VFSConfig{
		ReplicaURL:       "s3://bucket/original",
		WriteEnabled:     &writeEnabled,
		SyncInterval:     &syncInterval,
		HydrationEnabled: &hydrationEnabled,
		PollInterval:     &pollInterval,
		CacheSize:        &cacheSize,
	}
	SetVFSConfig("copy.db", cfg)

	cfg.ReplicaURL = "s3://bucket/mutated"
	writeEnabled = false
	syncInterval = 2 * time.Second
	hydrationEnabled = false
	pollInterval = 10 * time.Second
	cacheSize = 2048

	got := GetVFSConfig("copy.db")
	if got.ReplicaURL != "s3://bucket/original" {
		t.Fatalf("expected original url, got %q (SetVFSConfig did not copy)", got.ReplicaURL)
	}
	assertVFSConfigPointerValues(t, got, true, time.Second, true, 5*time.Second, 1024)

	got.ReplicaURL = "s3://bucket/mutated-via-get"
	*got.WriteEnabled = false
	*got.SyncInterval = 3 * time.Second
	*got.HydrationEnabled = false
	*got.PollInterval = 30 * time.Second
	*got.CacheSize = 4096

	got2 := GetVFSConfig("copy.db")
	if got2.ReplicaURL != "s3://bucket/original" {
		t.Fatalf("expected original url, got %q (GetVFSConfig did not copy)", got2.ReplicaURL)
	}
	assertVFSConfigPointerValues(t, got2, true, time.Second, true, 5*time.Second, 1024)
}

func TestVFSConfig_LogLevelUnsupported(t *testing.T) {
	cfg := &VFSConfig{}
	if err := cfg.Set("log_level", "debug"); err == nil {
		t.Fatal("expected unsupported log_level to return error")
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

	f, _, err := vfs.openMainDB("config-override.db", nil, 0x00000100)
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

func TestVFSConfig_URIOverridesRegistry(t *testing.T) {
	defer clearVFSConfigs()

	client := newMockReplicaClient()
	client.addFixture(t, buildLTXFixture(t, 1, 'a'))

	registryPoll := 3 * time.Second
	SetVFSConfig("uri-override.db", &VFSConfig{PollInterval: &registryPoll})

	vfs := NewVFS(client, slog.Default())

	f, _, err := vfs.openMainDB("uri-override.db", map[string]string{"poll_interval": "7s"}, 0x00000100)
	if err != nil {
		t.Fatalf("open main db: %v", err)
	}
	defer f.Close()

	vfsFile := f.(*VFSFile)
	if vfsFile.PollInterval != 7*time.Second {
		t.Fatalf("expected poll interval 7s, got %v", vfsFile.PollInterval)
	}
}

func TestVFS_NilClientReturnsError(t *testing.T) {
	defer clearVFSConfigs()

	vfs := NewVFS(nil, slog.Default())

	_, _, err := vfs.openMainDB("no-client.db", nil, 0x00000100)
	if err == nil {
		t.Fatal("expected error when no client configured, got nil")
	}
}

func clearVFSConfigs() {
	vfsConfigsMu.Lock()
	defer vfsConfigsMu.Unlock()
	vfsConfigs = make(map[string]*VFSConfig)
}

func assertVFSConfigPointerValues(tb testing.TB, cfg *VFSConfig, writeEnabled bool, syncInterval time.Duration, hydrationEnabled bool, pollInterval time.Duration, cacheSize int) {
	tb.Helper()
	if cfg.WriteEnabled == nil || *cfg.WriteEnabled != writeEnabled {
		tb.Fatalf("expected write enabled %v, got %v", writeEnabled, cfg.WriteEnabled)
	}
	if cfg.SyncInterval == nil || *cfg.SyncInterval != syncInterval {
		tb.Fatalf("expected sync interval %v, got %v", syncInterval, cfg.SyncInterval)
	}
	if cfg.HydrationEnabled == nil || *cfg.HydrationEnabled != hydrationEnabled {
		tb.Fatalf("expected hydration enabled %v, got %v", hydrationEnabled, cfg.HydrationEnabled)
	}
	if cfg.PollInterval == nil || *cfg.PollInterval != pollInterval {
		tb.Fatalf("expected poll interval %v, got %v", pollInterval, cfg.PollInterval)
	}
	if cfg.CacheSize == nil || *cfg.CacheSize != cacheSize {
		tb.Fatalf("expected cache size %v, got %v", cacheSize, cfg.CacheSize)
	}
}
