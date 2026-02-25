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

	cfg := &VFSConfig{ReplicaURL: "s3://bucket/original"}
	SetVFSConfig("copy.db", cfg)

	cfg.ReplicaURL = "s3://bucket/mutated"

	got := GetVFSConfig("copy.db")
	if got.ReplicaURL != "s3://bucket/original" {
		t.Fatalf("expected original url, got %q (SetVFSConfig did not copy)", got.ReplicaURL)
	}

	got.ReplicaURL = "s3://bucket/mutated-via-get"
	got2 := GetVFSConfig("copy.db")
	if got2.ReplicaURL != "s3://bucket/original" {
		t.Fatalf("expected original url, got %q (GetVFSConfig did not copy)", got2.ReplicaURL)
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

	f, _, err := vfs.openMainDB("config-override.db", 0x00000100)
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

func TestVFS_NilClientReturnsError(t *testing.T) {
	defer clearVFSConfigs()

	vfs := NewVFS(nil, slog.Default())

	_, _, err := vfs.openMainDB("no-client.db", 0x00000100)
	if err == nil {
		t.Fatal("expected error when no client configured, got nil")
	}
}

func clearVFSConfigs() {
	vfsConfigsMu.Lock()
	defer vfsConfigsMu.Unlock()
	vfsConfigs = make(map[string]*VFSConfig)
}
